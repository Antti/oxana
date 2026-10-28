use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;

use crate::WorkerBatchConfig;
use crate::context::ContextValue;
use crate::error::OxanaError;
use crate::executor::ExecutionOutcome;
use crate::job_envelope::JobEnvelope;
use crate::queue::{QueueConcurrency, QueueConfig, QueueKind, QueueRuntimeConfig};
use crate::result_collector::WorkerResult;
use crate::runtime::Runtime;
use crate::semaphores_map::{QueueControlsMap, QueuePermit};
use crate::worker_event::WorkerJob;
use crate::{
    dispatcher, executor,
    result_collector::{self, Stats},
};

#[cfg(test)]
mod connection_tests;

pub async fn run<DT>(
    config: Arc<Runtime<DT>>,
    stats: Arc<Mutex<Stats>>,
    ctx: ContextValue<DT>,
    queue_config: QueueConfig,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let channel_capacity = queue_config.concurrency.default_concurrency().max(1);
    let (result_tx, result_rx) = mpsc::channel::<WorkerResult>(channel_capacity);
    let (job_tx, mut job_rx) = mpsc::channel::<WorkerJob>(channel_capacity);
    let queue_controls = Arc::new(QueueControlsMap::new());
    let mut joinset = JoinSet::new();
    let mut batchers: HashMap<BatchKey, mpsc::Sender<PendingJob>> = HashMap::new();

    joinset.spawn(config.tasks.track_future(result_collector::run(
        result_rx,
        Arc::clone(&config),
        Arc::clone(&stats),
    )));
    joinset.spawn(config.tasks.track_future(run_queue_watcher(
        Arc::clone(&config),
        queue_config.clone(),
        job_tx.clone(),
        Arc::clone(&queue_controls),
    )));

    loop {
        tokio::select! {
            job = job_rx.recv() => {
                if let Some(job) = job
                    && let Err(error) = route_job(
                        Arc::clone(&config),
                        ctx.clone(),
                        result_tx.clone(),
                        job,
                        &mut batchers,
                        &mut joinset,
                    )
                    .await {
                    config.fail(error);
                    break;
                }
            }
            Some(task_result) = joinset.join_next() => {
                if let Err(error) = task_result.map_err(OxanaError::from).and_then(|result| result) {
                    config.fail(error);
                    break;
                }
            }
            _ = config.cancel_token.cancelled() => {
                break;
            }
        }
    }

    drop(batchers);
    // Claimed jobs still buffered here have not executed. Leave them in Redis's
    // processing list for resurrection, but release their concurrency permits.
    drop(job_rx);
    drop(job_tx);
    // Every worker and batcher retains a result sender until it finishes. The
    // collector therefore drains results before exiting, without a separate
    // permit-count wait that could hide task errors behind a blocked worker.
    drop(result_tx);

    while let Some(task_result) = joinset.join_next().await {
        if let Err(error) = task_result
            .map_err(OxanaError::from)
            .and_then(|result| result)
        {
            config.fail(error);
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
struct BatchKey {
    worker: String,
    queue: String,
}

impl BatchKey {
    fn from_envelope(envelope: &JobEnvelope) -> Self {
        Self {
            worker: envelope.job.name.clone(),
            queue: envelope.queue.clone(),
        }
    }
}

struct PendingJob {
    envelope: JobEnvelope,
    permit: QueuePermit,
}

async fn route_job<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    job_event: WorkerJob,
    batchers: &mut HashMap<BatchKey, mpsc::Sender<PendingJob>>,
    joinset: &mut JoinSet<Result<(), OxanaError>>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let pending = match load_pending_job(Arc::clone(&config), job_event).await? {
        Some(pending) => pending,
        None => return Ok(()),
    };

    if let Some(batch_config) = config.registry.batch_config(&pending.envelope.job.name) {
        send_to_batcher(config, ctx, result_tx, batchers, pending, batch_config).await;
        return Ok(());
    }

    joinset.spawn(config.tasks.track_future(process_pending_job(
        Arc::clone(&config),
        ctx,
        result_tx,
        pending,
    )));

    Ok(())
}

async fn load_pending_job<DT>(
    config: Arc<Runtime<DT>>,
    job_event: WorkerJob,
) -> Result<Option<PendingJob>, OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    tracing::trace!("Processing job: {:?}", job_event);

    let result = tokio::select! {
        biased;
        // Cancellation leaves the claim for ordinary shutdown recovery. It
        // must not be treated as a missing payload and delete the job.
        _ = config.cancel_token.cancelled() => return Ok(None),
        result = config.storage.internal.get_claimed_job(&job_event.job_id) => result,
    };
    let envelope: JobEnvelope = match result {
        Ok(Some(envelope)) => envelope,
        Ok(None) => {
            tracing::warn!("Job {} not found", job_event.job_id);
            if let Err(e) = config.storage.internal.delete_job(&job_event.job_id).await {
                #[cfg(feature = "sentry")]
                sentry_core::capture_error(&e);
                tracing::error!("Failed to delete job: {}", e);
                return Err(e);
            }
            return Ok(None);
        }
        Err(e) => {
            #[cfg(feature = "sentry")]
            sentry_core::capture_error(&e);
            tracing::error!("Failed to get job envelope: {}", e);
            // This job is already claimed. Propagate the failure so shutdown
            // makes its processing list eligible for resurrection.
            return Err(e);
        }
    };

    tracing::debug!(
        job_id = envelope.id,
        latency_ms = envelope.meta.latency_millis(),
        envelope = %serde_json::to_value(&envelope)?,
        "Received envelope"
    );

    Ok(Some(PendingJob {
        envelope,
        permit: job_event.permit,
    }))
}

async fn process_pending_job<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    pending: PendingJob,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let mut envelope = pending.envelope;
    let job = match config
        .registry
        .build(&envelope.job.name, envelope.job.args.clone(), &ctx.0)
    {
        Ok(job) => job,
        Err(e) => {
            let err_msg = format!("Invalid job: {} - {}", envelope.job.name, e);
            tracing::error!("{}", err_msg);
            if let Err(e) = config.storage.internal.kill(&envelope, err_msg).await {
                #[cfg(feature = "sentry")]
                sentry_core::capture_error(&e);
                tracing::error!("Failed to kill job: {}", e);
                return Err(e);
            }
            return Ok(());
        }
    };

    let result = executor::run(Arc::clone(&config), job, &mut envelope).await?;
    drop(pending.permit);
    process_result(result_tx, result, vec![envelope]).await;

    Ok(())
}

async fn send_to_batcher<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    batchers: &mut HashMap<BatchKey, mpsc::Sender<PendingJob>>,
    pending: PendingJob,
    batch_config: WorkerBatchConfig,
) where
    DT: Send + Sync + Clone + 'static,
{
    let key = BatchKey::from_envelope(&pending.envelope);
    let mut pending = pending;

    loop {
        let tx = batchers
            .entry(key.clone())
            .or_insert_with(|| {
                spawn_batcher(
                    Arc::clone(&config),
                    ctx.clone(),
                    result_tx.clone(),
                    batch_config.clone(),
                )
            })
            .clone();

        match tx.send(pending).await {
            Ok(()) => break,
            Err(err) => {
                pending = err.0;
                batchers.remove(&key);
            }
        }
    }
}

fn spawn_batcher<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    batch_config: WorkerBatchConfig,
) -> mpsc::Sender<PendingJob>
where
    DT: Send + Sync + Clone + 'static,
{
    let batch_size = batch_config.size();
    let (tx, rx) = mpsc::channel(batch_size * 2);
    let task_config = Arc::clone(&config);
    config.spawn(async move {
        if let Err(e) =
            run_batcher(Arc::clone(&task_config), ctx, result_tx, batch_config, rx).await
        {
            tracing::error!(error = %e, "Batcher exited with error");
            task_config.fail(e);
        }
    });
    tx
}

async fn run_batcher<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    batch_config: WorkerBatchConfig,
    mut rx: mpsc::Receiver<PendingJob>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let batch_size = batch_config.size();
    let mut pending = Vec::with_capacity(batch_size);

    loop {
        tokio::select! {
            job = rx.recv(), if pending.is_empty() => {
                match job {
                    Some(job) => pending.push(job),
                    None => return Ok(()),
                }
            }
            _ = config.cancel_token.cancelled(), if pending.is_empty() => {
                flush_batcher(
                    Arc::clone(&config),
                    ctx.clone(),
                    result_tx.clone(),
                    &mut rx,
                    &mut pending,
                    batch_size,
                )
                .await;
                return Ok(());
            }
        }

        if pending.len() >= batch_size || batch_config.timeout().is_zero() {
            spawn_batch(
                Arc::clone(&config),
                ctx.clone(),
                result_tx.clone(),
                &mut pending,
            );
            continue;
        }

        let timeout = tokio::time::sleep(batch_config.timeout());
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                job = rx.recv() => {
                    match job {
                        Some(job) => {
                            pending.push(job);
                            if pending.len() >= batch_size {
                                break;
                            }
                        }
                        None => {
                            spawn_batch(
                                Arc::clone(&config),
                                ctx.clone(),
                                result_tx.clone(),
                                &mut pending,
                            );
                            return Ok(());
                        }
                    }
                }
                _ = &mut timeout => {
                    break;
                }
                _ = config.cancel_token.cancelled() => {
                    flush_batcher(
                        Arc::clone(&config),
                        ctx.clone(),
                        result_tx.clone(),
                        &mut rx,
                        &mut pending,
                        batch_size,
                    )
                    .await;
                    return Ok(());
                }
            }
        }

        spawn_batch(
            Arc::clone(&config),
            ctx.clone(),
            result_tx.clone(),
            &mut pending,
        );
    }
}

async fn flush_batcher<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    rx: &mut mpsc::Receiver<PendingJob>,
    pending: &mut Vec<PendingJob>,
    batch_size: usize,
) where
    DT: Send + Sync + Clone + 'static,
{
    rx.close();

    while let Some(job) = rx.recv().await {
        pending.push(job);
        if pending.len() >= batch_size {
            spawn_batch(Arc::clone(&config), ctx.clone(), result_tx.clone(), pending);
        }
    }

    spawn_batch(config, ctx, result_tx, pending);
}

fn spawn_batch<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    pending: &mut Vec<PendingJob>,
) where
    DT: Send + Sync + Clone + 'static,
{
    if pending.is_empty() {
        return;
    }

    let batch = std::mem::take(pending);
    let task_config = Arc::clone(&config);
    config.spawn(async move {
        if let Err(e) = process_pending_batch(Arc::clone(&task_config), ctx, result_tx, batch).await
        {
            tracing::error!(error = %e, "Failed to process job batch");
            task_config.fail(e);
        }
    });
}

async fn process_pending_batch<DT>(
    config: Arc<Runtime<DT>>,
    ctx: ContextValue<DT>,
    result_tx: mpsc::Sender<WorkerResult>,
    mut pending: Vec<PendingJob>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let Some(first_job) = pending.first() else {
        return Ok(());
    };
    let worker_name = first_job.envelope.job.name.clone();
    let args = pending
        .iter()
        .map(|job| job.envelope.job.args.clone())
        .collect();
    let batch = match config.registry.build_batch(&worker_name, args, &ctx.0) {
        Ok(batch) => batch,
        Err(e) => {
            let err_msg = format!("Invalid job batch: {worker_name} - {e}");
            tracing::error!("{}", err_msg);
            for job in &pending {
                if let Err(e) = config
                    .storage
                    .internal
                    .kill(&job.envelope, err_msg.clone())
                    .await
                {
                    #[cfg(feature = "sentry")]
                    sentry_core::capture_error(&e);
                    tracing::error!("Failed to kill job: {}", e);
                    return Err(e);
                }
            }
            return Ok(());
        }
    };

    let invalid_by_index = invalid_jobs_by_index(batch.invalid, pending.len());
    if !invalid_by_index.is_empty() {
        let mut valid = Vec::with_capacity(pending.len().saturating_sub(invalid_by_index.len()));

        for (index, job) in pending.into_iter().enumerate() {
            if let Some(error) = invalid_by_index.get(&index) {
                let err_msg = format!("Invalid job: {worker_name} - {error}");
                tracing::error!("{}", err_msg);

                if let Err(e) = config.storage.internal.kill(&job.envelope, err_msg).await {
                    #[cfg(feature = "sentry")]
                    sentry_core::capture_error(&e);
                    tracing::error!("Failed to kill job: {}", e);
                    return Err(e);
                }

                drop(job.permit);
            } else {
                valid.push(job);
            }
        }

        pending = valid;
    }

    let Some(job) = batch.job else {
        return Ok(());
    };

    let (mut envelopes, permits): (Vec<_>, Vec<_>) = pending
        .into_iter()
        .map(|job| (job.envelope, job.permit))
        .unzip();
    let outcome = executor::run_batch(Arc::clone(&config), job, &mut envelopes).await?;
    drop(permits);
    process_result(result_tx, outcome, envelopes).await;

    Ok(())
}

fn invalid_jobs_by_index(
    invalid_jobs: Vec<crate::worker_registry::InvalidBatchJob>,
    envelope_count: usize,
) -> HashMap<usize, String> {
    let mut invalid_by_index = HashMap::new();
    for invalid in invalid_jobs {
        if invalid.index < envelope_count {
            invalid_by_index
                .entry(invalid.index)
                .or_insert(invalid.error);
        }
    }
    invalid_by_index
}

async fn process_result(
    result_tx: mpsc::Sender<WorkerResult>,
    outcome: ExecutionOutcome,
    envelopes: Vec<JobEnvelope>,
) {
    let job_count = u64::try_from(envelopes.len()).unwrap_or(u64::MAX);
    let Some(first_envelope) = envelopes.into_iter().next() else {
        tracing::warn!(
            "Worker result with no envelopes, dropping {:?}",
            outcome.kind
        );
        return;
    };

    result_tx
        .send(WorkerResult {
            kind: outcome.kind,
            worker_name: first_envelope.job.name,
            queue: first_envelope.queue,
            execution_ms: outcome.duration_ms,
            job_count,
        })
        .await
        .ok();
}

async fn run_queue_watcher<DT>(
    config: Arc<Runtime<DT>>,
    queue_config: QueueConfig,
    job_tx: mpsc::Sender<WorkerJob>,
    queue_controls: Arc<QueueControlsMap>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let mut tracked_queues = HashSet::new();
    let queue_concurrency = queue_config.concurrency;
    let default_runtime_config = queue_concurrency.stored_runtime_default();
    let base_queue_key = queue_config.key_or_prefix();

    loop {
        let all_queues: HashSet<String> = match &queue_config.kind {
            QueueKind::Static { key } => HashSet::from([key.clone()]),
            QueueKind::Dynamic { prefix, .. } => config
                .storage
                .internal
                .track_redis_result(
                    config.storage.internal.queues(&format!("{prefix}*")).await,
                    config.settings.redis_failure_tolerance,
                )?
                .map(|queues| queues.into_iter().collect())
                .unwrap_or_default(),
        };
        let new_queues: HashSet<String> = all_queues.difference(&tracked_queues).cloned().collect();

        for queue in new_queues {
            tracing::info!(
                queue = queue,
                config.throttle = format!("{:?}", queue_config.throttle),
                "Tracking queue"
            );

            let runtime_config = runtime_queue_config(
                &config,
                &queue,
                &base_queue_key,
                default_runtime_config.clone(),
            )
            .await?;
            let runtime_config = queue_concurrency.effective_runtime_config(runtime_config);
            let queue_control = queue_controls
                .get_or_create(queue.clone(), runtime_config.clone())
                .await;
            queue_control.apply_config(runtime_config);

            let watcher_config = Arc::clone(&config);
            let watcher_queue = queue.clone();
            let watcher_control = Arc::clone(&queue_control);
            let watcher_base_queue_key = base_queue_key.clone();
            let watcher_default_runtime_config = default_runtime_config.clone();
            spawn_queue_task(
                Arc::clone(&config),
                "queue config watcher",
                run_queue_config_watcher(
                    watcher_config,
                    watcher_queue,
                    watcher_base_queue_key,
                    watcher_default_runtime_config,
                    queue_concurrency,
                    watcher_control,
                ),
            );

            let dispatcher_config = Arc::clone(&config);
            let dispatcher_queue_config = queue_config.clone();
            let dispatcher_job_tx = job_tx.clone();
            let dispatcher_queue_control = Arc::clone(&queue_control);
            let dispatcher_queue = queue.clone();
            spawn_queue_task(
                Arc::clone(&config),
                "dispatcher",
                dispatcher::run(
                    dispatcher_config,
                    dispatcher_queue_config,
                    dispatcher_queue,
                    dispatcher_job_tx,
                    dispatcher_queue_control,
                ),
            );

            tracked_queues.insert(queue);
        }

        if config.cancel_token.is_cancelled() {
            return Ok(());
        } else if let QueueKind::Dynamic {
            discovery_interval, ..
        } = queue_config.kind
        {
            tokio::time::sleep(discovery_interval).await;
        } else {
            return Ok(());
        }
    }
}

fn spawn_queue_task<DT, F>(config: Arc<Runtime<DT>>, task: &'static str, future: F)
where
    DT: Send + Sync + 'static,
    F: Future<Output = Result<(), OxanaError>> + Send + 'static,
{
    let task_config = Arc::clone(&config);
    config.spawn(async move {
        if let Err(error) = future.await {
            tracing::error!(task, error = %error, "Queue task exited with error");
            task_config.fail(error);
        }
    });
}

async fn runtime_queue_config<DT>(
    config: &Runtime<DT>,
    queue_key: &str,
    base_queue_key: &str,
    default_runtime_config: QueueRuntimeConfig,
) -> Result<QueueRuntimeConfig, OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    if queue_key == base_queue_key {
        return config
            .storage
            .internal
            .queue_config_or_default(queue_key, default_runtime_config)
            .await;
    }

    let base_runtime_config = config
        .storage
        .internal
        .queue_config_or_default(base_queue_key, default_runtime_config)
        .await?;

    config
        .storage
        .internal
        .queue_config_or_default(queue_key, base_runtime_config)
        .await
}

async fn run_queue_config_watcher<DT>(
    config: Arc<Runtime<DT>>,
    queue_key: String,
    base_queue_key: String,
    default_runtime_config: QueueRuntimeConfig,
    queue_concurrency: QueueConcurrency,
    queue_control: Arc<crate::semaphores_map::QueueControl>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = config.cancel_token.cancelled() => {
                return Ok(());
            }
            _ = interval.tick() => {
                if let Some(runtime_config) = config.storage.internal.track_redis_result(
                    runtime_queue_config(
                        &config,
                        &queue_key,
                        &base_queue_key,
                        default_runtime_config.clone(),
                    )
                    .await,
                    config.settings.redis_failure_tolerance,
                )? {
                    let runtime_config = queue_concurrency.effective_runtime_config(runtime_config);
                    queue_control.apply_config(runtime_config);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PendingJob, load_pending_job, process_pending_batch, process_pending_job, spawn_queue_task,
    };
    use crate::QueueRuntimeConfig;
    use crate::config::{Config, RuntimeSettings};
    use crate::context::ContextValue;
    use crate::runtime::Runtime;
    use crate::semaphores_map::QueueControlsMap;
    use crate::test_helper::{random_string, redis_pool};
    use crate::worker_registry::{self, BatchBuild, InvalidBatchJob, WorkerConfigKind};
    use crate::{Job, JobEnvelope, Storage, Worker, WorkerConfig};
    use deadpool_redis::redis::AsyncCommands;
    use serde::{Deserialize, Serialize};
    use std::{sync::Arc, time::Duration};
    use testresult::TestResult;
    use tokio::sync::{Mutex, mpsc};

    #[derive(Debug, Serialize, Deserialize)]
    struct UnsortedInvalidJob;

    impl Job for UnsortedInvalidJob {}

    struct UnsortedInvalidWorker;

    impl crate::FromContext<()> for UnsortedInvalidWorker {
        fn from_context(_ctx: &()) -> Self {
            Self
        }
    }

    #[async_trait::async_trait]
    impl Worker<UnsortedInvalidJob> for UnsortedInvalidWorker {
        type Error = std::io::Error;

        async fn run_batch(
            &self,
            _jobs: Vec<crate::BatchItem<UnsortedInvalidJob>>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    fn unsorted_invalid_batch_factory(
        _values: Vec<serde_json::Value>,
        _ctx: &(),
    ) -> Result<BatchBuild, crate::OxanaError> {
        Ok(BatchBuild {
            job: None,
            invalid: vec![
                InvalidBatchJob {
                    index: 1,
                    error: "second invalid".to_string(),
                },
                InvalidBatchJob {
                    index: 0,
                    error: "first invalid".to_string(),
                },
            ],
        })
    }

    #[tokio::test]
    async fn missing_job_cleanup_error_is_propagated() -> TestResult {
        let pool = redis_pool().await?;
        let storage = Storage::builder()
            .namespace(random_string())
            .build_from_pool(pool.clone())?;
        let queue = random_string();
        let envelope = JobEnvelope::new(queue.clone(), UnsortedInvalidJob)?;
        storage.internal.enqueue(envelope.clone()).await?;
        storage.internal.dequeue(&queue).await?;
        let mut redis = pool.get().await?;
        let _: () = redis
            .hdel(format!("{}:jobs", storage.namespace()), &envelope.id)
            .await?;
        // The missing payload is normal; failure to clean up its claim is not.
        let _: () = redis
            .set(format!("{}:schedule", storage.namespace()), "wrong-type")
            .await?;
        let controls = QueueControlsMap::new();
        let control = controls
            .get_or_create(queue, QueueRuntimeConfig::new(1))
            .await;
        let event = crate::worker_event::WorkerJob {
            job_id: envelope.id,
            permit: control.acquire().await,
        };
        let runtime = Arc::new(Runtime::new(
            storage,
            Config::<()>::new(),
            RuntimeSettings::new(),
        ));

        let result = load_pending_job(runtime, event).await;
        let Err(crate::OxanaError::DeadpoolRedisError(error)) = result else {
            panic!("expected the cleanup Redis error");
        };
        assert!(
            error
                .into_server_errors()
                .unwrap()
                .iter()
                .any(|(_, error)| error.code() == "WRONGTYPE")
        );
        assert_eq!(controls.busy_count().await, 0);
        Ok(())
    }

    enum InvalidDispatch {
        Single,
        BatchFactory,
        BatchItems,
    }

    #[tokio::test]
    async fn invalid_single_job_cleanup_error_is_propagated() -> TestResult {
        invalid_job_cleanup_error(InvalidDispatch::Single).await
    }

    #[tokio::test]
    async fn invalid_batch_factory_cleanup_error_is_propagated() -> TestResult {
        invalid_job_cleanup_error(InvalidDispatch::BatchFactory).await
    }

    #[tokio::test]
    async fn invalid_batch_items_cleanup_error_is_propagated() -> TestResult {
        invalid_job_cleanup_error(InvalidDispatch::BatchItems).await
    }

    async fn invalid_job_cleanup_error(dispatch: InvalidDispatch) -> TestResult {
        let pool = redis_pool().await?;
        let storage = Storage::builder()
            .namespace(random_string())
            .build_from_pool(pool.clone())?;
        let queue = random_string();
        let mut config = Config::<()>::new();
        if matches!(dispatch, InvalidDispatch::BatchItems) {
            config.register_worker_with(WorkerConfig {
                name: UnsortedInvalidJob::name().to_string(),
                legacy_names: Vec::new(),
                factory: worker_registry::job_factory::<
                    UnsortedInvalidWorker,
                    UnsortedInvalidJob,
                    (),
                >,
                batch_factory: unsorted_invalid_batch_factory,
                batch_config: Some(crate::WorkerBatchConfig::new(2, Duration::from_millis(100))),
                on_demand: None,
                kind: WorkerConfigKind::Normal,
            });
        }
        let count = if matches!(dispatch, InvalidDispatch::Single) {
            1
        } else {
            2
        };
        let controls = QueueControlsMap::new();
        let control = controls
            .get_or_create(queue.clone(), QueueRuntimeConfig::new(count))
            .await;
        let mut pending = Vec::new();
        for _ in 0..count {
            let envelope = JobEnvelope::new(queue.clone(), UnsortedInvalidJob)?;
            storage.internal.enqueue(envelope.clone()).await?;
            storage.internal.dequeue(&queue).await?;
            pending.push(PendingJob {
                envelope,
                permit: control.acquire().await,
            });
        }
        let mut redis = pool.get().await?;
        let processing: Vec<String> = redis
            .keys(format!("{}:processing:*", storage.namespace()))
            .await?;
        assert_eq!(processing.len(), 1);
        // Fail before kill can perform any writes. Keep an existing connection
        // to verify the claims survive and all local permits are released.
        pool.close();
        let runtime = Arc::new(Runtime::new(storage, config, RuntimeSettings::new()));
        let (tx, _rx) = mpsc::channel(1);
        let result = match dispatch {
            InvalidDispatch::Single => {
                process_pending_job(runtime, ContextValue::new(()), tx, pending.pop().unwrap())
                    .await
            }
            InvalidDispatch::BatchFactory | InvalidDispatch::BatchItems => {
                process_pending_batch(runtime, ContextValue::new(()), tx, pending).await
            }
        };
        assert!(matches!(
            result,
            Err(crate::OxanaError::DeadpoolRedisPoolError(_))
        ));
        assert_eq!(controls.busy_count().await, 0);
        let retained: usize = redis.llen(processing.first().unwrap()).await?;
        assert_eq!(retained, count);
        Ok(())
    }

    #[tokio::test]
    async fn process_pending_batch_handles_unsorted_invalid_indexes() -> TestResult {
        let pool = redis_pool().await?;
        let storage = Storage::builder()
            .namespace(random_string())
            .build_from_pool(pool)?;
        let queue = random_string();
        let mut config = Config::new();
        config.register_worker_with(WorkerConfig {
            name: UnsortedInvalidJob::name().to_string(),
            legacy_names: Vec::new(),
            factory: worker_registry::job_factory::<UnsortedInvalidWorker, UnsortedInvalidJob, ()>,
            batch_factory: unsorted_invalid_batch_factory,
            batch_config: Some(crate::WorkerBatchConfig::new(
                2,
                std::time::Duration::from_millis(100),
            )),
            on_demand: None,
            kind: WorkerConfigKind::Normal,
        });

        let envelopes = vec![
            JobEnvelope::new(queue.clone(), UnsortedInvalidJob)?,
            JobEnvelope::new(queue.clone(), UnsortedInvalidJob)?,
        ];
        for envelope in &envelopes {
            storage.internal.enqueue(envelope.clone()).await?;
        }
        for _ in &envelopes {
            storage.internal.dequeue(&queue).await?;
        }

        let queue_controls = QueueControlsMap::new();
        let queue_control = queue_controls
            .get_or_create(queue.clone(), QueueRuntimeConfig::new(envelopes.len()))
            .await;
        let mut pending = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            pending.push(PendingJob {
                envelope,
                permit: queue_control.acquire().await,
            });
        }
        let (result_tx, _result_rx) = mpsc::channel(1);

        process_pending_batch(
            Arc::new(Runtime::new(
                storage.clone(),
                config,
                RuntimeSettings::new(),
            )),
            ContextValue::new(()),
            result_tx,
            pending,
        )
        .await?;

        assert_eq!(storage.dead_count().await?, 2);
        assert_eq!(storage.jobs_count().await?, 0);
        assert_eq!(queue_controls.busy_count().await, 0);

        Ok(())
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct PermitCheckJob {
        value: usize,
    }

    impl Job for PermitCheckJob {}

    #[derive(Clone)]
    struct PermitCheckContext {
        controls: Arc<QueueControlsMap>,
        observed: Arc<Mutex<Vec<(usize, String)>>>,
    }

    struct PermitCheckWorker(PermitCheckContext);

    impl crate::FromContext<PermitCheckContext> for PermitCheckWorker {
        fn from_context(ctx: &PermitCheckContext) -> Self {
            Self(ctx.clone())
        }
    }

    #[async_trait::async_trait]
    impl Worker<PermitCheckJob> for PermitCheckWorker {
        type Error = std::io::Error;

        async fn run_batch(
            &self,
            jobs: Vec<crate::BatchItem<PermitCheckJob>>,
        ) -> Result<(), Self::Error> {
            assert_eq!(self.0.controls.busy_count().await, jobs.len());
            *self.0.observed.lock().await = jobs
                .into_iter()
                .map(|item| (item.job.value, item.ctx.meta.id))
                .collect();
            Ok(())
        }
    }

    #[tokio::test]
    async fn mixed_batch_preserves_job_context_order_and_permit_lifetimes() -> TestResult {
        let storage = Storage::builder()
            .namespace(random_string())
            .build_from_pool(redis_pool().await?)?;
        let queue = random_string();
        let controls = Arc::new(QueueControlsMap::new());
        let ctx = PermitCheckContext {
            controls: Arc::clone(&controls),
            observed: Arc::new(Mutex::new(Vec::new())),
        };
        let mut config = Config::new();
        config.register_worker_with(WorkerConfig {
            name: PermitCheckJob::name().to_string(),
            legacy_names: Vec::new(),
            factory: worker_registry::job_factory::<
                PermitCheckWorker,
                PermitCheckJob,
                PermitCheckContext,
            >,
            batch_factory: |values, ctx| {
                let mut batch = worker_registry::job_batch_factory::<
                    PermitCheckWorker,
                    PermitCheckJob,
                    PermitCheckContext,
                >(values, ctx)?;
                batch.invalid.reverse();
                batch.invalid.extend([
                    InvalidBatchJob {
                        index: 1,
                        error: "duplicate index".to_string(),
                    },
                    InvalidBatchJob {
                        index: usize::MAX,
                        error: "out-of-range index".to_string(),
                    },
                ]);
                Ok(batch)
            },
            batch_config: Some(crate::WorkerBatchConfig::new(4, Duration::from_millis(100))),
            on_demand: None,
            kind: WorkerConfigKind::Normal,
        });

        let control = controls
            .get_or_create(queue.clone(), QueueRuntimeConfig::new(4))
            .await;
        let mut pending = Vec::new();
        let mut expected = Vec::new();
        for value in 0..4 {
            let mut envelope = JobEnvelope::new(queue.clone(), PermitCheckJob { value })?;
            if value % 2 == 0 {
                expected.push((value, envelope.id.clone()));
            } else {
                envelope.job.args = serde_json::json!({"value": "invalid"});
            }
            storage.internal.enqueue(envelope.clone()).await?;
            storage.internal.dequeue(&queue).await?;
            pending.push(PendingJob {
                envelope,
                permit: control.acquire().await,
            });
        }
        assert_eq!(controls.busy_count().await, 4);
        let (result_tx, mut result_rx) = mpsc::channel(1);

        process_pending_batch(
            Arc::new(Runtime::new(
                storage.clone(),
                config,
                RuntimeSettings::new(),
            )),
            ContextValue::new(ctx.clone()),
            result_tx,
            pending,
        )
        .await?;

        let result = result_rx.recv().await.expect("valid jobs should run");
        assert!(matches!(
            result.kind,
            crate::result_collector::WorkerResultKind::Success
        ));
        assert_eq!(result.job_count, 2);
        assert_eq!(*ctx.observed.lock().await, expected);
        assert_eq!(controls.busy_count().await, 0);
        assert_eq!(storage.dead_count().await?, 2);
        assert_eq!(storage.jobs_count().await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn queue_task_errors_initiate_shutdown() -> TestResult {
        let storage = Storage::builder()
            .namespace(random_string())
            .build_from_pool(redis_pool().await?)?;
        let runtime = Arc::new(Runtime::new(
            storage,
            Config::<()>::new(),
            RuntimeSettings::new(),
        ));
        spawn_queue_task(Arc::clone(&runtime), "test queue task", async {
            Err(crate::OxanaError::GenericError(
                "dispatcher failed".to_string(),
            ))
        });
        runtime.tasks.close();
        tokio::time::timeout(Duration::from_secs(1), runtime.tasks.wait()).await?;
        assert!(runtime.cancel_token.is_cancelled());
        assert!(matches!(
            runtime.take_shutdown_error(),
            Some(crate::OxanaError::GenericError(message)) if message == "dispatcher failed"
        ));
        Ok(())
    }
}
