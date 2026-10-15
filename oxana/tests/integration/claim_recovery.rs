mod redis_proxy;

use std::sync::Arc;
use std::time::Duration;

use deadpool_redis::redis::AsyncCommands;
use oxana::{QueueConfig, QueueThrottle, Storage};
use testresult::TestResult;
use tokio::sync::{Mutex, Notify};

use crate::shared::{random_string, setup};
use redis_proxy::RedisProxy;

const WAIT: Duration = Duration::from_secs(5);

#[derive(serde::Serialize, serde::Deserialize)]
struct RecoveryJob {
    block: bool,
}

impl oxana::Job for RecoveryJob {}

struct RecoveryQueue(String);

impl oxana::Queue for RecoveryQueue {
    fn key(&self) -> String {
        self.0.clone()
    }

    fn to_config() -> QueueConfig {
        QueueConfig::as_dynamic("recovery")
    }
}

#[derive(Default)]
struct State {
    executed: Mutex<Vec<String>>,
    started: Notify,
    release: Notify,
}

struct RecoveryWorker(Arc<State>);

impl oxana::FromContext<Arc<State>> for RecoveryWorker {
    fn from_context(state: &Arc<State>) -> Self {
        Self(Arc::clone(state))
    }
}

#[async_trait::async_trait]
impl oxana::Worker<RecoveryJob> for RecoveryWorker {
    type Error = std::io::Error;

    async fn run_batch(&self, jobs: Vec<oxana::BatchItem<RecoveryJob>>) -> Result<(), Self::Error> {
        for job in jobs {
            self.0.executed.lock().await.push(job.ctx.meta.id);
            if job.job.block {
                self.0.started.notify_one();
                self.0.release.notified().await;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Fault {
    ClaimReply,
    Payload,
    ThrottledPayload,
    ThrottleConsume,
}

#[tokio::test]
async fn committed_claim_with_lost_reply_is_recovered() -> TestResult {
    recovers_after_claim_error(Fault::ClaimReply, false).await
}

#[tokio::test]
async fn payload_error_drains_active_handler_then_recovers_claim() -> TestResult {
    recovers_after_claim_error(Fault::Payload, true).await
}

#[tokio::test]
async fn throttled_payload_error_is_recovered() -> TestResult {
    recovers_after_claim_error(Fault::ThrottledPayload, false).await
}

#[tokio::test]
async fn throttle_consume_error_is_recovered() -> TestResult {
    recovers_after_claim_error(Fault::ThrottleConsume, false).await
}

#[tokio::test]
async fn throttle_state_error_before_claim_is_retried() -> TestResult {
    let pool = setup();
    let queue = random_string();
    let storage = Storage::builder()
        .namespace(random_string())
        .build_from_pool(pool)?;
    let job_id = storage
        .enqueue(RecoveryQueue(queue.clone()), RecoveryJob { block: false })
        .await?;
    let proxy = RedisProxy::start(
        vec!["ZCARD".into(), format!("oxana:throttler:{queue}")],
        false,
    )
    .await?;
    let runtime_storage = Storage::builder()
        .namespace(storage.namespace())
        .build_from_pool(proxy.pool.clone())?;
    let state = Arc::new(State::default());
    let runtime = runtime_storage
        .runtime(Arc::clone(&state))
        .queue_with(QueueConfig::as_static(queue).throttle(QueueThrottle {
            limit: 100,
            window_ms: 1000,
        }))
        .worker::<RecoveryWorker, RecoveryJob>()
        .dispatcher_idle_sleep(Duration::from_millis(10))
        .exit_when_processed(1);
    let stats = tokio::time::timeout(WAIT, runtime.run()).await??;
    assert!(proxy.injected());
    assert_eq!(stats.processed, 1);
    assert_eq!(*state.executed.lock().await, vec![job_id.clone()]);
    assert!(storage.get_job(&job_id).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn payload_connection_loss_recovers_without_restart() -> TestResult {
    recovers_transient_payload_read(false).await
}

#[tokio::test]
async fn throttled_payload_connection_loss_recovers_without_restart() -> TestResult {
    recovers_transient_payload_read(true).await
}

#[tokio::test]
async fn sustained_payload_failure_exhausts_its_own_budget() -> TestResult {
    interrupted_payload_read_retains_claim(false, false).await
}

#[tokio::test]
async fn sustained_throttled_payload_failure_exhausts_its_own_budget() -> TestResult {
    interrupted_payload_read_retains_claim(false, true).await
}

#[tokio::test]
async fn cancellation_interrupts_payload_read_retry() -> TestResult {
    interrupted_payload_read_retains_claim(true, false).await
}

#[tokio::test]
async fn cancellation_interrupts_throttled_payload_read_retry() -> TestResult {
    interrupted_payload_read_retains_claim(true, true).await
}

#[tokio::test]
async fn shared_brief_outage_recovers_claimed_jobs_without_restarting_runtimes() -> TestResult {
    let pool = setup();
    let namespace = random_string();
    let queue = random_string();
    let storage = Storage::builder()
        .namespace(&namespace)
        .build_from_pool(pool.clone())?;
    let mut expected = Vec::new();
    for _ in 0..2 {
        expected.push(
            storage
                .enqueue(RecoveryQueue(queue.clone()), RecoveryJob { block: false })
                .await?,
        );
    }
    let proxy = RedisProxy::failing_reads(vec!["HGET".into(), format!("{namespace}:jobs")]).await?;
    let state = Arc::new(State::default());
    let mut runners = tokio::task::JoinSet::new();
    for _ in 0..2 {
        let runtime_storage = Storage::builder()
            .namespace(&namespace)
            .build_from_pool(proxy.pool.clone())?;
        let runtime = runtime_storage
            .runtime(Arc::clone(&state))
            .queue_with(QueueConfig::as_static(&queue).concurrency(1))
            .worker::<RecoveryWorker, RecoveryJob>()
            .heartbeat_interval(Duration::from_millis(50))
            .resurrect_scan_interval(Duration::from_millis(20))
            .exit_when_processed(1);
        runners.spawn(runtime.run());
    }
    tokio::time::timeout(WAIT, async {
        while proxy.claim_count() < 2 || proxy.fault_count() < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;

    let mut redis = pool.get().await?;
    let processing: Vec<String> = redis.keys(format!("{namespace}:processing:*")).await?;
    assert_eq!(processing.len(), 2, "each runtime must own one claim");
    for key in &processing {
        let count: usize = redis.llen(key).await?;
        assert_eq!(count, 1);
    }

    // Both runtimes have known claims before the outage. Reject all commands
    // through their proxy; never interrupt Redis shared with other tests.
    proxy.set_outage(true);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(runners.try_join_next().is_none());
    assert!(state.executed.lock().await.is_empty());
    proxy.set_outage(false);
    proxy.restore_reads();

    for _ in 0..2 {
        let stats = tokio::time::timeout(WAIT, runners.join_next())
            .await?
            .unwrap()??;
        assert_eq!(stats.processed, 1);
    }
    expected.sort();
    let mut executed = state.executed.lock().await.clone();
    executed.sort();
    assert_eq!(executed, expected);
    assert_eq!(proxy.claim_count(), 2);
    assert!(proxy.fault_count() <= 28);
    for key in processing {
        let remaining: usize = redis.llen(key).await?;
        assert_eq!(remaining, 0);
    }
    Ok(())
}

async fn recovers_after_claim_error(fault: Fault, active_handler: bool) -> TestResult {
    let pool = setup();
    let namespace = random_string();
    let queue = random_string();
    let storage = Storage::builder()
        .namespace(&namespace)
        .build_from_pool(pool.clone())?;
    let active_id = if active_handler {
        Some(
            storage
                .enqueue(RecoveryQueue(queue.clone()), RecoveryJob { block: true })
                .await?,
        )
    } else {
        None
    };
    let job_id = storage
        .enqueue(RecoveryQueue(queue.clone()), RecoveryJob { block: false })
        .await?;
    let prefix = match fault {
        Fault::ClaimReply => vec!["LMOVE".into(), format!("{namespace}:queue:{queue}")],
        Fault::Payload | Fault::ThrottledPayload => {
            vec!["HGET".into(), format!("{namespace}:jobs"), job_id.clone()]
        }
        Fault::ThrottleConsume => vec!["ZADD".into(), format!("oxana:throttler:{queue}")],
    };
    let proxy = RedisProxy::start(prefix, matches!(fault, Fault::ClaimReply)).await?;
    let runtime_storage = Storage::builder()
        .namespace(&namespace)
        .build_from_pool(proxy.pool.clone())?;
    let mut queue_config = QueueConfig::as_static(&queue).concurrency(2);
    if matches!(fault, Fault::ThrottledPayload | Fault::ThrottleConsume) {
        queue_config = queue_config.throttle(QueueThrottle {
            limit: 100,
            window_ms: 1000,
        });
    }
    let state = Arc::new(State::default());
    // Leave redis_failure_tolerance at its default: a single claim failure must
    // initiate shutdown even while other Redis operations continue succeeding.
    let runtime = runtime_storage
        .runtime(Arc::clone(&state))
        .queue_with(queue_config.clone())
        .worker::<RecoveryWorker, RecoveryJob>()
        .heartbeat_interval(Duration::from_millis(20))
        .dequeue_timeout(Duration::from_millis(10))
        .shutdown_timeout(WAIT);
    let mut runners = tokio::task::JoinSet::new();
    runners.spawn(runtime.run());

    if active_handler {
        tokio::time::timeout(WAIT, state.started.notified()).await?;
    }
    let mut redis = pool.get().await?;
    tokio::time::timeout(WAIT, async {
        while !proxy.injected() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;

    let processes_key = format!("{namespace}:processes");
    if active_handler {
        wait_for_heartbeat(&mut redis, &processes_key).await?;
        assert!(
            runners.try_join_next().is_none(),
            "shutdown must wait for the active handler"
        );
        assert_eq!(
            *state.executed.lock().await,
            vec![active_id.clone().unwrap()]
        );
        state.release.notify_one();
    }

    let result = tokio::time::timeout(WAIT, runners.join_next())
        .await?
        .unwrap()?;
    let Err(oxana::OxanaError::DeadpoolRedisError(error)) = result else {
        panic!("expected the initiating Redis error, got {result:?}");
    };
    if matches!(fault, Fault::ClaimReply) {
        assert!(error.is_io_error(), "{error}");
    } else {
        assert!(
            error.to_string().contains("injected claim failure"),
            "{error}"
        );
    }
    let processes: Vec<String> = redis.zrange(&processes_key, 0, -1).await?;
    assert!(
        processes.is_empty(),
        "registration must be removed after draining"
    );
    let processing: Vec<String> = redis.keys(format!("{namespace}:processing:*")).await?;
    assert_eq!(processing.len(), 1);
    let claimed: Vec<String> = redis.lrange(&processing[0], 0, -1).await?;
    assert_eq!(claimed, vec![job_id.clone()]);
    assert!(!state.executed.lock().await.contains(&job_id));
    let envelope = storage.get_job(&job_id).await?.unwrap();
    assert_eq!(envelope.meta.retries, 0);

    // A distinct process identity uses the ordinary resurrection path.
    let stats = run_replacement(&storage, queue_config, Arc::clone(&state)).await?;
    assert_eq!(stats.processed, 1);
    let mut expected: Vec<String> = active_id.into_iter().collect();
    expected.push(job_id.clone());
    assert_eq!(*state.executed.lock().await, expected);
    assert!(storage.get_job(&job_id).await?.is_none());
    let remaining: usize = redis.llen(&processing[0]).await?;
    assert_eq!(remaining, 0);
    Ok(())
}

async fn recovers_transient_payload_read(throttled: bool) -> TestResult {
    let namespace = random_string();
    let queue = random_string();
    let storage = Storage::builder()
        .namespace(&namespace)
        .build_from_pool(setup())?;
    let job_id = storage
        .enqueue(RecoveryQueue(queue.clone()), RecoveryJob { block: false })
        .await?;
    let proxy = RedisProxy::start(
        vec!["HGET".into(), format!("{namespace}:jobs"), job_id.clone()],
        true,
    )
    .await?;
    let runtime_storage = Storage::builder()
        .namespace(&namespace)
        .build_from_pool(proxy.pool.clone())?;
    let mut queue_config = QueueConfig::as_static(&queue).concurrency(1);
    if throttled {
        queue_config = queue_config.throttle(QueueThrottle {
            limit: 100,
            window_ms: 1000,
        });
    }
    let state = Arc::new(State::default());
    let runtime = runtime_storage
        .runtime(Arc::clone(&state))
        .queue_with(queue_config)
        .worker::<RecoveryWorker, RecoveryJob>()
        .exit_when_processed(1);
    let stats = tokio::time::timeout(WAIT, runtime.run()).await??;
    assert!(proxy.injected());
    assert_eq!(proxy.claim_count(), 1);
    assert_eq!(stats.processed, 1);
    assert_eq!(*state.executed.lock().await, vec![job_id.clone()]);
    assert!(storage.get_job(&job_id).await?.is_none());
    Ok(())
}

async fn interrupted_payload_read_retains_claim(cancel: bool, throttled: bool) -> TestResult {
    let pool = setup();
    let namespace = random_string();
    let queue = random_string();
    let storage = Storage::builder()
        .namespace(&namespace)
        .build_from_pool(pool.clone())?;
    let job_id = storage
        .enqueue(RecoveryQueue(queue.clone()), RecoveryJob { block: false })
        .await?;
    let proxy = RedisProxy::failing_reads(vec![
        "HGET".into(),
        format!("{namespace}:jobs"),
        job_id.clone(),
    ])
    .await?;
    let runtime_storage = Storage::builder()
        .namespace(&namespace)
        .build_from_pool(proxy.pool.clone())?;
    let mut queue_config = QueueConfig::as_static(&queue).concurrency(1);
    if throttled {
        queue_config = queue_config.throttle(QueueThrottle {
            limit: 100,
            window_ms: 1000,
        });
    }
    let state = Arc::new(State::default());
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let runtime = runtime_storage
        .runtime(Arc::clone(&state))
        .queue_with(queue_config.clone())
        .worker::<RecoveryWorker, RecoveryJob>()
        .heartbeat_interval(Duration::from_millis(20))
        .shutdown_timeout(WAIT)
        .shutdown_on(async move {
            shutdown_rx.await.map_err(std::io::Error::other)?;
            Ok(())
        });
    let started = tokio::time::Instant::now();
    let mut runners = tokio::task::JoinSet::new();
    runners.spawn(runtime.run());
    tokio::time::timeout(WAIT, async {
        while !proxy.injected() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;

    // Successful heartbeats must not reset this particular read's retry budget.
    let mut redis = pool.get().await?;
    let processes_key = format!("{namespace}:processes");
    wait_for_heartbeat(&mut redis, &processes_key).await?;

    let finish_within = if cancel {
        shutdown_tx.send(()).unwrap();
        Duration::from_secs(1)
    } else {
        Duration::from_secs(8)
    };
    let result = tokio::time::timeout(finish_within, runners.join_next())
        .await?
        .unwrap()?;
    if cancel {
        assert_eq!(result?.processed, 0);
    } else {
        let Err(oxana::OxanaError::DeadpoolRedisError(error)) = result else {
            panic!("expected the last payload read failure, got {result:?}");
        };
        assert!(error.to_string().contains("injected payload read failure"));
        assert!(started.elapsed() >= Duration::from_secs(5));
        assert!(proxy.fault_count() > 1);
    }
    assert!(proxy.fault_count() <= 14);
    assert_eq!(proxy.claim_count(), 1);
    assert!(state.executed.lock().await.is_empty());
    assert_eq!(storage.get_job(&job_id).await?.unwrap().meta.retries, 0);
    let processes: Vec<String> = redis.zrange(&processes_key, 0, -1).await?;
    assert!(processes.is_empty());
    let processing: Vec<String> = redis.keys(format!("{namespace}:processing:*")).await?;
    assert_eq!(processing.len(), 1);
    let claimed: Vec<String> = redis.lrange(&processing[0], 0, -1).await?;
    assert_eq!(claimed, vec![job_id.clone()]);

    proxy.restore_reads();
    let stats = run_replacement(&storage, queue_config, Arc::clone(&state)).await?;
    assert_eq!(stats.processed, 1);
    assert_eq!(*state.executed.lock().await, vec![job_id.clone()]);
    assert!(storage.get_job(&job_id).await?.is_none());
    let remaining: usize = redis.llen(&processing[0]).await?;
    assert_eq!(remaining, 0);
    Ok(())
}

async fn wait_for_heartbeat(
    redis: &mut deadpool_redis::Connection,
    processes_key: &str,
) -> TestResult {
    let processes: Vec<String> = redis.zrange(processes_key, 0, -1).await?;
    assert_eq!(processes.len(), 1);
    let heartbeat: f64 = redis.zscore(processes_key, &processes[0]).await?;
    tokio::time::timeout(WAIT, async {
        loop {
            let later: f64 = redis.zscore(processes_key, &processes[0]).await?;
            if later > heartbeat {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        TestResult::Ok(())
    })
    .await?
}

async fn run_replacement(
    storage: &Storage,
    queue_config: QueueConfig,
    state: Arc<State>,
) -> TestResult<oxana::RunStats> {
    let replacement = storage
        .runtime(state)
        .queue_with(queue_config)
        .worker::<RecoveryWorker, RecoveryJob>()
        .dequeue_timeout(Duration::from_millis(10))
        .resurrect_scan_interval(Duration::from_millis(20))
        .exit_when_processed(1);
    Ok(tokio::time::timeout(WAIT, replacement.run()).await??)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct FailureJob {
    terminal: bool,
}
impl oxana::Job for FailureJob {}
struct FailureWorker(bool);
impl oxana::FromContext<bool> for FailureWorker {
    fn from_context(fail: &bool) -> Self {
        Self(*fail)
    }
}
#[async_trait::async_trait]
impl oxana::Worker<FailureJob> for FailureWorker {
    type Error = std::io::Error;
    async fn process(&self, _: FailureJob, _: &oxana::JobContext) -> Result<(), Self::Error> {
        if self.0 {
            Err(std::io::Error::other("injected handler failure"))
        } else {
            Ok(())
        }
    }
    fn max_retries(&self, job: &FailureJob) -> u32 {
        if job.terminal { 0 } else { 11 }
    }
    fn retry_delay(&self, _: &FailureJob, _: u32) -> u64 {
        3600
    }
}

#[tokio::test]
async fn failed_handoff_retains_recoverable_work_before_or_after_commit() -> TestResult {
    for terminal in [false, true] {
        for committed in [false, true] {
            let pool = setup();
            let namespace = random_string();
            let queue = random_string();
            let storage = Storage::builder()
                .namespace(&namespace)
                .build_from_pool(pool.clone())?;
            let id = storage
                .enqueue(RecoveryQueue(queue.clone()), FailureJob { terminal })
                .await?;
            let script = deadpool_redis::redis::Script::new(if terminal {
                include_str!("../../src/finish_dead.lua")
            } else {
                include_str!("../../src/retry_failed.lua")
            });
            let prefix = vec![
                "EVALSHA".into(),
                script.get_hash().into(),
                "3".into(),
                format!("{namespace}:jobs"),
            ];
            let proxy = if committed {
                RedisProxy::start(prefix, true).await?
            } else {
                RedisProxy::reject_once(prefix).await?
            };
            let runtime_storage = Storage::builder()
                .namespace(&namespace)
                .build_from_pool(proxy.pool.clone())?;
            let result = tokio::time::timeout(
                WAIT,
                runtime_storage
                    .runtime(true)
                    .queue_with(QueueConfig::as_static(&queue).concurrency(1))
                    .worker::<FailureWorker, FailureJob>()
                    .dequeue_timeout(Duration::from_millis(10))
                    .run(),
            )
            .await?;
            assert!(proxy.injected());
            assert!(
                result.is_err(),
                "a failed handoff must stop heartbeating its abandoned claims"
            );
            if terminal && committed {
                assert_eq!(storage.dead_count().await?, 1);
                assert!(storage.get_job(&id).await?.is_none());
            } else {
                assert!(storage.get_job(&id).await?.is_some());
                storage.retry_all_now().await?;
                let stats = tokio::time::timeout(
                    WAIT,
                    storage
                        .runtime(false)
                        .queue_with(QueueConfig::as_static(&queue).concurrency(1))
                        .worker::<FailureWorker, FailureJob>()
                        .resurrect_scan_interval(Duration::from_millis(10))
                        .dequeue_timeout(Duration::from_millis(10))
                        .exit_when_processed(1)
                        .run(),
                )
                .await??;
                assert_eq!(stats.succeeded, 1);
                assert!(storage.get_job(&id).await?.is_none());
            }
            assert_eq!(storage.enqueued_count(RecoveryQueue(queue)).await?, 0);
            assert_eq!(storage.retries_count().await?, 0);
            let mut redis = pool.get().await?;
            let keys: Vec<String> = redis.keys(format!("{namespace}:processing:*")).await?;
            for key in keys {
                let remaining: usize = redis.llen(key).await?;
                assert_eq!(remaining, 0);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn acknowledgment_failure_stops_runtime_and_recovers_only_uncommitted_work() -> TestResult {
    for committed in [false, true] {
        let pool = setup();
        let namespace = random_string();
        let queue = random_string();
        let storage = Storage::builder()
            .namespace(&namespace)
            .build_from_pool(pool.clone())?;
        let id = storage
            .enqueue(RecoveryQueue(queue.clone()), RecoveryJob { block: false })
            .await?;
        let script =
            deadpool_redis::redis::Script::new(include_str!("../../src/finish_success.lua"));
        let prefix = vec![
            "EVALSHA".into(),
            script.get_hash().into(),
            "2".into(),
            format!("{namespace}:jobs"),
        ];
        let proxy = if committed {
            RedisProxy::start(prefix, true).await?
        } else {
            RedisProxy::reject_once(prefix).await?
        };
        let runtime_storage = Storage::builder()
            .namespace(&namespace)
            .build_from_pool(proxy.pool.clone())?;
        let state = Arc::new(State::default());
        let queue_config = QueueConfig::as_static(&queue).concurrency(1);
        let result = tokio::time::timeout(
            WAIT,
            runtime_storage
                .runtime(Arc::clone(&state))
                .queue_with(queue_config.clone())
                .worker::<RecoveryWorker, RecoveryJob>()
                .dequeue_timeout(Duration::from_millis(10))
                .run(),
        )
        .await?;
        assert!(proxy.injected());
        assert!(
            result.is_err(),
            "unacknowledged claims must not retain a live heartbeat"
        );
        assert_eq!(*state.executed.lock().await, vec![id.clone()]);
        if !committed {
            assert!(storage.get_job(&id).await?.is_some());
            assert_eq!(
                run_replacement(&storage, queue_config, Arc::clone(&state))
                    .await?
                    .succeeded,
                1
            );
            assert_eq!(*state.executed.lock().await, vec![id.clone(), id.clone()]);
        }
        assert!(storage.get_job(&id).await?.is_none());
        assert_eq!(storage.enqueued_count(RecoveryQueue(queue)).await?, 0);
        let mut redis = pool.get().await?;
        let keys: Vec<String> = redis.keys(format!("{namespace}:processing:*")).await?;
        for key in keys {
            assert_eq!(redis.llen::<_, usize>(key).await?, 0);
        }
    }
    Ok(())
}
