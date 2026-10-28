use super::*;
use crate::config::{Config, RuntimeSettings};
use crate::test_helper::{random_string, redis_proxy::RedisProxy};
use crate::{BatchItem, FromContext, Job, Storage, Worker};
use deadpool_redis::redis::{AsyncCommands, ConnectionAddr};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use testresult::TestResult;
use tokio::net::{TcpListener, TcpStream};

#[derive(Serialize, Deserialize)]
struct ConnectionJob;
impl Job for ConnectionJob {}

struct ConnectionWorker(Arc<AtomicUsize>);

impl FromContext<Arc<AtomicUsize>> for ConnectionWorker {
    fn from_context(ctx: &Arc<AtomicUsize>) -> Self {
        Self(Arc::clone(ctx))
    }
}

#[async_trait::async_trait]
impl Worker<ConnectionJob> for ConnectionWorker {
    type Error = std::io::Error;

    async fn run_batch(&self, jobs: Vec<BatchItem<ConnectionJob>>) -> Result<(), Self::Error> {
        self.0.fetch_add(jobs.len(), Ordering::SeqCst);
        Ok(())
    }
}

/// Hold new connections before the Redis handshake, producing the same Create
/// timeout as production without interfering with already established sockets.
struct ConnectionProxy {
    pool: deadpool_redis::Pool,
    paused: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl ConnectionProxy {
    async fn new() -> TestResult<Self> {
        dotenvy::from_filename(".env.test").ok();
        let client = deadpool_redis::redis::Client::open(std::env::var("REDIS_URL")?)?;
        let info = client.get_connection_info().clone();
        let ConnectionAddr::Tcp(host, port) = info.addr().clone() else {
            return Err("Connection fault tests require a TCP Redis URL".into());
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let mut cfg = deadpool_redis::Config::from_connection_info(info.set_addr(
            ConnectionAddr::Tcp(address.ip().to_string(), address.port()),
        ));
        cfg.pool = Some(deadpool_redis::PoolConfig {
            max_size: 4,
            timeouts: crate::StorageBuilderTimeouts::new(Duration::from_millis(100)).into(),
            queue_mode: Default::default(),
        });
        let pool = cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
        let paused = Arc::new(AtomicBool::new(false));
        let task_paused = Arc::clone(&paused);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        let paused = Arc::clone(&task_paused);
                        let host = host.clone();
                        connections.spawn(async move {
                            while paused.load(Ordering::SeqCst) {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                            let mut upstream = TcpStream::connect((host.as_str(), port)).await.unwrap();
                            let _ = tokio::io::copy_bidirectional(&mut socket, &mut upstream).await;
                        });
                    }
                    Some(_) = connections.join_next() => {}
                }
            }
        });
        Ok(Self { pool, paused, task })
    }

    fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
        self.pool.retain(|_, _| false);
    }
}

impl Drop for ConnectionProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn claimed_jobs(
    storage: &Storage,
    count: usize,
) -> TestResult<(Vec<PendingJob>, QueueControlsMap)> {
    let controls = QueueControlsMap::new();
    let control = controls
        .get_or_create("connection_test".into(), QueueRuntimeConfig::new(count))
        .await;
    let mut pending = Vec::new();
    for _ in 0..count {
        let envelope = JobEnvelope::new("connection_test".into(), ConnectionJob)?;
        storage.internal.enqueue(envelope.clone()).await?;
        assert_eq!(
            storage.internal.dequeue(&envelope.queue).await?,
            Some(envelope.id.clone())
        );
        pending.push(PendingJob {
            envelope,
            permit: control.acquire().await,
        });
    }
    Ok((pending, controls))
}

async fn processing_claims(storage: &Storage) -> TestResult<Vec<String>> {
    let mut redis = storage.internal.connection().await?;
    let keys: Vec<String> = redis
        .keys(format!("{}:processing:*", storage.namespace()))
        .await?;
    let mut claims = Vec::new();
    for key in keys {
        let ids: Vec<String> = redis.lrange(key, 0, -1).await?;
        claims.extend(ids);
    }
    Ok(claims)
}

fn start_jobs(
    storage: Storage,
    calls: &Arc<AtomicUsize>,
    mut pending: Vec<PendingJob>,
) -> Arc<Runtime<Arc<AtomicUsize>>> {
    let runtime = Arc::new(Runtime::new(
        storage,
        Config::new().register_worker::<ConnectionWorker, ConnectionJob>(),
        RuntimeSettings::new(),
    ));
    let (tx, _rx) = mpsc::channel(1);
    if pending.len() == 1 {
        spawn_queue_task(
            Arc::clone(&runtime),
            "single connection test",
            process_pending_job(
                Arc::clone(&runtime),
                ContextValue::new(Arc::clone(calls)),
                tx,
                pending.pop().unwrap(),
            ),
        );
    } else {
        spawn_batch(
            Arc::clone(&runtime),
            ContextValue::new(Arc::clone(calls)),
            tx,
            &mut pending,
        );
    }
    runtime.tasks.close();
    runtime
}

#[tokio::test]
async fn brief_create_timeout_does_not_restart_single_or_batch_workers() -> TestResult {
    for count in [1, 18] {
        recover_create_timeout(count, false).await?;
    }
    Ok(())
}

#[tokio::test]
async fn graceful_shutdown_drains_a_batch_waiting_for_a_connection() -> TestResult {
    recover_create_timeout(2, true).await
}

async fn recover_create_timeout(count: usize, graceful_shutdown: bool) -> TestResult {
    let proxy = ConnectionProxy::new().await?;
    let storage = Storage::builder()
        .namespace(random_string())
        .build_from_pool(proxy.pool.clone())?;
    let (pending, controls) = claimed_jobs(&storage, count).await?;
    proxy.pause();
    let calls = Arc::new(AtomicUsize::new(0));
    let runtime = start_jobs(storage.clone(), &calls, pending);
    // Longer than the configured 100ms create timeout: the runtime must
    // retain the claim and permit instead of exiting after that first error.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!runtime.cancel_token.is_cancelled());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(controls.busy_count().await, count);
    if graceful_shutdown {
        runtime.cancel_token.cancel();
    }
    proxy.paused.store(false, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), runtime.tasks.wait()).await?;
    assert_eq!(runtime.cancel_token.is_cancelled(), graceful_shutdown);
    assert!(runtime.take_shutdown_error().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), count);
    assert_eq!(controls.busy_count().await, 0);
    assert!(processing_claims(&storage).await?.is_empty());
    assert_eq!(storage.jobs_count().await?, 0);
    Ok(())
}

#[tokio::test]
async fn sustained_create_timeout_preserves_claims_and_stops_runtime() -> TestResult {
    let proxy = ConnectionProxy::new().await?;
    let storage = Storage::builder()
        .namespace(random_string())
        .build_from_pool(proxy.pool.clone())?;
    let (pending, controls) = claimed_jobs(&storage, 2).await?;
    let ids: Vec<_> = pending.iter().map(|job| job.envelope.id.clone()).collect();
    proxy.pause();
    let calls = Arc::new(AtomicUsize::new(0));
    let start = tokio::time::Instant::now();
    let runtime = start_jobs(storage.clone(), &calls, pending);
    tokio::time::timeout(Duration::from_secs(4), runtime.tasks.wait()).await?;
    assert!(start.elapsed() >= Duration::from_secs(2));
    assert!(runtime.cancel_token.is_cancelled());
    assert!(matches!(
        runtime.take_shutdown_error(),
        Some(OxanaError::DeadpoolRedisPoolError(
            deadpool_redis::PoolError::Timeout(deadpool_redis::TimeoutType::Create)
        ))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(controls.busy_count().await, 0);
    proxy.paused.store(false, Ordering::SeqCst);
    let claims = processing_claims(&storage).await?;
    assert_eq!(claims.len(), ids.len());
    for id in ids {
        assert!(claims.contains(&id));
        let job = storage.get_job(&id).await?.unwrap();
        assert!(job.meta.started_at.is_none());
        assert_eq!(job.meta.retries, 0);
    }
    Ok(())
}

#[tokio::test]
async fn force_cancel_drops_connection_retry_and_retains_claims() -> TestResult {
    let proxy = ConnectionProxy::new().await?;
    let storage = Storage::builder()
        .namespace(random_string())
        .build_from_pool(proxy.pool.clone())?;
    let (pending, controls) = claimed_jobs(&storage, 2).await?;
    proxy.pause();
    let calls = Arc::new(AtomicUsize::new(0));
    let runtime = start_jobs(storage.clone(), &calls, pending);
    tokio::time::sleep(Duration::from_millis(300)).await;
    runtime.force_cancel_token.cancel();
    tokio::time::timeout(Duration::from_secs(1), runtime.tasks.wait()).await?;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(controls.busy_count().await, 0);
    proxy.paused.store(false, Ordering::SeqCst);
    assert_eq!(processing_claims(&storage).await?.len(), 2);
    assert_eq!(storage.jobs_count().await?, 2);
    Ok(())
}

#[tokio::test]
async fn lost_start_reply_is_not_replayed_and_keeps_claims() -> TestResult {
    dotenvy::from_filename(".env.test").ok();
    let namespace = random_string();
    let proxy = RedisProxy::start(vec!["HMSET".into(), format!("{namespace}:jobs")], true).await?;
    let storage = Storage::builder()
        .namespace(namespace)
        .build_from_pool(proxy.pool.clone())?;
    let (pending, controls) = claimed_jobs(&storage, 2).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let runtime = start_jobs(storage.clone(), &calls, pending);
    tokio::time::timeout(Duration::from_secs(1), runtime.tasks.wait()).await?;
    assert!(proxy.injected());
    assert_eq!(proxy.matching_request_count(), 1);
    assert!(runtime.cancel_token.is_cancelled());
    assert!(runtime.take_shutdown_error().is_some());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(controls.busy_count().await, 0);
    let claims = processing_claims(&storage).await?;
    assert_eq!(claims.len(), 2);
    for id in claims {
        let job = storage.get_job(&id).await?.unwrap();
        assert!(job.meta.started_at.is_some());
        assert_eq!(job.meta.retries, 0);
    }
    Ok(())
}

#[derive(Clone)]
struct FailingBatchWorker {
    proxy: Arc<ConnectionProxy>,
    terminal: bool,
    panics: bool,
}

impl FromContext<Self> for FailingBatchWorker {
    fn from_context(ctx: &Self) -> Self {
        ctx.clone()
    }
}

#[async_trait::async_trait]
impl Worker<ConnectionJob> for FailingBatchWorker {
    type Error = std::io::Error;

    async fn run_batch(&self, _jobs: Vec<BatchItem<ConnectionJob>>) -> Result<(), Self::Error> {
        // Starting the batch succeeds. Only its retry/dead transitions lose
        // Redis connectivity, after the handler has already run.
        self.proxy.pause();
        assert!(!self.panics, "injected batch panic");
        Err(std::io::Error::other("injected batch failure"))
    }

    fn max_retries(&self, _: &ConnectionJob) -> u32 {
        if self.terminal { 0 } else { 2 }
    }
}

#[tokio::test]
async fn failed_batch_stops_after_first_transition_connection_failure() -> TestResult {
    stops_after_first_transition_connection_failure(false).await
}

#[tokio::test]
async fn panicked_batch_stops_after_first_transition_connection_failure() -> TestResult {
    stops_after_first_transition_connection_failure(true).await
}

async fn stops_after_first_transition_connection_failure(panics: bool) -> TestResult {
    for terminal in [false, true] {
        let proxy = Arc::new(ConnectionProxy::new().await?);
        let storage = Storage::builder()
            .namespace(random_string())
            .build_from_pool(proxy.pool.clone())?;
        let (mut pending, controls) = claimed_jobs(&storage, 4).await?;
        let ids: HashSet<_> = pending.iter().map(|job| job.envelope.id.clone()).collect();
        let context = FailingBatchWorker {
            proxy: Arc::clone(&proxy),
            terminal,
            panics,
        };
        let runtime = Arc::new(Runtime::new(
            storage.clone(),
            Config::new().register_worker::<FailingBatchWorker, ConnectionJob>(),
            RuntimeSettings::new(),
        ));
        let (tx, _rx) = mpsc::channel(1);
        let start = tokio::time::Instant::now();
        spawn_batch(
            Arc::clone(&runtime),
            ContextValue::new(context),
            tx,
            &mut pending,
        );
        runtime.tasks.close();
        // One 100ms create attempt plus one 2s retry window is allowed.
        // Retrying all four transitions would take over eight seconds.
        tokio::time::timeout(Duration::from_secs(4), runtime.tasks.wait()).await?;
        assert!(start.elapsed() >= Duration::from_secs(2));
        assert!(runtime.cancel_token.is_cancelled());
        assert!(matches!(
            runtime.take_shutdown_error(),
            Some(OxanaError::DeadpoolRedisPoolError(
                deadpool_redis::PoolError::Timeout(deadpool_redis::TimeoutType::Create)
            ))
        ));
        assert_eq!(controls.busy_count().await, 0);

        proxy.paused.store(false, Ordering::SeqCst);
        let claims: HashSet<_> = processing_claims(&storage).await?.into_iter().collect();
        assert_eq!(claims, ids);
        assert_eq!(storage.retries_count().await?, 0);
        assert_eq!(storage.dead_count().await?, 0);
        for id in ids {
            let job = storage.get_job(&id).await?.unwrap();
            assert!(job.meta.started_at.is_some());
            assert_eq!(job.meta.retries, 0);
        }
    }
    Ok(())
}
