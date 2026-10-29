use super::*;
use crate::test_helper::{random_string, redis_pool};
use futures::FutureExt;
use serde::Serialize;
use testresult::TestResult;

#[derive(Serialize)]
struct ConnectionJob;
impl crate::Job for ConnectionJob {}

#[derive(Clone, Copy, Debug)]
enum Transition {
    Start,
    Success,
    Retry,
    Dead,
}

async fn transition(
    storage: &StorageInternal,
    kind: Transition,
    job: &mut JobEnvelope,
) -> Result<(), OxanaError> {
    match kind {
        Transition::Start => {
            storage
                .set_started_at_batch(std::slice::from_mut(job))
                .await
        }
        Transition::Success => storage.finish_with_success(job).await,
        Transition::Retry => storage.retry_in(job.id.clone(), 60, "failed".into()).await,
        Transition::Dead => storage.kill(job, "failed".into()).await,
    }
}

#[tokio::test]
async fn claimed_transitions_retry_pool_wait_without_consuming_job_retries() -> TestResult {
    dotenvy::from_filename(".env.test").ok();
    for kind in [
        Transition::Start,
        Transition::Success,
        Transition::Retry,
        Transition::Dead,
    ] {
        let storage = crate::Storage::builder()
            .namespace(random_string())
            .max_pool_size(1)
            .timeouts(crate::StorageBuilderTimeouts {
                wait: Some(Duration::from_millis(20)),
                ..Default::default()
            })
            .build_from_redis_url(std::env::var("REDIS_URL")?)?
            .internal;
        let mut job = JobEnvelope::new(random_string(), ConnectionJob)?;
        storage.enqueue(job.clone()).await?;
        assert_eq!(storage.dequeue(&job.queue).await?, Some(job.id.clone()));

        {
            let held = storage.connection().await?;
            let operation = transition(&storage, kind, &mut job);
            tokio::pin!(operation);
            // Poll through at least one pool timeout before releasing the slot.
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut operation)
                    .await
                    .is_err()
            );
            drop(held);
            tokio::time::timeout(Duration::from_secs(2), operation).await??;
        }

        let stored = storage.get_job(&job.id).await?;
        let mut redis = storage.connection().await?;
        let claims: Vec<String> = redis
            .lrange(storage.current_processing_queue(), 0, -1)
            .await?;
        match kind {
            Transition::Start => {
                let stored = stored.unwrap();
                assert!(stored.meta.started_at.is_some());
                assert_eq!(stored.meta.retries, 0);
                assert_eq!(claims, vec![job.id]);
            }
            Transition::Success => {
                assert!(stored.is_none());
                assert!(claims.is_empty());
            }
            Transition::Retry => {
                assert_eq!(stored.unwrap().meta.retries, 1);
                assert!(claims.is_empty());
                let retries: Vec<String> = redis.zrange(&storage.keys.retry, 0, -1).await?;
                assert_eq!(retries, vec![job.id]);
            }
            Transition::Dead => {
                assert!(stored.is_none());
                assert!(claims.is_empty());
                let dead: Vec<String> = redis.lrange(&storage.keys.dead, 0, -1).await?;
                assert_eq!(dead.len(), 1);
                let dead: JobEnvelope = serde_json::from_str(dead.first().unwrap())?;
                assert_eq!(dead.id, job.id);
                assert_eq!(dead.meta.retries, 0);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn claimed_connection_recovers_after_long_pool_wait_timeouts() -> TestResult {
    dotenvy::from_filename(".env.test").ok();
    let storage = crate::Storage::builder()
        .namespace(random_string())
        .max_pool_size(1)
        .timeouts(crate::StorageBuilderTimeouts::new(Duration::from_secs(3)))
        .build_from_redis_url(std::env::var("REDIS_URL")?)?
        .internal;
    let held = storage.connection().await?;
    tokio::time::pause();
    let connection = storage.claimed_connection("start");
    tokio::pin!(connection);
    assert!(connection.as_mut().now_or_never().is_none());

    // The first three-second failure starts a separate six-second retry budget.
    tokio::time::advance(Duration::from_secs(3)).await;
    assert!(connection.as_mut().now_or_never().is_none());
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(connection.as_mut().now_or_never().is_none());
    // A second acquisition can use its full timeout without exhausting the budget.
    // The former fixed two-second budget would already have returned an error.
    tokio::time::advance(Duration::from_secs(3)).await;
    assert!(connection.as_mut().now_or_never().is_none());

    // Resume real time before recycling the connection over the Redis socket.
    tokio::time::resume();
    drop(held);
    let _connection = tokio::time::timeout(Duration::from_secs(2), connection).await??;
    Ok(())
}

#[tokio::test]
async fn claimed_connection_exhausts_default_and_scaled_pool_wait_budgets() -> TestResult {
    dotenvy::from_filename(".env.test").ok();
    for (wait, budget) in [
        (Duration::from_millis(300), Duration::from_secs(2)),
        (Duration::from_secs(3), Duration::from_secs(6)),
    ] {
        let storage = crate::Storage::builder()
            .max_pool_size(1)
            .timeouts(crate::StorageBuilderTimeouts::new(wait))
            .build_from_redis_url(std::env::var("REDIS_URL")?)?
            .internal;
        let _held = storage.connection().await?;
        tokio::time::pause();
        let started = tokio::time::Instant::now();
        let result = storage.claimed_connection("start").await;
        let elapsed = started.elapsed();
        tokio::time::resume();
        assert!(matches!(
            result,
            Err(OxanaError::DeadpoolRedisPoolError(
                deadpool_redis::PoolError::Timeout(deadpool_redis::TimeoutType::Wait)
            ))
        ));
        assert!(elapsed >= wait + budget);
        assert!(elapsed <= wait + budget + Duration::from_millis(10));
        assert_eq!(storage.pool.status().waiting, 0);
    }
    Ok(())
}

#[tokio::test]
async fn claimed_connection_does_not_retry_closed_pool() -> TestResult {
    let pool = redis_pool().await?;
    let storage = StorageInternal::new(pool.clone(), Some(random_string()));
    pool.close();
    let result = storage.claimed_connection("start").now_or_never().unwrap();
    assert!(matches!(
        result,
        Err(OxanaError::DeadpoolRedisPoolError(
            deadpool_redis::PoolError::Closed
        ))
    ));
    Ok(())
}

#[tokio::test]
async fn claimed_start_does_not_retry_command_errors() -> TestResult {
    let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
    let mut redis = storage.connection().await?;
    let _: () = redis.set(&storage.keys.jobs, "wrong type").await?;
    let mut job = JobEnvelope::new(random_string(), ConnectionJob)?;
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        storage.set_started_at_batch(std::slice::from_mut(&mut job)),
    )
    .await?;
    assert!(matches!(result, Err(OxanaError::DeadpoolRedisError(_))));
    Ok(())
}
