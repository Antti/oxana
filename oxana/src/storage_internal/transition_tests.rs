#[path = "../../tests/integration/claim_recovery/redis_proxy.rs"]
#[allow(dead_code)]
mod redis_proxy;

use super::*;
use crate::test_helper::{random_string, redis_pool};
use redis_proxy::RedisProxy;
use testresult::TestResult;

#[derive(serde::Serialize)]
struct TransitionJob;
impl crate::Job for TransitionJob {}

async fn fixture() -> TestResult<(StorageInternal, JobEnvelope)> {
    let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
    let envelope = JobEnvelope::new(random_string(), TransitionJob)?;
    storage.enqueue(envelope.clone()).await?;
    Ok((storage, envelope))
}

async fn schedule(
    storage: &StorageInternal,
    envelope: &JobEnvelope,
    retry: bool,
) -> TestResult<String> {
    let key = if retry {
        &storage.keys.retry
    } else {
        &storage.keys.schedule
    };
    let mut redis = storage.connection().await?;
    let _: () = redis::pipe()
        .lrem(storage.namespace_queue(&envelope.queue), 1, &envelope.id)
        .zadd(key, &envelope.id, 1)
        .query_async(&mut redis)
        .await?;
    Ok(key.clone())
}

fn script_prefix(script: &str, storage: &StorageInternal, keys: usize) -> Vec<String> {
    vec![
        "EVALSHA".into(),
        redis::Script::new(script).get_hash().into(),
        keys.to_string(),
        storage.keys.jobs.clone(),
    ]
}

#[tokio::test]
async fn regression_retry_missing_payload_releases_only_its_claim() -> TestResult {
    let (storage, envelope) = fixture().await?;
    storage.dequeue(&envelope.queue).await?;
    let mut redis = storage.connection().await?;
    let _: () = redis.hdel(&storage.keys.jobs, &envelope.id).await?;

    storage.retry_in(envelope.id, 0, "failed".into()).await?;

    assert_eq!(
        redis
            .llen::<_, usize>(storage.current_processing_queue())
            .await?,
        0
    );
    assert_eq!(storage.retries_count().await?, 0);
    assert_eq!(storage.jobs_count().await?, 0);
    Ok(())
}

#[tokio::test]
async fn regression_missing_payload_claim_cleanup_errors_are_propagated() -> TestResult {
    let (storage, envelope) = fixture().await?;
    storage.dequeue(&envelope.queue).await?;
    let mut redis = storage.connection().await?;
    let _: () = redis.hdel(&storage.keys.jobs, &envelope.id).await?;
    let proxy =
        RedisProxy::reject_once(vec!["LREM".into(), storage.current_processing_queue()]).await?;
    let mut through_proxy = storage.clone();
    through_proxy.pool = proxy.pool.clone();

    assert!(
        through_proxy
            .retry_in(envelope.id.clone(), 0, "failed".into())
            .await
            .is_err()
    );
    assert!(proxy.injected());
    assert_eq!(
        storage.currently_processing_job_ids().await?,
        vec![envelope.id]
    );
    assert_eq!(storage.retries_count().await?, 0);
    Ok(())
}

#[tokio::test]
async fn regression_malformed_scheduled_payload_does_not_block_healthy_jobs() -> TestResult {
    for retry in [false, true] {
        let (storage, malformed) = fixture().await?;
        let source = schedule(&storage, &malformed, retry).await?;
        let healthy = JobEnvelope::new(malformed.queue.clone(), TransitionJob)?;
        storage.enqueue(healthy.clone()).await?;
        schedule(&storage, &healthy, retry).await?;
        let mut redis = storage.connection().await?;
        let _: () = redis::pipe()
            .hset(&storage.keys.jobs, &malformed.id, "{invalid json")
            .zadd(&source, &healthy.id, 2)
            .query_async(&mut redis)
            .await?;

        assert_eq!(storage.enqueue_scheduled(&source).await?, 1);
        assert_eq!(storage.enqueue_scheduled(&source).await?, 0);
        let retained: Vec<String> = redis.zrange(&source, 0, -1).await?;
        assert_eq!(retained, vec![malformed.id.clone()]);
        assert_eq!(
            redis
                .hget::<_, _, String>(&storage.keys.jobs, &malformed.id)
                .await?,
            "{invalid json"
        );
        assert_eq!(storage.enqueued_count(&healthy.queue).await?, 1);

        storage.update_job(&malformed).await?;
        assert_eq!(storage.enqueue_scheduled(&source).await?, 1);
        assert_eq!(redis.zcard::<_, usize>(&source).await?, 0);
        assert_eq!(storage.enqueued_count(&healthy.queue).await?, 2);
    }
    Ok(())
}

#[tokio::test]
async fn regression_malformed_claims_do_not_block_recovery() -> TestResult {
    let (storage, first) = fixture().await?;
    // More than one recovery batch, with malformed claims at both ends and
    // in the middle, exercises scanning past retained entries.
    let mut jobs = vec![first.clone()];
    for _ in 0..24 {
        let job = JobEnvelope::new(first.queue.clone(), TransitionJob)?;
        storage.enqueue(job.clone()).await?;
        jobs.push(job);
    }
    for _ in &jobs {
        storage.dequeue(&first.queue).await?;
    }
    let other = storage.for_new_process();
    let other_job = JobEnvelope::new(first.queue.clone(), TransitionJob)?;
    other.enqueue(other_job.clone()).await?;
    other.dequeue(&first.queue).await?;

    let mut redis = storage.connection().await?;
    let malformed = [
        jobs.first().unwrap(),
        jobs.get(12).unwrap(),
        jobs.last().unwrap(),
    ];
    for job in malformed {
        let _: () = redis
            .hset(&storage.keys.jobs, &job.id, "{invalid json")
            .await?;
    }

    for _ in 0..2 {
        tokio::time::timeout(
            Duration::from_secs(5),
            storage.resurrect(Duration::from_secs(5)),
        )
        .await??;
        assert_eq!(storage.enqueued_count(&first.queue).await?, 23);
        let retained: Vec<String> = redis
            .lrange(storage.current_processing_queue(), 0, -1)
            .await?;
        assert_eq!(retained.len(), malformed.len());
        for job in malformed {
            assert!(retained.contains(&job.id));
            assert_eq!(
                redis
                    .hget::<_, _, String>(&storage.keys.jobs, &job.id)
                    .await?,
                "{invalid json"
            );
        }
        assert_eq!(
            redis
                .llen::<_, usize>(other.current_processing_queue())
                .await?,
            0
        );
    }

    for job in malformed {
        storage.update_job(job).await?;
    }
    storage.resurrect(Duration::from_secs(5)).await?;
    assert_eq!(storage.enqueued_count(&first.queue).await?, 26);
    assert_eq!(
        redis
            .llen::<_, usize>(storage.current_processing_queue())
            .await?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn scheduled_and_retry_transfers_survive_rejected_commands_and_lost_replies() -> TestResult {
    for retry in [false, true] {
        for committed in [false, true] {
            let (storage, envelope) = fixture().await?;
            let source = schedule(&storage, &envelope, retry).await?;
            let prefix = script_prefix(include_str!("../promote_scheduled.lua"), &storage, 3);
            let proxy = if committed {
                RedisProxy::start(prefix, true).await?
            } else {
                RedisProxy::reject_once(prefix).await?
            };
            let through_proxy =
                StorageInternal::new(proxy.pool.clone(), Some(storage.namespace().into()));
            assert!(through_proxy.enqueue_scheduled(&source).await.is_err());
            assert!(proxy.injected());
            let mut redis = storage.connection().await?;
            assert_eq!(
                redis.zcard::<_, usize>(&source).await?,
                usize::from(!committed)
            );
            assert_eq!(
                storage.enqueued_count(&envelope.queue).await?,
                usize::from(committed)
            );
            assert!(storage.get_job(&envelope.id).await?.is_some());
            storage.enqueue_scheduled(&source).await?;
            storage.enqueue_scheduled(&source).await?;
            assert_eq!(redis.zcard::<_, usize>(&source).await?, 0);
            assert_eq!(storage.enqueued_count(&envelope.queue).await?, 1);
        }
    }
    Ok(())
}

#[tokio::test]
async fn scheduled_payload_read_failure_keeps_the_source() -> TestResult {
    let (storage, envelope) = fixture().await?;
    let source = schedule(&storage, &envelope, true).await?;
    let proxy = RedisProxy::reject_once(vec!["HGET".into(), storage.keys.jobs.clone()]).await?;
    let through_proxy = StorageInternal::new(proxy.pool.clone(), Some(storage.namespace().into()));
    assert!(through_proxy.retry_all_now().await.is_err());
    assert!(proxy.injected());
    assert_eq!(storage.retries_count().await?, 1);
    assert_eq!(storage.enqueue_scheduled(&source).await?, 1);
    Ok(())
}

#[tokio::test]
async fn invalid_ready_queue_keeps_scheduled_work_recoverable() -> TestResult {
    let (storage, envelope) = fixture().await?;
    let source = schedule(&storage, &envelope, false).await?;
    let mut redis = storage.connection().await?;
    let ready = storage.namespace_queue(&envelope.queue);
    let _: () = redis.set(&ready, "wrong type").await?;
    assert!(storage.enqueue_scheduled(&source).await.is_err());
    assert_eq!(storage.scheduled_count().await?, 1);
    assert!(storage.get_job(&envelope.id).await?.is_some());
    let _: () = redis.del(&ready).await?;
    assert_eq!(storage.enqueue_scheduled(&source).await?, 1);
    Ok(())
}

#[tokio::test]
async fn competing_schedulers_enqueue_each_job_once() -> TestResult {
    let (storage, envelope) = fixture().await?;
    let source = schedule(&storage, &envelope, true).await?;
    let (a, b) = tokio::join!(
        storage.enqueue_scheduled(&source),
        storage.enqueue_scheduled(&source)
    );
    assert_eq!(a? + b?, 1);
    assert_eq!(storage.enqueued_count(&envelope.queue).await?, 1);
    Ok(())
}

#[tokio::test]
async fn stale_scheduler_does_not_promote_a_new_retry() -> TestResult {
    let (storage, envelope) = fixture().await?;
    let source = schedule(&storage, &envelope, true).await?;
    let mut redis = storage.connection().await?;
    let _: () = redis.zadd(&source, &envelope.id, 2).await?;
    assert_eq!(
        storage
            .promote_scheduled_jobs(&mut redis, &source, vec![(envelope.id.clone(), 1.0)])
            .await?,
        0
    );
    assert_eq!(storage.retries_count().await?, 1);
    assert_eq!(storage.enqueued_count(&envelope.queue).await?, 0);
    Ok(())
}

#[tokio::test]
async fn recovery_survives_rejected_commands_and_lost_replies() -> TestResult {
    for committed in [false, true] {
        let (storage, envelope) = fixture().await?;
        storage.dequeue(&envelope.queue).await?;
        let prefix = script_prefix(include_str!("../resurrect_claim.lua"), &storage, 3);
        let proxy = if committed {
            RedisProxy::start(prefix, true).await?
        } else {
            RedisProxy::reject_once(prefix).await?
        };
        let through_proxy =
            StorageInternal::new(proxy.pool.clone(), Some(storage.namespace().into()));
        assert!(
            through_proxy
                .resurrect(Duration::from_secs(5))
                .await
                .is_err()
        );
        assert!(proxy.injected());
        assert_eq!(
            storage.currently_processing_job_ids().await?.len(),
            usize::from(!committed)
        );
        assert_eq!(
            storage.enqueued_count(&envelope.queue).await?,
            usize::from(committed)
        );
        assert!(storage.get_job(&envelope.id).await?.is_some());
        storage.resurrect(Duration::from_secs(5)).await?;
        storage.resurrect(Duration::from_secs(5)).await?;
        assert!(storage.currently_processing_job_ids().await?.is_empty());
        assert_eq!(storage.enqueued_count(&envelope.queue).await?, 1);
    }
    Ok(())
}

#[tokio::test]
async fn recovery_payload_read_failure_keeps_the_claim() -> TestResult {
    let (storage, envelope) = fixture().await?;
    storage.dequeue(&envelope.queue).await?;
    let proxy = RedisProxy::reject_once(vec!["HGET".into(), storage.keys.jobs.clone()]).await?;
    let through_proxy = StorageInternal::new(proxy.pool.clone(), Some(storage.namespace().into()));
    assert!(
        through_proxy
            .resurrect(Duration::from_secs(5))
            .await
            .is_err()
    );
    assert!(proxy.injected());
    assert_eq!(
        storage.currently_processing_job_ids().await?,
        vec![envelope.id]
    );
    storage.resurrect(Duration::from_secs(5)).await?;
    assert_eq!(storage.enqueued_count(&envelope.queue).await?, 1);
    Ok(())
}

#[tokio::test]
async fn invalid_ready_queue_keeps_recovery_claims() -> TestResult {
    let (storage, envelope) = fixture().await?;
    storage.dequeue(&envelope.queue).await?;
    let mut redis = storage.connection().await?;
    let ready = storage.namespace_queue(&envelope.queue);
    let _: () = redis.set(&ready, "wrong type").await?;
    assert!(storage.resurrect(Duration::from_secs(5)).await.is_err());
    assert_eq!(
        storage.currently_processing_job_ids().await?,
        vec![envelope.id.clone()]
    );
    assert!(storage.get_job(&envelope.id).await?.is_some());
    let _: () = redis.del(&ready).await?;
    storage.resurrect(Duration::from_secs(5)).await?;
    assert_eq!(storage.enqueued_count(&envelope.queue).await?, 1);
    Ok(())
}

#[tokio::test]
async fn competing_recovery_processes_do_not_duplicate_claims() -> TestResult {
    let (storage, envelope) = fixture().await?;
    for _ in 0..12 {
        storage
            .enqueue(JobEnvelope::new(envelope.queue.clone(), TransitionJob)?)
            .await?;
    }
    for _ in 0..13 {
        storage.dequeue(&envelope.queue).await?;
    }
    let (a, b) = tokio::join!(
        storage.resurrect(Duration::from_secs(5)),
        storage.resurrect(Duration::from_secs(5))
    );
    a?;
    b?;
    assert!(storage.currently_processing_job_ids().await?.is_empty());
    assert_eq!(storage.enqueued_count(&envelope.queue).await?, 13);
    Ok(())
}

#[tokio::test]
async fn recovery_snapshot_survives_concurrent_claim_removal() -> TestResult {
    let (storage, envelope) = fixture().await?;
    for _ in 0..12 {
        storage
            .enqueue(JobEnvelope::new(envelope.queue.clone(), TransitionJob)?)
            .await?;
    }
    for _ in 0..13 {
        storage.dequeue(&envelope.queue).await?;
    }
    let processing = storage.current_processing_queue();
    let ready = storage.namespace_queue(&envelope.queue);
    let proxy = RedisProxy::pause_once(vec!["LRANGE".into(), processing.clone()]).await?;
    let through_proxy = StorageInternal::new(proxy.pool.clone(), Some(storage.namespace().into()));
    let mut redis = storage.connection().await?;

    let (recovery, competitor) = tokio::join!(
        tokio::time::timeout(
            Duration::from_secs(5),
            through_proxy.resurrect(Duration::from_secs(5))
        ),
        async {
            tokio::time::timeout(Duration::from_secs(5), proxy.wait_until_paused()).await?;
            // Another recovery pass completes five claims after our LRANGE
            // snapshot. Their removal must not advance our cursor past the tail.
            for _ in 0..5 {
                let _: String = redis::cmd("LMOVE")
                    .arg(&processing)
                    .arg(&ready)
                    .arg("LEFT")
                    .arg("LEFT")
                    .query_async(&mut redis)
                    .await?;
            }
            proxy.resume();
            TestResult::Ok(())
        }
    );
    competitor?;
    recovery??;
    assert_eq!(redis.llen::<_, usize>(&processing).await?, 0);
    let recovered: Vec<String> = redis.lrange(&ready, 0, -1).await?;
    assert_eq!(recovered.len(), 13);
    assert_eq!(recovered.into_iter().collect::<HashSet<_>>().len(), 13);
    Ok(())
}

#[tokio::test]
async fn recovery_removes_missing_and_non_resurrectable_jobs() -> TestResult {
    for missing in [false, true] {
        let (storage, mut envelope) = fixture().await?;
        storage.dequeue(&envelope.queue).await?;
        if missing {
            let mut redis = storage.connection().await?;
            let _: () = redis.hdel(&storage.keys.jobs, &envelope.id).await?;
        } else {
            envelope.meta.resurrect = false;
            storage.update_job(&envelope).await?;
        }
        storage.resurrect(Duration::from_secs(5)).await?;
        assert!(storage.currently_processing_job_ids().await?.is_empty());
        assert_eq!(storage.jobs_count().await?, 0);
        assert_eq!(storage.enqueued_count(&envelope.queue).await?, 0);
    }
    Ok(())
}

#[tokio::test]
async fn acknowledgments_survive_rejected_commands_and_lost_replies() -> TestResult {
    for committed in [false, true] {
        let (storage, envelope) = fixture().await?;
        storage.dequeue(&envelope.queue).await?;
        let prefix = script_prefix(include_str!("../finish_success.lua"), &storage, 2);
        let proxy = if committed {
            RedisProxy::start(prefix, true).await?
        } else {
            RedisProxy::reject_once(prefix).await?
        };
        // Use the original process identity so the acknowledgment targets its claim.
        let mut through_proxy = storage.clone();
        through_proxy.pool = proxy.pool.clone();
        assert!(through_proxy.finish_with_success(&envelope).await.is_err());
        assert!(proxy.injected());
        assert_eq!(
            storage.currently_processing_job_ids().await?.len(),
            usize::from(!committed)
        );
        assert_eq!(storage.jobs_count().await?, usize::from(!committed));
        storage.finish_with_success(&envelope).await?;
        storage.finish_with_success(&envelope).await?;
        assert!(storage.currently_processing_job_ids().await?.is_empty());
        assert_eq!(storage.jobs_count().await?, 0);
    }
    Ok(())
}

#[tokio::test]
async fn invalid_acknowledgment_key_does_not_delete_payloads() -> TestResult {
    let (storage, envelope) = fixture().await?;
    let mut redis = storage.connection().await?;
    let _: () = redis
        .set(storage.current_processing_queue(), "wrong type")
        .await?;
    assert!(storage.finish_with_success(&envelope).await.is_err());
    assert!(storage.get_job(&envelope.id).await?.is_some());
    Ok(())
}

#[tokio::test]
async fn transfers_ignore_payloads_replaced_after_the_snapshot() -> TestResult {
    for recovery in [false, true] {
        let (storage, envelope) = fixture().await?;
        let source = if recovery {
            storage.dequeue(&envelope.queue).await?;
            storage.current_processing_queue()
        } else {
            schedule(&storage, &envelope, false).await?
        };
        let mut redis = storage.connection().await?;
        let (snapshot, _) = storage
            .transition_snapshot(&mut redis, &envelope.id)
            .await?;
        let mut replacement = envelope.clone();
        replacement.queue = random_string();
        storage.update_job(&replacement).await?;
        let script = redis::Script::new(if recovery {
            include_str!("../resurrect_claim.lua")
        } else {
            include_str!("../promote_scheduled.lua")
        });
        let moved: usize = script
            .key(&storage.keys.jobs)
            .key(&source)
            .key(storage.namespace_queue(&envelope.queue))
            .arg(&envelope.id)
            .arg(snapshot.unwrap())
            .arg(1)
            .invoke_async(&mut *redis)
            .await?;
        assert_eq!(moved, 0);
        assert_eq!(storage.enqueued_count(&envelope.queue).await?, 0);
        if recovery {
            assert_eq!(
                storage.currently_processing_job_ids().await?,
                vec![envelope.id.clone()]
            );
            storage.resurrect(Duration::from_secs(5)).await?;
        } else {
            assert_eq!(storage.scheduled_count().await?, 1);
            storage.enqueue_scheduled(&source).await?;
        }
        assert_eq!(storage.enqueued_count(&replacement.queue).await?, 1);
    }
    Ok(())
}

#[tokio::test]
async fn scheduled_missing_payload_is_cleaned_without_an_empty_job() -> TestResult {
    let (storage, envelope) = fixture().await?;
    let source = schedule(&storage, &envelope, true).await?;
    let mut redis = storage.connection().await?;
    let _: () = redis.hdel(&storage.keys.jobs, &envelope.id).await?;
    assert_eq!(storage.enqueue_scheduled(&source).await?, 0);
    assert_eq!(storage.retries_count().await?, 0);
    assert_eq!(storage.enqueued_count(&envelope.queue).await?, 0);
    Ok(())
}
