use super::*;
use crate::test_helper::{random_string, redis_pool, redis_proxy::RedisProxy};
use testresult::TestResult;

#[derive(serde::Serialize)]
struct EnqueueJob;
impl crate::Job for EnqueueJob {}

async fn fixture(strategy: JobConflictStrategy) -> TestResult<(StorageInternal, JobEnvelope)> {
    let storage = StorageInternal::new(redis_pool().await?, Some(random_string()));
    let mut envelope = JobEnvelope::new(random_string(), EnqueueJob)?;
    envelope.meta.unique = true;
    envelope.meta.on_conflict = Some(strategy);
    Ok((storage, envelope))
}

async fn enqueue_script_prefix(storage: &StorageInternal) -> TestResult<Vec<String>> {
    let script = redis::Script::new(include_str!("../enqueue.lua"));
    script.load_async(&mut *storage.connection().await?).await?;
    Ok(vec!["EVALSHA".into(), script.get_hash().into()])
}

#[tokio::test]
async fn bulk_skip_remembers_jobs_it_wrote_after_workers_finish_between_chunks() -> TestResult {
    for initially_present in [false, true] {
        let (storage, original) = fixture(JobConflictStrategy::Skip).await?;
        if initially_present {
            storage.enqueue(original.clone()).await?;
        }
        let prefix = if initially_present {
            vec![
                "HEXISTS".into(),
                storage.keys.jobs.clone(),
                original.id.clone(),
            ]
        } else {
            enqueue_script_prefix(&storage).await?
        };
        let proxy = RedisProxy::pause_once(prefix).await?;
        let mut through_proxy = storage.clone();
        through_proxy.pool = proxy.pool.clone();
        let envelopes = (0..101)
            .map(|marker| {
                let mut envelope = original.clone();
                envelope.job.args = serde_json::json!({"marker": marker});
                envelope
            })
            .collect();
        let enqueue = tokio::spawn(async move { through_proxy.enqueue_list(envelopes).await });
        tokio::time::timeout(Duration::from_secs(5), proxy.wait_until_paused()).await?;

        assert_eq!(
            storage.dequeue(&original.queue).await?,
            Some(original.id.clone())
        );
        storage.finish_with_success(&original).await?;
        proxy.resume();
        assert_eq!(enqueue.await??, vec![original.id.clone(); 101]);

        // Only writes made by this request suppress later occurrences. A job
        // that already existed before the request may be enqueued after it finishes.
        assert_eq!(
            storage.enqueued_count(&original.queue).await?,
            usize::from(initially_present)
        );
        let remaining = storage.get_job(&original.id).await?;
        if initially_present {
            assert_eq!(
                remaining.unwrap().job.args,
                serde_json::json!({"marker": 100})
            );
        } else {
            assert!(remaining.is_none());
        }
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_replacements_across_queues_all_succeed() -> TestResult {
    for initially_present in [false, true] {
        let (storage, original) = fixture(JobConflictStrategy::Replace).await?;
        if initially_present {
            storage.enqueue(original.clone()).await?;
        }
        let queues: Vec<_> = (0..24).map(|_| random_string()).collect();
        for _ in 0..4 {
            let barrier = Arc::new(tokio::sync::Barrier::new(queues.len()));
            let mut tasks = tokio::task::JoinSet::new();
            for queue in &queues {
                let storage = storage.clone();
                let barrier = Arc::clone(&barrier);
                let mut replacement = original.clone();
                replacement.queue.clone_from(queue);
                tasks.spawn(async move {
                    barrier.wait().await;
                    storage.enqueue(replacement).await
                });
            }
            while let Some(result) = tasks.join_next().await {
                assert_eq!(result??, original.id);
            }
            let saved = storage.get_job(&original.id).await?.unwrap();
            assert_eq!(storage.enqueued_count(&original.queue).await?, 0);
            for queue in &queues {
                assert_eq!(
                    storage.enqueued_count(queue).await?,
                    usize::from(queue == &saved.queue)
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn skipping_duplicates_does_not_transfer_the_stored_payload() -> TestResult {
    let (storage, mut original) = fixture(JobConflictStrategy::Skip).await?;
    original.job.args = serde_json::json!({"blob": "x".repeat(256 * 1024)});
    storage.enqueue(original.clone()).await?;
    let proxy = RedisProxy::observe().await?;
    let mut through_proxy = storage.clone();
    through_proxy.pool = proxy.pool.clone();
    let mut duplicate = original.clone();
    duplicate.job.args = serde_json::json!({});

    assert_eq!(
        through_proxy.enqueue_list(vec![duplicate; 100]).await?,
        vec![original.id.clone(); 100]
    );

    let (request_bytes, response_bytes) = proxy.traffic_bytes();
    assert!(request_bytes < 64 * 1024, "sent {request_bytes} bytes");
    assert!(
        response_bytes < 64 * 1024,
        "received {response_bytes} bytes"
    );
    assert_eq!(storage.enqueued_count(&original.queue).await?, 1);
    assert_eq!(
        storage.get_job(&original.id).await?.unwrap().job.args,
        original.job.args
    );
    Ok(())
}

#[tokio::test]
async fn skipping_existing_jobs_does_not_transfer_incoming_payloads() -> TestResult {
    let (storage, original) = fixture(JobConflictStrategy::Skip).await?;
    let existing: Vec<_> = (0..100)
        .map(|_| {
            let mut envelope = original.clone();
            envelope.id = random_string();
            envelope
        })
        .collect();
    let expected_ids = storage.enqueue_list(existing.clone()).await?;
    let incoming = existing
        .iter()
        .map(|envelope| {
            let mut envelope = envelope.clone();
            envelope.job.args = serde_json::json!({"blob": "x".repeat(256 * 1024)});
            envelope
        })
        .collect();
    let proxy = RedisProxy::observe().await?;
    let mut through_proxy = storage.clone();
    through_proxy.pool = proxy.pool.clone();

    assert_eq!(through_proxy.enqueue_list(incoming).await?, expected_ids);

    let (request_bytes, response_bytes) = proxy.traffic_bytes();
    assert!(request_bytes < 64 * 1024, "sent {request_bytes} bytes");
    assert!(
        response_bytes < 64 * 1024,
        "received {response_bytes} bytes"
    );
    assert_eq!(storage.enqueued_count(&original.queue).await?, 100);
    for envelope in existing {
        assert_eq!(
            storage.get_job(&envelope.id).await?.unwrap().job.args,
            envelope.job.args
        );
    }
    Ok(())
}

#[tokio::test]
async fn replacement_snapshots_do_not_retransmit_large_arguments_or_state() -> TestResult {
    for large_state in [false, true] {
        let (storage, mut original) = fixture(JobConflictStrategy::Replace).await?;
        let blob = "x".repeat(256 * 1024);
        if large_state {
            original.meta.state = Some(serde_json::json!({"blob": blob}));
        } else {
            original.job.args = serde_json::json!({"blob": blob});
        }
        storage.enqueue(original.clone()).await?;
        let proxy = RedisProxy::observe().await?;
        let mut through_proxy = storage.clone();
        through_proxy.pool = proxy.pool.clone();
        let mut replacement = original.clone();
        replacement.queue = random_string();
        replacement.job.args = serde_json::json!({"value": u64::MAX});
        replacement.meta.state = None;

        assert_eq!(
            through_proxy.enqueue(replacement.clone()).await?,
            original.id
        );

        let (request_bytes, response_bytes) = proxy.traffic_bytes();
        assert!(request_bytes < 64 * 1024, "sent {request_bytes} bytes");
        assert!(response_bytes >= 256 * 1024);
        assert_eq!(storage.enqueued_count(&original.queue).await?, 0);
        assert_eq!(storage.enqueued_count(&replacement.queue).await?, 1);
        assert_eq!(
            serde_json::to_string(&storage.get_job(&original.id).await?.unwrap())?,
            serde_json::to_string(&replacement)?
        );
    }
    Ok(())
}

#[tokio::test]
async fn replacement_does_not_retry_for_saved_state_changes_with_the_same_queue() -> TestResult {
    let (storage, mut original) = fixture(JobConflictStrategy::Replace).await?;
    original.meta.state = Some(serde_json::json!({"blob": "x".repeat(256 * 1024)}));
    storage.enqueue(original.clone()).await?;
    let proxy = RedisProxy::pause_once(vec!["HMGET".into(), storage.keys.jobs.clone()]).await?;
    let mut through_proxy = storage.clone();
    through_proxy.pool = proxy.pool.clone();
    let mut replacement = original.clone();
    replacement.queue = random_string();
    replacement.meta.state = None;
    let incoming = replacement.clone();
    let enqueue = tokio::spawn(async move { through_proxy.enqueue(incoming).await });
    tokio::time::timeout(Duration::from_secs(5), proxy.wait_until_paused()).await?;

    storage
        .update_state(
            &original.id,
            serde_json::json!({"blob": "y".repeat(256 * 1024)}),
        )
        .await?;
    proxy.resume();

    assert_eq!(enqueue.await??, original.id);
    let (request_bytes, response_bytes) = proxy.traffic_bytes();
    assert!(request_bytes < 64 * 1024, "sent {request_bytes} bytes");
    // One initial read is required. A retry would return another large snapshot.
    assert!(
        (256 * 1024..320 * 1024).contains(&response_bytes),
        "received {response_bytes} bytes"
    );
    assert_eq!(storage.enqueued_count(&original.queue).await?, 0);
    assert_eq!(storage.enqueued_count(&replacement.queue).await?, 1);
    assert_eq!(
        serde_json::to_string(&storage.get_job(&original.id).await?.unwrap())?,
        serde_json::to_string(&replacement)?
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_unique_skip_keeps_one_destination() -> TestResult {
    for strategy in [Some(JobConflictStrategy::Skip), None] {
        let (storage, mut envelope) = fixture(JobConflictStrategy::Skip).await?;
        envelope.meta.on_conflict = strategy;
        envelope.meta.scheduled_at = chrono::Utc::now().timestamp_micros() + 60_000_000;
        let queues: Vec<_> = (0..24).map(|_| random_string()).collect();
        let barrier = Arc::new(tokio::sync::Barrier::new(queues.len()));
        let mut tasks = tokio::task::JoinSet::new();
        for (index, queue) in queues.iter().enumerate() {
            let storage = storage.clone();
            let barrier = Arc::clone(&barrier);
            let mut envelope = envelope.clone();
            envelope.queue.clone_from(queue);
            envelope.job.args = serde_json::json!({"producer": index});
            tasks.spawn(async move {
                barrier.wait().await;
                match index % 3 {
                    0 => storage.enqueue(envelope).await.map(|id| vec![id]),
                    1 => storage.enqueue_at(envelope).await.map(|id| vec![id]),
                    _ => storage.enqueue_list(vec![envelope.clone(), envelope]).await,
                }
            });
        }
        while let Some(result) = tasks.join_next().await {
            assert!(result??.iter().all(|id| id == &envelope.id));
        }

        let saved = storage.get_job(&envelope.id).await?.unwrap();
        let producer = saved
            .job
            .args
            .get("producer")
            .and_then(serde_json::Value::as_u64)
            .unwrap();
        let mut ready_count = 0;
        for queue in &queues {
            let count = storage.enqueued_count(queue).await?;
            if count != 0 {
                assert_eq!(queue, &saved.queue);
            }
            ready_count += count;
        }
        let scheduled_count = storage.scheduled_count().await?;
        assert_eq!(ready_count + scheduled_count, 1);
        assert_eq!(scheduled_count, usize::from(producer % 3 == 1));
        assert_eq!(storage.jobs_count().await?, 1);
    }
    Ok(())
}

#[tokio::test]
async fn skip_rechecks_absence_before_inserting() -> TestResult {
    let (storage, original) = fixture(JobConflictStrategy::Skip).await?;
    let proxy = RedisProxy::pause_once(vec!["HEXISTS".into(), storage.keys.jobs.clone()]).await?;
    let mut through_proxy = storage.clone();
    through_proxy.pool = proxy.pool.clone();
    let incoming = original.clone();
    let enqueue = tokio::spawn(async move { through_proxy.enqueue(incoming).await });
    tokio::time::timeout(Duration::from_secs(5), proxy.wait_until_paused()).await?;

    let mut concurrent = original.clone();
    concurrent.queue = random_string();
    concurrent.meta.scheduled_at = chrono::Utc::now().timestamp_micros() + 60_000_000;
    storage.enqueue_at(concurrent.clone()).await?;
    proxy.resume();

    assert_eq!(enqueue.await??, original.id);
    assert_eq!(storage.enqueued_count(&original.queue).await?, 0);
    assert_eq!(storage.scheduled_count().await?, 1);
    assert_eq!(
        serde_json::to_string(&storage.get_job(&original.id).await?.unwrap())?,
        serde_json::to_string(&concurrent)?
    );
    Ok(())
}

#[tokio::test]
async fn try_enqueue_reports_skip_when_another_producer_wins_after_preflight() -> TestResult {
    let (storage, original) = fixture(JobConflictStrategy::Skip).await?;
    let proxy = RedisProxy::pause_once(vec!["HEXISTS".into(), storage.keys.jobs.clone()]).await?;
    let mut through_proxy = storage.clone();
    through_proxy.pool = proxy.pool.clone();
    let incoming = original.clone();
    let enqueue = tokio::spawn(async move { through_proxy.try_enqueue(incoming).await });
    tokio::time::timeout(Duration::from_secs(5), proxy.wait_until_paused()).await?;

    let mut winner = original.clone();
    winner.job.args = serde_json::json!({"producer": "winner"});
    storage.enqueue(winner.clone()).await?;
    proxy.resume();

    assert_eq!(enqueue.await??, None);
    assert_eq!(storage.enqueued_count(&original.queue).await?, 1);
    assert_eq!(
        storage.get_job(&original.id).await?.unwrap().job.args,
        winner.job.args
    );
    Ok(())
}

#[tokio::test]
async fn try_enqueue_preserves_skip_after_the_existing_job_finishes() -> TestResult {
    let (storage, original) = fixture(JobConflictStrategy::Skip).await?;
    storage.enqueue(original.clone()).await?;
    let proxy = RedisProxy::pause_once(vec!["HEXISTS".into(), storage.keys.jobs.clone()]).await?;
    let mut through_proxy = storage.clone();
    through_proxy.pool = proxy.pool.clone();
    let incoming = original.clone();
    let enqueue = tokio::spawn(async move { through_proxy.try_enqueue(incoming).await });
    tokio::time::timeout(Duration::from_secs(5), proxy.wait_until_paused()).await?;

    assert_eq!(
        storage.dequeue(&original.queue).await?,
        Some(original.id.clone())
    );
    storage.finish_with_success(&original).await?;
    proxy.resume();

    assert_eq!(enqueue.await??, None);
    assert!(storage.get_job(&original.id).await?.is_none());
    assert_eq!(storage.enqueued_count(&original.queue).await?, 0);
    Ok(())
}

#[tokio::test]
async fn try_enqueue_preserves_success_after_the_written_job_finishes() -> TestResult {
    for strategy in [
        None,
        Some(JobConflictStrategy::Skip),
        Some(JobConflictStrategy::Replace),
    ] {
        let (storage, mut original) = fixture(JobConflictStrategy::Skip).await?;
        original.meta.unique = strategy.is_some();
        original.meta.on_conflict.clone_from(&strategy);
        if strategy == Some(JobConflictStrategy::Replace) {
            storage.enqueue(original.clone()).await?;
        }
        let prefix = if original.meta.unique {
            enqueue_script_prefix(&storage).await?
        } else {
            vec!["LPUSH".into(), storage.namespace_queue(&original.queue)]
        };
        let proxy = RedisProxy::pause_once(prefix).await?;
        let mut through_proxy = storage.clone();
        through_proxy.pool = proxy.pool.clone();
        let incoming = original.clone();
        let enqueue = tokio::spawn(async move { through_proxy.try_enqueue(incoming).await });
        tokio::time::timeout(Duration::from_secs(5), proxy.wait_until_paused()).await?;

        assert_eq!(
            storage.dequeue(&original.queue).await?,
            Some(original.id.clone())
        );
        storage.finish_with_success(&original).await?;
        proxy.resume();

        assert_eq!(enqueue.await??, Some(original.id.clone()));
        assert!(storage.get_job(&original.id).await?.is_none());
        assert_eq!(storage.enqueued_count(&original.queue).await?, 0);
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_unique_replace_keeps_the_last_payload_of_one_batch() -> TestResult {
    let (storage, envelope) = fixture(JobConflictStrategy::Replace).await?;
    let barrier = Arc::new(tokio::sync::Barrier::new(24));
    let mut tasks = tokio::task::JoinSet::new();
    for producer in 0..24 {
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        let mut first = envelope.clone();
        first.job.args = serde_json::json!({"producer": producer, "last": false});
        let mut last = first.clone();
        last.job.args = serde_json::json!({"producer": producer, "last": true});
        tasks.spawn(async move {
            barrier.wait().await;
            if producer % 2 == 0 {
                storage.enqueue(last).await.map(|id| vec![id])
            } else {
                storage.enqueue_list(vec![first, last]).await
            }
        });
    }
    while let Some(result) = tasks.join_next().await {
        assert!(result??.iter().all(|id| id == &envelope.id));
    }
    assert_eq!(storage.enqueued_count(&envelope.queue).await?, 1);
    let saved = storage.get_job(&envelope.id).await?.unwrap();
    assert_eq!(saved.job.args.get("last"), Some(&serde_json::json!(true)));
    Ok(())
}

#[tokio::test]
async fn replacement_retries_changed_snapshots_without_reordering_the_batch() -> TestResult {
    for bulk in [false, true] {
        let (storage, original) = fixture(JobConflictStrategy::Replace).await?;
        storage.enqueue(original.clone()).await?;
        let proxy = RedisProxy::pause_once(vec!["HMGET".into(), storage.keys.jobs.clone()]).await?;
        let mut through_proxy = storage.clone();
        through_proxy.pool = proxy.pool.clone();
        let ordinary = JobEnvelope::new(random_string(), EnqueueJob)?;
        let mut first = original.clone();
        first.queue = random_string();
        let mut last = first.clone();
        last.queue = random_string();
        // Keep the original JSON bytes, including integers outside Lua's exact range.
        last.job.args = serde_json::json!({"value": u64::MAX});
        let envelopes = if bulk {
            vec![ordinary.clone(), first.clone(), last.clone()]
        } else {
            vec![last.clone()]
        };
        let expected_ids: Vec<_> = envelopes.iter().map(|job| job.id.clone()).collect();
        let replacement = tokio::spawn(async move {
            if bulk {
                through_proxy.enqueue_list(envelopes).await
            } else {
                through_proxy
                    .enqueue(envelopes.into_iter().next().unwrap())
                    .await
                    .map(|id| vec![id])
            }
        });
        tokio::time::timeout(Duration::from_secs(5), proxy.wait_until_paused()).await?;

        let mut concurrent = original.clone();
        concurrent.queue = random_string();
        storage.enqueue(concurrent.clone()).await?;
        proxy.resume();
        assert_eq!(replacement.await??, expected_ids);

        for queue in [&original.queue, &concurrent.queue, &first.queue] {
            assert_eq!(storage.enqueued_count(queue).await?, 0);
        }
        assert_eq!(storage.enqueued_count(&last.queue).await?, 1);
        assert_eq!(
            storage.enqueued_count(&ordinary.queue).await?,
            usize::from(bulk)
        );
        assert_eq!(
            serde_json::to_string(&storage.get_job(&last.id).await?.unwrap())?,
            serde_json::to_string(&last)?
        );
    }
    Ok(())
}

#[tokio::test]
async fn replacement_validates_the_root_queue_across_json_layouts() -> TestResult {
    // Exercise a short suffix, a short prefix, and the full-payload fallback.
    for (pretty, large_state) in [(false, false), (false, true), (true, true)] {
        let (storage, mut original) = fixture(JobConflictStrategy::Replace).await?;
        original.queue = format!("queue-🦀-\"\\\n-{}", original.queue);
        let blob = "x".repeat(256 * 1024);
        original.job.args = serde_json::json!({
            "queue": "nested argument queue",
            "text": ",\"queue\":\"misleading string\",\"meta\":{}}",
            "blob": if large_state { "" } else { &blob }
        });
        original.meta.state = Some(serde_json::json!({
            "queue": "nested state queue",
            "blob": if large_state { &blob } else { "" }
        }));
        storage.enqueue(original.clone()).await?;
        if pretty {
            let mut redis = storage.connection().await?;
            let _: () = redis
                .hset(
                    &storage.keys.jobs,
                    &original.id,
                    serde_json::to_string_pretty(&original)?,
                )
                .await?;
        }

        let proxy = RedisProxy::pause_once(vec!["HMGET".into(), storage.keys.jobs.clone()]).await?;
        let mut through_proxy = storage.clone();
        through_proxy.pool = proxy.pool.clone();
        let mut replacement = original.clone();
        replacement.queue = random_string();
        let incoming = replacement.clone();
        let enqueue = tokio::spawn(async move { through_proxy.enqueue(incoming).await });
        tokio::time::timeout(Duration::from_secs(5), proxy.wait_until_paused()).await?;

        let mut concurrent = original.clone();
        concurrent.queue = random_string();
        storage.enqueue(concurrent.clone()).await?;
        proxy.resume();

        assert_eq!(enqueue.await??, original.id);
        for queue in [&original.queue, &concurrent.queue] {
            assert_eq!(storage.enqueued_count(queue).await?, 0);
        }
        assert_eq!(storage.enqueued_count(&replacement.queue).await?, 1);
        assert_eq!(
            storage.get_job(&original.id).await?.unwrap().queue,
            replacement.queue
        );
    }
    Ok(())
}

#[tokio::test]
async fn replacement_retries_jobs_created_or_finished_after_its_snapshot() -> TestResult {
    for initially_present in [false, true] {
        for scheduled in [false, true] {
            let (storage, original) = fixture(JobConflictStrategy::Replace).await?;
            if initially_present {
                storage.enqueue(original.clone()).await?;
            }
            let proxy =
                RedisProxy::pause_once(vec!["HMGET".into(), storage.keys.jobs.clone()]).await?;
            let mut through_proxy = storage.clone();
            through_proxy.pool = proxy.pool.clone();
            let mut replacement = original.clone();
            replacement.queue = random_string();
            replacement.meta.scheduled_at = chrono::Utc::now().timestamp_micros() + 60_000_000;
            let incoming = replacement.clone();
            let enqueue = tokio::spawn(async move {
                if scheduled {
                    through_proxy.enqueue_at(incoming).await
                } else {
                    through_proxy.enqueue(incoming).await
                }
            });
            tokio::time::timeout(Duration::from_secs(5), proxy.wait_until_paused()).await?;

            if initially_present {
                assert_eq!(
                    storage.dequeue(&original.queue).await?,
                    Some(original.id.clone())
                );
                storage.finish_with_success(&original).await?;
            } else {
                storage.enqueue(original.clone()).await?;
            }
            proxy.resume();

            assert_eq!(enqueue.await??, replacement.id);
            assert_eq!(storage.enqueued_count(&original.queue).await?, 0);
            assert_eq!(
                storage.enqueued_count(&replacement.queue).await?,
                usize::from(!scheduled)
            );
            assert_eq!(storage.scheduled_count().await?, usize::from(scheduled));
            assert_eq!(
                serde_json::to_string(&storage.get_job(&replacement.id).await?.unwrap())?,
                serde_json::to_string(&replacement)?
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn malformed_replacement_snapshot_preserves_the_entire_chunk() -> TestResult {
    for changed_after_read in [false, true] {
        let (storage, original) = fixture(JobConflictStrategy::Replace).await?;
        storage.enqueue(original.clone()).await?;
        let mut redis = storage.connection().await?;
        if !changed_after_read {
            let _: () = redis
                .hset(&storage.keys.jobs, &original.id, "{invalid json")
                .await?;
        }
        let proxy = RedisProxy::pause_once(vec!["HMGET".into(), storage.keys.jobs.clone()]).await?;
        let mut through_proxy = storage.clone();
        through_proxy.pool = proxy.pool.clone();
        let ordinary = JobEnvelope::new(random_string(), EnqueueJob)?;
        let mut replacement = original.clone();
        replacement.queue = random_string();
        let batch = vec![ordinary.clone(), replacement];
        let enqueue = tokio::spawn(async move { through_proxy.enqueue_list(batch).await });
        tokio::time::timeout(Duration::from_secs(5), proxy.wait_until_paused()).await?;
        if changed_after_read {
            let _: () = redis
                .hset(&storage.keys.jobs, &original.id, "{invalid json")
                .await?;
        }
        proxy.resume();

        assert!(enqueue.await?.is_err());
        assert!(storage.get_job(&ordinary.id).await?.is_none());
        assert_eq!(storage.enqueued_count(&ordinary.queue).await?, 0);
        assert_eq!(storage.enqueued_count(&original.queue).await?, 1);
        assert_eq!(
            redis
                .hget::<_, _, String>(&storage.keys.jobs, &original.id)
                .await?,
            "{invalid json"
        );
    }
    Ok(())
}

#[tokio::test]
async fn replacement_uses_preceding_ordinary_write_instead_of_stored_payload() -> TestResult {
    for preceding_skip in [false, true] {
        let (storage, mut original) = fixture(JobConflictStrategy::Skip).await?;
        let mut redis = storage.connection().await?;
        let _: () = redis
            .hset(&storage.keys.jobs, &original.id, "{invalid json")
            .await?;
        let mut batch = Vec::new();
        if preceding_skip {
            batch.push(original.clone());
        }
        original.meta.unique = false;
        batch.push(original.clone());
        let mut replacement = original.clone();
        replacement.meta.unique = true;
        replacement.meta.on_conflict = Some(JobConflictStrategy::Replace);
        replacement.queue = random_string();
        batch.push(replacement.clone());

        storage.enqueue_list(batch).await?;

        assert_eq!(storage.enqueued_count(&original.queue).await?, 0);
        assert_eq!(storage.enqueued_count(&replacement.queue).await?, 1);
        assert_eq!(
            serde_json::to_string(&storage.get_job(&replacement.id).await?.unwrap())?,
            serde_json::to_string(&replacement)?
        );
    }
    Ok(())
}

#[tokio::test]
async fn enqueue_type_errors_preserve_the_entire_chunk() -> TestResult {
    for broken_key in ["destination", "schedule", "retry"] {
        let (storage, original) = fixture(JobConflictStrategy::Replace).await?;
        storage.enqueue(original.clone()).await?;
        let ordinary = JobEnvelope::new(random_string(), EnqueueJob)?;
        let mut replacement = original.clone();
        replacement.queue = random_string();
        let key = match broken_key {
            "schedule" => storage.keys.schedule.clone(),
            "retry" => storage.keys.retry.clone(),
            _ => storage.namespace_queue(&replacement.queue),
        };
        let mut redis = storage.connection().await?;
        let _: () = redis.set(&key, "wrong type").await?;

        assert!(
            storage
                .enqueue_list(vec![ordinary.clone(), replacement])
                .await
                .is_err()
        );

        assert_eq!(storage.enqueued_count(&original.queue).await?, 1);
        assert_eq!(storage.enqueued_count(&ordinary.queue).await?, 0);
        assert!(storage.get_job(&ordinary.id).await?.is_none());
        assert_eq!(redis.get::<_, String>(&key).await?, "wrong type");
        assert_eq!(
            serde_json::to_string(&storage.get_job(&original.id).await?.unwrap())?,
            serde_json::to_string(&original)?
        );
    }
    Ok(())
}

#[tokio::test]
async fn enqueue_rejects_queue_aliases_before_creating_the_jobs_hash() -> TestResult {
    let (storage, mut envelope) = fixture(JobConflictStrategy::Skip).await?;
    envelope.queue.clone_from(&storage.keys.jobs);

    assert!(storage.enqueue(envelope).await.is_err());
    assert_eq!(storage.jobs_count().await?, 0);
    Ok(())
}

#[tokio::test]
async fn retrying_unique_enqueue_after_a_lost_reply_does_not_duplicate_jobs() -> TestResult {
    for strategy in [JobConflictStrategy::Skip, JobConflictStrategy::Replace] {
        let (storage, first) = fixture(strategy.clone()).await?;
        let proxy = RedisProxy::start(enqueue_script_prefix(&storage).await?, true).await?;
        let mut through_proxy = storage.clone();
        through_proxy.pool = proxy.pool.clone();
        let mut last = first.clone();
        last.job.args = serde_json::json!({"last": true});
        let batch = vec![first.clone(), last.clone()];

        assert!(through_proxy.enqueue_list(batch.clone()).await.is_err());
        assert!(proxy.injected());
        assert_eq!(
            storage.enqueue_list(batch).await?,
            vec![first.id.clone(), first.id.clone()]
        );
        assert_eq!(storage.enqueued_count(&first.queue).await?, 1);
        let expected = if strategy == JobConflictStrategy::Skip {
            first
        } else {
            last
        };
        assert_eq!(
            serde_json::to_string(&storage.get_job(&expected.id).await?.unwrap())?,
            serde_json::to_string(&expected)?
        );
    }
    Ok(())
}

#[tokio::test]
async fn replacement_clears_ready_scheduled_and_retry_memberships() -> TestResult {
    let (storage, mut original) = fixture(JobConflictStrategy::Replace).await?;
    // Already-namespaced queue names remain valid cleanup targets.
    original.queue = storage.namespace_queue(&original.queue);
    storage.enqueue(original.clone()).await?;
    let mut redis = storage.connection().await?;
    let _: () = redis.zadd(&storage.keys.retry, &original.id, 1).await?;

    let mut scheduled = original.clone();
    scheduled.queue = random_string();
    scheduled.meta.scheduled_at = chrono::Utc::now().timestamp_micros() + 60_000_000;
    storage.enqueue_at(scheduled.clone()).await?;
    assert_eq!(storage.enqueued_count(&original.queue).await?, 0);
    assert_eq!(storage.enqueued_count(&scheduled.queue).await?, 0);
    assert_eq!(storage.retries_count().await?, 0);
    assert_eq!(storage.scheduled_count().await?, 1);
    assert_eq!(
        redis
            .zscore::<_, _, i64>(&storage.keys.schedule, &original.id)
            .await?,
        scheduled.meta.scheduled_at
    );

    let mut ready = scheduled.clone();
    ready.queue = random_string();
    storage.enqueue(ready.clone()).await?;
    assert_eq!(storage.scheduled_count().await?, 0);
    assert_eq!(storage.enqueued_count(&ready.queue).await?, 1);
    assert_eq!(storage.jobs_count().await?, 1);
    Ok(())
}

#[tokio::test]
async fn unique_skip_preserves_malformed_existing_payloads() -> TestResult {
    for payload in ["", "{invalid json"] {
        let (storage, envelope) = fixture(JobConflictStrategy::Skip).await?;
        let mut redis = storage.connection().await?;
        let _: () = redis
            .hset(&storage.keys.jobs, &envelope.id, payload)
            .await?;

        storage.enqueue(envelope.clone()).await?;

        assert_eq!(
            redis
                .hget::<_, _, String>(&storage.keys.jobs, &envelope.id)
                .await?,
            payload
        );
        assert_eq!(storage.enqueued_count(&envelope.queue).await?, 0);
    }
    Ok(())
}
