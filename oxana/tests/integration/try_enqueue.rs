use std::sync::Arc;

use deadpool_redis::redis::AsyncCommands;
use oxana::{JobConflictStrategy, OxanaError, Storage};
use serde::Serialize;
use testresult::TestResult;

use crate::shared::{QueueOne, WorkerNoopJob, random_string, setup};

#[derive(Clone, Serialize)]
struct UniqueJob {
    value: usize,
    conflict: JobConflictStrategy,
}

impl oxana::Job for UniqueJob {
    fn unique_id(&self) -> Option<String> {
        Some("singleton".into())
    }

    fn on_conflict(&self) -> JobConflictStrategy {
        self.conflict.clone()
    }
}

fn storage() -> TestResult<Storage> {
    Ok(Storage::builder()
        .namespace(random_string())
        .build_from_pool(setup())?)
}

#[tokio::test]
async fn ordinary_jobs_always_return_their_new_ids() -> TestResult {
    let storage = storage()?;
    let first = storage
        .try_enqueue(QueueOne, WorkerNoopJob {})
        .await?
        .expect("ordinary job should be inserted");
    let second = storage
        .try_enqueue(QueueOne, WorkerNoopJob {})
        .await?
        .expect("another ordinary job should be inserted");

    assert_ne!(first, second);
    assert!(storage.get_job(&first).await?.is_some());
    assert!(storage.get_job(&second).await?.is_some());
    assert_eq!(storage.enqueued_count(QueueOne).await?, 2);
    assert_eq!(storage.jobs_count().await?, 2);
    Ok(())
}

#[tokio::test]
async fn skip_reports_none_and_preserves_legacy_enqueue_results() -> TestResult {
    let storage = storage()?;
    let mut job = UniqueJob {
        value: 1,
        conflict: JobConflictStrategy::Skip,
    };
    let id = storage
        .try_enqueue(QueueOne, job.clone())
        .await?
        .expect("first unique job should be inserted");

    job.value = 2;
    assert_eq!(storage.try_enqueue(QueueOne, job.clone()).await?, None);
    assert_eq!(storage.enqueue(QueueOne, job.clone()).await?, id);
    assert_eq!(storage.get_job(&id).await?.unwrap().job.args["value"], 1);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);

    storage.delete_job(&id).await?;
    assert_eq!(storage.try_enqueue(QueueOne, job).await?, Some(id.clone()));
    assert_eq!(storage.get_job(&id).await?.unwrap().job.args["value"], 2);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);
    Ok(())
}

#[tokio::test]
async fn replace_reports_some_for_ready_and_scheduled_jobs() -> TestResult {
    for scheduled in [false, true] {
        let storage = storage()?;
        let mut job = UniqueJob {
            value: 1,
            conflict: JobConflictStrategy::Replace,
        };
        let id = if scheduled {
            storage.enqueue_in(QueueOne, job.clone(), 60).await?
        } else {
            storage
                .try_enqueue(QueueOne, job.clone())
                .await?
                .expect("first unique job should be inserted")
        };

        job.value = 2;
        assert_eq!(storage.try_enqueue(QueueOne, job).await?, Some(id.clone()));
        assert_eq!(storage.get_job(&id).await?.unwrap().job.args["value"], 2);
        assert_eq!(storage.enqueued_count(QueueOne).await?, 1);
        assert_eq!(storage.scheduled_count().await?, 0);
        assert_eq!(storage.jobs_count().await?, 1);
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_skip_producers_report_exactly_one_insert() -> TestResult {
    let storage = storage()?;
    let producers = 16;
    let barrier = Arc::new(tokio::sync::Barrier::new(producers));
    let mut tasks = tokio::task::JoinSet::new();
    for value in 0..producers {
        let storage = storage.clone();
        let barrier = Arc::clone(&barrier);
        tasks.spawn(async move {
            barrier.wait().await;
            let result = storage
                .try_enqueue(
                    QueueOne,
                    UniqueJob {
                        value,
                        conflict: JobConflictStrategy::Skip,
                    },
                )
                .await;
            (value, result)
        });
    }

    let mut inserted = Vec::new();
    let mut skipped = 0;
    while let Some(result) = tasks.join_next().await {
        let (value, outcome) = result?;
        match outcome? {
            Some(id) => inserted.push((value, id)),
            None => skipped += 1,
        }
    }
    assert_eq!(inserted.len(), 1);
    assert_eq!(skipped, producers - 1);
    let (value, id) = inserted.pop().unwrap();
    assert_eq!(
        storage.get_job(&id).await?.unwrap().job.args["value"],
        value
    );
    assert_eq!(storage.enqueued_count(QueueOne).await?, 1);
    assert_eq!(storage.jobs_count().await?, 1);
    Ok(())
}

#[tokio::test]
async fn serialization_failure_is_an_error() -> TestResult {
    struct Unserializable;

    impl Serialize for Unserializable {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("cannot serialize job"))
        }
    }

    impl oxana::Job for Unserializable {}

    let storage = storage()?;
    let error = storage
        .try_enqueue(QueueOne, Unserializable)
        .await
        .expect_err("serialization failure must not be reported as a skip");
    assert!(matches!(error, OxanaError::JsonError(_)));
    assert_eq!(storage.jobs_count().await?, 0);
    assert_eq!(storage.enqueued_count(QueueOne).await?, 0);
    Ok(())
}

#[tokio::test]
async fn redis_failure_is_an_error_for_ordinary_and_unique_jobs() -> TestResult {
    let pool = setup();
    let storage = Storage::builder()
        .namespace(random_string())
        .build_from_pool(pool.clone())?;
    let mut redis = pool.get().await?;
    let _: () = redis
        .set(format!("{}:jobs", storage.namespace()), "wrong-type")
        .await?;

    let outcomes = [
        storage.try_enqueue(QueueOne, WorkerNoopJob {}).await,
        storage
            .try_enqueue(
                QueueOne,
                UniqueJob {
                    value: 1,
                    conflict: JobConflictStrategy::Skip,
                },
            )
            .await,
        storage
            .try_enqueue(
                QueueOne,
                UniqueJob {
                    value: 1,
                    conflict: JobConflictStrategy::Replace,
                },
            )
            .await,
    ];
    for outcome in outcomes {
        let Err(OxanaError::DeadpoolRedisError(error)) = outcome else {
            panic!("expected Redis error, got {outcome:?}");
        };
        let errors = error
            .into_server_errors()
            .expect("Redis command errors should be preserved, including pipeline errors");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].1.code(), "WRONGTYPE");
    }
    Ok(())
}
