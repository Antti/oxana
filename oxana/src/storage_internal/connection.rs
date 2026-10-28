use std::{future::Future, time::Duration};

use crate::OxanaError;

const RETRY_WINDOW: Duration = Duration::from_secs(5);

pub(super) async fn get(
    pool: &deadpool_redis::Pool,
) -> Result<deadpool_redis::Connection, OxanaError> {
    // Acquiring a connection has not sent the caller's queue command yet.
    retry_pool_acquisition(|| async {
        pool.get().await.map_err(OxanaError::DeadpoolRedisPoolError)
    })
    .await
}

async fn retry_pool_acquisition<T, F, Fut>(mut acquire: F) -> Result<T, OxanaError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, OxanaError>>,
{
    let mut result = acquire().await;
    let deadline = tokio::time::Instant::now() + RETRY_WINDOW;
    let mut backoff_ms = 100_u64;
    loop {
        let error = match result {
            Ok(connection) => return Ok(connection),
            Err(error) if super::transient_redis_error(&error) => error,
            Err(error) => return Err(error),
        };
        let half = backoff_ms / 2;
        let delay_ms = half + uuid::Uuid::new_v4().as_u64_pair().1 % (half + 1);
        tracing::warn!(error = %error, retry_in_ms = delay_ms, "Retrying Redis pool acquisition");
        result = match tokio::time::timeout_at(deadline, async {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            acquire().await
        })
        .await
        {
            Ok(result) => result,
            Err(_) => return Err(error),
        };
        backoff_ms = (backoff_ms * 2).min(1000);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deadpool_redis::{PoolError, TimeoutType};
    use std::{cell::Cell, collections::VecDeque, future::ready};

    fn timeout(kind: TimeoutType) -> OxanaError {
        OxanaError::DeadpoolRedisPoolError(PoolError::Timeout(kind))
    }

    #[tokio::test(start_paused = true)]
    async fn recovers_from_pool_timeouts_and_connection_errors() {
        let mut attempts = VecDeque::from([
            Err(timeout(TimeoutType::Create)),
            Err(timeout(TimeoutType::Wait)),
            Err(timeout(TimeoutType::Recycle)),
            Err(OxanaError::DeadpoolRedisPoolError(PoolError::Backend(
                std::io::Error::from(std::io::ErrorKind::ConnectionReset).into(),
            ))),
            Ok(42),
        ]);
        assert_eq!(
            retry_pool_acquisition(|| ready(attempts.pop_front().unwrap()))
                .await
                .unwrap(),
            42
        );
        assert!(attempts.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn successful_checkout_does_not_wait_or_retry() {
        let start = tokio::time::Instant::now();
        let calls = Cell::new(0);
        retry_pool_acquisition(|| {
            calls.set(calls.get() + 1);
            ready(Ok(()))
        })
        .await
        .unwrap();
        assert_eq!(calls.get(), 1);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn closed_pool_fails_immediately() {
        let start = tokio::time::Instant::now();
        let result = retry_pool_acquisition::<(), _, _>(|| {
            ready(Err(OxanaError::DeadpoolRedisPoolError(PoolError::Closed)))
        })
        .await;
        assert!(matches!(
            result,
            Err(OxanaError::DeadpoolRedisPoolError(PoolError::Closed))
        ));
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn persistent_failure_exhausts_the_retry_budget() {
        let start = tokio::time::Instant::now();
        let calls = Cell::new(0);
        let result = retry_pool_acquisition::<(), _, _>(|| {
            calls.set(calls.get() + 1);
            ready(Err(timeout(TimeoutType::Create)))
        })
        .await;
        assert!(matches!(
            result,
            Err(OxanaError::DeadpoolRedisPoolError(PoolError::Timeout(
                TimeoutType::Create
            )))
        ));
        assert!(calls.get() > 1);
        assert_eq!(start.elapsed(), RETRY_WINDOW);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_budget_also_bounds_an_unresponsive_checkout() {
        let start = tokio::time::Instant::now();
        let calls = Cell::new(0);
        let result = retry_pool_acquisition::<(), _, _>(|| {
            calls.set(calls.get() + 1);
            let attempt = calls.get();
            async move {
                if attempt > 1 {
                    std::future::pending::<()>().await;
                }
                Err(timeout(TimeoutType::Create))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.get(), 2);
        assert_eq!(start.elapsed(), RETRY_WINDOW);
    }
}
