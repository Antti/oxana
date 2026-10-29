use std::future::Future;

use crate::{OxanaError, PoolRetryConfig};

pub(super) async fn get(
    pool: &deadpool_redis::Pool,
    config: Option<PoolRetryConfig>,
) -> Result<deadpool_redis::Connection, OxanaError> {
    // Acquiring a connection has not sent the caller's queue command yet.
    retry_pool_acquisition(config, || async {
        pool.get().await.map_err(OxanaError::DeadpoolRedisPoolError)
    })
    .await
}

async fn retry_pool_acquisition<T, F, Fut>(
    config: Option<PoolRetryConfig>,
    mut acquire: F,
) -> Result<T, OxanaError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, OxanaError>>,
{
    let Some(config) = config else {
        return acquire().await;
    };
    let deadline = tokio::time::Instant::now()
        .checked_add(config.retry_window)
        .ok_or_else(|| OxanaError::ConfigError("pool retry deadline is out of range".into()))?;
    let mut result = tokio::time::timeout_at(deadline, acquire())
        .await
        .map_err(|elapsed| {
            OxanaError::DeadpoolRedisError(
                std::io::Error::new(std::io::ErrorKind::TimedOut, elapsed).into(),
            )
        })?;
    let mut backoff = config.initial_backoff;
    loop {
        let error = match result {
            Ok(connection) => return Ok(connection),
            Err(error) if super::transient_redis_error(&error) => error,
            Err(error) => return Err(error),
        };
        let half = backoff / 2;
        let jitter = u32::from(uuid::Uuid::new_v4().as_bytes()[0]);
        // Scale before multiplying to avoid overflowing long backoff durations.
        let delay = half + (backoff - half) / 255 * jitter;
        tracing::warn!(error = %error, retry_in = ?delay, "Retrying Redis pool acquisition");
        result = match tokio::time::timeout_at(deadline, async {
            tokio::time::sleep(delay).await;
            acquire().await
        })
        .await
        {
            Ok(result) => result,
            Err(_) => return Err(error),
        };
        backoff = backoff.saturating_mul(2).min(config.max_backoff);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deadpool_redis::{PoolError, TimeoutType};
    use std::{cell::Cell, collections::VecDeque, future::ready, time::Duration};

    fn timeout(kind: TimeoutType) -> OxanaError {
        OxanaError::DeadpoolRedisPoolError(PoolError::Timeout(kind))
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_retries_return_the_first_transient_failure() {
        let start = tokio::time::Instant::now();
        let calls = Cell::new(0);
        let result = retry_pool_acquisition::<(), _, _>(None, || {
            calls.set(calls.get() + 1);
            ready(Err(timeout(TimeoutType::Create)))
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.get(), 1);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn custom_window_includes_the_initial_attempt_and_backoff() {
        let config = PoolRetryConfig {
            retry_window: Duration::from_millis(2300),
            initial_backoff: Duration::from_millis(400),
            max_backoff: Duration::from_millis(500),
        };
        let start = tokio::time::Instant::now();
        let attempts = std::cell::RefCell::new(Vec::new());
        let result = retry_pool_acquisition::<(), _, _>(Some(config), || {
            attempts.borrow_mut().push(tokio::time::Instant::now());
            let first = attempts.borrow().len() == 1;
            async move {
                if first {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(timeout(TimeoutType::Wait))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(start.elapsed(), config.retry_window);
        let attempts = attempts.into_inner();
        assert!(attempts.len() >= 3);
        let mut previous = start + Duration::from_secs(1);
        let mut ceiling = config.initial_backoff;
        for attempt in attempts.into_iter().skip(1) {
            let delay = attempt - previous;
            assert!(delay >= ceiling / 2);
            // Tokio's timer rounds deadlines up to millisecond precision.
            assert!(delay <= ceiling + Duration::from_millis(1));
            previous = attempt;
            ceiling = ceiling.saturating_mul(2).min(config.max_backoff);
        }
    }

    #[test]
    fn retry_policy_applies_to_all_builders_and_new_processes() -> testresult::TestResult {
        let config = PoolRetryConfig {
            retry_window: Duration::from_secs(2),
            ..PoolRetryConfig::default()
        };
        let url = "redis://127.0.0.1/0";
        let pool = deadpool_redis::Config::from_url(url)
            .create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
        let builder = || crate::Storage::builder().pool_retries(Some(config));
        for storage in [
            builder().build_from_redis_url(url)?,
            builder().build_from_redis_urls(url, url)?,
            builder().build_from_pool(pool.clone())?,
            builder().build_from_pools(pool.clone(), pool.clone())?,
        ] {
            assert_eq!(storage.internal.pool_retries, Some(config));
            assert_eq!(
                storage.internal.for_new_process().pool_retries,
                Some(config)
            );
        }
        let default = crate::Storage::builder().build_from_pool(pool.clone())?;
        assert_eq!(default.internal.pool_retries, None);
        let disabled = builder().pool_retries(None).build_from_pool(pool)?;
        assert_eq!(disabled.internal.pool_retries, None);
        Ok(())
    }

    #[test]
    fn invalid_retry_policy_is_rejected_by_every_builder() -> testresult::TestResult {
        let url = "redis://127.0.0.1/0";
        let pool = deadpool_redis::Config::from_url(url)
            .create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
        for config in [
            PoolRetryConfig {
                retry_window: Duration::ZERO,
                ..PoolRetryConfig::default()
            },
            PoolRetryConfig {
                retry_window: Duration::MAX,
                ..PoolRetryConfig::default()
            },
            PoolRetryConfig {
                initial_backoff: Duration::ZERO,
                ..PoolRetryConfig::default()
            },
            PoolRetryConfig {
                max_backoff: Duration::from_millis(50),
                ..PoolRetryConfig::default()
            },
            PoolRetryConfig {
                max_backoff: Duration::MAX,
                ..PoolRetryConfig::default()
            },
        ] {
            let builder = || crate::Storage::builder().pool_retries(Some(config));
            for result in [
                builder().build_from_redis_url(url),
                builder().build_from_redis_urls(url, url),
                builder().build_from_pool(pool.clone()),
                builder().build_from_pools(pool.clone(), pool.clone()),
            ] {
                assert!(matches!(result, Err(OxanaError::ConfigError(_))));
            }
        }
        Ok(())
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
            retry_pool_acquisition(Some(PoolRetryConfig::default()), || ready(
                attempts.pop_front().unwrap()
            ))
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
        retry_pool_acquisition(Some(PoolRetryConfig::default()), || {
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
        let result = retry_pool_acquisition::<(), _, _>(Some(PoolRetryConfig::default()), || {
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
        let result = retry_pool_acquisition::<(), _, _>(Some(PoolRetryConfig::default()), || {
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
        assert_eq!(start.elapsed(), PoolRetryConfig::default().retry_window);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_budget_also_bounds_an_unresponsive_checkout() {
        let start = tokio::time::Instant::now();
        let calls = Cell::new(0);
        let result = retry_pool_acquisition::<(), _, _>(Some(PoolRetryConfig::default()), || {
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
        assert_eq!(start.elapsed(), PoolRetryConfig::default().retry_window);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_budget_bounds_an_unresponsive_first_checkout() {
        let start = tokio::time::Instant::now();
        let result = retry_pool_acquisition::<(), _, _>(
            Some(PoolRetryConfig::default()),
            std::future::pending,
        )
        .await;
        assert!(matches!(result, Err(OxanaError::DeadpoolRedisError(error)) if error.is_timeout()));
        assert_eq!(start.elapsed(), PoolRetryConfig::default().retry_window);
    }
}
