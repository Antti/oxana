use std::time::Duration;

use crate::OxanaError;

/// Bounded retries for transient Redis pool acquisition failures.
///
/// Retries are disabled unless enabled with [`crate::StorageBuilder::pool_retries`].
/// Queue commands are never replayed by this policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolRetryConfig {
    /// Maximum time for the entire checkout, including its first attempt,
    /// subsequent attempts, and backoff. The pool's own timeouts still apply.
    /// Runtime liveness thresholds must exceed this window plus the heartbeat
    /// interval; configure the same threshold on every consumer of a namespace.
    pub retry_window: Duration,
    /// Initial backoff ceiling, at least one millisecond. Each delay is randomly
    /// selected between half and all of its ceiling.
    pub initial_backoff: Duration,
    /// Maximum backoff ceiling. The ceiling doubles after each retry.
    pub max_backoff: Duration,
}

impl Default for PoolRetryConfig {
    fn default() -> Self {
        Self {
            retry_window: Duration::from_secs(5),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        }
    }
}

impl PoolRetryConfig {
    pub(crate) fn validate(self) -> Result<(), OxanaError> {
        if self.retry_window.is_zero()
            || std::time::Instant::now()
                .checked_add(self.retry_window)
                .is_none()
        {
            return Err(OxanaError::ConfigError(
                "pool retry window must be positive and fit a monotonic deadline".into(),
            ));
        }
        if self.initial_backoff < Duration::from_millis(1)
            || self.max_backoff < self.initial_backoff
            || std::time::Instant::now()
                .checked_add(self.max_backoff)
                .is_none()
        {
            return Err(OxanaError::ConfigError(
                "pool retry backoff must satisfy 1ms <= initial_backoff <= max_backoff and fit a monotonic deadline".into(),
            ));
        }
        Ok(())
    }
}
