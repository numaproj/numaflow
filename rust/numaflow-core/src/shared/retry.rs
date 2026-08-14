//! Shared retry machinery for UDF components (transformer, map) that honor a user-configured
//! `retryStrategy` when a message is explicitly failed (via the reserved FAIL tag).

use backoff::strategy::exponential::Exponential;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::config::components::sink::{OnFailureStrategy, RetryConfig};

/// The action a caller must take after the UDF reported a failed message and the controller
/// advanced one tick via [`RetryController::next_step`].
pub(crate) enum RetryStep {
    /// Retry the UDF again — either the backoff interval has already been waited out, or no retry
    /// strategy is configured (retry indefinitely with no delay, matching the sink).
    Again,
    /// Shutdown fired while waiting out the backoff interval; the caller should give up (nack).
    Cancelled,
    /// Retries were exhausted under `onFailure: drop` — the caller should drop the message
    Drop,
    /// Retries were exhausted under `onFailure: retry` — the caller should nack / propagate.
    Nack,
}

/// Drives the retry lifecycle for a single failed-message stream: owns the bounded exponential
/// backoff iterator, the attempt counter, and the configured on-failure strategy.
///
/// Construct once per unit of retry — per message for unary/stream/transformer, per batch for
/// batch map. `Clone` copies the current backoff state; the transformer clones an *un-advanced*
/// template so each per-message task (and each redrive) gets its own fresh iterator.
#[derive(Clone)]
pub(crate) struct RetryController {
    strategy: Option<OnFailureStrategy>,
    backoff: Option<Exponential>,
    attempt: u64,
}

impl RetryController {
    /// Builds a controller from the optional retry config. When `retry_config` is `None`, the
    /// controller retries forever with no backoff (matching sink behavior).
    pub(crate) fn new(retry_config: &Option<RetryConfig>) -> Self {
        let strategy = retry_config
            .as_ref()
            .map(|rc| rc.sink_retry_on_fail_strategy.clone());
        let backoff = retry_config.as_ref().map(|rc| {
            Exponential::from_millis(
                rc.sink_initial_retry_interval_in_ms,
                rc.sink_max_retry_interval_in_ms,
                rc.sink_retry_factor,
                rc.sink_retry_jitter,
                Some(rc.sink_max_retry_attempts),
            )
        });
        Self {
            strategy,
            backoff,
            attempt: 0,
        }
    }

    /// Advances one retry tick after a message failed.
    ///
    /// When a backoff is configured, waits out the next interval (aborting promptly if `token` is
    /// cancelled, so a long interval cannot stall the graceful-shutdown window) and returns
    /// [`RetryStep::Again`]; once the bounded backoff is exhausted, returns the terminal action
    /// dictated by the configured strategy. When no retry strategy is configured, returns
    /// [`RetryStep::Again`] immediately — infinite retry with no delay.
    pub(crate) async fn next_step(&mut self, token: &CancellationToken) -> RetryStep {
        // No retry strategy configured: retry forever with no delay, matching the sink.
        let Some(backoff) = self.backoff.as_mut() else {
            return RetryStep::Again;
        };

        match backoff.next() {
            Some(delay) => {
                self.attempt += 1;
                warn!(retry_attempt = self.attempt, "Retrying UDF after failure");
                tokio::select! {
                    _ = tokio::time::sleep(delay) => RetryStep::Again,
                    _ = token.cancelled() => RetryStep::Cancelled,
                }
            }
            // Retries exhausted: dispatch on the configured strategy.
            None => match self.strategy {
                // The backoff iterator is only constructed when a strategy is configured, so a
                // missing strategy here is unreachable.
                None => unreachable!(
                    "Retry strategy missing at runtime when it was initially configured"
                ),
                // Fallback is invalid for these components and is rejected during spec validation;
                // panic if it somehow reaches runtime.
                Some(OnFailureStrategy::Fallback) => {
                    panic!("Invalid fallback failure strategy configuration detected at runtime")
                }
                Some(OnFailureStrategy::Drop) => RetryStep::Drop,
                Some(OnFailureStrategy::Retry) => RetryStep::Nack,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(strategy: OnFailureStrategy, max_attempts: u16, interval_ms: u32) -> RetryConfig {
        RetryConfig {
            sink_max_retry_attempts: max_attempts,
            sink_initial_retry_interval_in_ms: interval_ms,
            sink_retry_factor: 1.0,
            sink_retry_jitter: 0.0,
            sink_max_retry_interval_in_ms: interval_ms,
            sink_retry_on_fail_strategy: strategy,
        }
    }

    #[tokio::test]
    async fn no_config_retries_forever_without_delay() {
        let mut rc = RetryController::new(&None);
        let token = CancellationToken::new();
        // No backoff, no terminal state — always Again.
        for _ in 0..5 {
            assert!(matches!(rc.next_step(&token).await, RetryStep::Again));
        }
    }

    #[tokio::test]
    async fn drop_strategy_yields_drop_after_exhaustion() {
        let mut rc = RetryController::new(&Some(cfg(OnFailureStrategy::Drop, 2, 1)));
        let token = CancellationToken::new();
        assert!(matches!(rc.next_step(&token).await, RetryStep::Again));
        assert!(matches!(rc.next_step(&token).await, RetryStep::Again));
        assert!(matches!(rc.next_step(&token).await, RetryStep::Drop));
    }

    #[tokio::test]
    async fn retry_strategy_yields_nack_after_exhaustion() {
        let mut rc = RetryController::new(&Some(cfg(OnFailureStrategy::Retry, 1, 1)));
        let token = CancellationToken::new();
        assert!(matches!(rc.next_step(&token).await, RetryStep::Again));
        assert!(matches!(rc.next_step(&token).await, RetryStep::Nack));
    }

    #[tokio::test]
    async fn cancellation_during_backoff_yields_cancelled() {
        // Long interval so the cancelled branch wins the select deterministically.
        let mut rc = RetryController::new(&Some(cfg(OnFailureStrategy::Retry, 5, 60_000)));
        let token = CancellationToken::new();
        token.cancel();
        assert!(matches!(rc.next_step(&token).await, RetryStep::Cancelled));
    }

    #[tokio::test]
    async fn clones_of_an_unadvanced_template_are_independent() {
        // Mirrors how the transformer fans a template out across per-message tasks: each clone of
        // an un-advanced controller gets its own fresh iterator.
        let template = RetryController::new(&Some(cfg(OnFailureStrategy::Drop, 1, 1)));
        let token = CancellationToken::new();

        let mut a = template.clone();
        assert!(matches!(a.next_step(&token).await, RetryStep::Again));
        assert!(matches!(a.next_step(&token).await, RetryStep::Drop));

        // `b` is unaffected by `a` having been advanced to exhaustion.
        let mut b = template.clone();
        assert!(matches!(b.next_step(&token).await, RetryStep::Again));
        assert!(matches!(b.next_step(&token).await, RetryStep::Drop));
    }
}
