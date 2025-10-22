use std::time::Duration;

use rand::Rng;

/// An Exponential Backoff strategy that increases the delay exponentially with each retry.
///
/// The delay is calculated as: `base_interval * factor^(attempt - 1)`
/// with optional jitter applied.
///
/// # Example
/// ```
/// use backoff::strategy::exponential::Exponential;
/// use std::time::Duration;
///
/// let mut backoff = Exponential::new(
///     Duration::from_millis(100),  // base_interval
///     Duration::from_secs(10),     // max_interval
///     2.0,                         // factor
///     0.5,                         // jitter
///     Some(5),                     // max_attempts
/// );
///
/// // First retry: ~100ms (with jitter)
/// let delay1 = backoff.next();
/// // Second retry: ~200ms (with jitter)
/// let delay2 = backoff.next();
/// // Third retry: ~400ms (with jitter)
/// let delay3 = backoff.next();
/// ```
pub struct Exponential {
    /// The base retry interval (starting point).
    base_interval: Duration,
    /// The maximum retry interval (cap).
    max_interval: Duration,
    /// The factor to multiply the interval with for each retry attempt.
    factor: f64,
    /// Jitter value between 0.0 and 1.0 for randomization.
    jitter: f64,
    /// The maximum number of retry attempts. If None, retries indefinitely.
    max_attempts: Option<u16>,
    /// The current retry attempt count (tracks state across next() calls).
    current_attempt: u16,
}

impl Exponential {
    /// Creates a new Exponential backoff strategy.
    ///
    /// # Arguments
    /// * `base_interval` - The initial retry interval
    /// * `max_interval` - The maximum retry interval (cap)
    /// * `factor` - The multiplier for exponential growth
    /// * `jitter` - Randomization factor (0.0 to 1.0)
    /// * `max_attempts` - Maximum number of retry attempts (None for unlimited)
    pub fn new(
        base_interval: Duration,
        max_interval: Duration,
        factor: f64,
        jitter: f64,
        max_attempts: Option<u16>,
    ) -> Self {
        Self {
            base_interval,
            max_interval,
            factor,
            jitter,
            max_attempts,
            current_attempt: 0,
        }
    }

    /// Creates an Exponential backoff from milliseconds.
    pub fn from_millis(
        base_interval_ms: u32,
        max_interval_ms: u32,
        factor: f64,
        jitter: f64,
        max_attempts: Option<u16>,
    ) -> Self {
        Self::new(
            Duration::from_millis(base_interval_ms as u64),
            Duration::from_millis(max_interval_ms as u64),
            factor,
            jitter,
            max_attempts,
        )
    }

    /// Resets the backoff to its initial state.
    /// This resets the attempt counter back to 0.
    pub fn reset(&mut self) {
        self.current_attempt = 0;
    }

    /// Returns the current attempt count.
    /// This is useful for tracking how many retries have been attempted.
    pub fn current_attempt(&self) -> u16 {
        self.current_attempt
    }

    /// Calculates exponential backoff delay with optional jitter.
    /// Returns the computed delay (in milliseconds) capped by max retry interval.
    fn calculate_delay(&self, attempt: u16) -> Duration {
        // Calculate the base delay using the base interval and the factor.
        // The base delay is calculated as: base_interval * factor^(attempt - 1)
        //
        // NOTE: clamp attempt to at least 1 to avoid powi(-1) panic when attempt = 0.
        let safe_attempt = attempt.max(1);
        let base_delay_ms =
            (self.base_interval.as_millis() as f64) * self.factor.powi((safe_attempt - 1) as i32);

        // If jitter is 0, skip randomization and cap to max interval.
        if self.jitter == 0.0 {
            return Duration::from_millis(base_delay_ms as u64).min(self.max_interval);
        }

        // Apply jitter to the base delay.
        // jitter is a value between 0 and 1
        // 1.0 - jitter gives us the lower bound and 1.0 + jitter gives us the upper bound
        let jitter_factor: f64 = rand::rng().random_range(1.0 - self.jitter..=1.0 + self.jitter);
        let delay_ms = base_delay_ms * jitter_factor;

        // Always cap the delay to the configured maximum retry interval.
        Duration::from_millis(delay_ms as u64).min(self.max_interval)
    }
}

impl Iterator for Exponential {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        // Check if we've exceeded max attempts
        if let Some(max_attempts) = self.max_attempts
            && self.current_attempt >= max_attempts
        {
            return None;
        }

        self.current_attempt += 1;
        Some(self.calculate_delay(self.current_attempt))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let backoff = Exponential::new(
            Duration::from_millis(100),
            Duration::from_secs(10),
            2.0,
            0.5,
            Some(5),
        );
        assert_eq!(backoff.base_interval, Duration::from_millis(100));
        assert_eq!(backoff.max_interval, Duration::from_secs(10));
        assert_eq!(backoff.factor, 2.0);
        assert_eq!(backoff.jitter, 0.5);
        assert_eq!(backoff.max_attempts, Some(5));
    }

    #[test]
    fn test_from_millis() {
        let backoff = Exponential::from_millis(100, 10000, 2.0, 0.0, Some(3));
        assert_eq!(backoff.base_interval, Duration::from_millis(100));
        assert_eq!(backoff.max_interval, Duration::from_millis(10000));
        assert_eq!(backoff.factor, 2.0);
        assert_eq!(backoff.jitter, 0.0);
        assert_eq!(backoff.max_attempts, Some(3));
    }

    #[test]
    fn test_exponential_growth_no_jitter() {
        let mut backoff = Exponential::from_millis(100, 10000, 2.0, 0.0, None);

        // First retry: 100ms
        assert_eq!(backoff.next(), Some(Duration::from_millis(100)));
        // Second retry: 200ms
        assert_eq!(backoff.next(), Some(Duration::from_millis(200)));
        // Third retry: 400ms
        assert_eq!(backoff.next(), Some(Duration::from_millis(400)));
        // Fourth retry: 800ms
        assert_eq!(backoff.next(), Some(Duration::from_millis(800)));
    }

    #[test]
    fn test_max_interval_cap() {
        let mut backoff = Exponential::from_millis(100, 300, 2.0, 0.0, None);

        assert_eq!(backoff.next(), Some(Duration::from_millis(100)));
        assert_eq!(backoff.next(), Some(Duration::from_millis(200)));
        // Should be capped at 300ms
        assert_eq!(backoff.next(), Some(Duration::from_millis(300)));
        assert_eq!(backoff.next(), Some(Duration::from_millis(300)));
    }

    #[test]
    fn test_max_attempts() {
        let mut backoff = Exponential::from_millis(100, 10000, 2.0, 0.0, Some(3));

        assert!(backoff.next().is_some());
        assert!(backoff.next().is_some());
        assert!(backoff.next().is_some());
        // Fourth attempt should return None
        assert_eq!(backoff.next(), None);
    }

    #[test]
    fn test_reset() {
        let mut backoff = Exponential::from_millis(100, 10000, 2.0, 0.0, None);

        assert_eq!(backoff.next(), Some(Duration::from_millis(100)));
        assert_eq!(backoff.next(), Some(Duration::from_millis(200)));

        backoff.reset();

        // Should start from base interval again
        assert_eq!(backoff.next(), Some(Duration::from_millis(100)));
        assert_eq!(backoff.next(), Some(Duration::from_millis(200)));
    }

    #[test]
    fn test_jitter_applied() {
        let mut backoff = Exponential::from_millis(100, 10000, 2.0, 0.5, None);

        // With 50% jitter, the delay should be in range [50ms, 150ms]
        let delay = backoff.next().unwrap();
        assert!(delay >= Duration::from_millis(50));
        assert!(delay <= Duration::from_millis(150));
    }

    #[test]
    fn test_attempt_tracking() {
        let mut backoff = Exponential::from_millis(100, 10000, 2.0, 0.0, None);

        assert_eq!(backoff.current_attempt(), 0);

        backoff.next();
        assert_eq!(backoff.current_attempt(), 1);

        backoff.next();
        assert_eq!(backoff.current_attempt(), 2);

        backoff.next();
        assert_eq!(backoff.current_attempt(), 3);

        backoff.reset();
        assert_eq!(backoff.current_attempt(), 0);

        backoff.next();
        assert_eq!(backoff.current_attempt(), 1);
    }
}
