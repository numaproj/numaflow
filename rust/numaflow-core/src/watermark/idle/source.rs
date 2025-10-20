//! Source Idle Manager resolves the following conundrums:
//!
//! > How to decide if the source is idling?
//!
//! If the source is not reading any messages for threshold (provided by the user)
//! time, then it is considered idling.
//!
//! > When to publish the idle watermark?
//!
//! If the source is idling and the step interval has passed (also provided by the user).
//!
//! > What to publish as the idle watermark?
//!
//! The current watermark + increment_by (provided by the user). We will ensure that the
//! increment will never cross `(time.now() - max_delay)`.

use chrono::{DateTime, Utc};
use tracing::warn;

use crate::config::pipeline::watermark::IdleConfig;

/// Responsible for detecting the idle state of the source and publishing idle watermarks.
#[derive(Clone)]
pub(crate) struct SourceIdleDetector {
    config: IdleConfig,
    /// last_idle_wm_published_time is for comparing with the step interval
    last_idle_wm_published_time: DateTime<Utc>,
    updated_ts: DateTime<Utc>,
}

impl SourceIdleDetector {
    /// Creates a new instance of SourceIdleManager.
    pub fn new(config: IdleConfig) -> Self {
        let default_time = DateTime::from_timestamp_millis(-1).expect("Invalid timestamp");
        SourceIdleDetector {
            config,
            updated_ts: Utc::now(),
            last_idle_wm_published_time: default_time,
        }
    }

    /// Returns true if the source has been idling and the step interval has passed.
    pub(crate) fn is_source_idling(&self) -> bool {
        self.is_source_idling_internal() && self.has_step_interval_passed()
    }

    /// Checks if the source is idling by comparing the last updated timestamp with the threshold.
    fn is_source_idling_internal(&self) -> bool {
        Utc::now().timestamp_millis() - self.updated_ts.timestamp_millis()
            >= self.config.threshold.as_millis() as i64
    }

    /// Get idle watermark when the source is idling from the start (no messages read/produced ever)
    /// Checks if `init_source_delay` duration has passed before progressing the watermark for the
    /// idling source.
    fn get_source_idling_from_init_wm(&mut self) -> i64 {
        let now = Utc::now();

        let Some(init_source_delay) = self.config.init_source_delay else {
            return -1;
        };

        if now.timestamp_millis() - self.updated_ts.timestamp_millis()
            >= init_source_delay.as_millis() as i64
        {
            self.last_idle_wm_published_time = now;
            now.timestamp_millis()
        } else {
            -1
        }
    }

    /// Verifies if the step interval has passed.
    fn has_step_interval_passed(&self) -> bool {
        self.last_idle_wm_published_time.timestamp_millis() == -1
            || Utc::now().timestamp_millis() - self.last_idle_wm_published_time.timestamp_millis()
                > self.config.step_interval.as_millis() as i64
    }

    /// Resets the updated_ts to the current time.
    pub(crate) fn reset(&mut self) {
        self.updated_ts = Utc::now();
        self.last_idle_wm_published_time =
            DateTime::from_timestamp_millis(-1).expect("Invalid timestamp");
    }

    /// Updates and gets the idle watermark to be published.
    pub(crate) fn update_and_fetch_idle_wm(&mut self, computed_wm: i64) -> i64 {
        let increment_by = self.config.increment_by.as_millis() as i64;
        // check if the computed watermark is -1
        // last computed watermark can be -1, when the pod is restarted or when the processor entity is not created yet.
        if computed_wm == -1 {
            return self.get_source_idling_from_init_wm();
        }

        let mut idle_wm = computed_wm + increment_by;
        // do not assign future timestamps for WM.
        // this could happen if step interval and increment-by are set aggressively
        let now = Utc::now().timestamp_millis();
        if idle_wm > now {
            warn!(
                ?idle_wm,
                "idle config is aggressive (reduce step/increment-by), wm > now(), resetting to now()"
            );
            idle_wm = now;
        }

        self.last_idle_wm_published_time = Utc::now();
        idle_wm
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::pipeline::watermark::IdleConfig;
    use std::time::Duration;

    #[test]
    fn test_is_source_idling() {
        let config = IdleConfig {
            threshold: Duration::from_millis(100),
            step_interval: Duration::from_millis(50),
            increment_by: Duration::from_millis(10),
            init_source_delay: None,
        };
        let mut manager = SourceIdleDetector::new(config);

        // Initially, the source should not be idling
        assert!(!manager.is_source_idling());

        // Simulate the source idling by advancing the updated timestamp
        manager.updated_ts = Utc::now() - chrono::Duration::milliseconds(200);
        assert!(manager.is_source_idling());
    }

    #[test]
    fn test_reset() {
        let config = IdleConfig {
            threshold: Duration::from_millis(100),
            step_interval: Duration::from_millis(50),
            increment_by: Duration::from_millis(10),
            init_source_delay: None,
        };
        let mut manager = SourceIdleDetector::new(config);

        // Simulate the source idling by advancing the updated timestamp
        manager.updated_ts = Utc::now() - chrono::Duration::milliseconds(200);
        assert!(manager.is_source_idling());

        // Reset the manager and check if the source is no longer idling
        manager.reset();
        assert!(!manager.is_source_idling());
    }

    #[test]
    fn test_update_and_fetch_idle_wm() {
        let config = IdleConfig {
            threshold: Duration::from_millis(100),
            step_interval: Duration::from_millis(50),
            increment_by: Duration::from_millis(10),
            init_source_delay: None,
        };
        let mut manager = SourceIdleDetector::new(config);

        // Update and fetch idle watermark with computed_wm = -1
        let idle_wm = manager.update_and_fetch_idle_wm(-1);
        assert_eq!(idle_wm, -1);

        // Update and fetch idle watermark with a valid computed_wm
        let idle_wm = manager.update_and_fetch_idle_wm(1000);
        assert_eq!(idle_wm, 1010);
    }

    #[test]
    fn test_update_and_fetch_idle_wm_with_init_source_delay() {
        let config = IdleConfig {
            threshold: Duration::from_millis(100),
            step_interval: Duration::from_millis(50),
            increment_by: Duration::from_millis(10),
            init_source_delay: Some(Duration::from_millis(100)),
        };
        let mut manager = SourceIdleDetector::new(config.clone());

        // Update and fetch idle watermark with computed_wm = -1
        // Since init_source_delay hasn't passed, idle_wm should be -1
        let idle_wm = manager.update_and_fetch_idle_wm(-1);
        assert_eq!(idle_wm, -1);

        std::thread::sleep(Duration::from_millis(100));

        // Since init_source_delay has passed, idle_wm should be
        // last_idle_wm_published_time + init_source_delay
        let idle_wm = manager.update_and_fetch_idle_wm(-1);
        let expected_wm = manager.last_idle_wm_published_time.timestamp_millis();
        assert_eq!(idle_wm, expected_wm);

        // Update and fetch idle watermark with a valid computed_wm
        let idle_wm = manager.update_and_fetch_idle_wm(1000);
        assert_eq!(idle_wm, 1010);
    }
}
