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
use tracing::{info, warn};

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
        let now = Utc::now();

        info!(
            threshold_ms = config.threshold.as_millis(),
            step_interval_ms = config.step_interval.as_millis(),
            increment_by_ms = config.increment_by.as_millis(),
            created_at = %now,
            "SourceIdleDetector created with configuration"
        );

        SourceIdleDetector {
            config,
            updated_ts: now,
            last_idle_wm_published_time: default_time,
        }
    }

    /// Returns true if the source has been idling and the step interval has passed.
    pub(crate) fn is_source_idling(&self) -> bool {
        let is_idling_internal = self.is_source_idling_internal();
        let step_interval_passed = self.has_step_interval_passed();
        let result = is_idling_internal && step_interval_passed;

        info!(
            is_idling_internal,
            step_interval_passed,
            result,
            updated_ts = %self.updated_ts,
            last_idle_wm_published_time = %self.last_idle_wm_published_time,
            "Source idling check completed"
        );

        result
    }

    /// Checks if the source is idling by comparing the last updated timestamp with the threshold.
    fn is_source_idling_internal(&self) -> bool {
        let now = Utc::now().timestamp_millis();
        let updated_ts = self.updated_ts.timestamp_millis();
        let time_since_update = now - updated_ts;
        let threshold_ms = self.config.threshold.as_millis() as i64;
        let is_idling = time_since_update >= threshold_ms;

        info!(
            now_ms = now,
            updated_ts_ms = updated_ts,
            time_since_update_ms = time_since_update,
            threshold_ms,
            is_idling,
            "Source idling internal check"
        );

        is_idling
    }

    /// Verifies if the step interval has passed.
    fn has_step_interval_passed(&self) -> bool {
        let now = Utc::now().timestamp_millis();
        let last_published = self.last_idle_wm_published_time.timestamp_millis();
        let step_interval_ms = self.config.step_interval.as_millis() as i64;

        let is_first_time = last_published == -1;
        let time_since_last_publish = if is_first_time {
            0
        } else {
            now - last_published
        };
        let step_interval_passed = is_first_time || time_since_last_publish > step_interval_ms;

        info!(
            now_ms = now,
            last_published_ms = last_published,
            is_first_time,
            time_since_last_publish_ms = time_since_last_publish,
            step_interval_ms,
            step_interval_passed,
            "Step interval check"
        );

        step_interval_passed
    }

    /// Resets the updated_ts to the current time.
    pub(crate) fn reset(&mut self) {
        let now = Utc::now();
        let previous_updated_ts = self.updated_ts;
        let previous_last_published = self.last_idle_wm_published_time;

        self.updated_ts = now;
        self.last_idle_wm_published_time =
            DateTime::from_timestamp_millis(-1).expect("Invalid timestamp");

        info!(
            previous_updated_ts = %previous_updated_ts,
            previous_last_published = %previous_last_published,
            new_updated_ts = %now,
            "SourceIdleDetector reset - source is now active"
        );
    }

    /// Updates and gets the idle watermark to be published.
    pub(crate) fn update_and_fetch_idle_wm(&mut self, computed_wm: i64) -> i64 {
        let increment_by = self.config.increment_by.as_millis() as i64;

        info!(
            computed_wm,
            increment_by_ms = increment_by,
            "Starting idle watermark calculation"
        );

        // check if the computed watermark is -1
        // last computed watermark can be -1, when the pod is restarted or when the processor entity is not created yet.
        if computed_wm == -1 {
            info!("Computed watermark is -1, returning -1 for idle watermark");
            return -1;
        }

        let mut idle_wm = computed_wm + increment_by;
        let now = Utc::now().timestamp_millis();
        let was_clamped = idle_wm > now;

        // do not assign future timestamps for WM.
        // this could happen if step interval and increment-by are set aggressively
        if idle_wm > now {
            warn!(
                ?idle_wm,
                "idle config is aggressive (reduce step/increment-by), wm > now(), resetting to now()"
            );
            idle_wm = now;
        }

        let publish_time = Utc::now();
        self.last_idle_wm_published_time = publish_time;

        info!(
            computed_wm,
            increment_by_ms = increment_by,
            calculated_idle_wm = computed_wm + increment_by,
            final_idle_wm = idle_wm,
            now_ms = now,
            was_clamped,
            published_at = %publish_time,
            "Idle watermark calculation completed"
        );

        idle_wm
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::pipeline::watermark::IdleConfig;

    #[test]
    fn test_is_source_idling() {
        let config = IdleConfig {
            threshold: Duration::from_millis(100),
            step_interval: Duration::from_millis(50),
            increment_by: Duration::from_millis(10),
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
        };
        let mut manager = SourceIdleDetector::new(config);

        // Update and fetch idle watermark with computed_wm = -1
        let idle_wm = manager.update_and_fetch_idle_wm(-1);
        assert_eq!(idle_wm, -1);

        // Update and fetch idle watermark with a valid computed_wm
        let idle_wm = manager.update_and_fetch_idle_wm(1000);
        assert_eq!(idle_wm, 1010);
    }
}
