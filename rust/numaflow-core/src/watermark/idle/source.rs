use crate::config::pipeline::watermark::IdleConfig;
use chrono::{DateTime, Utc};

/// Used to determine if the source is idling if idle configuration is provided.
pub(crate) struct SourceIdleManager {
    config: IdleConfig,
    last_published_idle_wm: DateTime<Utc>,
    last_idle_wm_published_time: DateTime<Utc>,
    updated_ts: DateTime<Utc>,
}

impl SourceIdleManager {
    /// Creates a new instance of SourceIdleManager.
    pub fn new(config: IdleConfig) -> Self {
        let default_time = DateTime::from_timestamp_millis(-1).expect("Invalid timestamp");
        SourceIdleManager {
            config,
            updated_ts: Utc::now(),
            last_idle_wm_published_time: default_time,
            last_published_idle_wm: default_time,
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
        let mut idle_wm = if computed_wm == -1 {
            self.last_published_idle_wm.timestamp_millis() + increment_by
        } else {
            computed_wm + increment_by
        };

        let now = Utc::now().timestamp_millis();
        if idle_wm > now {
            idle_wm = now;
        }

        self.last_published_idle_wm =
            DateTime::from_timestamp_millis(idle_wm).expect("Invalid timestamp");
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
        };
        let mut manager = SourceIdleManager::new(config);

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
        let mut manager = SourceIdleManager::new(config);

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
        let mut manager = SourceIdleManager::new(config);

        // Update and fetch idle watermark with computed_wm = -1
        let idle_wm = manager.update_and_fetch_idle_wm(-1);
        assert!(idle_wm > 0);

        // Update and fetch idle watermark with a valid computed_wm
        let idle_wm = manager.update_and_fetch_idle_wm(1000);
        assert_eq!(idle_wm, 1010);
    }
}
