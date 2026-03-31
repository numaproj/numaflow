//! Source Idle Manager resolves the following conundrums:
//!
//! > How to decide if a partition is idling?
//!
//! If a partition is not reading any messages for threshold (provided by the user)
//! time, then it is considered idling. Each partition is tracked independently.
//!
//! > When to publish the watermark?
//!
//! Watermarks serve as heartbeats for downstream vertices via the KV entry timestamps.
//! We publish watermarks when the step interval has passed. The watermark value depends
//! on whether the partition is truly idle (increment the value) or just needs a heartbeat
//! (use current value).
//!
//! > What to publish as the watermark?
//!
//! If partition is idle: current watermark + increment_by (provided by the user). We ensure
//! the increment never crosses `(time.now() - max_delay)`.
//! If partition is not idle but step interval passed: current watermark (no increment).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use tracing::warn;

use crate::config::pipeline::watermark::IdleConfig;

/// Per-partition idle state tracking.
#[derive(Clone)]
struct PartitionIdleState {
    /// last_wm_published_time tracks when we last published a watermark (for step interval check)
    last_wm_published_time: DateTime<Utc>,
    /// updated_ts tracks when we last received data (for idle threshold check)
    updated_ts: DateTime<Utc>,
}

impl PartitionIdleState {
    fn new() -> Self {
        let default_time = DateTime::from_timestamp_millis(-1).expect("Invalid timestamp");
        Self {
            updated_ts: Utc::now(),
            last_wm_published_time: default_time,
        }
    }
}

/// Responsible for detecting the idle state of source partitions and managing watermark publishing.
/// Watermarks serve as heartbeats for downstream vertices - the KV entry timestamp signals liveness.
/// Each partition is tracked independently for idle detection.
#[derive(Clone)]
pub(crate) struct SourceIdleDetector {
    config: IdleConfig,
    /// Per-partition idle state tracking
    partition_states: HashMap<u16, PartitionIdleState>,
}

impl SourceIdleDetector {
    /// Creates a new instance of SourceIdleDetector.
    pub fn new(config: IdleConfig) -> Self {
        SourceIdleDetector {
            config,
            partition_states: HashMap::new(),
        }
    }

    /// Ensures a partition has an idle state entry, creating one if needed.
    fn ensure_partition(&mut self, partition: u16) {
        self.partition_states
            .entry(partition)
            .or_insert_with(PartitionIdleState::new);
    }

    /// Returns partitions that need watermark publishing (step interval has passed).
    /// Watermarks are published when the step interval has passed - they serve as heartbeats
    /// for downstream vertices via the KV entry timestamps.
    pub(crate) fn partitions_needing_publish(&self) -> Vec<u16> {
        self.partition_states
            .iter()
            .filter(|(_, state)| self.has_step_interval_passed(state))
            .map(|(partition, _)| *partition)
            .collect()
    }

    /// Returns true if the partition is truly idle (no data for threshold duration).
    /// When idle, we increment the watermark value. When not idle, we publish current value.
    ///
    /// Note: If threshold is 0, the partition is never considered idle (user hasn't configured
    /// idle detection). We still publish heartbeats, but don't mark as idle or increment watermark.
    pub(crate) fn is_partition_idle(&self, partition: u16) -> bool {
        // If threshold is 0, never mark as idle (user hasn't configured idle detection)
        if self.config.threshold.is_zero() {
            return false;
        }

        self.partition_states
            .get(&partition)
            .map(|state| {
                Utc::now().timestamp_millis() - state.updated_ts.timestamp_millis()
                    >= self.config.threshold.as_millis() as i64
            })
            .unwrap_or(false)
    }

    /// Get idle watermark when the partition is idling from the start (no messages read/produced ever)
    /// Checks if `init_source_delay` duration has passed before progressing the watermark for the
    /// idling partition.
    fn get_partition_idling_from_init_wm(&mut self, partition: u16) -> i64 {
        let now = Utc::now();

        let Some(init_source_delay) = self.config.init_source_delay else {
            return -1;
        };

        let Some(state) = self.partition_states.get_mut(&partition) else {
            return -1;
        };

        if now.timestamp_millis() - state.updated_ts.timestamp_millis()
            >= init_source_delay.as_millis() as i64
        {
            state.last_wm_published_time = now;
            now.timestamp_millis()
        } else {
            -1
        }
    }

    /// Verifies if the step interval has passed since last watermark publish for a partition.
    fn has_step_interval_passed(&self, state: &PartitionIdleState) -> bool {
        state.last_wm_published_time.timestamp_millis() == -1
            || Utc::now().timestamp_millis() - state.last_wm_published_time.timestamp_millis()
                > self.config.step_interval.as_millis() as i64
    }

    /// Resets the idle detector for a partition when data is received.
    pub(crate) fn reset(&mut self, partition: u16) {
        self.ensure_partition(partition);
        if let Some(state) = self.partition_states.get_mut(&partition) {
            state.updated_ts = Utc::now();
            state.last_wm_published_time =
                DateTime::from_timestamp_millis(-1).expect("Invalid timestamp");
        }
    }

    /// Initializes partitions for idle tracking.
    /// This replaces the current set of partitions - any partitions not in the new list
    /// will be removed to handle dynamic partition changes (e.g., Kafka rebalancing).
    pub(crate) fn initialize_partitions(&mut self, partitions: &[u16]) {
        let new_partitions: std::collections::HashSet<u16> = partitions.iter().copied().collect();

        // Remove partitions that are no longer active
        self.partition_states
            .retain(|partition, _| new_partitions.contains(partition));

        // Add new partitions
        for partition in partitions {
            self.ensure_partition(*partition);
        }
    }

    /// Computes and returns the watermark value to publish for a partition.
    /// If partition is idle, increments the watermark. Otherwise returns the current value.
    /// Also updates the last published time.
    pub(crate) fn compute_watermark(&mut self, partition: u16, computed_wm: i64) -> i64 {
        self.ensure_partition(partition);

        // If partition is not idle, just return the current watermark and update publish time
        if !self.is_partition_idle(partition) {
            if let Some(state) = self.partition_states.get_mut(&partition) {
                state.last_wm_published_time = Utc::now();
            }
            return computed_wm;
        }

        // Partition is idle - increment the watermark
        let increment_by = self.config.increment_by.as_millis() as i64;

        // check if the computed watermark is -1
        // last computed watermark can be -1, when the pod is restarted or when the processor entity is not created yet.
        if computed_wm == -1 {
            return self.get_partition_idling_from_init_wm(partition);
        }

        let mut idle_wm = computed_wm + increment_by;
        // do not assign future timestamps for WM.
        // this could happen if step interval and increment-by are set aggressively
        let now = Utc::now().timestamp_millis();
        if idle_wm > now {
            warn!(
                ?idle_wm,
                partition,
                "idle config is aggressive (reduce step/increment-by), wm > now(), resetting to now()"
            );
            idle_wm = now;
        }

        if let Some(state) = self.partition_states.get_mut(&partition) {
            state.last_wm_published_time = Utc::now();
        }
        idle_wm
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::pipeline::watermark::IdleConfig;
    use std::time::Duration;

    const TEST_PARTITION: u16 = 0;

    #[test]
    fn test_is_partition_idle() {
        let config = IdleConfig {
            threshold: Duration::from_millis(100),
            step_interval: Duration::from_millis(50),
            increment_by: Duration::from_millis(10),
            init_source_delay: None,
        };
        let mut manager = SourceIdleDetector::new(config);
        manager.initialize_partitions(&[TEST_PARTITION]);

        // Initially, the partition should not be idle
        assert!(!manager.is_partition_idle(TEST_PARTITION));

        // Simulate the partition idling by advancing the updated timestamp
        if let Some(state) = manager.partition_states.get_mut(&TEST_PARTITION) {
            state.updated_ts = Utc::now() - chrono::Duration::milliseconds(200);
        }
        assert!(manager.is_partition_idle(TEST_PARTITION));
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
        manager.initialize_partitions(&[TEST_PARTITION]);

        // Simulate the partition idling by advancing the updated timestamp
        if let Some(state) = manager.partition_states.get_mut(&TEST_PARTITION) {
            state.updated_ts = Utc::now() - chrono::Duration::milliseconds(200);
        }
        assert!(manager.is_partition_idle(TEST_PARTITION));

        // Reset the partition and check if it is no longer idle
        manager.reset(TEST_PARTITION);
        assert!(!manager.is_partition_idle(TEST_PARTITION));
    }

    #[test]
    fn test_compute_watermark_when_idle() {
        let config = IdleConfig {
            threshold: Duration::from_millis(100),
            step_interval: Duration::from_millis(50),
            increment_by: Duration::from_millis(10),
            init_source_delay: None,
        };
        let mut manager = SourceIdleDetector::new(config);
        manager.initialize_partitions(&[TEST_PARTITION]);

        // Make partition idle
        if let Some(state) = manager.partition_states.get_mut(&TEST_PARTITION) {
            state.updated_ts = Utc::now() - chrono::Duration::milliseconds(200);
        }

        // When idle with computed_wm = -1, should return -1 (no init_source_delay)
        let wm = manager.compute_watermark(TEST_PARTITION, -1);
        assert_eq!(wm, -1);

        // When idle with a valid computed_wm, should increment
        let wm = manager.compute_watermark(TEST_PARTITION, 1000);
        assert_eq!(wm, 1010);
    }

    #[test]
    fn test_compute_watermark_when_not_idle() {
        let config = IdleConfig {
            threshold: Duration::from_millis(100),
            step_interval: Duration::from_millis(50),
            increment_by: Duration::from_millis(10),
            init_source_delay: None,
        };
        let mut manager = SourceIdleDetector::new(config);
        manager.initialize_partitions(&[TEST_PARTITION]);

        // Partition is not idle, should return current watermark without increment
        let wm = manager.compute_watermark(TEST_PARTITION, 1000);
        assert_eq!(wm, 1000);
    }

    #[test]
    fn test_compute_watermark_with_init_source_delay() {
        let config = IdleConfig {
            threshold: Duration::from_millis(50),
            step_interval: Duration::from_millis(50),
            increment_by: Duration::from_millis(10),
            init_source_delay: Some(Duration::from_millis(200)),
        };
        let mut manager = SourceIdleDetector::new(config.clone());
        manager.initialize_partitions(&[TEST_PARTITION]);

        // Make partition idle (threshold is 50ms, so 100ms ago makes it idle)
        // But init_source_delay is 200ms, so it hasn't passed yet
        if let Some(state) = manager.partition_states.get_mut(&TEST_PARTITION) {
            state.updated_ts = Utc::now() - chrono::Duration::milliseconds(100);
        }

        // Partition is idle, computed_wm = -1, but init_source_delay (200ms) hasn't passed
        // since updated_ts was only 100ms ago. Should return -1.
        let wm = manager.compute_watermark(TEST_PARTITION, -1);
        assert_eq!(wm, -1);

        // Now set updated_ts to 300ms ago so init_source_delay (200ms) has passed
        if let Some(state) = manager.partition_states.get_mut(&TEST_PARTITION) {
            state.updated_ts = Utc::now() - chrono::Duration::milliseconds(300);
        }

        // Since init_source_delay has passed, should return current time
        let wm = manager.compute_watermark(TEST_PARTITION, -1);
        let expected_wm = manager
            .partition_states
            .get(&TEST_PARTITION)
            .unwrap()
            .last_wm_published_time
            .timestamp_millis();
        assert_eq!(wm, expected_wm);

        // With a valid computed_wm, should increment
        let wm = manager.compute_watermark(TEST_PARTITION, 1000);
        assert_eq!(wm, 1010);
    }

    #[test]
    fn test_partitions_needing_publish() {
        let config = IdleConfig {
            threshold: Duration::from_millis(100),
            step_interval: Duration::from_millis(50),
            increment_by: Duration::from_millis(10),
            init_source_delay: None,
        };
        let mut manager = SourceIdleDetector::new(config);
        manager.initialize_partitions(&[TEST_PARTITION]);

        // Initially, partitions_needing_publish should include the partition (never published)
        assert!(
            manager
                .partitions_needing_publish()
                .contains(&TEST_PARTITION)
        );

        // After computing watermark (which updates last published time), should be empty
        manager.compute_watermark(TEST_PARTITION, 1000);
        assert!(manager.partitions_needing_publish().is_empty());

        // After step interval passes, should include the partition again
        std::thread::sleep(Duration::from_millis(60));
        assert!(
            manager
                .partitions_needing_publish()
                .contains(&TEST_PARTITION)
        );
    }

    #[test]
    fn test_watermark_publish_timing_vs_idle() {
        let config = IdleConfig {
            threshold: Duration::from_millis(100),
            step_interval: Duration::from_millis(50),
            increment_by: Duration::from_millis(10),
            init_source_delay: None,
        };
        let mut manager = SourceIdleDetector::new(config);
        manager.initialize_partitions(&[TEST_PARTITION]);

        // Initially, partition is not idle but watermark should be published
        assert!(!manager.is_partition_idle(TEST_PARTITION));
        assert!(
            manager
                .partitions_needing_publish()
                .contains(&TEST_PARTITION)
        );

        // After step interval passes but before threshold, should publish but not idle
        std::thread::sleep(Duration::from_millis(60));
        assert!(!manager.is_partition_idle(TEST_PARTITION)); // threshold not passed
        assert!(
            manager
                .partitions_needing_publish()
                .contains(&TEST_PARTITION)
        ); // step interval passed

        // After threshold passes, both should be true
        std::thread::sleep(Duration::from_millis(50));
        assert!(manager.is_partition_idle(TEST_PARTITION)); // threshold passed
        assert!(
            manager
                .partitions_needing_publish()
                .contains(&TEST_PARTITION)
        ); // step interval passed
    }

    #[test]
    fn test_multiple_partitions_independent_idle() {
        let config = IdleConfig {
            threshold: Duration::from_millis(100),
            step_interval: Duration::from_millis(50),
            increment_by: Duration::from_millis(10),
            init_source_delay: None,
        };
        let mut manager = SourceIdleDetector::new(config);
        manager.initialize_partitions(&[0, 1, 2]);

        // Make only partition 0 idle
        if let Some(state) = manager.partition_states.get_mut(&0) {
            state.updated_ts = Utc::now() - chrono::Duration::milliseconds(200);
        }

        // Only partition 0 should be idle
        assert!(manager.is_partition_idle(0));
        assert!(!manager.is_partition_idle(1));
        assert!(!manager.is_partition_idle(2));

        // Reset partition 0, make partition 1 idle
        manager.reset(0);
        if let Some(state) = manager.partition_states.get_mut(&1) {
            state.updated_ts = Utc::now() - chrono::Duration::milliseconds(200);
        }

        // Now only partition 1 should be idle
        assert!(!manager.is_partition_idle(0));
        assert!(manager.is_partition_idle(1));
        assert!(!manager.is_partition_idle(2));
    }

    #[test]
    fn test_partition_cleanup_on_rebalance() {
        let config = IdleConfig {
            threshold: Duration::from_millis(100),
            step_interval: Duration::from_millis(50),
            increment_by: Duration::from_millis(10),
            init_source_delay: None,
        };
        let mut manager = SourceIdleDetector::new(config);

        // Initialize with partitions [0, 5, 10, 15, 20]
        manager.initialize_partitions(&[0, 5, 10, 15, 20]);
        assert_eq!(manager.partition_states.len(), 5);
        assert!(manager.partition_states.contains_key(&0));
        assert!(manager.partition_states.contains_key(&5));
        assert!(manager.partition_states.contains_key(&10));
        assert!(manager.partition_states.contains_key(&15));
        assert!(manager.partition_states.contains_key(&20));

        // Simulate rebalance: now only partitions [5, 10, 20] are active
        manager.initialize_partitions(&[5, 10, 20]);
        assert_eq!(manager.partition_states.len(), 3);
        assert!(!manager.partition_states.contains_key(&0)); // removed
        assert!(manager.partition_states.contains_key(&5));
        assert!(manager.partition_states.contains_key(&10));
        assert!(!manager.partition_states.contains_key(&15)); // removed
        assert!(manager.partition_states.contains_key(&20));

        // Simulate another rebalance: add new partition 25, remove 5
        manager.initialize_partitions(&[10, 20, 25]);
        assert_eq!(manager.partition_states.len(), 3);
        assert!(!manager.partition_states.contains_key(&5)); // removed
        assert!(manager.partition_states.contains_key(&10));
        assert!(manager.partition_states.contains_key(&20));
        assert!(manager.partition_states.contains_key(&25)); // added
    }

    #[test]
    fn test_default_heartbeat_without_idle_detection() {
        // When threshold is 0 (default), partitions should never be marked as idle
        // but heartbeats should still be published (step_interval still applies)
        let config = IdleConfig {
            threshold: Duration::from_millis(0), // 0 = never mark as idle
            step_interval: Duration::from_millis(50), // heartbeat every 50ms
            increment_by: Duration::from_millis(10),
            init_source_delay: None,
        };
        let mut manager = SourceIdleDetector::new(config);
        manager.initialize_partitions(&[0, 1, 2]);

        // Even after a long time, partitions should NOT be marked as idle
        // because threshold is 0
        if let Some(state) = manager.partition_states.get_mut(&0) {
            state.updated_ts = Utc::now() - chrono::Duration::milliseconds(10000);
        }
        assert!(!manager.is_partition_idle(0)); // threshold=0 means never idle

        // But step interval should still work for heartbeat publishing
        std::thread::sleep(Duration::from_millis(60));
        let partitions_needing_publish = manager.partitions_needing_publish();
        assert!(!partitions_needing_publish.is_empty()); // heartbeats should be published
    }
}
