//! Windower is responsible for managing the windows and exposes functions for assigning windows to
//! messages and closing windows when the watermark has advanced beyond the window end time.
//! Windows managed by the Windower are purely based on the event-time and is oblivious of the
//! keys. The multiplexing of windows to keyed windows is done at the Reduce server (SDK).

mod fixed;
mod sliding;

use chrono::{DateTime, DurationRound, TimeZone, Utc};

pub use fixed::FixedWindowManager;
pub use sliding::{SlidingWindowManager, SlidingWindowSnapshot};

pub use crate::model::Window;

// Technically we can have a trait for AlignedWindowManager and implement it for Fixed and Sliding, but
// the generics are getting in all the way from the bootup code. Also, we do not expect any other
// window types in the future.

/// Window operations that can be performed on a [`Window`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlignedWindowAction {
    /// Open is create a new Window (Open the Book).
    Open { window: Window },
    /// Close operation for the [`Window`] (Close of Book). Only the window on the SDK side will be closed,
    /// other windows for the same partition can be open.
    Close { window: Window },
    /// Append inserts more data into the opened Window.
    Append { window: Window },
}

/// AlignedWindowManager enum that can be either a FixedWindowManager or a SlidingWindowManager.
#[derive(Debug, Clone)]
pub enum AlignedWindowManager {
    /// Fixed window manager.
    Fixed(FixedWindowManager),
    /// Sliding window manager.
    Sliding(SlidingWindowManager),
}

impl AlignedWindowManager {
    /// Assigns windows to an event time, dropping events with event time earlier than the oldest window's start time
    pub fn assign_windows(&self, event_time: DateTime<Utc>) -> Vec<AlignedWindowAction> {
        match self {
            AlignedWindowManager::Fixed(manager) => manager.assign_windows(event_time),
            AlignedWindowManager::Sliding(manager) => manager.assign_windows(event_time),
        }
    }

    /// Closes any windows that can be closed because the Watermark has advanced beyond the window
    /// end time.
    pub fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<AlignedWindowAction> {
        match self {
            AlignedWindowManager::Fixed(manager) => manager.close_windows(watermark),
            AlignedWindowManager::Sliding(manager) => manager.close_windows(watermark),
        }
    }

    /// Drains all active windows in end-time order (equivalent to a terminal watermark).
    pub fn close_all(&self) -> Vec<AlignedWindowAction> {
        match self {
            AlignedWindowManager::Fixed(manager) => manager.close_all(),
            AlignedWindowManager::Sliding(manager) => manager.close_all(),
        }
    }

    /// Deletes a window is called after the window is closed and GC is done.
    pub fn gc_window(&self, window: Window) {
        match self {
            AlignedWindowManager::Fixed(manager) => manager.gc_window(window),
            AlignedWindowManager::Sliding(manager) => manager.gc_window(window),
        }
    }

    /// Returns the oldest window yet to be completed. This will be the lowest Watermark in the Vertex.
    pub fn oldest_window(&self) -> Option<Window> {
        match self {
            AlignedWindowManager::Fixed(manager) => manager.oldest_window(),
            AlignedWindowManager::Sliding(manager) => manager.oldest_window(),
        }
    }

    /// Returns the number of currently active windows.
    pub fn active_window_count(&self) -> usize {
        match self {
            AlignedWindowManager::Fixed(manager) => manager.active_window_count(),
            AlignedWindowManager::Sliding(manager) => manager.active_window_count(),
        }
    }

    /// Returns the number of closed windows awaiting GC.
    pub fn closed_window_count(&self) -> usize {
        match self {
            AlignedWindowManager::Fixed(manager) => manager.closed_window_count(),
            AlignedWindowManager::Sliding(manager) => manager.closed_window_count(),
        }
    }
}

/// Truncates a timestamp to the nearest multiple of the given duration.
pub fn truncate_to_duration(timestamp_millis: i64, duration_millis: i64) -> i64 {
    // Convert timestamp to DateTime
    let dt = Utc.timestamp_millis_opt(timestamp_millis).unwrap();
    // Convert duration_millis to TimeDelta
    let duration = chrono::TimeDelta::try_milliseconds(duration_millis)
        .expect("Failed to convert duration to TimeDelta");
    // Use DurationRound to truncate
    let truncated = dt
        .duration_trunc(duration)
        .expect("Failed to truncate timestamp");

    // Return as milliseconds
    truncated.timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_to_duration() {
        // Test with timestamp 100 and duration 30
        let result = truncate_to_duration(100, 30);
        // Expected result: 90
        // Explanation: 100 milliseconds truncated to the nearest multiple of 30 milliseconds
        // should be 90 (3 * 30 = 90)
        assert_eq!(result, 90);

        // Additional test cases for verification
        assert_eq!(truncate_to_duration(59, 30), 30);
        assert_eq!(truncate_to_duration(60, 30), 60);
        assert_eq!(truncate_to_duration(61, 30), 60);
        assert_eq!(truncate_to_duration(89, 30), 60);
        assert_eq!(truncate_to_duration(90, 30), 90);

        assert_eq!(truncate_to_duration(810, 70), 770);
    }
}
