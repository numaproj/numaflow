//! Windower is responsible for managing the windows and exposes functions for assigning windows to
//! messages and closing windows when the watermark has advanced beyond the window end time.
//! Windows managed by the Windower are purely based on the event-time and is oblivious of the
//! keys. The multiplexing of windows to keyed windows is done at the Reduce server (SDK).

use crate::message::Message;
use crate::shared::grpc::{prost_timestamp_from_utc, utc_from_timestamp};
use bytes::Bytes;
use chrono::{DateTime, DurationRound, TimeZone, Utc};
use fixed::FixedWindowManager;
use numaflow_pb::objects::wal::GcEvent;
use sliding::SlidingWindowManager;
use std::cmp::Ordering;

/// Fixed Window Operations.
pub(crate) mod fixed;
/// Sliding Window Operations.
pub(crate) mod sliding;

// Technically we can have a trait for AlignedWindowManager and implement it for Fixed and Sliding, but
// the generics are getting in all the way from the bootup code. Also, we do not expect any other
// window types in the future.
/// AlignedWindowManager enum that can be either a FixedWindowManager or a SlidingWindowManager.
#[derive(Debug, Clone)]
pub(crate) enum AlignedWindowManager {
    /// Fixed window manager.
    Fixed(FixedWindowManager),
    /// Sliding window manager.
    Sliding(SlidingWindowManager),
}

impl AlignedWindowManager {
    /// Assigns windows to a message, dropping messages with event time earlier than the oldest window's start time
    pub(crate) fn assign_windows(&self, msg: Message) -> Vec<AlignedWindowMessage> {
        match self {
            AlignedWindowManager::Fixed(manager) => manager.assign_windows(msg),
            AlignedWindowManager::Sliding(manager) => manager.assign_windows(msg),
        }
    }

    /// Closes any windows that can be closed because the Watermark has advanced beyond the window
    /// end time.
    pub(crate) fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<AlignedWindowMessage> {
        match self {
            AlignedWindowManager::Fixed(manager) => manager.close_windows(watermark),
            AlignedWindowManager::Sliding(manager) => manager.close_windows(watermark),
        }
    }

    /// Deletes a window is called after the window is closed and GC is done.
    pub(crate) fn gc_window(&self, window: Window) {
        match self {
            AlignedWindowManager::Fixed(manager) => manager.gc_window(window),
            AlignedWindowManager::Sliding(manager) => manager.gc_window(window),
        }
    }

    /// Returns the oldest window yet to be completed. This will be the lowest Watermark in the Vertex.
    pub(crate) fn oldest_window(&self) -> Option<Window> {
        match self {
            AlignedWindowManager::Fixed(manager) => manager.oldest_window(),
            AlignedWindowManager::Sliding(manager) => manager.oldest_window(),
        }
    }
}

/// A Window is represented by its start and end time. All the data which event time falls within
/// this window will be reduced by the Reduce function associated with it. The Windows when sorted
/// are sorted by the end time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Window {
    /// Start time of the window.
    pub(crate) start_time: DateTime<Utc>,
    /// End time of the window.
    pub(crate) end_time: DateTime<Utc>,
}

impl Ord for Window {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare based on end time
        self.end_time.cmp(&other.end_time)
    }
}

impl PartialOrd for Window {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl From<Window> for GcEvent {
    fn from(value: Window) -> Self {
        Self {
            start_time: Some(prost_timestamp_from_utc(value.start_time)),
            end_time: Some(prost_timestamp_from_utc(value.end_time)),
            keys: vec![],
        }
    }
}

impl From<&Window> for numaflow_pb::objects::wal::Window {
    fn from(window: &Window) -> Self {
        Self {
            start_time: Some(prost_timestamp_from_utc(window.start_time)),
            end_time: Some(prost_timestamp_from_utc(window.end_time)),
        }
    }
}

impl From<&numaflow_pb::objects::wal::Window> for Window {
    fn from(proto: &numaflow_pb::objects::wal::Window) -> Self {
        let start_time = proto.start_time.as_ref().map(|ts| utc_from_timestamp(*ts));
        let end_time = proto.end_time.as_ref().map(|ts| utc_from_timestamp(*ts));

        Self::new(
            start_time.expect("start time should be present"),
            end_time.expect("end time should be present"),
        )
    }
}

impl Window {
    /// Creates a new Window.
    pub(crate) fn new(start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> Self {
        Self {
            start_time,
            end_time,
        }
    }
}

/// Window operations that can be performed on a [Window]. It is derived from the [Message] and the
/// window kind.
#[derive(Debug, Clone)]
pub(crate) enum AlignedWindowOperation {
    /// Open is create a new Window (Open the Book).
    Open { message: Message, window: Window },
    /// Close operation for the [Window] (Close of Book). Only the window on the SDK side will be closed,
    /// other windows for the same partition can be open.
    Close { window: Window },
    /// Append inserts more data into the opened Window.
    Append { message: Message, window: Window },
}

/// Aligned Window Message.
#[derive(Debug, Clone)]
pub(crate) struct AlignedWindowMessage {
    pub(crate) operation: AlignedWindowOperation,
    /// PNF slot for the window operation. This is stored as a field is to quickly access this in
    /// different code path without recreating it all the time.
    pub(crate) pnf_slot: Bytes,
}

/// Helper function to construct PNF slot from a window.
/// The PNF slot is used as a unique identifier for the window.
pub(crate) fn window_pnf_slot(window: &Window) -> Bytes {
    format!(
        "{}-{}",
        window.start_time.timestamp_millis(),
        window.end_time.timestamp_millis(),
    )
    .into()
}

/// Truncates a timestamp to the nearest multiple of the given duration.
pub(crate) fn truncate_to_duration(timestamp_millis: i64, duration_millis: i64) -> i64 {
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
