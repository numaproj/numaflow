use crate::message::Message;
use crate::shared::grpc::prost_timestamp_from_utc;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use numaflow_pb::objects::wal::GcEvent;
use std::cmp::Ordering;

/// Fixed Window Operations.
pub(crate) mod fixed;
/// Sliding Window Operations.
pub(crate) mod sliding;

use fixed::FixedWindowManager;
use sliding::SlidingWindowManager;

/// WindowManager enum that can be either a FixedWindowManager or a SlidingWindowManager
#[derive(Debug, Clone)]
pub(crate) enum WindowManager {
    Fixed(FixedWindowManager),
    Sliding(SlidingWindowManager),
}

impl WindowManager {
    /// Assigns windows to a message. There can be more than one for Sliding Window.
    pub(crate) fn assign_windows(&self, msg: Message) -> Vec<AlignedWindowMessage> {
        match self {
            WindowManager::Fixed(manager) => manager.assign_windows(msg),
            WindowManager::Sliding(manager) => manager.assign_windows(msg),
        }
    }

    /// Closes any windows that can be closed because the Watermark has advanced beyond the window
    /// end time.
    pub(crate) fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<AlignedWindowMessage> {
        match self {
            WindowManager::Fixed(manager) => manager.close_windows(watermark),
            WindowManager::Sliding(manager) => manager.close_windows(watermark),
        }
    }

    /// Deletes a window is called after the window is closed and GC is done.
    pub(crate) fn delete_window(&self, window: Window) {
        match self {
            WindowManager::Fixed(manager) => manager.delete_window(window),
            WindowManager::Sliding(manager) => manager.delete_window(window),
        }
    }

    /// Returns the oldest window yet to be completed. This will be the lowest Watermark in the Vertex.
    pub(crate) fn oldest_window(&self) -> Option<Window> {
        match self {
            WindowManager::Fixed(manager) => manager.oldest_window(),
            WindowManager::Sliding(manager) => manager.oldest_window(),
        }
    }
}

/// A Window is represented by its start and end time. All the data which event time falls within
/// this window will be reduced by the Reduce function associated with it. The association is via the
/// id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Window {
    /// Start time of the window.
    pub(crate) start_time: DateTime<Utc>,
    /// End time of the window.
    pub(crate) end_time: DateTime<Utc>,
    /// Unique id of the reduce function for this window.
    pub(crate) id: Bytes,
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

impl Window {
    /// Creates a new Window.
    pub(crate) fn new(start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> Self {
        Self {
            start_time,
            end_time,
            id: format!(
                "{}-{}",
                start_time.timestamp_millis(),
                end_time.timestamp_millis(),
            )
            .into(),
        }
    }

    /// Returns the slot for the PNF.
    pub(crate) fn pnf_slot(&self) -> Bytes {
        self.id.clone()
    }
}

/// Window operations that can be performed on a [Window]. It is derived from the [Message] and the
/// window kind.
#[derive(Debug, Clone)]
pub(crate) enum WindowOperation {
    /// Open is create a new Window (Open the Book).
    Open(Message),
    /// Close operation for the [Window] (Close of Book). Only the window on the SDK side will be closed,
    /// other windows for the same partition can be open.
    Close,
    /// Append inserts more data into the opened Window.
    Append(Message),
}

/// Aligned Window Message.
#[derive(Debug, Clone)]
pub(crate) struct AlignedWindowMessage {
    pub(crate) operation: WindowOperation,
    pub(crate) window: Window,
}
