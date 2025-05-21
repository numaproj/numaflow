use crate::message::Message;
use crate::reduce::reducer::unaligned::windower::accumulator::AccumulatorWindowManager;
use crate::reduce::reducer::unaligned::windower::session::SessionWindowManager;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::sync::Arc;

mod accumulator;
mod session;

const SHARED_PNF_SLOT: &str = "GLOBAL_SLOT";

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
    /// Merge operation for merging more than one windows
    Merge(Message),
    /// Expand operation for expanding the window length
    Expand(Message),
}

/// A Window is represented by its start and end time. All the data which event time falls within
/// this window will be reduced by the Reduce function associated with it. The association is via the
/// id. The Windows when sorted are sorted by the end time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Window {
    /// Start time of the window.
    pub(crate) start_time: DateTime<Utc>,
    /// End time of the window.
    pub(crate) end_time: DateTime<Utc>,
    /// Unique id of the reduce function for this window.
    pub(crate) id: Bytes,
    /// Keys for the window
    pub(crate) keys: Arc<[String]>,
}

impl Window {
    /// Creates a new Window.
    pub(crate) fn new(
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        keys: Arc<[String]>,
    ) -> Self {
        Self {
            start_time,
            end_time,
            id: format!(
                "{}-{}-{}",
                start_time.timestamp_millis(),
                end_time.timestamp_millis(),
                keys.iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(":"),
            )
            .into(),
            keys,
        }
    }

    pub(crate) fn pnf_slot() -> Bytes {
        // all the windows share the same slot
        SHARED_PNF_SLOT.to_string().into()
    }
}

/// Unaligned Window Message.
#[derive(Debug, Clone)]
pub(crate) struct UnalignedWindowMessage {
    pub(crate) operation: WindowOperation,
    pub(crate) windows: Vec<Window>,
}

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
    pub(crate) fn delete_window(&self, window: Window) {
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

#[derive(Debug, Clone)]
pub(crate) enum UnalignedWindowManager {
    Accumulator(AccumulatorWindowManager),
    Session(SessionWindowManager),
}

impl UnalignedWindowManager {
    pub(crate) fn assign_windows(&self, msg: Message) -> Vec<UnalignedWindowMessage> {
        match self {
            UnalignedWindowManager::Accumulator(manager) => manager.assign_windows(msg),
            UnalignedWindowManager::Session(manager) => manager.assign_windows(msg),
        }
    }

    pub(crate) fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<UnalignedWindowMessage> {
        match self {
            UnalignedWindowManager::Accumulator(manager) => manager.close_windows(watermark),
            UnalignedWindowManager::Session(manager) => manager.close_windows(watermark),
        }
    }

    pub(crate) fn delete_closed_window(&self, window: Window) {
        match self {
            UnalignedWindowManager::Accumulator(manager) => manager.delete_closed_window(window),
            UnalignedWindowManager::Session(manager) => manager.delete_closed_window(window),
        }
    }

    pub(crate) fn oldest_window_end_time(&self) -> Option<DateTime<Utc>> {
        match self {
            UnalignedWindowManager::Accumulator(manager) => manager.oldest_window_end_time(),
            UnalignedWindowManager::Session(manager) => manager.oldest_window_end_time(),
        }
    }
}
