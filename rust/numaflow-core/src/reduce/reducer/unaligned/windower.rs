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
pub(crate) enum UnalignedWindowMessage {
    // Opening a new window
    Open { msg: Message, window: Window },
    // Closing a window
    Close(Window),
    // Appending to an existing window
    Append { msg: Message, window: Window },
    // Merging windows
    Merge { msg: Message, windows: Vec<Window> },
    // Expanding a window
    Expand { msg: Message, windows: Vec<Window> },
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
