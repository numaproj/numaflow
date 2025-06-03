use crate::message::Message;
use crate::reduce::reducer::unaligned::windower::accumulator::AccumulatorWindowManager;
use crate::reduce::reducer::unaligned::windower::session::SessionWindowManager;
use crate::shared::grpc::utc_from_timestamp;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use numaflow_pb::clients::accumulator::KeyedWindow;
use numaflow_pb::clients::sessionreduce;
use std::fmt::Display;
use std::sync::Arc;

pub(crate) mod accumulator;
pub(crate) mod session;

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

impl Display for Window {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Window {{ start: {}, end: {}, keys: {:?} }}",
            self.start_time.timestamp_millis(),
            self.end_time.timestamp_millis(),
            self.keys
        )
    }
}

impl From<sessionreduce::KeyedWindow> for Window {
    fn from(value: sessionreduce::KeyedWindow) -> Self {
        Self {
            start_time: value
                .start
                .map(utc_from_timestamp)
                .expect("start time should be present"),
            end_time: value
                .end
                .map(utc_from_timestamp)
                .expect("end time should be present"),
            id: format!(
                "{}-{}-{}",
                utc_from_timestamp(value.start.expect("start time should be present"))
                    .timestamp_millis(),
                utc_from_timestamp(value.end.expect("end time should be present"))
                    .timestamp_millis(),
                value
                    .keys
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(":"),
            )
            .into(),
            keys: Arc::from(value.keys),
        }
    }
}

impl From<KeyedWindow> for Window {
    fn from(value: KeyedWindow) -> Self {
        Self {
            start_time: value
                .start
                .map(utc_from_timestamp)
                .expect("start time should be present"),
            end_time: value
                .end
                .map(utc_from_timestamp)
                .expect("end time should be present"),
            id: format!(
                "{}-{}-{}",
                utc_from_timestamp(value.start.expect("start time should be present"))
                    .timestamp_millis(),
                utc_from_timestamp(value.end.expect("end time should be present"))
                    .timestamp_millis(),
                value
                    .keys
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(":"),
            )
            .into(),
            keys: Arc::from(value.keys),
        }
    }
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
}

#[derive(Debug, Clone)]
pub(crate) struct UnalignedWindowMessage {
    pub(crate) operation: UnalignedWindowOperation,
    pub(crate) pnf_slot: Bytes,
}

/// Unaligned Window Message.
#[derive(Debug, Clone)]
pub(crate) enum UnalignedWindowOperation {
    // Opening a new window
    Open {
        message: Message,
        window: Window,
    },
    // Closing a window
    Close {
        window: Window,
    },
    // Appending to an existing window
    Append {
        message: Message,
        window: Window,
    },
    // Merging windows
    Merge {
        windows: Vec<Window>,
    },
    // Expanding a window
    Expand {
        message: Message,
        windows: Vec<Window>,
    },
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

    pub(crate) fn delete_window(&self, window: Window) {
        match self {
            UnalignedWindowManager::Accumulator(manager) => manager.delete_window(window),
            UnalignedWindowManager::Session(manager) => manager.delete_window(window),
        }
    }

    pub(crate) fn oldest_window_end_time(&self) -> Option<DateTime<Utc>> {
        match self {
            UnalignedWindowManager::Accumulator(manager) => manager.oldest_window_end_time(),
            UnalignedWindowManager::Session(manager) => manager.oldest_window_end_time(),
        }
    }
}
