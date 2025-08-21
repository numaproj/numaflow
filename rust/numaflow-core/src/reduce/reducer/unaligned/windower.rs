//! Windower is responsible for managing the windows and exposes functions for assigning windows to
//! messages and closing windows when there are no messages for a timeout duration. In case of
//! unaligned, windows are tracked at the key level and assignment happens based on the event-time
//! of the message.
use crate::message::Message;
use crate::reduce::reducer::unaligned::windower::accumulator::AccumulatorWindowManager;
use crate::reduce::reducer::unaligned::windower::session::SessionWindowManager;
use crate::shared::grpc::{prost_timestamp_from_utc, utc_from_timestamp};
use chrono::{DateTime, Utc};
use numaflow_pb::clients::accumulator::KeyedWindow;
use numaflow_pb::clients::sessionreduce;
use numaflow_pb::objects::wal::GcEvent;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::sync::Arc;

pub(crate) mod accumulator;
pub(crate) mod session;

const SHARED_PNF_SLOT: &str = "GLOBAL_SLOT";

/// A Window is represented by its start and end time and the keys. All the data which event time falls within
/// this window will be reduced by the Reduce function associated with it. The association is via the
/// id. The Windows when sorted are sorted by the end time.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Window {
    /// Start time of the window.
    pub(crate) start_time: DateTime<Utc>,
    /// End time of the window.
    pub(crate) end_time: DateTime<Utc>,
    /// Keys for the window
    pub(crate) keys: Arc<[String]>,
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

impl Debug for Window {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
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
            keys: Arc::from(value.keys),
        }
    }
}

impl From<&Window> for GcEvent {
    fn from(value: &Window) -> Self {
        Self {
            start_time: Some(prost_timestamp_from_utc(value.start_time)),
            end_time: Some(prost_timestamp_from_utc(value.end_time)),
            keys: value.keys.to_vec(),
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
            keys,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct UnalignedWindowMessage {
    pub(crate) operation: UnalignedWindowOperation,
    pub(crate) pnf_slot: &'static str,
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

/// UnalignedWindowManager enum that can be either a AccumulatorWindowManager or a SessionWindowManager.
#[derive(Debug, Clone)]
pub(crate) enum UnalignedWindowManager {
    Accumulator(AccumulatorWindowManager),
    Session(SessionWindowManager),
}

impl UnalignedWindowManager {
    /// Assigns windows to a message.
    pub(crate) fn assign_windows(&self, msg: Message) -> Vec<UnalignedWindowMessage> {
        match self {
            UnalignedWindowManager::Accumulator(manager) => manager.assign_windows(msg),
            UnalignedWindowManager::Session(manager) => manager.assign_windows(msg),
        }
    }

    /// Closes any windows that can be closed because the Watermark has advanced beyond the window
    /// end time.
    pub(crate) fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<UnalignedWindowMessage> {
        match self {
            UnalignedWindowManager::Accumulator(manager) => manager.close_windows(watermark),
            UnalignedWindowManager::Session(manager) => manager.close_windows(watermark),
        }
    }

    /// Deletes a window from the tracked windows.
    pub(crate) fn delete_window(&self, window: Window) {
        match self {
            UnalignedWindowManager::Accumulator(manager) => manager.delete_window(window),
            UnalignedWindowManager::Session(manager) => manager.delete_window(window),
        }
    }

    /// Returns the end time of the oldest window
    pub(crate) fn oldest_window_end_time(&self) -> Option<DateTime<Utc>> {
        match self {
            UnalignedWindowManager::Accumulator(manager) => manager.oldest_window_end_time(),
            UnalignedWindowManager::Session(manager) => manager.oldest_window_end_time(),
        }
    }
}

/// Combines keys into a single string for use as a map key
fn combine_keys(keys: &[String]) -> String {
    keys.join(":")
}
