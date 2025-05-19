use crate::message::Message;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::sync::Arc;

mod accumulator;
mod session;

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

/// Unaligned Window Message.
#[derive(Debug, Clone)]
pub(crate) struct UnalignedWindowMessage {
    pub(crate) operation: WindowOperation,
    pub(crate) windows: Vec<Window>,
}
