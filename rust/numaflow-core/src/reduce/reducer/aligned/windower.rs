use crate::message::Message;
use crate::shared::grpc::prost_timestamp_from_utc;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use numaflow_pb::objects::wal::GcEvent;

/// Fixed Window Operations.
pub(crate) mod fixed;
/// Sliding Window Operations.
pub(crate) mod sliding;

pub(crate) trait WindowManager: Send + Sync + Clone + 'static {
    /// Assigns windows to a message. There can be more than one for Sliding Window.
    fn assign_windows(&self, msg: Message) -> Vec<AlignedWindowMessage>;

    /// Closes any windows that can be closed because the Watermark has advanced beyond the window
    /// end time.
    fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<AlignedWindowMessage>;

    /// Deletes a window is called after the window is closed and GC is done.
    fn delete_window(&self, window: Window);

    /// Returns the oldest window yet to be completed. This will be the lowest Watermark in the Vertex.
    fn oldest_window(&self) -> Option<Window>;
}

/// A Window is represented by its start and end time. All the data which event time falls within
/// this window will be reduced by the Reduce function associated with it. The association is via the
/// id.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Window {
    /// Start time of the window.
    pub(in crate::reduce) start_time: DateTime<Utc>,
    /// End time of the window.
    pub(in crate::reduce) end_time: DateTime<Utc>,
    /// Unique id of the reduce function for this window.
    pub(in crate::reduce) id: Bytes,
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
