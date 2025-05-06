use crate::message::Message;
use crate::shared::grpc::prost_timestamp_from_utc;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use numaflow_pb::objects::wal::GcEvent;

pub(super) mod fixed;
pub(super) mod sliding;

pub(crate) trait Windower {
    /// Assigns windows to a message
    fn assign_windows(&self, msg: Message) -> Vec<AlignedWindowMessage>;

    /// Closes any pending windows
    fn close_windows(&self) -> Vec<AlignedWindowMessage>;

    /// Deletes a window
    fn delete_window(&self, window: Window);

    /// Returns the oldest window end time
    fn oldest_window_endtime(&self) -> DateTime<Utc>;
}

#[derive(Debug, Clone)]
pub(crate) enum WindowKind {
    Fixed,
    Sliding,
}

#[derive(Debug, Clone)]
pub(crate) struct Window {
    pub(in crate::reduce) start_time: DateTime<Utc>,
    pub(in crate::reduce) end_time: DateTime<Utc>,
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

    pub(crate) fn pnf_slot(&self) -> Bytes {
        self.id.clone()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum WindowOperation {
    Open(Message),
    Close,
    Append(Message),
}

#[derive(Debug, Clone)]
pub(crate) enum AlignedWindowMessage {
    Fixed(FixedWindowMessage),
    Sliding(SlidingWindowMessage),
}

#[derive(Debug, Clone)]
pub(crate) struct FixedWindowMessage {
    pub(crate) operation: WindowOperation,
    pub(crate) window: Window,
}

#[derive(Debug, Clone)]
pub(crate) struct SlidingWindowMessage {
    pub(crate) operation: WindowOperation,
    pub(crate) window: Window,
}
