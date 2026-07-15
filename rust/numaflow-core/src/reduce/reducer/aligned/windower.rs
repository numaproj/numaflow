//! Windower is responsible for managing the windows and exposes functions for assigning windows to
//! messages and closing windows when the watermark has advanced beyond the window end time.
//! Window managers live in `numaflow_udf_client`; this module adds core-specific envelopes and
//! adapters (PNF slots, WAL/GC protobuf mapping).

use crate::message::Message;
use crate::shared::grpc::{prost_timestamp_from_utc, utc_from_timestamp};
use bytes::Bytes;
use numaflow_pb::objects::wal::GcEvent;

pub(crate) use numaflow_udf_client::{
    AlignedWindowAction, AlignedWindowManager, FixedWindowManager, SlidingWindowManager, Window,
};

/// Window operations that can be performed on a [Window]. It is derived from the [Message] and the
/// window kind.
#[derive(Debug, Clone)]
pub(crate) enum AlignedWindowOperation {
    /// Open is create a new Window (Open the Book).
    Open { message: Message, window: Window },
    /// Close operation for the [Window] (Close of Book). Only the window on the SDK side will be closed,
    /// other windows for the same partition can be open. The window is retained for diagnostics;
    /// routing uses [`AlignedWindowMessage::pnf_slot`].
    Close {
        #[allow(dead_code)]
        window: Window,
    },
    /// Append inserts more data into the opened Window.
    Append { message: Message, window: Window },
}

/// Aligned Window Message.
#[derive(Debug, Clone)]
pub(crate) struct AlignedWindowMessage {
    pub(crate) operation: AlignedWindowOperation,
    /// PNF slot for the window operation. This is stored as a field as to quickly access this in
    /// different code path without recreating it all the time.
    pub(crate) pnf_slot: Bytes,
}

/// Helper function to construct PNF slot from a window.
/// The PNF slot is used as a unique identifier for the window.
pub(crate) fn window_to_pnf_slot(window: &Window) -> Bytes {
    format!(
        "{}-{}",
        window.start_time.timestamp_millis(),
        window.end_time.timestamp_millis(),
    )
    .into()
}

pub(crate) fn gc_event_from_window(window: &Window) -> GcEvent {
    GcEvent {
        start_time: Some(prost_timestamp_from_utc(window.start_time)),
        end_time: Some(prost_timestamp_from_utc(window.end_time)),
        keys: vec![],
    }
}

pub(crate) fn wal_window_from_window(window: &Window) -> numaflow_pb::objects::wal::Window {
    numaflow_pb::objects::wal::Window {
        start_time: Some(prost_timestamp_from_utc(window.start_time)),
        end_time: Some(prost_timestamp_from_utc(window.end_time)),
    }
}

pub(crate) fn window_from_wal_window(proto: &numaflow_pb::objects::wal::Window) -> Window {
    let start_time = proto
        .start_time
        .as_ref()
        .map(|ts| utc_from_timestamp(*ts))
        .expect("start time should be present");
    let end_time = proto
        .end_time
        .as_ref()
        .map(|ts| utc_from_timestamp(*ts))
        .expect("end time should be present");

    Window::new(start_time, end_time)
}

/// Converts close actions from the shared window manager into core window envelopes.
pub(crate) fn window_messages_for_close(
    actions: Vec<AlignedWindowAction>,
) -> Vec<AlignedWindowMessage> {
    actions
        .into_iter()
        .filter_map(|action| match action {
            AlignedWindowAction::Close { window } => Some(AlignedWindowMessage {
                operation: AlignedWindowOperation::Close {
                    window: window.clone(),
                },
                pnf_slot: window_to_pnf_slot(&window),
            }),
            AlignedWindowAction::Open { .. } | AlignedWindowAction::Append { .. } => None,
        })
        .collect()
}

/// Assigns windows for the message event time and attaches the message to Open/Append actions.
pub(crate) fn window_messages_for_assign(
    manager: &AlignedWindowManager,
    msg: Message,
) -> Vec<AlignedWindowMessage> {
    let actions = manager.assign_windows(msg.event_time);
    actions
        .into_iter()
        .filter_map(|action| {
            let (operation, window) = match action {
                AlignedWindowAction::Open { window } => (
                    AlignedWindowOperation::Open {
                        message: msg.clone(),
                        window: window.clone(),
                    },
                    window,
                ),
                AlignedWindowAction::Append { window } => (
                    AlignedWindowOperation::Append {
                        message: msg.clone(),
                        window: window.clone(),
                    },
                    window,
                ),
                AlignedWindowAction::Close { .. } => return None,
            };
            Some(AlignedWindowMessage {
                operation,
                pnf_slot: window_to_pnf_slot(&window),
            })
        })
        .collect()
}
