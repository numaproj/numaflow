use crate::message::Message;
use chrono::{DateTime, Utc};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) enum WindowKind {
    Aligned,
    Unaligned(Arc<String>),
}

#[derive(Debug, Clone)]
pub(crate) struct Window {
    window_kind: WindowKind,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
}

impl Window {
    pub(crate) fn new(
        window_kind: WindowKind,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Self {
        Self {
            window_kind,
            start_time,
            end_time,
        }
    }

    pub(crate) fn pnf_slot(&self) -> String {
        match &self.window_kind {
            WindowKind::Aligned => {
                format!(
                    "{}-{}",
                    self.start_time.timestamp_millis(),
                    self.end_time.timestamp_millis(),
                )
            }
            WindowKind::Unaligned(_) => {
                format!("{}-{}", 0, u64::MAX,)
            }
        }
    }
}

/// TODO: move Message to this enum
#[derive(Debug, Clone)]
pub(crate) enum WindowOperation {
    Open(Message),
    Close,
    Append(Message),
    Expand(Message),
    Merge(Message),
}

#[derive(Debug, Clone)]
pub(crate) struct WindowMessage {
    pub(crate) operation: WindowOperation,
    pub(crate) windows: Vec<Window>,
}
