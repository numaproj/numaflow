use crate::message::Message;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub(crate) struct Window {
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    keys: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub(crate) enum WindowOperation {
    Open,
    Close,
    Append,
    Expand,
    Merge,
}

#[derive(Debug, Clone)]
pub(crate) struct WindowMessage {
    pub(crate) operation: WindowOperation,
    pub(crate) message: Option<Message>,
    pub(crate) windows: Vec<Window>,
}
