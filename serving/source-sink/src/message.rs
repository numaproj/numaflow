use std::collections::HashMap;

use chrono::{DateTime, Utc};

/// A message that is sent from the source to the sink.
#[derive(Debug, Clone)]
pub(crate) struct Message {
    /// keys of the message
    pub(crate) keys: Vec<String>,
    /// actual payload of the message
    pub(crate) value: Vec<u8>,
    /// offset of the message
    pub(crate) offset: Offset,
    /// event time of the message
    pub(crate) event_time: DateTime<Utc>,
    /// headers of the message
    pub(crate) headers: HashMap<String, String>,
}

/// Offset of the message which will be used to acknowledge the message.
#[derive(Debug, Clone)]
pub(crate) struct Offset {
    /// unique identifier of the message
    pub(crate) offset: String,
    /// partition id of the message
    pub(crate) partition_id: i32,
}
