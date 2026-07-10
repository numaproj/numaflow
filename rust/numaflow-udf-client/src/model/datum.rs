use std::collections::HashMap;

use bytes::Bytes;
use chrono::{DateTime, Utc};

/// Protocol-neutral input for one UDF invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdfDatum {
    pub id: String,
    pub keys: Vec<String>,
    pub value: Bytes,
    pub event_time: DateTime<Utc>,
    pub watermark: Option<DateTime<Utc>>,
    pub headers: HashMap<String, String>,
    pub metadata: Option<UdfMetadata>,
}

/// Metadata attached to a UDF input or output.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UdfMetadata {
    pub previous_vertex: String,
    pub sys_metadata: HashMap<String, KeyValueGroup>,
    pub user_metadata: HashMap<String, KeyValueGroup>,
}

/// A named group of binary metadata values.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyValueGroup {
    pub key_value: HashMap<String, Bytes>,
}
