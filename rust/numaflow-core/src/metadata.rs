use bytes::Bytes;
use numaflow_pb::common::metadata;
use std::collections::HashMap;

/// Group name under which sink-specific system metadata is stored in `sys_metadata`.
pub(crate) const SINK_METADATA_GROUP: &str = "sink";

/// Key under which the current retry attempt count is stored within the
/// [`SINK_METADATA_GROUP`] group of `sys_metadata`. It is `0` on the first
/// attempt and incremented by one for every subsequent retry.
pub(crate) const RETRY_COUNT_KEY: &str = "retry_count";

/// Group name under which message-level system metadata is stored in `sys_metadata`.
pub(crate) const MESSAGE_METADATA_GROUP: &str = "message";

/// Key under which the number of source/buffer delivery attempts is stored within the
/// [`MESSAGE_METADATA_GROUP`] group of `sys_metadata`.
pub(crate) const NUM_DELIVERED_KEY: &str = "num_delivered";

/// Metadata representation
#[derive(Debug, Clone)]
pub(crate) struct Metadata {
    /// name of the previous vertex.
    pub(crate) previous_vertex: String,
    pub(crate) sys_metadata: HashMap<String, KeyValueGroup>,
    pub(crate) user_metadata: HashMap<String, KeyValueGroup>,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            previous_vertex: "".to_string(),
            sys_metadata: HashMap::new(),
            user_metadata: HashMap::new(),
        }
    }
}

impl Metadata {
    /// Records the current sink retry attempt count under `sys_metadata["sink"]`
    /// so that user-defined sinks can observe how many times a message has been retried.
    /// `retry_count` is stored as the decimal string representation of `count`.
    pub(crate) fn set_sink_retry_count(&mut self, count: u64) {
        let sink_metadata = self
            .sys_metadata
            .entry(SINK_METADATA_GROUP.to_string())
            .or_insert_with(|| KeyValueGroup {
                key_value: HashMap::new(),
            });

        sink_metadata.key_value.insert(
            RETRY_COUNT_KEY.to_string(),
            Bytes::from(count.to_string().into_bytes()),
        );
    }

    /// Records the number of times this message has been delivered for processing.
    /// `num_delivered` is stored as the decimal string representation of `count`.
    pub(crate) fn set_num_delivered(&mut self, count: u64) {
        let message_metadata = self
            .sys_metadata
            .entry(MESSAGE_METADATA_GROUP.to_string())
            .or_insert_with(|| KeyValueGroup {
                key_value: HashMap::new(),
            });

        message_metadata.key_value.insert(
            NUM_DELIVERED_KEY.to_string(),
            Bytes::from(count.to_string().into_bytes()),
        );
    }

    #[cfg(test)]
    pub(crate) fn num_delivered(&self) -> Option<u64> {
        self.sys_metadata
            .get(MESSAGE_METADATA_GROUP)
            .and_then(|g| g.key_value.get(NUM_DELIVERED_KEY))
            .and_then(|v| std::str::from_utf8(v).ok())
            .and_then(|v| v.parse().ok())
    }
}

/// Key-value group
#[derive(Debug, Clone)]
pub(crate) struct KeyValueGroup {
    /// Key-value pairs
    pub(crate) key_value: HashMap<String, Bytes>,
}

// Conversion implementations between protobuf and internal type
impl From<metadata::Metadata> for Metadata {
    fn from(metadata: metadata::Metadata) -> Self {
        Self {
            previous_vertex: metadata.previous_vertex,
            sys_metadata: metadata
                .sys_metadata
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            user_metadata: metadata
                .user_metadata
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}

impl From<Metadata> for metadata::Metadata {
    fn from(metadata: Metadata) -> Self {
        metadata::Metadata {
            previous_vertex: metadata.previous_vertex,
            sys_metadata: metadata
                .sys_metadata
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            user_metadata: metadata
                .user_metadata
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}

impl From<metadata::KeyValueGroup> for KeyValueGroup {
    fn from(group: metadata::KeyValueGroup) -> Self {
        Self {
            key_value: group
                .key_value
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}

impl From<KeyValueGroup> for metadata::KeyValueGroup {
    fn from(group: KeyValueGroup) -> Self {
        Self {
            key_value: group
                .key_value
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}
