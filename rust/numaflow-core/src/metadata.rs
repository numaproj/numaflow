use bytes::Bytes;
use numaflow_pb::common::metadata;
use std::collections::HashMap;

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
