use crate::config::get_vertex_name;
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
        Self::from_proto(metadata)
    }
}

impl From<Metadata> for metadata::Metadata {
    fn from(metadata: Metadata) -> Self {
        // For JetStream writes: set previous_vertex to current vertex name
        // so the next vertex knows who sent the message
        metadata.to_proto_with_previous_vertex(get_vertex_name().to_string())
    }
}

impl Metadata {
    /// Convert from protobuf metadata to internal representation.
    /// Preserves all fields as-is from the protobuf.
    pub(crate) fn from_proto(metadata: metadata::Metadata) -> Self {
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

    /// Convert to protobuf metadata for UDF requests.
    /// Preserves the original previous_vertex so UDFs know the actual sender.
    pub(crate) fn to_proto(self) -> metadata::Metadata {
        let previous_vertex = self.previous_vertex.clone();
        self.to_proto_with_previous_vertex(previous_vertex)
    }

    /// Convert to protobuf metadata with a specific previous_vertex value.
    /// Used internally by both conversion methods.
    fn to_proto_with_previous_vertex(self, previous_vertex: String) -> metadata::Metadata {
        metadata::Metadata {
            previous_vertex,
            sys_metadata: self
                .sys_metadata
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            user_metadata: self
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
