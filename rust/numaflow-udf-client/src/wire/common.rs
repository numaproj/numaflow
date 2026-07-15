use std::collections::HashMap;

use chrono::{DateTime, TimeZone, Utc};
use numaflow_pb::common::metadata;
use prost_types::Timestamp;

use crate::error::{Result, UdfClientError};
use crate::model::{KeyValueGroup, UdfMetadata};

pub(crate) fn timestamp(value: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

/// Decodes a protobuf timestamp into UTC, rejecting out-of-range values.
pub(crate) fn decode_timestamp(value: Timestamp) -> Result<DateTime<Utc>> {
    Utc.timestamp_opt(value.seconds, value.nanos as u32)
        .single()
        .ok_or_else(|| {
            UdfClientError::InvalidReduceResponseTimestamp(format!(
                "seconds={}, nanos={}",
                value.seconds, value.nanos
            ))
        })
}

pub(crate) fn encode_metadata(value: UdfMetadata) -> metadata::Metadata {
    metadata::Metadata {
        previous_vertex: value.previous_vertex,
        sys_metadata: encode_groups(value.sys_metadata),
        user_metadata: encode_groups(value.user_metadata),
    }
}

fn encode_groups(
    groups: HashMap<String, KeyValueGroup>,
) -> HashMap<String, metadata::KeyValueGroup> {
    groups
        .into_iter()
        .map(|(name, group)| {
            (
                name,
                metadata::KeyValueGroup {
                    key_value: group
                        .key_value
                        .into_iter()
                        .map(|(key, value)| (key, value.to_vec()))
                        .collect(),
                },
            )
        })
        .collect()
}

pub(crate) fn decode_metadata(value: metadata::Metadata) -> UdfMetadata {
    UdfMetadata {
        previous_vertex: value.previous_vertex,
        sys_metadata: decode_groups(value.sys_metadata),
        user_metadata: decode_groups(value.user_metadata),
    }
}

fn decode_groups(
    groups: HashMap<String, metadata::KeyValueGroup>,
) -> HashMap<String, KeyValueGroup> {
    groups
        .into_iter()
        .map(|(name, group)| {
            (
                name,
                KeyValueGroup {
                    key_value: group
                        .key_value
                        .into_iter()
                        .map(|(key, value)| (key, value.into()))
                        .collect(),
                },
            )
        })
        .collect()
}
