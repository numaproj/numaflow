use std::collections::HashMap;

use chrono::{DateTime, Utc};
use numaflow_pb::clients::map::{
    Handshake, MapRequest, MapResponse, TransmissionStatus, map_request, map_response,
};
use numaflow_pb::common::{metadata, nack_options};
use prost_types::Timestamp;

use crate::error::Result;
use crate::model::{
    KeyValueGroup, MapResult, UdfDatum, UdfMetadata, UdfNackOptions, UnaryMapResponse,
};

pub(crate) fn handshake_request() -> MapRequest {
    MapRequest {
        request: None,
        id: String::new(),
        handshake: Some(Handshake { sot: true }),
        status: None,
    }
}

pub(crate) fn data_request(datum: UdfDatum) -> MapRequest {
    MapRequest {
        request: Some(map_request::Request {
            keys: datum.keys,
            value: datum.value.to_vec(),
            event_time: Some(timestamp(datum.event_time)),
            watermark: datum.watermark.map(timestamp),
            headers: datum.headers,
            metadata: datum.metadata.map(encode_metadata),
        }),
        id: datum.id,
        handshake: None,
        status: None,
    }
}

pub(crate) fn eot_request() -> MapRequest {
    MapRequest {
        request: None,
        id: String::new(),
        handshake: None,
        status: Some(TransmissionStatus { eot: true }),
    }
}

pub(crate) fn decode_results(response: MapResponse) -> Result<UnaryMapResponse> {
    Ok(UnaryMapResponse {
        id: response.id,
        results: response.results.into_iter().map(decode_result).collect(),
    })
}

fn timestamp(value: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

fn encode_metadata(value: UdfMetadata) -> metadata::Metadata {
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

fn decode_result(value: map_response::Result) -> MapResult {
    MapResult {
        keys: value.keys,
        value: value.value.into(),
        tags: value.tags,
        metadata: value.metadata.map(decode_metadata),
        nack_options: value.nack_options.map(decode_nack_options),
    }
}

fn decode_metadata(value: metadata::Metadata) -> UdfMetadata {
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

fn decode_nack_options(value: nack_options::NackOptions) -> UdfNackOptions {
    UdfNackOptions {
        reason: value.reason,
        max_deliveries: value.max_deliveries,
        delay: value.delay,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use chrono::{TimeZone, Utc};
    use numaflow_pb::clients::map::{MapResponse, map_response};
    use numaflow_pb::common::{metadata, nack_options};

    use super::*;

    fn datum(watermark: Option<DateTime<Utc>>) -> UdfDatum {
        UdfDatum {
            id: "A".to_string(),
            keys: vec!["one".to_string(), "two".to_string()],
            value: Bytes::from_static(b"value"),
            event_time: Utc.timestamp_opt(1_700_000_000, 123_456_789).unwrap(),
            watermark,
            headers: HashMap::from([("trace".to_string(), "abc".to_string())]),
            metadata: Some(UdfMetadata {
                previous_vertex: "source".to_string(),
                sys_metadata: HashMap::from([(
                    "system".to_string(),
                    KeyValueGroup {
                        key_value: HashMap::from([(
                            "binary".to_string(),
                            Bytes::from_static(&[0, 1, 255]),
                        )]),
                    },
                )]),
                user_metadata: HashMap::from([(
                    "user".to_string(),
                    KeyValueGroup {
                        key_value: HashMap::from([(
                            "name".to_string(),
                            Bytes::from_static(b"numaflow"),
                        )]),
                    },
                )]),
            }),
        }
    }

    #[test]
    fn handshake_has_exact_control_shape() {
        let request = handshake_request();

        assert_eq!(request.id, "");
        assert!(request.request.is_none());
        assert_eq!(request.handshake.map(|handshake| handshake.sot), Some(true));
        assert!(request.status.is_none());
    }

    #[test]
    fn data_request_contains_every_field_without_control_frames() {
        let watermark = Utc.timestamp_opt(1_700_000_100, 987_654_321).unwrap();
        let request = data_request(datum(Some(watermark)));
        let data = request.request.expect("data request");

        assert_eq!(request.id, "A");
        assert_eq!(data.keys, ["one", "two"]);
        assert_eq!(data.value, b"value");
        assert_eq!(
            data.event_time,
            Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 123_456_789,
            })
        );
        assert_eq!(
            data.watermark,
            Some(Timestamp {
                seconds: 1_700_000_100,
                nanos: 987_654_321,
            })
        );
        assert_eq!(data.headers.get("trace").map(String::as_str), Some("abc"));
        let metadata = data.metadata.expect("metadata");
        assert_eq!(metadata.previous_vertex, "source");
        let system = metadata.sys_metadata.get("system").expect("system group");
        assert_eq!(
            system
                .key_value
                .get("binary")
                .expect("binary system value")
                .as_slice(),
            [0, 1, 255]
        );
        let user = metadata.user_metadata.get("user").expect("user group");
        assert_eq!(user.key_value.get("name").expect("user name"), b"numaflow");
        assert!(request.handshake.is_none());
        assert!(request.status.is_none(), "unary data must not contain EOT");
    }

    #[test]
    fn eot_has_exact_control_shape() {
        let request = eot_request();

        assert_eq!(request.id, "");
        assert!(request.request.is_none());
        assert!(request.handshake.is_none());
        assert_eq!(request.status.map(|status| status.eot), Some(true));
    }

    #[test]
    fn absent_watermark_stays_absent() {
        let request = data_request(datum(None));
        assert!(request.request.expect("data request").watermark.is_none());
    }

    #[test]
    fn response_fields_decode_without_loss() {
        let response = MapResponse {
            id: "A".to_string(),
            results: vec![map_response::Result {
                keys: vec!["key".to_string()],
                value: b"result".to_vec(),
                tags: vec!["tag".to_string()],
                metadata: Some(metadata::Metadata {
                    previous_vertex: "map".to_string(),
                    sys_metadata: HashMap::new(),
                    user_metadata: HashMap::from([(
                        "group".to_string(),
                        metadata::KeyValueGroup {
                            key_value: HashMap::from([("binary".to_string(), vec![0, 128, 255])]),
                        },
                    )]),
                }),
                nack_options: Some(nack_options::NackOptions {
                    reason: Some("retry".to_string()),
                    max_deliveries: Some(3),
                    delay: Some(5_000),
                }),
            }],
            handshake: None,
            status: None,
        };

        let decoded = decode_results(response).expect("decode response");
        assert_eq!(decoded.id, "A");
        let result = decoded.results.first().expect("one result");
        assert_eq!(result.keys, ["key"]);
        assert_eq!(result.value, Bytes::from_static(b"result"));
        assert_eq!(result.tags, ["tag"]);
        let metadata = result.metadata.as_ref().expect("metadata");
        let group = metadata.user_metadata.get("group").expect("user group");
        assert_eq!(
            group
                .key_value
                .get("binary")
                .expect("binary user value")
                .as_ref(),
            [0, 128, 255]
        );
        assert_eq!(
            result.nack_options,
            Some(UdfNackOptions {
                reason: Some("retry".to_string()),
                max_deliveries: Some(3),
                delay: Some(5_000),
            })
        );
    }
}
