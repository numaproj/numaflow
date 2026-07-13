use numaflow_pb::clients::reduce::reduce_request::window_operation::Event;
use numaflow_pb::clients::reduce::{
    ReduceRequest, ReduceResponse, Window as ReduceWindow, reduce_request,
};

use crate::error::{Result, UdfClientError};
use crate::model::{AlignedReduceResult, UdfDatum, Window};
use crate::wire::common::{decode_metadata, decode_timestamp, encode_metadata, timestamp};

const ALIGNED_REDUCE_SLOT: &str = "0";

pub(crate) fn payload_from_datum(datum: UdfDatum) -> reduce_request::Payload {
    reduce_request::Payload {
        keys: datum.keys,
        value: datum.value.to_vec(),
        event_time: Some(timestamp(datum.event_time)),
        watermark: datum.watermark.map(timestamp),
        headers: datum.headers,
        metadata: datum.metadata.map(encode_metadata),
    }
}

pub(crate) fn window_pb(window: &Window) -> ReduceWindow {
    ReduceWindow {
        start: Some(timestamp(window.start_time)),
        end: Some(timestamp(window.end_time)),
        slot: ALIGNED_REDUCE_SLOT.to_string(),
    }
}

pub(crate) fn open_request(window: &Window, datum: UdfDatum) -> ReduceRequest {
    ReduceRequest {
        payload: Some(payload_from_datum(datum)),
        operation: Some(reduce_request::WindowOperation {
            event: Event::Open as i32,
            windows: vec![window_pb(window)],
        }),
    }
}

pub(crate) fn append_request(window: &Window, datum: UdfDatum) -> ReduceRequest {
    ReduceRequest {
        payload: Some(payload_from_datum(datum)),
        operation: Some(reduce_request::WindowOperation {
            event: Event::Append as i32,
            windows: vec![window_pb(window)],
        }),
    }
}

pub(crate) fn is_eof(response: &ReduceResponse) -> bool {
    response.eof
}

pub(crate) fn decode_result(
    response: ReduceResponse,
    expected_window: Option<&Window>,
) -> Result<Option<AlignedReduceResult>> {
    if is_eof(&response) {
        return Ok(None);
    }

    let result = response
        .result
        .ok_or_else(|| UdfClientError::InvalidReduceResponseResult("missing result".to_string()))?;
    let window_proto = response
        .window
        .ok_or_else(|| UdfClientError::InvalidReduceResponseWindow("missing window".to_string()))?;
    let window = decode_response_window(window_proto)?;

    if let Some(expected) = expected_window
        && (window.start_time != expected.start_time || window.end_time != expected.end_time)
    {
        return Err(UdfClientError::ReduceResponseWindowMismatch {
            expected: expected.to_string(),
            actual: window.to_string(),
        });
    }

    Ok(Some(AlignedReduceResult {
        window,
        keys: result.keys,
        value: result.value.into(),
        tags: result.tags,
        metadata: result.metadata.map(decode_metadata),
    }))
}

fn decode_response_window(window: ReduceWindow) -> Result<Window> {
    let start = window.start.ok_or_else(|| {
        UdfClientError::InvalidReduceResponseWindow("missing window start".to_string())
    })?;
    let end = window.end.ok_or_else(|| {
        UdfClientError::InvalidReduceResponseWindow("missing window end".to_string())
    })?;
    Ok(Window::new(
        decode_timestamp(start)?,
        decode_timestamp(end)?,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use chrono::{DateTime, TimeZone, Utc};
    use numaflow_pb::clients::reduce::reduce_request::window_operation::Event;
    use numaflow_pb::clients::reduce::reduce_response;
    use numaflow_pb::common::metadata;
    use prost_types::Timestamp;

    use crate::model::{KeyValueGroup, UdfMetadata};

    use super::*;

    fn sample_window() -> Window {
        Window::new(
            Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            Utc.timestamp_opt(1_700_000_060, 0).unwrap(),
        )
    }

    fn datum(watermark: Option<DateTime<Utc>>) -> UdfDatum {
        UdfDatum {
            id: String::new(),
            keys: vec!["one".to_string(), "two".to_string()],
            value: Bytes::from_static(b"value"),
            event_time: Utc.timestamp_opt(1_700_000_000, 123_456_789).unwrap(),
            watermark,
            headers: HashMap::from([("trace".to_string(), "abc".to_string())]),
            metadata: Some(UdfMetadata {
                previous_vertex: "source".to_string(),
                sys_metadata: HashMap::new(),
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
    fn open_event_ordinal_and_window() {
        let window = sample_window();
        let request = open_request(&window, datum(None));
        let operation = request.operation.expect("operation");
        assert_eq!(operation.event, Event::Open as i32);
        let window_pb = operation.windows.first().expect("one window");
        assert_eq!(
            window_pb.start,
            Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            })
        );
        assert_eq!(
            window_pb.end,
            Some(Timestamp {
                seconds: 1_700_000_060,
                nanos: 0,
            })
        );
    }

    #[test]
    fn append_event_ordinal_and_window() {
        let window = sample_window();
        let request = append_request(&window, datum(None));
        let operation = request.operation.expect("operation");
        assert_eq!(operation.event, Event::Append as i32);
        assert_eq!(operation.windows.len(), 1);
    }

    #[test]
    fn slot_is_exactly_zero() {
        let window = sample_window();
        let request = open_request(&window, datum(None));
        let slot = request
            .operation
            .expect("operation")
            .windows
            .first()
            .expect("window")
            .slot
            .clone();
        assert_eq!(slot, "0");
    }

    #[test]
    fn no_close_request_builder_exists() {
        // Closing the request stream is the only close path; this module must not build CLOSE.
        let window = sample_window();
        let open = open_request(&window, datum(None));
        let append = append_request(&window, datum(None));
        for request in [open, append] {
            let event = request.operation.expect("operation").event;
            assert_ne!(event, Event::Close as i32);
        }
    }

    #[test]
    fn event_time_nanoseconds_survive_conversion() {
        let request = open_request(&sample_window(), datum(None));
        let event_time = request
            .payload
            .expect("payload")
            .event_time
            .expect("event time");
        assert_eq!(event_time.seconds, 1_700_000_000);
        assert_eq!(event_time.nanos, 123_456_789);
    }

    #[test]
    fn optional_watermark_remains_optional() {
        let without = open_request(&sample_window(), datum(None));
        assert!(without.payload.expect("payload").watermark.is_none());

        let watermark = Utc.timestamp_opt(1_700_000_100, 987_654_321).unwrap();
        let with = open_request(&sample_window(), datum(Some(watermark)));
        assert_eq!(
            with.payload.expect("payload").watermark,
            Some(Timestamp {
                seconds: 1_700_000_100,
                nanos: 987_654_321,
            })
        );
    }

    #[test]
    fn headers_and_metadata_round_trip_in_payload() {
        let request = open_request(&sample_window(), datum(None));
        let payload = request.payload.expect("payload");
        assert_eq!(
            payload.headers.get("trace").map(String::as_str),
            Some("abc")
        );
        let metadata = payload.metadata.expect("metadata");
        assert_eq!(metadata.previous_vertex, "source");
        let user = metadata.user_metadata.get("user").expect("user group");
        assert_eq!(user.key_value.get("name").expect("name"), b"numaflow");
    }

    #[test]
    fn result_keys_value_tags_metadata_decode() {
        let window = sample_window();
        let response = ReduceResponse {
            result: Some(reduce_response::Result {
                keys: vec!["key".to_string()],
                value: b"result".to_vec(),
                tags: vec!["tag".to_string()],
                metadata: Some(metadata::Metadata {
                    previous_vertex: "reduce".to_string(),
                    sys_metadata: HashMap::new(),
                    user_metadata: HashMap::from([(
                        "group".to_string(),
                        metadata::KeyValueGroup {
                            key_value: HashMap::from([("binary".to_string(), vec![0, 128, 255])]),
                        },
                    )]),
                }),
            }),
            window: Some(window_pb(&window)),
            eof: false,
        };

        let decoded = decode_result(response, None)
            .expect("decode")
            .expect("one result");
        assert_eq!(decoded.keys, ["key"]);
        assert_eq!(decoded.value, Bytes::from_static(b"result"));
        assert_eq!(decoded.tags, ["tag"]);
        let metadata = decoded.metadata.expect("metadata");
        let group = metadata.user_metadata.get("group").expect("group");
        assert_eq!(
            group.key_value.get("binary").expect("binary").as_ref(),
            [0, 128, 255]
        );
    }

    #[test]
    fn eof_is_recognized_without_result_or_window() {
        let response = ReduceResponse {
            result: None,
            window: None,
            eof: true,
        };
        assert!(is_eof(&response));
        assert!(decode_result(response, None).expect("eof decode").is_none());
    }

    #[test]
    fn non_eof_missing_result_is_error() {
        let response = ReduceResponse {
            result: None,
            window: Some(window_pb(&sample_window())),
            eof: false,
        };
        assert!(matches!(
            decode_result(response, None),
            Err(UdfClientError::InvalidReduceResponseResult(_))
        ));
    }

    #[test]
    fn non_eof_missing_window_is_error() {
        let response = ReduceResponse {
            result: Some(reduce_response::Result {
                keys: vec![],
                value: vec![],
                tags: vec![],
                metadata: None,
            }),
            window: None,
            eof: false,
        };
        assert!(matches!(
            decode_result(response, None),
            Err(UdfClientError::InvalidReduceResponseWindow(_))
        ));
    }

    #[test]
    fn invalid_timestamps_are_errors() {
        let window = sample_window();
        let response = ReduceResponse {
            result: Some(reduce_response::Result {
                keys: vec![],
                value: vec![],
                tags: vec![],
                metadata: None,
            }),
            window: Some(ReduceWindow {
                start: Some(Timestamp {
                    seconds: i64::MAX,
                    nanos: 2_000_000_000,
                }),
                end: Some(timestamp(window.end_time)),
                slot: "0".to_string(),
            }),
            eof: false,
        };
        assert!(matches!(
            decode_result(response, None),
            Err(UdfClientError::InvalidReduceResponseTimestamp(_))
        ));
    }

    #[test]
    fn response_window_mismatch_is_error() {
        let window = sample_window();
        let other = Window::new(
            Utc.timestamp_opt(1_700_000_120, 0).unwrap(),
            Utc.timestamp_opt(1_700_000_180, 0).unwrap(),
        );
        let response = ReduceResponse {
            result: Some(reduce_response::Result {
                keys: vec![],
                value: vec![],
                tags: vec![],
                metadata: None,
            }),
            window: Some(window_pb(&other)),
            eof: false,
        };
        assert!(matches!(
            decode_result(response, Some(&window)),
            Err(UdfClientError::ReduceResponseWindowMismatch { .. })
        ));
    }
}
