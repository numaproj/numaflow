use std::cmp::PartialEq;
use std::collections::HashMap;

use crate::shared::utils::{prost_timestamp_from_utc, utc_from_timestamp};
use crate::Error;
use crate::Result;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chrono::{DateTime, Utc};
use numaflow_grpc::clients::sink::sink_request::Request;
use numaflow_grpc::clients::sink::Status::{Failure, Fallback, Success};
use numaflow_grpc::clients::sink::{sink_response, SinkRequest, SinkResponse};
use numaflow_grpc::clients::source::{read_response, AckRequest};
use numaflow_grpc::clients::sourcetransformer::SourceTransformRequest;
use serde::{Deserialize, Serialize};

/// A message that is sent from the source to the sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Message {
    /// keys of the message
    pub(crate) keys: Vec<String>,
    /// actual payload of the message
    pub(crate) value: Vec<u8>,
    /// offset of the message
    pub(crate) offset: Offset,
    /// event time of the message
    pub(crate) event_time: DateTime<Utc>,
    /// id of the message
    pub(crate) id: String,
    /// headers of the message
    pub(crate) headers: HashMap<String, String>,
}

/// Offset of the message which will be used to acknowledge the message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Offset {
    /// unique identifier of the message
    pub(crate) offset: String,
    /// partition id of the message
    pub(crate) partition_id: i32,
}

impl From<Offset> for AckRequest {
    fn from(offset: Offset) -> Self {
        Self {
            request: Some(numaflow_grpc::clients::source::ack_request::Request {
                offset: Some(numaflow_grpc::clients::source::Offset {
                    offset: BASE64_STANDARD
                        .decode(offset.offset)
                        .expect("we control the encoding, so this should never fail"),
                    partition_id: offset.partition_id,
                }),
            }),
            handshake: None,
        }
    }
}

impl Message {
    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::Serde(e.to_string()))
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| Error::Serde(e.to_string()))
    }
}

/// Convert the [`Message`] to [`SourceTransformRequest`]
impl From<Message> for SourceTransformRequest {
    fn from(message: Message) -> Self {
        Self {
            request: Some(
                numaflow_grpc::clients::sourcetransformer::source_transform_request::Request {
                    id: message.id,
                    keys: message.keys,
                    value: message.value,
                    event_time: prost_timestamp_from_utc(message.event_time),
                    watermark: None,
                    headers: message.headers,
                },
            ),
            handshake: None,
        }
    }
}

/// Convert [`read_response::Result`] to [`Message`]
impl TryFrom<read_response::Result> for Message {
    type Error = Error;

    fn try_from(result: read_response::Result) -> Result<Self> {
        let source_offset = match result.offset {
            Some(o) => Offset {
                offset: BASE64_STANDARD.encode(o.offset),
                partition_id: o.partition_id,
            },
            None => return Err(Error::Source("Offset not found".to_string())),
        };

        Ok(Message {
            keys: result.keys,
            value: result.payload,
            offset: source_offset.clone(),
            event_time: utc_from_timestamp(result.event_time),
            id: format!("{}-{}", source_offset.partition_id, source_offset.offset),
            headers: result.headers,
        })
    }
}

/// Convert [`Message`] to [`proto::SinkRequest`]
impl From<Message> for SinkRequest {
    fn from(message: Message) -> Self {
        Self {
            request: Some(Request {
                keys: message.keys,
                value: message.value,
                event_time: prost_timestamp_from_utc(message.event_time),
                watermark: None,
                id: message.id,
                headers: message.headers,
            }),
            status: None,
            handshake: None,
        }
    }
}

/// Sink's status for each [Message] written to Sink.
#[derive(PartialEq)]
pub(crate) enum ResponseStatusFromSink {
    /// Successfully wrote to the Sink.
    Success,
    /// Failed with error message.
    Failed(String),
    /// Write to FallBack Sink.
    Fallback,
}

/// Sink will give a response per [Message].
pub(crate) struct ResponseFromSink {
    /// Unique id per [Message]. We need to track per [Message] status.
    pub(crate) id: String,
    /// Status of the "sink" operation per [Message].
    pub(crate) status: ResponseStatusFromSink,
}

impl From<ResponseFromSink> for SinkResponse {
    fn from(value: ResponseFromSink) -> Self {
        let (status, err_msg) = match value.status {
            ResponseStatusFromSink::Success => (Success, "".to_string()),
            ResponseStatusFromSink::Failed(err) => (Failure, err.to_string()),
            ResponseStatusFromSink::Fallback => (Fallback, "".to_string()),
        };

        Self {
            result: Some(sink_response::Result {
                id: value.id,
                status: status as i32,
                err_msg,
            }),
            handshake: None,
            status: None,
        }
    }
}

impl TryFrom<SinkResponse> for ResponseFromSink {
    type Error = Error;

    fn try_from(value: SinkResponse) -> Result<Self> {
        let value = value
            .result
            .ok_or(Error::Sink("result is empty".to_string()))?;

        let status = match value.status() {
            Success => ResponseStatusFromSink::Success,
            Failure => ResponseStatusFromSink::Failed(value.err_msg),
            Fallback => ResponseStatusFromSink::Fallback,
        };

        Ok(Self {
            id: value.id,
            status,
        })
    }
}
