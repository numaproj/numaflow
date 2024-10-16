use crate::shared::utils::{prost_timestamp_from_utc, utc_from_timestamp};
use crate::Error;
use crate::Result;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use numaflow_grpc::clients::sink::sink_request::Request;
use numaflow_grpc::clients::sink::Status::{Failure, Fallback, Success};
use numaflow_grpc::clients::sink::{sink_response, SinkRequest, SinkResponse};
use numaflow_grpc::clients::source::{read_response, AckRequest};
use numaflow_grpc::clients::sourcetransformer::SourceTransformRequest;
use prost::Message as ProtoMessage;
use serde::{Deserialize, Serialize};
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::fmt;

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
    pub(crate) id: MessageID,
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

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.offset, self.partition_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MessageID {
    pub(crate) vertex_name: String,
    pub(crate) offset: String,
    pub(crate) index: i32,
}

impl MessageID {
    fn new(vertex_name: String, offset: String, index: i32) -> Self {
        Self {
            vertex_name,
            offset,
            index,
        }
    }
}

impl fmt::Display for MessageID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}-{}", self.vertex_name, self.offset, self.index)
    }
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

impl TryFrom<Message> for Vec<u8> {
    type Error = Error;

    fn try_from(message: Message) -> std::result::Result<Self, Self::Error> {
        let proto_message = numaflow_grpc::objects::isb::Message {
            header: Some(numaflow_grpc::objects::isb::Header {
                message_info: Some(numaflow_grpc::objects::isb::MessageInfo {
                    event_time: prost_timestamp_from_utc(message.event_time),
                    is_late: false, // Set this according to your logic
                }),
                kind: numaflow_grpc::objects::isb::MessageKind::Data as i32,
                id: Some(numaflow_grpc::objects::isb::MessageId {
                    vertex_name: Default::default(),
                    offset: message.offset.to_string(),
                    index: 0,
                }),
                keys: message.keys.clone(),
                headers: message.headers.clone(),
            }),
            body: Some(numaflow_grpc::objects::isb::Body {
                payload: message.value.clone(),
            }),
        };

        let mut buf = Vec::new();
        proto_message
            .encode(&mut buf)
            .map_err(|e| Error::Proto(e.to_string()))?;
        Ok(buf)
    }
}

impl TryFrom<Vec<u8>> for Message {
    type Error = Error;

    fn try_from(bytes: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        let proto_message = numaflow_grpc::objects::isb::Message::decode(Bytes::from(bytes))
            .map_err(|e| Error::Proto(e.to_string()))?;

        let header = proto_message
            .header
            .ok_or(Error::Proto("Missing header".to_string()))?;
        let body = proto_message
            .body
            .ok_or(Error::Proto("Missing body".to_string()))?;
        let message_info = header
            .message_info
            .ok_or(Error::Proto("Missing message_info".to_string()))?;
        let id = header.id.ok_or(Error::Proto("Missing id".to_string()))?;

        Ok(Message {
            keys: header.keys,
            value: body.payload,
            offset: Offset {
                offset: id.offset.clone(),
                partition_id: 0, // Set this according to your logic
            },
            event_time: utc_from_timestamp(message_info.event_time),
            id: MessageID {
                vertex_name: id.vertex_name,
                offset: id.offset,
                index: id.index,
            },
            headers: header.headers,
        })
    }
}

/// Convert the [`Message`] to [`SourceTransformRequest`]
impl From<Message> for SourceTransformRequest {
    fn from(message: Message) -> Self {
        Self {
            request: Some(
                numaflow_grpc::clients::sourcetransformer::source_transform_request::Request {
                    id: message.id.to_string(),
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
            id: MessageID {
                vertex_name: "".to_string(),
                offset: source_offset.offset,
                index: 0,
            },
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
                id: message.id.to_string(),
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
