use std::cmp::PartialEq;
use std::collections::HashMap;
use std::fmt;

use async_nats::HeaderValue;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use numaflow_pb::clients::sink::sink_request::Request;
use numaflow_pb::clients::sink::Status::{Failure, Fallback, Success};
use numaflow_pb::clients::sink::{sink_response, SinkRequest};
use numaflow_pb::clients::source::read_response;
use numaflow_pb::clients::sourcetransformer::SourceTransformRequest;
use prost::Message as ProtoMessage;
use serde::{Deserialize, Serialize};

use crate::shared::grpc::prost_timestamp_from_utc;
use crate::shared::grpc::utc_from_timestamp;
use crate::Result;
use crate::{config, Error};

const DROP: &str = "U+005C__DROP__";

/// A message that is sent from the source to the sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Message {
    // FIXME: Arc<[Bytes]>
    /// keys of the message
    pub(crate) keys: Vec<String>,
    /// tags of the message
    pub(crate) tags: Option<Vec<String>>,
    /// actual payload of the message
    pub(crate) value: Bytes,
    /// offset of the message, it is optional because offset is only
    /// available when we read the message, and we don't persist the
    /// offset in the ISB.
    pub(crate) offset: Option<Offset>,
    /// event time of the message
    pub(crate) event_time: DateTime<Utc>,
    /// id of the message
    pub(crate) id: MessageID,
    /// headers of the message
    pub(crate) headers: HashMap<String, String>,
}

/// Offset of the message which will be used to acknowledge the message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Offset {
    Int(IntOffset),
    String(StringOffset),
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Offset::Int(offset) => write!(f, "{}", offset),
            Offset::String(offset) => write!(f, "{}", offset),
        }
    }
}

impl TryFrom<async_nats::Message> for Message {
    type Error = Error;

    fn try_from(message: async_nats::Message) -> std::result::Result<Self, Self::Error> {
        let payload = message.payload;
        let headers: HashMap<String, String> = message
            .headers
            .unwrap_or_default()
            .iter()
            .map(|(key, value)| {
                (
                    key.to_string(),
                    value.first().unwrap_or(&HeaderValue::from("")).to_string(),
                )
            })
            .collect();
        // FIXME(cr): we should not be using subject. keys are in the payload
        let keys = message.subject.split('.').map(|s| s.to_string()).collect();
        let event_time = Utc::now();
        let offset = None;
        let id = MessageID {
            vertex_name: config::get_vertex_name().to_string().into(),
            offset: "0".to_string().into(),
            index: 0,
        };

        Ok(Self {
            keys,
            tags: None,
            value: payload,
            offset,
            event_time,
            id,
            headers,
        })
    }
}

impl Message {
    // Check if the message should be dropped.
    pub(crate) fn dropped(&self) -> bool {
        self.tags
            .as_ref()
            .map_or(false, |tags| tags.contains(&DROP.to_string()))
    }
}

/// IntOffset is integer based offset enum type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntOffset {
    pub(crate) offset: u64,
    pub(crate) partition_idx: u16,
}

impl IntOffset {
    pub fn new(seq: u64, partition_idx: u16) -> Self {
        Self {
            offset: seq,
            partition_idx,
        }
    }
}

impl fmt::Display for IntOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.offset, self.partition_idx)
    }
}

/// StringOffset is string based offset enum type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StringOffset {
    offset: Bytes,
    partition_idx: u16,
}

impl StringOffset {
    pub fn new(seq: String, partition_idx: u16) -> Self {
        Self {
            offset: seq.into(),
            partition_idx,
        }
    }
}

impl fmt::Display for StringOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}",
            std::str::from_utf8(&self.offset).expect("it should be valid utf-8"),
            self.partition_idx
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum ReadAck {
    /// Message was successfully processed.
    Ack,
    /// Message will not be processed now and processing can move onto the next message, NAK’d message will be retried.
    Nak,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MessageID {
    pub(crate) vertex_name: Bytes,
    pub(crate) offset: Bytes,
    pub(crate) index: i32,
}

impl From<numaflow_pb::objects::isb::MessageId> for MessageID {
    fn from(id: numaflow_pb::objects::isb::MessageId) -> Self {
        Self {
            vertex_name: id.vertex_name.into(),
            offset: id.offset.into(),
            index: id.index,
        }
    }
}

impl From<MessageID> for numaflow_pb::objects::isb::MessageId {
    fn from(id: MessageID) -> Self {
        Self {
            vertex_name: String::from_utf8_lossy(&id.vertex_name).to_string(),
            offset: String::from_utf8_lossy(&id.offset).to_string(),
            index: id.index,
        }
    }
}
impl fmt::Display for MessageID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}-{}",
            std::str::from_utf8(&self.vertex_name).expect("it should be valid utf-8"),
            std::str::from_utf8(&self.offset).expect("it should be valid utf-8"),
            self.index
        )
    }
}

impl TryFrom<Offset> for numaflow_pb::clients::source::Offset {
    type Error = Error;

    fn try_from(offset: Offset) -> std::result::Result<Self, Self::Error> {
        match offset {
            Offset::Int(_) => Err(Error::Source("IntOffset not supported".to_string())),
            Offset::String(o) => Ok(numaflow_pb::clients::source::Offset {
                offset: BASE64_STANDARD
                    .decode(o.offset)
                    .expect("we control the encoding, so this should never fail"),
                partition_id: o.partition_idx as i32,
            }),
        }
    }
}

impl TryFrom<Message> for BytesMut {
    type Error = Error;

    fn try_from(message: Message) -> std::result::Result<Self, Self::Error> {
        let proto_message = numaflow_pb::objects::isb::Message {
            header: Some(numaflow_pb::objects::isb::Header {
                message_info: Some(numaflow_pb::objects::isb::MessageInfo {
                    event_time: prost_timestamp_from_utc(message.event_time),
                    is_late: false, // Set this according to your logic
                }),
                kind: numaflow_pb::objects::isb::MessageKind::Data as i32,
                id: Some(message.id.into()),
                keys: message.keys,
                headers: message.headers,
            }),
            body: Some(numaflow_pb::objects::isb::Body {
                payload: message.value.to_vec(),
            }),
        };

        let mut buf = BytesMut::new();
        proto_message
            .encode(&mut buf)
            .map_err(|e| Error::Proto(e.to_string()))?;
        Ok(buf)
    }
}

impl TryFrom<Bytes> for Message {
    type Error = Error;

    fn try_from(bytes: Bytes) -> std::result::Result<Self, Self::Error> {
        let proto_message = numaflow_pb::objects::isb::Message::decode(bytes)
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
            tags: None,
            value: body.payload.into(),
            offset: None,
            event_time: utc_from_timestamp(message_info.event_time),
            id: id.into(),
            headers: header.headers,
        })
    }
}

/// Convert the [`Message`] to [`SourceTransformRequest`]
impl From<Message> for SourceTransformRequest {
    fn from(message: Message) -> Self {
        Self {
            request: Some(
                numaflow_pb::clients::sourcetransformer::source_transform_request::Request {
                    id: message.id.to_string(),
                    keys: message.keys,
                    value: message.value.to_vec(),
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
            Some(o) => Offset::String(StringOffset {
                offset: BASE64_STANDARD.encode(o.offset).into(),
                partition_idx: o.partition_id as u16,
            }),
            None => return Err(Error::Source("Offset not found".to_string())),
        };

        Ok(Message {
            keys: result.keys,
            tags: None,
            value: result.payload.into(),
            offset: Some(source_offset.clone()),
            event_time: utc_from_timestamp(result.event_time),
            id: MessageID {
                vertex_name: config::get_vertex_name().to_string().into(),
                offset: source_offset.to_string().into(),
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
                value: message.value.to_vec(),
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
#[derive(PartialEq, Debug)]
pub(crate) enum ResponseStatusFromSink {
    /// Successfully wrote to the Sink.
    Success,
    /// Failed with error message.
    Failed(String),
    /// Write to FallBack Sink.
    Fallback,
}

/// Sink will give a response per [Message].
#[derive(Debug, PartialEq)]
pub(crate) struct ResponseFromSink {
    /// Unique id per [Message]. We need to track per [Message] status.
    pub(crate) id: String,
    /// Status of the "sink" operation per [Message].
    pub(crate) status: ResponseStatusFromSink,
}

impl From<ResponseFromSink> for sink_response::Result {
    fn from(value: ResponseFromSink) -> Self {
        let (status, err_msg) = match value.status {
            ResponseStatusFromSink::Success => (Success, "".to_string()),
            ResponseStatusFromSink::Failed(err) => (Failure, err.to_string()),
            ResponseStatusFromSink::Fallback => (Fallback, "".to_string()),
        };

        Self {
            id: value.id,
            status: status as i32,
            err_msg,
        }
    }
}

impl From<sink_response::Result> for ResponseFromSink {
    fn from(value: sink_response::Result) -> Self {
        let status = match value.status() {
            Success => ResponseStatusFromSink::Success,
            Failure => ResponseStatusFromSink::Failed(value.err_msg),
            Fallback => ResponseStatusFromSink::Fallback,
        };
        Self {
            id: value.id,
            status,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::TimeZone;
    use numaflow_pb::clients::sink::sink_response::Result as SinkResult;
    use numaflow_pb::clients::sink::SinkResponse;
    use numaflow_pb::clients::source::Offset as SourceOffset;
    use numaflow_pb::objects::isb::{
        Body, Header, Message as ProtoMessage, MessageId, MessageInfo,
    };

    use super::*;

    #[test]
    fn test_offset_display() {
        let offset = Offset::String(StringOffset {
            offset: "123".to_string().into(),
            partition_idx: 1,
        });
        assert_eq!(format!("{}", offset), "123-1");
    }

    #[test]
    fn test_message_id_display() {
        let message_id = MessageID {
            vertex_name: "vertex".to_string().into(),
            offset: "123".to_string().into(),
            index: 0,
        };
        assert_eq!(format!("{}", message_id), "vertex-123-0");
    }

    #[test]
    fn test_message_to_vec_u8() {
        let message = Message {
            keys: vec!["key1".to_string()],
            tags: None,
            value: vec![1, 2, 3].into(),
            offset: Some(Offset::String(StringOffset {
                offset: "123".to_string().into(),
                partition_idx: 0,
            })),
            event_time: Utc.timestamp_opt(1627846261, 0).unwrap(),
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "123".to_string().into(),
                index: 0,
            },
            headers: HashMap::new(),
        };

        let result: Result<BytesMut> = message.clone().try_into();
        assert!(result.is_ok());

        let proto_message = ProtoMessage {
            header: Some(Header {
                message_info: Some(MessageInfo {
                    event_time: prost_timestamp_from_utc(message.event_time),
                    is_late: false,
                }),
                kind: numaflow_pb::objects::isb::MessageKind::Data as i32,
                id: Some(message.id.into()),
                keys: message.keys,
                headers: message.headers,
            }),
            body: Some(Body {
                payload: message.value.clone().into(),
            }),
        };

        let mut buf = Vec::new();
        prost::Message::encode(&proto_message, &mut buf).unwrap();
        assert_eq!(result.unwrap(), buf);
    }

    #[test]
    fn test_vec_u8_to_message() {
        let proto_message = ProtoMessage {
            header: Some(Header {
                message_info: Some(MessageInfo {
                    event_time: prost_timestamp_from_utc(Utc.timestamp_opt(1627846261, 0).unwrap()),
                    is_late: false,
                }),
                kind: numaflow_pb::objects::isb::MessageKind::Data as i32,
                id: Some(MessageId {
                    vertex_name: "vertex".to_string(),
                    offset: "123".to_string(),
                    index: 0,
                }),
                keys: vec!["key1".to_string()],
                headers: HashMap::new(),
            }),
            body: Some(Body {
                payload: vec![1, 2, 3],
            }),
        };

        let mut buf = BytesMut::new();
        prost::Message::encode(&proto_message, &mut buf).unwrap();
        let buf = buf.freeze();

        let result: Result<Message> = buf.try_into();
        assert!(result.is_ok());

        let message = result.unwrap();
        assert_eq!(message.keys, vec!["key1".to_string()]);
        assert_eq!(message.value, vec![1, 2, 3]);
        assert_eq!(
            message.event_time,
            Utc.timestamp_opt(1627846261, 0).unwrap()
        );
    }

    #[test]
    fn test_message_to_source_transform_request() {
        let message = Message {
            keys: vec!["key1".to_string()],
            tags: None,
            value: vec![1, 2, 3].into(),
            offset: Some(Offset::String(StringOffset {
                offset: "123".to_string().into(),
                partition_idx: 0,
            })),
            event_time: Utc.timestamp_opt(1627846261, 0).unwrap(),
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "123".to_string().into(),
                index: 0,
            },
            headers: HashMap::new(),
        };

        let request: SourceTransformRequest = message.into();
        assert!(request.request.is_some());
    }

    #[test]
    fn test_read_response_result_to_message() {
        let result = read_response::Result {
            payload: vec![1, 2, 3],
            offset: Some(SourceOffset {
                offset: BASE64_STANDARD.encode("123").into_bytes(),
                partition_id: 0,
            }),
            event_time: Some(
                prost_timestamp_from_utc(Utc.timestamp_opt(1627846261, 0).unwrap()).unwrap(),
            ),
            keys: vec!["key1".to_string()],
            headers: HashMap::new(),
        };

        let message: Result<Message> = result.try_into();
        assert!(message.is_ok());

        let message = message.unwrap();
        assert_eq!(message.keys, vec!["key1".to_string()]);
        assert_eq!(message.value, vec![1, 2, 3]);
        assert_eq!(
            message.event_time,
            Utc.timestamp_opt(1627846261, 0).unwrap()
        );
    }

    #[test]
    fn test_message_to_sink_request() {
        let message = Message {
            keys: vec!["key1".to_string()],
            tags: None,
            value: vec![1, 2, 3].into(),
            offset: Some(Offset::String(StringOffset {
                offset: "123".to_string().into(),
                partition_idx: 0,
            })),
            event_time: Utc.timestamp_opt(1627846261, 0).unwrap(),
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "123".to_string().into(),
                index: 0,
            },
            headers: HashMap::new(),
        };

        let request: SinkRequest = message.into();
        assert!(request.request.is_some());
    }

    #[test]
    fn test_response_from_sink_to_sink_response() {
        let response = ResponseFromSink {
            id: "123".to_string(),
            status: ResponseStatusFromSink::Success,
        };

        let sink_result: sink_response::Result = response.into();
        assert_eq!(sink_result.status, Success as i32);
    }

    #[test]
    fn test_sink_response_to_response_from_sink() {
        let sink_response = SinkResponse {
            results: vec![SinkResult {
                id: "123".to_string(),
                status: Success as i32,
                err_msg: "".to_string(),
            }],
            handshake: None,
            status: None,
        };

        let results: Vec<ResponseFromSink> = sink_response
            .results
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        assert!(!results.is_empty());

        assert_eq!(results.get(0).unwrap().id, "123");
        assert_eq!(
            results.get(0).unwrap().status,
            ResponseStatusFromSink::Success
        );
    }

    #[test]
    fn test_message_id_from_proto() {
        let proto_id = MessageId {
            vertex_name: "vertex".to_string(),
            offset: "123".to_string(),
            index: 0,
        };
        let message_id: MessageID = proto_id.into();
        assert_eq!(message_id.vertex_name, "vertex");
        assert_eq!(message_id.offset, "123");
        assert_eq!(message_id.index, 0);
    }

    #[test]
    fn test_message_id_to_proto() {
        let message_id = MessageID {
            vertex_name: "vertex".to_string().into(),
            offset: "123".to_string().into(),
            index: 0,
        };
        let proto_id: MessageId = message_id.into();
        assert_eq!(proto_id.vertex_name, "vertex");
        assert_eq!(proto_id.offset, "123");
        assert_eq!(proto_id.index, 0);
    }

    #[test]
    fn test_offset_cases() {
        let int_offset = IntOffset::new(42, 1);
        assert_eq!(int_offset.offset, 42);
        assert_eq!(int_offset.partition_idx, 1);
        assert_eq!(format!("{}", int_offset), "42-1");

        let string_offset = StringOffset::new("42".to_string(), 1);
        assert_eq!(string_offset.offset, "42");
        assert_eq!(string_offset.partition_idx, 1);
        assert_eq!(format!("{}", string_offset), "42-1");

        let offset_int = Offset::Int(int_offset);
        assert_eq!(format!("{}", offset_int), "42-1");

        let offset_string = Offset::String(string_offset);
        assert_eq!(format!("{}", offset_string), "42-1");

        // Test conversion from Offset to AckRequest for StringOffset
        let offset = Offset::String(StringOffset::new(BASE64_STANDARD.encode("42"), 1));
        let offset: Result<numaflow_pb::clients::source::Offset> = offset.try_into();
        assert_eq!(offset.unwrap().partition_id, 1);

        // Test conversion from Offset to AckRequest for IntOffset (should fail)
        let offset = Offset::Int(IntOffset::new(42, 1));
        let result: Result<numaflow_pb::clients::source::Offset> = offset.try_into();
        assert!(result.is_err());
    }
}
