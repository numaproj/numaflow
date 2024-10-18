use std::cmp::PartialEq;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::{env, fmt};

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use numaflow_pb::clients::sink::sink_request::Request;
use numaflow_pb::clients::sink::Status::{Failure, Fallback, Success};
use numaflow_pb::clients::sink::{sink_response, SinkRequest, SinkResponse};
use numaflow_pb::clients::source::{read_response, AckRequest};
use numaflow_pb::clients::sourcetransformer::SourceTransformRequest;
use prost::Message as ProtoMessage;
use serde::{Deserialize, Serialize};

use crate::shared::utils::{prost_timestamp_from_utc, utc_from_timestamp};
use crate::Error;
use crate::Result;

const NUMAFLOW_MONO_VERTEX_NAME: &str = "NUMAFLOW_MONO_VERTEX_NAME";
const NUMAFLOW_VERTEX_NAME: &str = "NUMAFLOW_VERTEX_NAME";
const NUMAFLOW_REPLICA: &str = "NUMAFLOW_REPLICA";

static VERTEX_NAME: OnceLock<String> = OnceLock::new();

pub(crate) fn get_vertex_name() -> &'static str {
    VERTEX_NAME.get_or_init(|| {
        env::var(NUMAFLOW_MONO_VERTEX_NAME)
            .or_else(|_| env::var(NUMAFLOW_VERTEX_NAME))
            .unwrap_or_default()
    })
}

static VERTEX_REPLICA: OnceLock<u16> = OnceLock::new();

// fetch the vertex replica information from the environment variable
pub(crate) fn get_vertex_replica() -> &'static u16 {
    VERTEX_REPLICA.get_or_init(|| {
        env::var(NUMAFLOW_REPLICA)
            .unwrap_or_default()
            .parse()
            .unwrap_or_default()
    })
}

/// A message that is sent from the source to the sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Message {
    /// keys of the message
    pub(crate) keys: Vec<String>,
    /// actual payload of the message
    pub(crate) value: Vec<u8>,
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

/// IntOffset is integer based offset enum type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntOffset {
    offset: u64,
    partition_idx: u16,
}

impl IntOffset {
    pub fn new(seq: u64, partition_idx: u16) -> Self {
        Self {
            offset: seq,
            partition_idx,
        }
    }
}

impl IntOffset {
    fn sequence(&self) -> Result<u64> {
        Ok(self.offset)
    }

    fn partition_idx(&self) -> u16 {
        self.partition_idx
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
    offset: String,
    partition_idx: u16,
}

impl StringOffset {
    pub fn new(seq: String, partition_idx: u16) -> Self {
        Self {
            offset: seq,
            partition_idx,
        }
    }
}

impl StringOffset {
    fn sequence(&self) -> Result<u64> {
        Ok(self.offset.parse().unwrap())
    }

    fn partition_idx(&self) -> u16 {
        self.partition_idx
    }
}

impl fmt::Display for StringOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.offset, self.partition_idx)
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

impl From<numaflow_pb::objects::isb::MessageId> for MessageID {
    fn from(id: numaflow_pb::objects::isb::MessageId) -> Self {
        Self {
            vertex_name: id.vertex_name,
            offset: id.offset,
            index: id.index,
        }
    }
}

impl From<MessageID> for numaflow_pb::objects::isb::MessageId {
    fn from(id: MessageID) -> Self {
        Self {
            vertex_name: id.vertex_name,
            offset: id.offset,
            index: id.index,
        }
    }
}

impl fmt::Display for MessageID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}-{}", self.vertex_name, self.offset, self.index)
    }
}

impl TryFrom<Offset> for AckRequest {
    type Error = Error;

    fn try_from(value: Offset) -> std::result::Result<Self, Self::Error> {
        match value {
            Offset::Int(_) => Err(Error::Source("IntOffset not supported".to_string())),
            Offset::String(o) => Ok(Self {
                request: Some(numaflow_pb::clients::source::ack_request::Request {
                    offset: Some(numaflow_pb::clients::source::Offset {
                        offset: BASE64_STANDARD
                            .decode(o.offset)
                            .expect("we control the encoding, so this should never fail"),
                        partition_id: o.partition_idx as i32,
                    }),
                }),
                handshake: None,
            }),
        }
    }
}

impl TryFrom<Message> for Vec<u8> {
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
                keys: message.keys.clone(),
                headers: message.headers.clone(),
            }),
            body: Some(numaflow_pb::objects::isb::Body {
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
        let proto_message = numaflow_pb::objects::isb::Message::decode(Bytes::from(bytes))
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
            Some(o) => Offset::String(StringOffset {
                offset: BASE64_STANDARD.encode(o.offset),
                partition_idx: o.partition_id as u16,
            }),
            None => return Err(Error::Source("Offset not found".to_string())),
        };

        Ok(Message {
            keys: result.keys,
            value: result.payload,
            offset: Some(source_offset.clone()),
            event_time: utc_from_timestamp(result.event_time),
            id: MessageID {
                vertex_name: get_vertex_name().to_string(),
                offset: source_offset.to_string(),
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::TimeZone;
    use numaflow_pb::clients::sink::sink_response::Result as SinkResult;
    use numaflow_pb::clients::source::Offset as SourceOffset;
    use numaflow_pb::objects::isb::{
        Body, Header, Message as ProtoMessage, MessageId, MessageInfo,
    };

    use super::*;

    #[test]
    fn test_offset_display() {
        let offset = Offset::String(StringOffset {
            offset: "123".to_string(),
            partition_idx: 1,
        });
        assert_eq!(format!("{}", offset), "123-1");
    }

    #[test]
    fn test_message_id_display() {
        let message_id = MessageID {
            vertex_name: "vertex".to_string(),
            offset: "123".to_string(),
            index: 0,
        };
        assert_eq!(format!("{}", message_id), "vertex-123-0");
    }

    #[test]
    fn test_offset_to_ack_request() {
        let offset = Offset::String(StringOffset {
            offset: BASE64_STANDARD.encode("123"),
            partition_idx: 1,
        });
        let ack_request: AckRequest = offset.try_into().unwrap();
        assert_eq!(ack_request.request.unwrap().offset.unwrap().partition_id, 1);

        let offset = Offset::Int(IntOffset::new(42, 1));
        let result: Result<AckRequest> = offset.try_into();

        // Assert that the conversion results in an error
        assert!(result.is_err());

        if let Err(e) = result {
            assert_eq!(e.to_string(), "Source Error - IntOffset not supported");
        }
    }

    #[test]
    fn test_message_to_vec_u8() {
        let message = Message {
            keys: vec!["key1".to_string()],
            value: vec![1, 2, 3],
            offset: Some(Offset::String(StringOffset {
                offset: "123".to_string(),
                partition_idx: 0,
            })),
            event_time: Utc.timestamp_opt(1627846261, 0).unwrap(),
            id: MessageID {
                vertex_name: "vertex".to_string(),
                offset: "123".to_string(),
                index: 0,
            },
            headers: HashMap::new(),
        };

        let result: Result<Vec<u8>> = message.clone().try_into();
        assert!(result.is_ok());

        let proto_message = ProtoMessage {
            header: Some(Header {
                message_info: Some(MessageInfo {
                    event_time: prost_timestamp_from_utc(message.event_time),
                    is_late: false,
                }),
                kind: numaflow_pb::objects::isb::MessageKind::Data as i32,
                id: Some(message.id.into()),
                keys: message.keys.clone(),
                headers: message.headers.clone(),
            }),
            body: Some(Body {
                payload: message.value.clone(),
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

        let mut buf = Vec::new();
        prost::Message::encode(&proto_message, &mut buf).unwrap();

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
            value: vec![1, 2, 3],
            offset: Some(Offset::String(StringOffset {
                offset: "123".to_string(),
                partition_idx: 0,
            })),
            event_time: Utc.timestamp_opt(1627846261, 0).unwrap(),
            id: MessageID {
                vertex_name: "vertex".to_string(),
                offset: "123".to_string(),
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
            value: vec![1, 2, 3],
            offset: Some(Offset::String(StringOffset {
                offset: "123".to_string(),
                partition_idx: 0,
            })),
            event_time: Utc.timestamp_opt(1627846261, 0).unwrap(),
            id: MessageID {
                vertex_name: "vertex".to_string(),
                offset: "123".to_string(),
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

        let sink_response: SinkResponse = response.into();
        assert_eq!(sink_response.result.unwrap().status, Success as i32);
    }

    #[test]
    fn test_sink_response_to_response_from_sink() {
        let sink_response = SinkResponse {
            result: Some(SinkResult {
                id: "123".to_string(),
                status: Success as i32,
                err_msg: "".to_string(),
            }),
            handshake: None,
            status: None,
        };

        let response: Result<ResponseFromSink> = sink_response.try_into();
        assert!(response.is_ok());

        let response = response.unwrap();
        assert_eq!(response.id, "123");
        assert_eq!(response.status, ResponseStatusFromSink::Success);
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
        let message_id = MessageID::new("vertex".to_string(), "123".to_string(), 0);
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
        let result: Result<AckRequest> = offset.try_into();
        assert!(result.is_ok());
        let ack_request = result.unwrap();
        assert_eq!(ack_request.request.unwrap().offset.unwrap().partition_id, 1);

        // Test conversion from Offset to AckRequest for IntOffset (should fail)
        let offset = Offset::Int(IntOffset::new(42, 1));
        let result: Result<AckRequest> = offset.try_into();
        assert!(result.is_err());
    }
}
