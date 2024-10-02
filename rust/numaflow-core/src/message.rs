use std::collections::HashMap;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chrono::{DateTime, Utc};

use crate::error::Error;
use crate::monovertex::sink_pb::sink_request::Request;
use crate::monovertex::sink_pb::SinkRequest;
use crate::monovertex::{source_pb, sourcetransform_pb};
use crate::monovertex::source_pb::{read_response, AckRequest};
use crate::monovertex::sourcetransform_pb::SourceTransformRequest;
use crate::shared::utils::{prost_timestamp_from_utc, utc_from_timestamp};

/// A message that is sent from the source to the sink.
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
pub(crate) struct Offset {
    /// unique identifier of the message
    pub(crate) offset: String,
    /// partition id of the message
    pub(crate) partition_id: i32,
}

impl From<Offset> for AckRequest {
    fn from(offset: Offset) -> Self {
        Self {
            request: Some(source_pb::ack_request::Request {
                offset: Some(source_pb::Offset {
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

/// Convert the [`Message`] to [`SourceTransformRequest`]
impl From<Message> for SourceTransformRequest {
    fn from(message: Message) -> Self {
        Self {
            request: Some(sourcetransform_pb::source_transform_request::Request {
                id: message.id,
                keys: message.keys,
                value: message.value,
                event_time: prost_timestamp_from_utc(message.event_time),
                watermark: None,
                headers: message.headers,
            }),
            handshake: None,
        }
    }
}

/// Convert [`read_response::Result`] to [`Message`]
impl TryFrom<read_response::Result> for Message {
    type Error = crate::Error;

    fn try_from(result: read_response::Result) -> Result<Self, Self::Error> {
        let source_offset = match result.offset {
            Some(o) => Offset {
                offset: BASE64_STANDARD.encode(o.offset),
                partition_id: o.partition_id,
            },
            None => return Err(Error::SourceError("Offset not found".to_string())),
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
