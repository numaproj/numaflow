//! Write Ahead Log for both Aligned and Unaligned Reduce Operation. The WAL is to persist the data
//! we read from the ISB and store it until the processing is complete.

use crate::message::{IntOffset, Message, Offset};
use crate::reduce::error::Error;
use crate::shared::grpc::{prost_timestamp_from_utc, utc_from_timestamp};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;

/// A WAL Segment.
pub(crate) mod segment;

/// All the errors WAL could face.
pub(crate) mod error;

#[derive(Debug, Clone)]
pub(crate) struct WalMessage {
    pub(crate) message: Message,
}

// Convert Message to Bytes used to persist in WAL
impl TryFrom<WalMessage> for Bytes {
    type Error = Error;

    fn try_from(wal_message: WalMessage) -> Result<Self, Self::Error> {
        let message = wal_message.message;

        let Offset::Int(int_offset) = message.offset else {
            return Err(Error::Other("Invalid offset".to_string()));
        };

        let proto_message = numaflow_pb::objects::isb::ReadMessage {
            message: Some(numaflow_pb::objects::isb::Message {
                header: Some(numaflow_pb::objects::isb::Header {
                    message_info: Some(numaflow_pb::objects::isb::MessageInfo {
                        event_time: Some(prost_timestamp_from_utc(message.event_time)),
                        is_late: false,
                    }),
                    kind: message.typ.into(),
                    id: Some(message.id.into()),
                    keys: message.keys.to_vec(),
                    headers: message.headers,
                }),
                body: Some(numaflow_pb::objects::isb::Body {
                    payload: message.value.to_vec(),
                }),
            }),
            read_offset: int_offset.offset,
            watermark: message.watermark.map(prost_timestamp_from_utc),
            metadata: None,
        };

        let mut buf = BytesMut::new();
        prost::Message::encode(&proto_message, &mut buf)
            .map_err(|e| Error::Other(e.to_string()))?;

        Ok(buf.freeze())
    }
}

// Convert Bytes to Message, used while reading from WAL
impl TryFrom<Bytes> for WalMessage {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let proto_read_message: numaflow_pb::objects::isb::ReadMessage =
            prost::Message::decode(value.as_ref()).map_err(|e| Error::Other(e.to_string()))?;

        let proto_message = proto_read_message
            .message
            .ok_or_else(|| Error::Other("Missing inner message".to_string()))?;

        let header = proto_message
            .header
            .ok_or_else(|| Error::Other("Missing header".to_string()))?;
        let body = proto_message
            .body
            .ok_or_else(|| Error::Other("Missing body".to_string()))?;

        let msg = Message {
            typ: header.kind.into(),
            keys: Arc::from(header.keys),
            tags: None,
            value: Bytes::from(body.payload),
            offset: Offset::Int(IntOffset::new(proto_read_message.read_offset, 0)),
            event_time: header
                .message_info
                .expect("info can't be empty")
                .event_time
                .map(utc_from_timestamp)
                .expect("event time should be present"),
            watermark: proto_read_message.watermark.map(utc_from_timestamp),
            id: header.id.map(Into::into).unwrap_or_default(),
            headers: header.headers,
            metadata: None,
        };

        Ok(WalMessage { message: msg })
    }
}

impl From<WalMessage> for Message {
    fn from(value: WalMessage) -> Self {
        value.message
    }
}

impl From<Message> for WalMessage {
    fn from(value: Message) -> Self {
        WalMessage { message: value }
    }
}
