//! Message is the message read from the source or ISB and is passed around till it is forwarded to
//! the next vertex or the sink. The moment the message is read, it is inserted into the [crate::tracker]
//! and an ack task is spawned (for ISB we have an additional work-in-progress loop). This spawned task
//! is responsible for sending the ack/nak to the source once the [Message] has completed its life-cycle
//! (successfully processed (can include dropped) - ack, or failed - nack).
//! The spawned task exposes an [AckHandle] which implements [Drop] trait. As the message is processed,
//! and cloned (e.g., flat-map), the reference counted Handle will keep track and eventually will be
//! dropped once the all the copies of [Message] are dropped. This trigger the final ack/nak.
//!
//! [MessageHandle] is a wrapper around [Message] that holds the [AckHandle]. It should be explicitly
//! marked as success after processing via [MessageHandle::mark_success], so that it can be acked.
//! By default, it will be nacked if not marked as success.
//!
//! ## Macros
//! - [mark_success!] - Marks a single [MessageHandle] as success (consumes the handle).
//! - [mark_success_batch!] - Marks a batch of [MessageHandle]s as success (consumes the batch).

/// Marks a single [MessageHandle] as success (consumes the handle).
///
/// # Example
/// ```ignore
/// mark_success!(message_handle);
/// ```
#[macro_export]
macro_rules! mark_success {
    ($msg:expr) => {{
        $msg.mark_success();
        drop($msg);
    }};
}

/// Marks a batch of [MessageHandle]s as success (consumes the batch).
///
/// # Example
/// ```ignore
/// mark_success_batch!(message_handles);
/// ```
#[macro_export]
macro_rules! mark_success_batch {
    ($batch:expr) => {{
        for msg in $batch {
            msg.mark_success();
        }
    }};
}

use crate::Error;
use std::cmp::{Ordering, PartialEq};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crate::metadata::Metadata;
use crate::shared::grpc::prost_timestamp_from_utc;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use prost::Message as ProtoMessage;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

const DROP: &str = "U+005C__DROP__";

/// The message that is passed from the source to the sink.
/// NOTE: It is cheap to clone.
#[derive(Debug, Clone)]
pub(crate) struct Message {
    /// Type of the message that flows through the ISB.
    pub(crate) typ: MessageType,
    /// keys of the message
    pub(crate) keys: Arc<[String]>,
    /// tags of the message
    pub(crate) tags: Option<Arc<[String]>>,
    /// actual payload of the message
    pub(crate) value: Bytes,
    /// offset of the message, it is optional because offset is only
    /// available when we read the message, and we don't persist the
    /// offset in the ISB.
    pub(crate) offset: Offset,
    /// event time of the message
    pub(crate) event_time: DateTime<Utc>,
    /// watermark of the message
    pub(crate) watermark: Option<DateTime<Utc>>,
    /// id of the message
    pub(crate) id: MessageID,
    /// headers of the message
    pub(crate) headers: Arc<HashMap<String, String>>,
    /// Additional metadata that could be passed per message between the vertices.
    pub(crate) metadata: Option<Arc<Metadata>>,
    /// is_late is used to indicate if the message is a late data. Late data is data that arrives
    /// after the watermark has passed. This is set only at source.
    pub(crate) is_late: bool,
}

/// AckHandle is used to send the ack/nak to the source. It uses a reference count to track
/// the number of active references. When dropped, it sends NAK if ref_count != 0 (not all
/// references were marked as success), or ACK if ref_count == 0 (all references were marked
/// as success).
#[derive(Debug)]
pub(crate) struct AckHandle {
    pub(crate) ack_handle: Option<oneshot::Sender<ReadAck>>,
    /// Reference count to track active references. Starts at 1 when created.
    /// Incremented when cloned (via MessageHandle::clone), decremented when mark_success is called.
    /// On drop: NAK if ref_count != 0, ACK if ref_count == 0.
    pub(crate) ref_count: AtomicUsize,
}

impl AckHandle {
    /// create a new AckHandle for a message.
    pub(crate) fn new(ack_handle: oneshot::Sender<ReadAck>) -> Self {
        Self {
            ack_handle: Some(ack_handle),
            ref_count: AtomicUsize::new(1),
        }
    }
}

impl Drop for AckHandle {
    fn drop(&mut self) {
        if let Some(ack_handle) = self.ack_handle.take() {
            // NAK if ref_count is not 0 (meaning not all references were marked as success)
            if self.ref_count.load(std::sync::atomic::Ordering::SeqCst) != 0 {
                ack_handle.send(ReadAck::Nak).expect("Failed to send nak");
            } else {
                ack_handle.send(ReadAck::Ack).expect("Failed to send ack");
            }
        }
    }
}

/// MessageHandle is the message read from the ISB/Source. MessageHandle should be explicitly marked as
/// success after processing via [MessageHandle::mark_success], so that it can be acked/nacked.
/// By default, it will be nacked if not marked as success.
///
/// MessageHandle implements Clone - when cloned, the reference count is incremented, so each
/// clone must be marked as success for the original message to be ACK'd.
#[derive(Debug)]
pub(crate) struct MessageHandle {
    pub(crate) message: Message,
    pub(crate) ack_handle: Arc<AckHandle>,
}

impl Clone for MessageHandle {
    fn clone(&self) -> Self {
        self.ack_handle
            .ref_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self {
            message: self.message.clone(),
            ack_handle: Arc::clone(&self.ack_handle),
        }
    }
}

impl MessageHandle {
    /// Creates a new MessageHandle.
    pub(crate) fn new(message: Message, ack_handle: AckHandle) -> Self {
        Self {
            message,
            ack_handle: Arc::new(ack_handle),
        }
    }

    /// Creates a new MessageHandle from an existing Arc<AckHandle>.
    /// This is used when the AckHandle has already been wrapped in Arc.
    pub(crate) fn from_arc(message: Message, ack_handle: Arc<AckHandle>) -> Self {
        Self {
            message,
            ack_handle,
        }
    }

    /// Mark the message as successfully processed.
    /// This decrements the reference count. When all references are marked as success
    /// (ref_count reaches 0), the message will be ACK'd when the AckHandle is dropped.
    pub(crate) fn mark_success(&self) {
        self.ack_handle
            .ref_count
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// Get a reference to the inner message.
    pub(crate) fn message(&self) -> &Message {
        &self.message
    }

    /// Creates a new MessageHandle with a different message but sharing the same AckHandle.
    /// This is used for flatmap scenarios where one input message produces multiple output messages.
    /// Each output message shares the same AckHandle and increments the ref_count.
    /// All output messages must call mark_success() for the original message to be ACK'd.
    pub(crate) fn clone_with_message(&self, message: Message) -> Self {
        self.ack_handle
            .ref_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self {
            message,
            ack_handle: Arc::clone(&self.ack_handle),
        }
    }
}

/// Converts a [Message] into a [MessageHandle] without ack tracking.
/// This is used for newly created messages (e.g., reduce output, watermark messages)
/// that don't need to be acked back to a source.
/// The ack handle is a no-op - mark_success() and drop will have no effect.
impl From<Message> for MessageHandle {
    fn from(message: Message) -> Self {
        Self {
            message,
            ack_handle: Arc::new(AckHandle {
                ack_handle: None,
                ref_count: AtomicUsize::new(0), // Already "success" state
            }),
        }
    }
}

/// Type of the [Message].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) enum MessageType {
    /// the payload is Data
    #[default]
    Data,
    /// the payload is a control message.
    #[allow(clippy::upper_case_acronyms)]
    WMB,
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageType::Data => write!(f, "Data"),
            MessageType::WMB => write!(f, "WMB"),
        }
    }
}

// proto enum is an i32 type and WMB is defined as enum in the proto.
impl From<i32> for MessageType {
    fn from(kind: i32) -> Self {
        match kind {
            0 => MessageType::Data,
            _ => MessageType::WMB,
        }
    }
}

impl From<MessageType> for i32 {
    fn from(kind: MessageType) -> Self {
        match kind {
            MessageType::Data => 0,
            MessageType::WMB => 1,
        }
    }
}

impl Default for Message {
    fn default() -> Self {
        Self {
            keys: Arc::new([]),
            tags: None,
            value: Bytes::new(),
            offset: Default::default(),
            event_time: Utc::now(),
            watermark: None,
            id: Default::default(),
            headers: Arc::new(HashMap::new()),
            metadata: None,
            typ: Default::default(),
            is_late: false,
        }
    }
}

/// Offset of the message which will be used to acknowledge the message.
#[derive(Debug, Clone, Hash, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) enum Offset {
    Int(IntOffset),
    String(StringOffset),
}

impl Offset {
    pub(crate) fn partition_idx(&self) -> u16 {
        match self {
            Offset::Int(offset) => offset.partition_idx,
            Offset::String(offset) => offset.partition_idx,
        }
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Offset::Int(offset) => write!(f, "{offset}"),
            Offset::String(offset) => write!(f, "{offset}"),
        }
    }
}

impl PartialOrd for Offset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Offset {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Offset::Int(a), Offset::Int(b)) => a.cmp(b),
            (Offset::String(a), Offset::String(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
}

impl Default for Offset {
    fn default() -> Self {
        Offset::Int(Default::default())
    }
}

impl Message {
    // Check if the message should be dropped.
    pub(crate) fn dropped(&self) -> bool {
        self.tags
            .as_ref()
            .is_some_and(|tags| tags.contains(&DROP.to_string()))
    }
}

/// IntOffset is integer based offset enum type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub(crate) struct IntOffset {
    pub(crate) offset: i64,
    pub(crate) partition_idx: u16,
}

impl IntOffset {
    pub(crate) fn new(seq: i64, partition_idx: u16) -> Self {
        Self {
            offset: seq,
            partition_idx,
        }
    }
}

impl PartialOrd for IntOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IntOffset {
    fn cmp(&self, other: &Self) -> Ordering {
        self.offset.cmp(&other.offset)
    }
}

impl fmt::Display for IntOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.offset, self.partition_idx)
    }
}

/// StringOffset is string based offset enum type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct StringOffset {
    /// offset could be a complex base64 string.
    pub(crate) offset: Bytes,
    pub(crate) partition_idx: u16,
}

impl StringOffset {
    pub fn new(seq: String, partition_idx: u16) -> Self {
        Self {
            offset: seq.into(),
            partition_idx,
        }
    }
}

impl PartialOrd for StringOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StringOffset {
    fn cmp(&self, other: &Self) -> Ordering {
        self.offset.cmp(&other.offset)
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

/// Message ID which is used to uniquely identify a message. It cheap to clone this.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct MessageID {
    pub(crate) vertex_name: Bytes,
    pub(crate) offset: Bytes,
    /// Index is used to identify the index of the message in case of flatmap.
    pub(crate) index: i32,
}

impl Default for MessageID {
    fn default() -> Self {
        Self {
            vertex_name: Bytes::new(),
            offset: Bytes::new(),
            index: 0,
        }
    }
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

impl TryFrom<Message> for Bytes {
    type Error = Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        let b: BytesMut = value.try_into()?;
        Ok(b.freeze())
    }
}

impl TryFrom<Message> for BytesMut {
    type Error = Error;

    fn try_from(message: Message) -> Result<Self, Self::Error> {
        let proto_message = numaflow_pb::objects::isb::Message {
            header: Some(numaflow_pb::objects::isb::Header {
                message_info: Some(numaflow_pb::objects::isb::MessageInfo {
                    event_time: Some(prost_timestamp_from_utc(message.event_time)),
                    is_late: message.is_late,
                }),
                kind: message.typ.into(),
                id: Some(message.id.into()),
                keys: message.keys.to_vec(),
                headers: Arc::unwrap_or_clone(message.headers),
                metadata: message.metadata.map(|m| {
                    // When writing to JetStream, we set previous_vertex to current vertex name
                    // so that the next vertex knows who sent the message
                    let mut m = Arc::unwrap_or_clone(m);
                    m.previous_vertex = crate::config::get_vertex_name().to_string();
                    m.into()
                }),
            }),
            body: Some(numaflow_pb::objects::isb::Body {
                payload: message.value.to_vec(),
            }),
        };

        let mut buf = BytesMut::with_capacity(proto_message.encoded_len());
        proto_message
            .encode(&mut buf)
            .map_err(|e| Error::Proto(e.to_string()))?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use numaflow_pb::objects::isb::{
        Body, Header, Message as ProtoMessage, MessageId, MessageInfo,
    };

    use super::*;
    use crate::error::Result;

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
            typ: Default::default(),
            keys: Arc::from(vec!["key1".to_string()]),
            tags: None,
            value: vec![1, 2, 3].into(),
            offset: Offset::String(StringOffset {
                offset: "123".to_string().into(),
                partition_idx: 0,
            }),
            event_time: Utc.timestamp_opt(1627846261, 0).unwrap(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "123".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        let result: Result<BytesMut> = message.clone().try_into();
        assert!(result.is_ok());

        let proto_message = ProtoMessage {
            header: Some(Header {
                message_info: Some(MessageInfo {
                    event_time: Some(prost_timestamp_from_utc(message.event_time)),
                    is_late: false,
                }),
                kind: numaflow_pb::objects::isb::MessageKind::Data as i32,
                id: Some(message.id.into()),
                keys: message.keys.to_vec(),
                headers: Arc::unwrap_or_clone(message.headers),
                metadata: message.metadata.map(|m| Arc::unwrap_or_clone(m).into()),
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
    }

    #[tokio::test]
    async fn test_read_message_nack_by_default() {
        // When MessageHandle is dropped without calling mark_success, it should NAK
        let (ack_tx, ack_rx) = oneshot::channel();
        let message = Message::default();
        let ack_handle = AckHandle::new(ack_tx);
        let read_message = MessageHandle::new(message, ack_handle);

        // Drop the MessageHandle without calling mark_success
        drop(read_message);

        // Should receive NAK
        let result = ack_rx.await.unwrap();
        assert_eq!(result, ReadAck::Nak);
    }

    #[tokio::test]
    async fn test_read_message_ack_on_mark_success() {
        // When mark_success is called, it should ACK
        let (ack_tx, ack_rx) = oneshot::channel();
        let message = Message::default();
        let ack_handle = AckHandle::new(ack_tx);
        let read_message = MessageHandle::new(message, ack_handle);

        // Mark as success before dropping
        read_message.mark_success();
        drop(read_message);

        // Should receive ACK
        let result = ack_rx.await.unwrap();
        assert_eq!(result, ReadAck::Ack);
    }

    #[tokio::test]
    async fn test_read_message_clone_all_success() {
        // When cloning, all clones must be marked as success for ACK
        let (ack_tx, ack_rx) = oneshot::channel();
        let message = Message::default();
        let ack_handle = AckHandle::new(ack_tx);
        let read_message = MessageHandle::new(message, ack_handle);

        // Clone (simulating message split in map/transformer)
        let cloned = read_message.clone();

        // Mark both as success
        read_message.mark_success();
        cloned.mark_success();

        // Drop both
        drop(read_message);
        drop(cloned);

        // Should receive ACK since all were marked as success
        let result = ack_rx.await.unwrap();
        assert_eq!(result, ReadAck::Ack);
    }

    #[tokio::test]
    async fn test_read_message_clone_partial_success() {
        // When only some clones are marked as success, it should NAK
        let (ack_tx, ack_rx) = oneshot::channel();
        let message = Message::default();
        let ack_handle = AckHandle::new(ack_tx);
        let read_message = MessageHandle::new(message, ack_handle);

        // Clone
        let cloned = read_message.clone();

        // Only mark the original as success, not the clone
        read_message.mark_success();

        // Drop both
        drop(read_message);
        drop(cloned);

        // Should receive NAK since not all were marked as success
        let result = ack_rx.await.unwrap();
        assert_eq!(result, ReadAck::Nak);
    }
}
