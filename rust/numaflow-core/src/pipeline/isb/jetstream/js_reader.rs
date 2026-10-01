use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::Result;
use crate::config::get_vertex_name;
use crate::config::pipeline::isb::{CompressionType, ISBConfig, Stream};
use crate::error::Error;
use crate::message::{
    IntOffset, Message, MessageID, MessageType, NackOptions, Offset, message_from_isb_proto,
};
use crate::pipeline::isb::compression;
use crate::pipeline::isb::error::ISBError;
use async_nats::jetstream::{
    AckKind, Context, Message as JetstreamMessage, consumer, consumer::PullConsumer,
};
use prost::Message as ProtoMessage;
use serde_json::json;
use tokio_stream::StreamExt;
use tracing::warn;

/// JSWrappedMessage is a wrapper around the JetStream message that includes the
/// partition index and the vertex name.
#[derive(Debug)]
struct JSWrappedMessage {
    partition_idx: u16,
    message: async_nats::jetstream::Message,
    vertex_name: &'static str,
    /// Name of the buffer this message was read from. Part of the rewritten `MessageID`,
    /// which is what downstream writes use as the JetStream dedup key.
    source_stream: &'static str,
    compression_type: Option<CompressionType>,
}

impl JSWrappedMessage {
    async fn into_message(self) -> Result<Message> {
        let mut proto_message =
            numaflow_pb::objects::isb::Message::decode(self.message.payload.clone())
                .map_err(|e| Error::Proto(e.to_string()))?;

        let header = proto_message
            .header
            .as_ref()
            .ok_or(Error::Proto("Missing header".to_string()))?;
        let kind: MessageType = header.kind.into();
        if kind == MessageType::WMB {
            return Ok(Message {
                typ: kind,
                ..Default::default()
            });
        }

        let msg_info = self.message.info().map_err(|e| {
            Error::ISB(ISBError::Decode(format!(
                "Failed to get message info from JetStream: {e}"
            )))
        })?;

        let offset = Offset::Int(IntOffset::new(
            msg_info.stream_sequence as i64,
            self.partition_idx,
        ));

        // Decompress payload in-place before shared proto→Message mapping.
        if let Some(compression_type) = self.compression_type
            && let Some(ref mut body) = proto_message.body
        {
            body.payload = compression::decompress(compression_type, &body.payload)?;
        }

        let mut message = message_from_isb_proto(proto_message, offset.clone())?;
        // JetStream overwrites MessageID with vertex name and stream offset.
        //
        // The source buffer name is part of the id because `stream_sequence` is only unique
        // within one JetStream stream. Buffers are edge-owned, so a vertex with several
        // ingress edges reads several buffers whose sequences all start at 1 and collide.
        // Downstream writes use this id as the `Nats-Msg-Id` dedup key, so without the
        // buffer name a join would have messages from its second source silently dropped
        // by JetStream as duplicates.
        message.id = MessageID {
            vertex_name: self.vertex_name.into(),
            offset: format!("{}-{}", self.source_stream, offset).into(),
            index: 0,
        };
        Ok(message)
    }
}

/// JetStreamReader, exposes methods to read, ack, and nack messages from JetStream ISB.
#[derive(Clone)]
pub(crate) struct JetStreamReader {
    /// jetstream stream from which we are reading
    stream: Stream,
    /// jetstream consumer used to read messages
    read_consumer: Arc<PullConsumer>,
    /// js context to fetch pending messages from the stream
    js_context: Arc<Context>,
    /// compression_type is used to decompress the message body
    compression_type: Option<CompressionType>,
    /// jetstream needs complete message to ack/nack, so we need to keep track of them using the offset
    /// so that we can ack/nack them later using the offset.
    offset2jsmsg: Arc<RwLock<HashMap<Offset, JetstreamMessage>>>,
    /// interval at which we should send wip ack to avoid redelivery.
    wip_ack_interval: Duration,
}

impl JetStreamReader {
    pub(crate) async fn new(
        stream: Stream,
        js_ctx: Context,
        isb_config: Option<ISBConfig>,
    ) -> Result<Self> {
        let mut consumer: PullConsumer = js_ctx
            .get_consumer_from_stream(&stream.name, &stream.name)
            .await
            .map_err(|e| {
                Error::ISB(ISBError::Other(format!(
                    "Failed to get consumer for stream {e}"
                )))
            })?;

        let consumer_info = consumer
            .info()
            .await
            .map_err(|e| Error::ISB(ISBError::Other(format!("Failed to get consumer info {e}"))))?;

        let ack_wait_seconds = consumer_info.config.ack_wait.as_secs();
        Ok(Self {
            stream,
            read_consumer: Arc::new(consumer.clone()),
            js_context: Arc::new(js_ctx),
            compression_type: isb_config.map(|c| c.compression.compress_type),
            offset2jsmsg: Arc::new(RwLock::new(HashMap::new())),
            wip_ack_interval: Duration::from_secs(ack_wait_seconds / 3), // give 2 chances
        })
    }

    pub(crate) fn name(&self) -> &'static str {
        self.stream.name
    }

    /// Fetches messages from JetStream ISB in batches, it honors the batch size and timeout.
    pub(crate) async fn fetch(&self, max: usize, timeout: Duration) -> Result<Vec<Message>> {
        let mut out = Vec::with_capacity(max);
        let messages = match self
            .read_consumer
            .batch()
            .max_messages(max)
            .expires(timeout)
            .messages()
            .await
        {
            Ok(mut stream) => {
                let mut v = Vec::new();
                while let Some(next) = stream.next().await {
                    match next {
                        Ok(m) => v.push(m),
                        Err(e) => {
                            warn!(?e, stream=?self.stream, "Failed to receive individual message from batch stream (skipping)");
                        }
                    }
                }
                v
            }
            Err(e) => {
                warn!(?e, stream=?self.stream, "Failed to fetch message batch from Jetstream (ignoring)");
                Vec::new()
            }
        };

        for js_msg in messages {
            let info = js_msg.info().map_err(|e| {
                Error::ISB(ISBError::Decode(format!(
                    "Failed to get message info from JetStream: {e}"
                )))
            })?;
            let offset = Offset::Int(IntOffset::new(
                info.stream_sequence as i64,
                self.stream.partition,
            ));

            // Convert to core Message (including decompression) using existing wrapper
            let mut message = JSWrappedMessage {
                partition_idx: self.stream.partition,
                message: js_msg.clone(),
                vertex_name: get_vertex_name(),
                source_stream: self.stream.name,
                compression_type: self.compression_type,
            }
            .into_message()
            .await?;

            message.offset = offset.clone();

            // Track the actual message for doing ack/nack/wip by offset
            {
                let mut map = self.offset2jsmsg.write().expect("handles mutex poisoned");
                map.insert(offset, js_msg);
            }

            out.push(message);
        }

        Ok(out)
    }

    /// Mark message as in progress by sending work in progress ack.
    pub(crate) async fn mark_wip(&self, offset: &Offset) -> Result<()> {
        let msg = self
            .get_js_message(offset, false)
            .ok_or_else(|| Error::ISB(ISBError::OffsetNotFound(offset.to_string())))?;
        msg.ack_with(AckKind::Progress)
            .await
            .map_err(|e| Error::ISB(ISBError::WipAck(format!("offset {}: {}", offset, e))))?;
        Ok(())
    }

    /// Acknowledge the offset
    pub(crate) async fn ack(&self, offset: &Offset) -> Result<()> {
        let msg = self
            .get_js_message(offset, true)
            .ok_or_else(|| Error::ISB(ISBError::OffsetNotFound(offset.to_string())))?;
        msg.double_ack()
            .await
            .map_err(|e| Error::ISB(ISBError::Ack(format!("offset {}: {}", offset, e))))?;
        Ok(())
    }

    /// Negatively acknowledge the offset, optionally deferring redelivery by `delay`.
    pub(crate) async fn nack(
        &self,
        offset: &Offset,
        nack_options: Option<NackOptions>,
    ) -> Result<()> {
        let msg = self
            .get_js_message(offset, true)
            .ok_or_else(|| Error::ISB(ISBError::OffsetNotFound(offset.to_string())))?;
        let delay = nack_options
            .and_then(|option| option.delay)
            .map(Duration::from_millis);
        msg.ack_with(AckKind::Nak(delay))
            .await
            .map_err(|e| Error::ISB(ISBError::Nack(format!("offset {}: {}", offset, e))))?;
        Ok(())
    }

    /// Helper method to get the JetStream message for a given offset, optionally removing it from the map
    fn get_js_message(&self, offset: &Offset, remove: bool) -> Option<JetstreamMessage> {
        if remove {
            let mut map = self.offset2jsmsg.write().expect("handles mutex poisoned");
            map.remove(offset)
        } else {
            let map = self.offset2jsmsg.read().expect("handles mutex poisoned");
            map.get(offset).cloned()
        }
    }

    /// Returns the number of pending messages in the stream.
    pub(crate) async fn pending(&self) -> Result<Option<usize>> {
        let subject = format!("CONSUMER.INFO.{}.{}", self.stream.name, self.stream.name);
        let info: consumer::Info =
            self.js_context
                .request(subject, &json!({}))
                .await
                .map_err(|e| {
                    Error::ISB(ISBError::Pending(format!(
                        "Failed to get consumer info for stream {}: {}",
                        self.stream.name, e
                    )))
                })?;

        Ok(Some(info.num_pending as usize + info.num_ack_pending))
    }
}

impl crate::pipeline::isb::ISBReader for JetStreamReader {
    async fn fetch(&self, max: usize, timeout: Duration) -> Result<Vec<Message>> {
        JetStreamReader::fetch(self, max, timeout).await
    }

    async fn ack(&self, offset: &Offset) -> Result<()> {
        JetStreamReader::ack(self, offset).await
    }

    async fn nack(&self, offset: &Offset, nack_options: Option<NackOptions>) -> Result<()> {
        JetStreamReader::nack(self, offset, nack_options).await
    }

    async fn pending(&self) -> Result<Option<usize>> {
        JetStreamReader::pending(self).await
    }

    fn name(&self) -> &'static str {
        JetStreamReader::name(self)
    }

    async fn mark_wip(&self, offset: &Offset) -> Result<()> {
        JetStreamReader::mark_wip(self, offset).await
    }

    fn wip_ack_interval(&self) -> Option<Duration> {
        Some(self.wip_ack_interval)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::config::pipeline::isb::{Compression, CompressionType, ISBConfig};
    use crate::message::{IntOffset, Message, MessageID, Offset};
    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};
    use bytes::{Bytes, BytesMut};
    use chrono::Utc;
    use flate2::write::GzEncoder;

    /// Builds the `MessageID` exactly as `JSWrappedMessage::into_message` does, so the
    /// dedup-key format can be asserted without a live JetStream.
    fn dedup_key(
        vertex: &'static str,
        source_stream: &'static str,
        seq: i64,
        partition: u16,
    ) -> String {
        let offset = Offset::Int(IntOffset::new(seq, partition));
        MessageID {
            vertex_name: vertex.into(),
            offset: format!("{source_stream}-{offset}").into(),
            index: 0,
        }
        .to_string()
    }

    /// Two ingress edges at the same sequence must not collide.
    ///
    /// `stream_sequence` is only unique within a single JetStream stream. Buffers are
    /// edge-owned, so a join reads one buffer per source and both sequences start at 1.
    /// The id is used as the `Nats-Msg-Id` dedup key on the downstream write, so a
    /// collision here makes JetStream silently drop one source's messages.
    #[test]
    fn test_dedup_key_distinguishes_source_buffers() {
        let from_one = dedup_key("cat", "default-simple-pipeline-in-one-cat-0", 350, 0);
        let from_two = dedup_key("cat", "default-simple-pipeline-in-two-cat-0", 350, 0);
        assert_ne!(
            from_one, from_two,
            "same sequence on different ingress buffers must yield different dedup keys"
        );
    }

    /// Partition and sequence still participate in the key.
    #[test]
    fn test_dedup_key_distinguishes_partition_and_sequence() {
        let stream = "default-simple-pipeline-in-one-cat-0";
        assert_ne!(
            dedup_key("cat", stream, 350, 0),
            dedup_key("cat", stream, 351, 0),
            "different sequences must differ"
        );
        assert_ne!(
            dedup_key("cat", stream, 350, 0),
            dedup_key("cat", stream, 350, 1),
            "different partitions must differ"
        );
    }

    /// The same message read twice yields the same key, so genuine redeliveries are
    /// still deduplicated.
    #[test]
    fn test_dedup_key_is_stable_for_the_same_message() {
        let stream = "default-simple-pipeline-in-one-cat-0";
        assert_eq!(
            dedup_key("cat", stream, 350, 0),
            dedup_key("cat", stream, 350, 0)
        );
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_reader_direct_fetch() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_direct_fetch", "test", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                max_message_size: 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name,
            )
            .await
            .unwrap();

        // Create ISB config with gzip compression
        let isb_config = ISBConfig {
            compression: Compression {
                compress_type: CompressionType::Gzip,
            },
        };

        let js_reader = JetStreamReader::new(stream.clone(), context.clone(), Some(isb_config))
            .await
            .unwrap();

        let mut compressed = GzEncoder::new(Vec::new(), flate2::Compression::default());
        compressed
            .write_all(Bytes::from("test message for direct fetch").as_ref())
            .map_err(|e| {
                Error::ISB(ISBError::Other(format!(
                    "Failed to compress message (write_all): {}",
                    e
                )))
            })
            .unwrap();

        let body = Bytes::from(
            compressed
                .finish()
                .map_err(|e| {
                    Error::ISB(ISBError::Other(format!(
                        "Failed to compress message (finish): {}",
                        e
                    )))
                })
                .unwrap(),
        );

        // Create a message with empty payload
        let offset = Offset::Int(IntOffset::new(1, 0));
        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["test-key".to_string()]),
            tags: None,
            value: body, // Empty payload
            offset: offset.clone(),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "offset_1".into(),
                index: 0,
            },
            ..Default::default()
        };

        // Convert message to bytes and publish it
        let message_bytes: BytesMut = message.try_into().unwrap();
        context
            .publish(stream.name, message_bytes.into())
            .await
            .unwrap();

        // Read the message using direct fetch
        let messages = js_reader
            .fetch(1, Duration::from_millis(1000))
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);

        let message = messages.first().expect("Expected at least one message");
        assert_eq!(
            message.value.as_ref(),
            "test message for direct fetch".as_bytes()
        );
        assert_eq!(message.keys.as_ref(), &["test-key".to_string()]);

        // Test mark_wip, ack, and nack operations
        let offset = message.offset.clone();
        js_reader.mark_wip(&offset).await.unwrap();
        js_reader.nack(&offset, None).await.unwrap();
        // pending should be one
        let pending = js_reader.pending().await.unwrap();
        assert_eq!(pending, Some(1));

        // read again
        let messages = js_reader
            .fetch(1, Duration::from_millis(1000))
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);

        // ack the message and check pending again
        js_reader
            .ack(
                &messages
                    .first()
                    .expect("Expected at least one message")
                    .offset,
            )
            .await
            .unwrap();
        // pending should be zero
        let pending = js_reader.pending().await.unwrap();
        assert_eq!(pending, Some(0));

        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_ack_missing_offset() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_ack_missing_offset", "test", 0);
        let _ = context.delete_stream(stream.name).await;
        context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                max_message_size: 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name,
            )
            .await
            .unwrap();

        let js_reader = JetStreamReader::new(stream.clone(), context.clone(), None)
            .await
            .unwrap();

        // Try to ack an offset that doesn't exist in the tracker
        let missing_offset = Offset::Int(IntOffset::new(999, 0));
        let result = js_reader.ack(&missing_offset).await;

        assert!(result.is_err());
        if let Err(Error::ISB(ISBError::OffsetNotFound(msg))) = result {
            assert!(msg.contains("999"));
        } else {
            panic!("Expected ISBError::OffsetNotFound");
        }

        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_nack_missing_offset() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_nack_missing_offset", "test", 0);
        let _ = context.delete_stream(stream.name).await;
        context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                max_message_size: 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name,
            )
            .await
            .unwrap();

        let js_reader = JetStreamReader::new(stream.clone(), context.clone(), None)
            .await
            .unwrap();

        // Try to nack an offset that doesn't exist in the tracker
        let missing_offset = Offset::Int(IntOffset::new(999, 0));
        let result = js_reader.nack(&missing_offset, None).await;

        assert!(result.is_err());
        if let Err(Error::ISB(ISBError::OffsetNotFound(msg))) = result {
            assert!(msg.contains("999"));
        } else {
            panic!("Expected ISBError::OffsetNotFound");
        }

        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_mark_wip_missing_offset() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_mark_wip_missing_offset", "test", 0);
        let _ = context.delete_stream(stream.name).await;
        context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                max_message_size: 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name,
            )
            .await
            .unwrap();

        let js_reader = JetStreamReader::new(stream.clone(), context.clone(), None)
            .await
            .unwrap();

        // Try to mark_wip an offset that doesn't exist in the tracker
        let missing_offset = Offset::Int(IntOffset::new(999, 0));
        let result = js_reader.mark_wip(&missing_offset).await;

        assert!(result.is_err());
        if let Err(Error::ISB(ISBError::OffsetNotFound(msg))) = result {
            assert!(msg.contains("999"));
        } else {
            panic!("Expected ISBError::OffsetNotFound");
        }

        context.delete_stream(stream.name).await.unwrap();
    }
}
