use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::Result;
use crate::config::get_vertex_name;
use crate::config::pipeline::isb::{CompressionType, ISBConfig, Stream};
use crate::error::Error;
use crate::message::{IntOffset, Message, MessageID, MessageType, Offset};
use crate::metadata::Metadata;
use crate::pipeline::isb::compression;
use crate::shared::grpc::utc_from_timestamp;
use async_nats::jetstream::{
    AckKind, Context, Message as JetstreamMessage, consumer, consumer::PullConsumer,
};
use bytes::Bytes;
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
    compression_type: Option<CompressionType>,
}

impl JSWrappedMessage {
    async fn into_message(self) -> Result<Message> {
        let proto_message =
            numaflow_pb::objects::isb::Message::decode(self.message.payload.clone())
                .map_err(|e| Error::Proto(e.to_string()))?;

        let header = proto_message
            .header
            .ok_or(Error::Proto("Missing header".to_string()))?;
        let kind: MessageType = header.kind.into();
        if kind == MessageType::WMB {
            return Ok(Message {
                typ: kind,
                ..Default::default()
            });
        }

        let body = proto_message
            .body
            .ok_or(Error::Proto("Missing body".to_string()))?;
        let message_info = header
            .message_info
            .ok_or(Error::Proto("Missing message_info".to_string()))?;

        let msg_info = self
            .message
            .info()
            .map_err(|e| Error::ISB(format!("Failed to get message info from JetStream: {e}")))?;

        let offset = Offset::Int(IntOffset::new(
            msg_info.stream_sequence as i64,
            self.partition_idx,
        ));

        Ok(Message {
            typ: header.kind.into(),
            keys: Arc::from(header.keys.into_boxed_slice()),
            tags: None,
            value: match self.compression_type {
                None => body.payload.into(),
                Some(compression_type) => {
                    Bytes::from(compression::decompress(compression_type, &body.payload)?)
                }
            },
            offset: offset.clone(),
            event_time: message_info
                .event_time
                .map(utc_from_timestamp)
                .expect("event time should be present"),
            id: MessageID {
                vertex_name: self.vertex_name.into(),
                offset: offset.to_string().into(),
                index: 0,
            },
            headers: Arc::new(header.headers),
            watermark: None,
            metadata: header.metadata.map(|m| Arc::new(Metadata::from(m))),
            is_late: message_info.is_late,
            ack_handle: None,
        })
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
            .map_err(|e| Error::ISB(format!("Failed to get consumer for stream {e}")))?;

        let consumer_info = consumer
            .info()
            .await
            .map_err(|e| Error::ISB(format!("Failed to get consumer info {e}")))?;

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
    pub(crate) async fn fetch(&mut self, max: usize, timeout: Duration) -> Result<Vec<Message>> {
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
                    if let Ok(m) = next {
                        v.push(m);
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
                Error::ISB(format!("Failed to get message info from JetStream: {e}"))
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
        if let Some(msg) = self.get_js_message(offset, false) {
            let _ = msg.ack_with(AckKind::Progress).await;
        }
        Ok(())
    }

    /// Acknowledge the offset
    pub(crate) async fn ack(&self, offset: &Offset) -> Result<()> {
        if let Some(msg) = self.get_js_message(offset, true) {
            let _ = msg.double_ack().await;
        }
        Ok(())
    }

    /// Negatively acknowledge the offset
    pub(crate) async fn nack(&self, offset: &Offset) -> Result<()> {
        if let Some(msg) = self.get_js_message(offset, true) {
            let _ = msg.ack_with(AckKind::Nak(None)).await;
        }
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
    pub(crate) async fn pending(&mut self) -> Result<Option<usize>> {
        let subject = format!("CONSUMER.INFO.{}.{}", self.stream.name, self.stream.name);
        let info: consumer::Info =
            self.js_context
                .request(subject, &json!({}))
                .await
                .map_err(|e| {
                    Error::ISB(format!(
                        "Failed to get consumer info for stream {}: {}",
                        self.stream.name, e
                    ))
                })?;

        Ok(Some(info.num_pending as usize + info.num_ack_pending))
    }

    pub(crate) fn get_wip_ack_interval(&self) -> Duration {
        self.wip_ack_interval
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

        let mut js_reader = JetStreamReader::new(stream.clone(), context.clone(), Some(isb_config))
            .await
            .unwrap();

        let mut compressed = GzEncoder::new(Vec::new(), flate2::Compression::default());
        compressed
            .write_all(Bytes::from("test message for direct fetch").as_ref())
            .map_err(|e| Error::ISB(format!("Failed to compress message (write_all): {}", e)))
            .unwrap();

        let body = Bytes::from(
            compressed
                .finish()
                .map_err(|e| Error::ISB(format!("Failed to compress message (finish): {}", e)))
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

        let message = &messages[0];
        assert_eq!(
            message.value.as_ref(),
            "test message for direct fetch".as_bytes()
        );
        assert_eq!(message.keys.as_ref(), &["test-key".to_string()]);

        // Test mark_wip, ack, and nack operations
        let offset = message.offset.clone();
        js_reader.mark_wip(&offset).await.unwrap();
        js_reader.nack(&offset).await.unwrap();
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
        js_reader.ack(&messages[0].offset).await.unwrap();
        // pending should be zero
        let pending = js_reader.pending().await.unwrap();
        assert_eq!(pending, Some(0));

        context.delete_stream(stream.name).await.unwrap();
    }
}
