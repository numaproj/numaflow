use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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
    AckKind, Context, Message as JetstreamMessage, consumer::PullConsumer,
};
use bytes::Bytes;
use prost::Message as ProtoMessage;
use tokio_stream::StreamExt;
use tracing::warn;

/// JSWrappedMessage is a wrapper around the JetStream message that includes the
/// partition index and the vertex name.
#[derive(Debug)]
struct JSWrappedMessage {
    partition_idx: u16,
    message: async_nats::jetstream::Message,
    vertex_name: String,
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
            value: Bytes::from(compression::decompress(self.compression_type, &body.payload)?),
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
            headers: header.headers,
            watermark: None,
            metadata: header.metadata.map(Metadata::from),
            is_late: message_info.is_late,
        })
    }


}

/// JetStreamReader, exposes methods to read, ack, and nack messages from JetStream ISB.
#[derive(Clone)]
pub(crate) struct JetStreamReader {
    /// jetstream stream from which we are reading
    stream: Stream,
    /// jetstream consumer used to read messages
    consumer: PullConsumer,
    /// compression_type is used to decompress the message body
    compression_type: Option<CompressionType>,
    /// jetstream needs complete message to ack/nack, so we need to keep track of them using the offset
    /// so that we can ack/nack them later using the offset.
    handles: Arc<Mutex<HashMap<String, JetstreamMessage>>>,
}

impl JetStreamReader {
    pub(crate) async fn new(
        stream: Stream,
        js_ctx: Context,
        isb_config: Option<ISBConfig>,
    ) -> Result<Self> {
        let consumer: PullConsumer = js_ctx
            .get_consumer_from_stream(&stream.name, &stream.name)
            .await
            .map_err(|e| Error::ISB(format!("Failed to get consumer for stream {e}")))?;

        Ok(Self {
            stream,
            consumer,
            compression_type: isb_config.map(|c| c.compression.compress_type),
            handles: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub(crate) fn name(&self) -> &'static str {
        self.stream.name
    }

    /// Fetches messages from JetStream ISB in batches, it honors the batch size and timeout.
    pub(crate) async fn fetch(&mut self, max: usize, timeout: Duration) -> Result<Vec<Message>> {
        let mut out = Vec::with_capacity(max);
        let messages = match self
            .consumer
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
            let offset_key = offset.to_string();

            // Convert to core Message (including decompression) using existing wrapper
            let mut message = JSWrappedMessage {
                partition_idx: self.stream.partition,
                message: js_msg.clone(),
                vertex_name: get_vertex_name().to_string(),
                compression_type: self.compression_type,
            }
            .into_message()
            .await?;

            message.offset = offset.clone();

            // Track the actual message for doing ack/nack/wip by offset
            {
                let mut map = self.handles.lock().expect("handles mutex poisoned");
                map.insert(offset_key, js_msg);
            }

            out.push(message);
        }

        Ok(out)
    }

    /// Mark messages as in progress by sending work in progress acks.
    pub(crate) async fn mark_wip(&self, offsets: &[Offset]) -> Result<()> {
        let handles = self.get_message_handles(offsets, false);
        for h in handles {
            let _ = h.ack_with(AckKind::Progress).await;
        }
        Ok(())
    }

    /// Acknowledge the offsets
    pub(crate) async fn ack(&self, offsets: &[Offset]) -> Result<()> {
        let handles = self.get_message_handles(offsets, true);
        for h in handles {
            let _ = h.double_ack().await;
        }
        Ok(())
    }

    /// Negatively acknowledge the offsets
    pub(crate) async fn nack(&self, offsets: &[Offset]) -> Result<()> {
        let handles = self.get_message_handles(offsets, true);
        for h in handles {
            let _ = h.ack_with(AckKind::Nak(None)).await;
        }
        Ok(())
    }

    /// Helper method to collect handles for given offsets, optionally removing them from the map
    fn get_message_handles(&self, offsets: &[Offset], remove: bool) -> Vec<JetstreamMessage> {
        if remove {
            let mut map = self.handles.lock().expect("handles mutex poisoned");
            let mut v = Vec::with_capacity(offsets.len());
            for o in offsets {
                if let Some(h) = map.remove(&o.to_string()) {
                    v.push(h);
                }
            }
            v
        } else {
            let map = self.handles.lock().expect("handles mutex poisoned");
            offsets
                .iter()
                .filter_map(|o| map.get(&o.to_string()).cloned())
                .collect()
        }
    }

    /// Returns the number of pending messages in the stream.
    pub(crate) async fn pending(&mut self) -> Result<Option<usize>> {
        let info = self.consumer.info().await.map_err(|e| {
            Error::ISB(format!(
                "Failed to get consumer info for stream {}: {}",
                self.stream.name, e
            ))
        })?;
        Ok(Some(info.num_pending as usize + info.num_ack_pending))
    }
}


