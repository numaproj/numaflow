use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::Result;
use crate::config::get_vertex_name;
use crate::config::pipeline::isb::{CompressionType, ISBConfig, Stream};
use crate::error::Error;
use crate::message::{IntOffset, Message, MessageID, MessageType, Offset};
use crate::metadata::Metadata;
use crate::shared::grpc::utc_from_timestamp;
use async_nats::jetstream::{
    AckKind, Context, Message as JetstreamMessage, consumer::PullConsumer,
};
use bytes::Bytes;
use flate2::read::GzDecoder;
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
            value: Bytes::from(Self::decompress(self.compression_type, body)?),
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

    /// Decompress the message body based on the compression type.
    fn decompress(
        compression_type: Option<CompressionType>,
        body: numaflow_pb::objects::isb::Body,
    ) -> Result<Vec<u8>> {
        let body = match compression_type {
            Some(CompressionType::Gzip) => {
                let mut decoder: GzDecoder<&[u8]> = GzDecoder::new(body.payload.as_ref());
                let mut decompressed = vec![];
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| Error::ISB(format!("Failed to decompress message: {e}")))?;
                decompressed
            }
            Some(CompressionType::Zstd) => {
                let mut decoder: zstd::Decoder<'static, std::io::BufReader<&[u8]>> =
                    zstd::Decoder::new(body.payload.as_ref())
                        .map_err(|e| Error::ISB(format!("Failed to create zstd encoder: {e:?}")))?;
                let mut decompressed = vec![];
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| Error::ISB(format!("Failed to decompress message: {e}")))?;
                decompressed
            }
            Some(CompressionType::LZ4) => {
                let mut decoder: lz4::Decoder<&[u8]> = lz4::Decoder::new(body.payload.as_ref())
                    .map_err(|e| Error::ISB(format!("Failed to create lz4 encoder: {e:?}")))?;
                let mut decompressed = vec![];
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| Error::ISB(format!("Failed to decompress message: {e}")))?;
                decompressed
            }
            None | Some(CompressionType::None) => body.payload,
        };
        Ok(body)
    }
}

#[derive(Clone)]
pub(crate) struct JetStreamReader {
    stream: Stream,
    consumer: PullConsumer,
    compression_type: Option<CompressionType>,
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

    // Fetch up to `max` messages within `timeout`, convert to core Message, and retain JS handles by offset.
    pub(crate) async fn fetch(&mut self, max: usize, timeout: Duration) -> Result<Vec<Message>> {
        let mut out = Vec::with_capacity(max);
        let msgs = match self
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

        for js_msg in msgs {
            // Derive offset key (seq-partition) and store handle
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

            // Ensure offset is populated even for WMB types
            message.offset = offset.clone();

            // Track handle for later ack/nack/wip by offset
            {
                let mut map = self.handles.lock().expect("handles mutex poisoned");
                map.insert(offset_key, js_msg);
            }

            out.push(message);
        }

        Ok(out)
    }

    // Send progress/WIP for the provided offsets; does not remove handles
    pub(crate) async fn mark_wip(&self, offsets: &[Offset]) -> Result<()> {
        // Collect handles outside of mutex before awaiting
        let handles: Vec<JetstreamMessage> = {
            let map = self.handles.lock().expect("handles mutex poisoned");
            offsets
                .iter()
                .filter_map(|o| map.get(&o.to_string()).cloned())
                .collect()
        };
        for h in handles {
            let _ = h.ack_with(AckKind::Progress).await;
        }
        Ok(())
    }

    // Ack and remove handles
    pub(crate) async fn ack(&self, offsets: &[Offset]) -> Result<()> {
        let handles: Vec<JetstreamMessage> = {
            let mut map = self.handles.lock().expect("handles mutex poisoned");
            let mut v = Vec::with_capacity(offsets.len());
            for o in offsets {
                if let Some(h) = map.remove(&o.to_string()) {
                    v.push(h);
                }
            }
            v
        };
        for h in handles {
            let _ = h.double_ack().await;
        }
        Ok(())
    }

    // Nack and remove handles
    pub(crate) async fn nack(&self, offsets: &[Offset]) -> Result<()> {
        let handles: Vec<JetstreamMessage> = {
            let mut map = self.handles.lock().expect("handles mutex poisoned");
            let mut v = Vec::with_capacity(offsets.len());
            for o in offsets {
                if let Some(h) = map.remove(&o.to_string()) {
                    v.push(h);
                }
            }
            v
        };
        for h in handles {
            let _ = h.ack_with(AckKind::Nak(None)).await;
        }
        Ok(())
    }

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

// Unit tests for the decompress function
#[cfg(test)]
mod decompress_tests {
    use super::*;
    use crate::config::pipeline::isb::CompressionType;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use lz4::EncoderBuilder;
    use std::io::Write;
    use zstd::Encoder;

    fn create_test_body(payload: Vec<u8>) -> numaflow_pb::objects::isb::Body {
        numaflow_pb::objects::isb::Body { payload }
    }

    #[test]
    fn test_decompress_none_compression() {
        let test_data = b"Hello, World!".to_vec();
        let body = create_test_body(test_data.clone());

        let result = JSWrappedMessage::decompress(None, body).unwrap();
        assert_eq!(result, test_data);
    }

    #[test]
    fn test_decompress_none_compression_type() {
        let test_data = b"Hello, World!".to_vec();
        let body = create_test_body(test_data.clone());

        let result = JSWrappedMessage::decompress(Some(CompressionType::None), body).unwrap();
        assert_eq!(result, test_data);
    }

    #[test]
    fn test_decompress_gzip() {
        let test_data = b"Hello, World! This is a test message for gzip compression.";

        // Compress the data with gzip
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(test_data).unwrap();
        let compressed_data = encoder.finish().unwrap();

        let body = create_test_body(compressed_data);
        let result = JSWrappedMessage::decompress(Some(CompressionType::Gzip), body).unwrap();

        assert_eq!(result, test_data.to_vec());
    }

    #[test]
    fn test_decompress_zstd() {
        let test_data = b"Hello, World! This is a test message for zstd compression.";

        // Compress the data with zstd
        let mut encoder = Encoder::new(Vec::new(), 3).unwrap();
        encoder.write_all(test_data).unwrap();
        let compressed_data = encoder.finish().unwrap();

        let body = create_test_body(compressed_data);
        let result = JSWrappedMessage::decompress(Some(CompressionType::Zstd), body).unwrap();

        assert_eq!(result, test_data.to_vec());
    }

    #[test]
    fn test_decompress_lz4() {
        let test_data = b"Hello, World! This is a test message for lz4 compression.";

        // Compress the data with lz4
        let mut encoder = EncoderBuilder::new().build(Vec::new()).unwrap();
        encoder.write_all(test_data).unwrap();
        let (compressed_data, _) = encoder.finish();

        let body = create_test_body(compressed_data);
        let result = JSWrappedMessage::decompress(Some(CompressionType::LZ4), body).unwrap();

        assert_eq!(result, test_data.to_vec());
    }

    #[test]
    fn test_decompress_empty_payload_no_compression() {
        let body = create_test_body(vec![]);
        let result = JSWrappedMessage::decompress(None, body).unwrap();
        assert_eq!(result, Vec::<u8>::new());
    }

    #[test]
    fn test_decompress_empty_payload_gzip() {
        // Create an empty gzip stream
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&[]).unwrap();
        let compressed_data = encoder.finish().unwrap();

        let body = create_test_body(compressed_data);
        let result = JSWrappedMessage::decompress(Some(CompressionType::Gzip), body).unwrap();
        assert_eq!(result, Vec::<u8>::new());
    }

    #[test]
    fn test_decompress_empty_payload_zstd() {
        // Create an empty zstd stream
        let mut encoder = Encoder::new(Vec::new(), 3).unwrap();
        encoder.write_all(&[]).unwrap();
        let compressed_data = encoder.finish().unwrap();

        let body = create_test_body(compressed_data);
        let result = JSWrappedMessage::decompress(Some(CompressionType::Zstd), body).unwrap();
        assert_eq!(result, Vec::<u8>::new());
    }

    #[test]
    fn test_decompress_empty_payload_lz4() {
        // Create an empty lz4 stream
        let mut encoder = EncoderBuilder::new().build(Vec::new()).unwrap();
        encoder.write_all(&[]).unwrap();
        let (compressed_data, _) = encoder.finish();

        let body = create_test_body(compressed_data);
        let result = JSWrappedMessage::decompress(Some(CompressionType::LZ4), body).unwrap();
        assert_eq!(result, Vec::<u8>::new());
    }

    #[test]
    fn test_decompress_invalid_lz4_data() {
        let invalid_data = b"This is not lz4 compressed data".to_vec();
        let body = create_test_body(invalid_data);

        let result = JSWrappedMessage::decompress(Some(CompressionType::LZ4), body);
        assert!(result.is_err());

        if let Err(Error::ISB(msg)) = result {
            assert!(
                msg.contains("Failed to create lz4 encoder")
                    || msg.contains("Failed to decompress message")
            );
        } else {
            panic!("Expected ISB error with lz4 message");
        }
    }

    #[test]
    fn test_decompress_truncated_gzip_data() {
        let test_data = b"Hello, World! This is a test message for gzip compression.";

        // Compress the data with gzip
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(test_data).unwrap();
        let mut compressed_data = encoder.finish().unwrap();

        // Truncate the compressed data to simulate corruption
        compressed_data.truncate(compressed_data.len() / 2);

        let body = create_test_body(compressed_data);
        let result = JSWrappedMessage::decompress(Some(CompressionType::Gzip), body);

        assert!(result.is_err());
        if let Err(Error::ISB(msg)) = result {
            assert!(msg.contains("Failed to decompress message"));
        } else {
            panic!("Expected ISB error with decompression message");
        }
    }

    #[test]
    fn test_decompress_binary_data_zstd() {
        // Create binary test data with various byte values
        let test_data: Vec<u8> = (0..=255).collect();

        // Compress the data with zstd
        let mut encoder = Encoder::new(Vec::new(), 3).unwrap();
        encoder.write_all(&test_data).unwrap();
        let compressed_data = encoder.finish().unwrap();

        let body = create_test_body(compressed_data);
        let result = JSWrappedMessage::decompress(Some(CompressionType::Zstd), body).unwrap();

        assert_eq!(result, test_data);
    }
}
