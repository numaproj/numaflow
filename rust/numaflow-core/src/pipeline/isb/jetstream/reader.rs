use std::collections::HashMap;
use std::fmt;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::Result;
use crate::config::get_vertex_name;
use crate::config::pipeline::isb::{BufferReaderConfig, CompressionType, ISBConfig, Stream};
use crate::error::Error;
use crate::message::{IntOffset, Message, MessageID, MessageType, Offset};
use crate::metadata::Metadata;
use crate::shared::grpc::utc_from_timestamp;
use crate::tracker::TrackerHandle;
use crate::watermark::isb::ISBWatermarkHandle;
use async_nats::jetstream::{
    AckKind, Context, Message as JetstreamMessage, consumer::PullConsumer,
};
use bytes::Bytes;
use flate2::read::GzDecoder;
use prost::Message as ProtoMessage;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
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

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;

    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};
    use bytes::BytesMut;
    use chrono::Utc;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use tokio::time::sleep;

    use super::*;
    use crate::message::{Message, MessageID};
    use crate::pipeline::isb::reader::ISBReaderComponents;

    #[tokio::test]
    async fn simple_permit_test() {
        let sem = Arc::new(Semaphore::new(20));
        let mut permit = Arc::clone(&sem).acquire_many_owned(10).await.unwrap();
        assert_eq!(sem.available_permits(), 10);

        assert_eq!(permit.num_permits(), 10);
        let first_split = permit.split(5).unwrap();
        assert_eq!(first_split.num_permits(), 5);
        assert_eq!(permit.num_permits(), 5);

        let second_split = permit.split(3).unwrap();
        assert_eq!(second_split.num_permits(), 3);
        assert_eq!(permit.num_permits(), 2);

        assert_eq!(sem.available_permits(), 10);

        drop(first_split);
        assert_eq!(sem.available_permits(), 15);

        drop(second_split);
        assert_eq!(sem.available_permits(), 18);

        drop(permit);
        assert_eq!(sem.available_permits(), 20);
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_read() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_jetstream_read", "test", 0);
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

        let buf_reader_config = BufferReaderConfig {
            streams: vec![],
            wip_ack_interval: Duration::from_millis(5),
            ..Default::default()
        };
        let tracker = TrackerHandle::new(None);
        let js_reader: JetStreamReader<crate::typ::WithoutRateLimiter> = JetStreamReader::new(
            ISBReaderComponents {
                vertex_type: "Map".to_string(),
                stream: stream.clone(),
                js_ctx: context.clone(),
                config: buf_reader_config,
                tracker_handle: tracker.clone(),
                batch_size: 500,
                read_timeout: Duration::from_millis(100),
                watermark_handle: None,
                isb_config: None,
                cln_token: CancellationToken::new(),
            },
            None,
        )
        .await
        .unwrap();

        let reader_cancel_token = CancellationToken::new();
        let (mut js_reader_rx, js_reader_task) = js_reader
            .streaming_read(reader_cancel_token.clone())
            .await
            .unwrap();

        let mut offsets = vec![];
        for i in 0..10 {
            let offset = Offset::Int(IntOffset::new(i + 1, 0));
            offsets.push(offset.clone());
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: None,
                value: format!("message {}", i).as_bytes().to_vec().into(),
                offset,
                event_time: Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: format!("offset_{}", i).into(),
                    index: i as i32,
                },
                ..Default::default()
            };
            let message_bytes: BytesMut = message.try_into().unwrap();
            context
                .publish(stream.name, message_bytes.into())
                .await
                .unwrap();
        }

        let mut buffer = vec![];
        for _ in 0..10 {
            let Some(val) = js_reader_rx.next().await else {
                break;
            };
            buffer.push(val);
        }

        assert_eq!(
            buffer.len(),
            10,
            "Expected 10 messages from the jetstream reader"
        );

        reader_cancel_token.cancel();
        for offset in offsets {
            tracker.delete(offset).await.unwrap();
        }
        js_reader_task.await.unwrap().unwrap();
        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_ack() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let tracker_handle = TrackerHandle::new(None);

        let js_stream = Stream::new("test-ack", "test", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(js_stream.name).await;
        context
            .get_or_create_stream(stream::Config {
                name: js_stream.to_string(),
                subjects: vec![js_stream.to_string()],
                max_message_size: 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(js_stream.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                js_stream.name.to_string(),
            )
            .await
            .unwrap();

        let buf_reader_config = BufferReaderConfig {
            streams: vec![],
            wip_ack_interval: Duration::from_millis(5),
            ..Default::default()
        };
        let js_reader: JetStreamReader<crate::typ::WithoutRateLimiter> = JetStreamReader::new(
            ISBReaderComponents {
                vertex_type: "Map".to_string(),
                stream: js_stream.clone(),
                js_ctx: context.clone(),
                config: buf_reader_config,
                tracker_handle: tracker_handle.clone(),
                batch_size: 1,
                read_timeout: Duration::from_millis(100),
                watermark_handle: None,
                isb_config: None,
                cln_token: CancellationToken::new(),
            },
            None,
        )
        .await
        .unwrap();

        let reader_cancel_token = CancellationToken::new();
        let (mut js_reader_rx, js_reader_task) = js_reader
            .streaming_read(reader_cancel_token.clone())
            .await
            .unwrap();

        let mut offsets = vec![];
        // write 5 messages
        for i in 0..5 {
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: None,
                value: format!("message {}", i).as_bytes().to_vec().into(),
                offset: Offset::Int(IntOffset::new(i + 1, 0)),
                event_time: Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: format!("{}-0", i + 1).into(),
                    index: i as i32,
                },
                ..Default::default()
            };
            offsets.push(message.offset.clone());
            let message_bytes: BytesMut = message.try_into().unwrap();
            context
                .publish(js_stream.name, message_bytes.into())
                .await
                .unwrap();
        }

        for _ in 0..5 {
            let Some(_val) = js_reader_rx.next().await else {
                break;
            };
        }

        // after reading messages remove from the tracker so that the messages are acked
        for offset in offsets {
            tracker_handle.delete(offset).await.unwrap();
        }

        // wait until the tracker becomes empty, don't wait more than 1 second
        tokio::time::timeout(Duration::from_secs(1), async {
            while !tracker_handle.is_empty().await.unwrap() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Tracker is not empty after 1 second");

        let mut consumer: PullConsumer = context
            .get_consumer_from_stream(js_stream.name, js_stream.name)
            .await
            .unwrap();

        let consumer_info = consumer.info().await.unwrap();

        assert_eq!(consumer_info.num_pending, 0);
        assert_eq!(consumer_info.num_ack_pending, 0);

        reader_cancel_token.cancel();
        js_reader_task.await.unwrap().unwrap();

        context.delete_stream(js_stream.name).await.unwrap();
    }

    #[tokio::test]
    async fn test_child_tasks_not_aborted() {
        use tokio::task;
        use tokio::time::{Duration, sleep};

        // Parent task
        let parent_task = task::spawn(async {
            // Spawn a child task
            task::spawn(async {
                for _ in 1..=5 {
                    sleep(Duration::from_secs(1)).await;
                }
            });

            // Parent task logic
            sleep(Duration::from_secs(2)).await;
        });

        drop(parent_task);

        // Give some time to observe the child task behavior
        sleep(Duration::from_secs(8)).await;
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_compression_with_empty_payload() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_compression_empty", "test", 0);
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
            compression: crate::config::pipeline::isb::Compression {
                compress_type: CompressionType::Gzip,
            },
        };

        let buf_reader_config = BufferReaderConfig {
            streams: vec![],
            wip_ack_interval: Duration::from_millis(5),
            ..Default::default()
        };
        let tracker = TrackerHandle::new(None);
        let js_reader: JetStreamReader<crate::typ::WithoutRateLimiter> = JetStreamReader::new(
            ISBReaderComponents {
                vertex_type: "Map".to_string(),
                stream: stream.clone(),
                js_ctx: context.clone(),
                config: buf_reader_config,
                tracker_handle: tracker.clone(),
                batch_size: 500,
                read_timeout: Duration::from_millis(100),
                watermark_handle: None,
                isb_config: Some(isb_config.clone()),
                cln_token: CancellationToken::new(),
            },
            None,
        )
        .await
        .unwrap();

        let reader_cancel_token = CancellationToken::new();
        let (mut js_reader_rx, js_reader_task) = js_reader
            .streaming_read(reader_cancel_token.clone())
            .await
            .unwrap();

        let mut compressed = GzEncoder::new(Vec::new(), Compression::default());
        compressed
            .write_all(Bytes::new().as_ref())
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
            keys: Arc::from(vec!["empty_key".to_string()]),
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

        // Read the message back
        let received_message = js_reader_rx.next().await.expect("Should receive a message");

        // Verify the message was correctly decompressed
        assert_eq!(
            received_message.value.len(),
            0,
            "Empty payload should remain empty after compression/decompression"
        );
        assert_eq!(received_message.keys.as_ref(), &["empty_key".to_string()]);
        assert_eq!(received_message.offset.to_string(), offset.to_string());

        // Clean up
        tracker.delete(offset).await.unwrap();
        reader_cancel_token.cancel();
        js_reader_task.await.unwrap().unwrap();
        context.delete_stream(stream.name).await.unwrap();
    }

    // Unit tests for the decompress function
    mod decompress_tests {
        use super::*;
        use crate::config::pipeline::isb::CompressionType;
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
}
