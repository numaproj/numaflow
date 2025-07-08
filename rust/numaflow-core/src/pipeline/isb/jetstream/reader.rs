use std::fmt;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use crate::Result;
use crate::config::get_vertex_name;
use crate::config::pipeline::isb::{BufferReaderConfig, Stream};
use crate::config::pipeline::isb_config::{CompressionType, ISBConfig};
use crate::error::Error;
use crate::message::{IntOffset, Message, MessageID, MessageType, Metadata, Offset, ReadAck};
use crate::metrics::{PIPELINE_PARTITION_NAME_LABEL, pipeline_metric_labels, pipeline_metrics};
use crate::shared::grpc::utc_from_timestamp;
use crate::tracker::TrackerHandle;
use async_nats::jetstream::{
    AckKind, Context, Message as JetstreamMessage, consumer::PullConsumer,
};
use backoff::retry::Retry;
use backoff::strategy::fixed;
use bytes::Bytes;
use chrono::Utc;
use flate2::read::GzDecoder;
use prost::Message as ProtoMessage;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use tracing::{error, info};

/// Configuration for creating a JetStreamReader
#[derive(Clone)]
pub(crate) struct ISBReaderConfig {
    pub vertex_type: String,
    pub stream: Stream,
    pub js_ctx: Context,
    pub config: BufferReaderConfig,
    pub tracker_handle: TrackerHandle,
    pub batch_size: usize,
    pub read_timeout: Duration,
    pub watermark_handle: Option<ISBWatermarkHandle>,
    pub isb_config: Option<ISBConfig>,
}
use crate::watermark::isb::ISBWatermarkHandle;

const ACK_RETRY_INTERVAL: u64 = 100;
const ACK_RETRY_ATTEMPTS: usize = usize::MAX;
const MAX_ACK_PENDING: usize = 25000;

/// The JetStreamReader is a handle to the background actor that continuously fetches messages from JetStream.
/// It can be used to cancel the background task and stop reading from JetStream.
///
/// The sender end of the channel is not stored in this struct, since the struct is clone-able and the mpsc channel
/// is only closed when all the senders are dropped. Storing the Sender end of channel in this struct would make it
/// difficult to close the channel with `cancel` method.
///
/// Error handling and shutdown: The JetStreamReader will stop reading from JetStream when the cancellation token is
/// cancelled(critical error in downstream or SIGTERM), there can't be any other cases where the JetStreamReader
/// should stop reading messages. We just drop the tokio stream so that the downstream components can stop gracefully
/// and before exiting we make sure all the work-in-progress(tasks) are completed.
#[derive(Clone)]
pub(crate) struct JetStreamReader {
    stream: Stream,
    config: BufferReaderConfig,
    consumer: PullConsumer,
    tracker_handle: TrackerHandle,
    batch_size: usize,
    read_timeout: Duration,
    watermark_handle: Option<ISBWatermarkHandle>,
    vertex_type: String,
    compression_type: Option<CompressionType>,
}

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

        let msg_info = self.message.info().map_err(|e| {
            Error::ISB(format!(
                "Failed to get message info from JetStream: {:?}",
                e
            ))
        })?;

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
            event_time: message_info.event_time.map(utc_from_timestamp).unwrap(),
            id: MessageID {
                vertex_name: self.vertex_name.into(),
                offset: offset.to_string().into(),
                index: 0,
            },
            headers: header.headers,
            watermark: None,
            metadata: Some(Metadata {
                previous_vertex: header
                    .id
                    .ok_or(Error::Proto("Missing id".to_string()))?
                    .vertex_name,
            }),
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
                    .map_err(|e| Error::ISB(format!("Failed to decompress message: {}", e)))?;
                decompressed
            }
            Some(CompressionType::Zstd) => {
                let mut decoder: zstd::Decoder<'static, std::io::BufReader<&[u8]>> =
                    zstd::Decoder::new(body.payload.as_ref())
                        .map_err(|e| Error::ISB(format!("Failed to create zstd encoder: {e:?}")))?;
                let mut decompressed = vec![];
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| Error::ISB(format!("Failed to decompress message: {}", e)))?;
                decompressed
            }
            Some(CompressionType::LZ4) => {
                let mut decoder: lz4::Decoder<&[u8]> = lz4::Decoder::new(body.payload.as_ref())
                    .map_err(|e| Error::ISB(format!("Failed to create lz4 encoder: {e:?}")))?;
                let mut decompressed = vec![];
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| Error::ISB(format!("Failed to decompress message: {}", e)))?;
                decompressed
            }
            None | Some(CompressionType::None) => body.payload,
        };
        Ok(body)
    }
}

impl JetStreamReader {
    pub(crate) async fn new(reader_config: ISBReaderConfig) -> Result<Self> {
        let mut buffer_config = reader_config.config;

        let mut consumer: PullConsumer = reader_config
            .js_ctx
            .get_consumer_from_stream(&reader_config.stream.name, &reader_config.stream.name)
            .await
            .map_err(|e| Error::ISB(format!("Failed to get consumer for stream {}", e)))?;

        let consumer_info = consumer
            .info()
            .await
            .map_err(|e| Error::ISB(format!("Failed to get consumer info {}", e)))?;

        // Calculate inProgressTickSeconds based on the ack_wait_seconds.
        let ack_wait_seconds = consumer_info.config.ack_wait.as_secs();
        let wip_ack_interval = Duration::from_secs(std::cmp::max(
            buffer_config.wip_ack_interval.as_secs(),
            ack_wait_seconds * 2 / 3,
        ));
        buffer_config.wip_ack_interval = wip_ack_interval;

        Ok(Self {
            vertex_type: reader_config.vertex_type,
            stream: reader_config.stream,
            config: buffer_config,
            consumer,
            tracker_handle: reader_config.tracker_handle,
            batch_size: reader_config.batch_size,
            read_timeout: reader_config.read_timeout,
            watermark_handle: reader_config.watermark_handle,
            compression_type: reader_config
                .isb_config
                .map(|c| c.compression.compress_type),
        })
    }

    /// streaming_read is a background task that continuously fetches messages from JetStream and
    /// emits them on a channel. When we encounter an error, we log the error and return from the
    /// function. This drops the sender end of the channel. The closing of the channel should propagate
    /// to the receiver end and the receiver should exit gracefully. Within the loop, we only consider
    /// cancellationToken cancellation during the permit reservation and fetching messages,
    /// since rest of the operations should finish immediately.
    pub(crate) async fn streaming_read(
        mut self,
        cancel_token: CancellationToken,
    ) -> Result<(ReceiverStream<Message>, JoinHandle<Result<()>>)> {
        let batch_size = self.batch_size;
        let read_timeout = self.read_timeout;

        let (messages_tx, messages_rx) = mpsc::channel(batch_size);
        let handle: JoinHandle<Result<()>> = tokio::spawn({
            async move {
                let mut labels = pipeline_metric_labels(&self.vertex_type).clone();
                labels.push((
                    PIPELINE_PARTITION_NAME_LABEL.to_string(),
                    self.stream.name.to_string(),
                ));

                let semaphore = Arc::new(Semaphore::new(MAX_ACK_PENDING));
                let mut processed_msgs_count: usize = 0;
                let mut last_logged_at = Instant::now();

                loop {
                    if cancel_token.is_cancelled() {
                        info!(stream=?self.stream, "Cancellation token received, stopping the reader.");
                        break;
                    }

                    let read_messages = self
                        .fetch_messages(
                            batch_size,
                            read_timeout,
                            &semaphore,
                            &labels,
                            &cancel_token,
                        )
                        .await?;

                    if read_messages.is_empty() {
                        continue;
                    }

                    let mut batch_count = 0;
                    for message in read_messages {
                        Self::update_metrics(&labels, &message);
                        if let Err(e) = messages_tx.send(message).await {
                            error!(?e, "Failed to send message to channel");
                            break;
                        }
                        batch_count += 1;
                    }

                    processed_msgs_count += batch_count;
                    if last_logged_at.elapsed().as_secs() >= 1 {
                        info!(
                            "Processed {} messages in {:?}",
                            processed_msgs_count,
                            Utc::now()
                        );
                        processed_msgs_count = 0;
                        last_logged_at = Instant::now();
                    }
                }

                self.wait_for_ack_completion(semaphore).await;
                Ok(())
            }
        });
        Ok((ReceiverStream::new(messages_rx), handle))
    }

    /// fetches messages in batch from JetStream.
    async fn fetch_messages(
        &mut self,
        batch_size: usize,
        read_timeout: Duration,
        semaphore: &Arc<Semaphore>,
        labels: &[(String, String)],
        cancel_token: &CancellationToken,
    ) -> Result<Vec<Message>> {
        let start = Instant::now();
        let jetstream_messages = match self
            .consumer
            .batch()
            .max_messages(batch_size)
            .expires(read_timeout)
            .messages()
            .await
        {
            Ok(mut messages) => {
                let mut batch = Vec::with_capacity(batch_size);
                while let Some(message) = messages.next().await {
                    match message {
                        Ok(msg) => batch.push(msg),
                        Err(e) => {
                            warn!(?e, stream=?self.stream, "Failed to fetch a message from batch (ignoring, will be retried)");
                        }
                    }
                }
                batch
            }
            Err(e) => {
                warn!(?e, stream=?self.stream, "Failed to get message batch from Jetstream (ignoring, will be retried)");
                vec![]
            }
        };

        pipeline_metrics()
            .forwarder
            .read_processing_time
            .get_or_create(&labels.to_vec())
            .observe(start.elapsed().as_micros() as f64);

        debug!(
            time_taken_ms = ?start.elapsed().as_millis(),
            count = ?jetstream_messages.len(),
            "Fetched messages from Jetstream"
        );

        let mut read_messages = Vec::with_capacity(jetstream_messages.len());
        for jetstream_message in jetstream_messages {
            let js_message = JSWrappedMessage {
                partition_idx: self.stream.partition,
                message: jetstream_message.clone(),
                vertex_name: get_vertex_name().to_string(),
                compression_type: self.compression_type,
            };

            let mut message: Message = js_message.into_message().await.map_err(|e| {
                error!(
                    ?e,
                    "Failed to convert Jetstream message to Numaflow message"
                );
                e
            })?;

            // we can ignore the wmb messages
            if let MessageType::WMB = message.typ {
                // ack the message and continue
                if let Err(e) = jetstream_message.ack().await {
                    error!(?e, "Failed to ack the wmb message");
                }
                continue;
            }

            if let Some(watermark_handle) = &mut self.watermark_handle {
                let watermark = watermark_handle
                    .fetch_watermark(message.offset.clone())
                    .await;
                message.watermark = Some(watermark);
            }

            // Insert into tracker and start work in progress
            let (ack_tx, ack_rx) = oneshot::channel();
            self.tracker_handle.insert(&message, ack_tx).await?;
            let permit = Arc::clone(semaphore)
                .acquire_owned()
                .await
                .expect("Failed to acquire semaphore permit");

            tokio::spawn(Self::start_work_in_progress(
                labels.to_vec(),
                message.offset.clone(),
                jetstream_message,
                ack_rx,
                self.config.wip_ack_interval,
                permit,
                cancel_token.clone(),
                start,
            ));

            read_messages.push(message);
        }

        Ok(read_messages)
    }

    /// Waits for all the work-in-progress tasks to be completed.
    async fn wait_for_ack_completion(&self, semaphore: Arc<Semaphore>) {
        info!(stream=?self.stream, "Jetstream reader stopped, waiting for ack tasks to complete");
        let _permit = Arc::clone(&semaphore)
            .acquire_many_owned(MAX_ACK_PENDING as u32)
            .await
            .expect("Failed to acquire semaphore permit");
        info!(stream=?self.stream, "All inflight messages are successfully acked/nacked");
    }

    fn update_metrics(labels: &[(String, String)], message: &Message) {
        let message_bytes = message.value.len();
        let labels_vec = labels.to_vec();
        pipeline_metrics()
            .forwarder
            .read_total
            .get_or_create(&labels_vec)
            .inc();
        pipeline_metrics()
            .forwarder
            .data_read_total
            .get_or_create(&labels_vec)
            .inc();
        pipeline_metrics()
            .forwarder
            .read_bytes_total
            .get_or_create(&labels_vec)
            .inc_by(message_bytes as u64);
        pipeline_metrics()
            .forwarder
            .data_read_bytes_total
            .get_or_create(&labels_vec)
            .inc_by(message_bytes as u64);
    }

    /// A background task which waits for the `Ack`, meanwhile it continuously sends `WIP` acks
    /// until the final ack/nak is received. This will continuously retry if there is an error in acknowledging.
    /// If the sender's end of the ack_rx channel was dropped before
    /// sending the final `Ack` or `Nak` (due to some unhandled/unknown failure), we will send `Nak` to Jetstream.
    #[allow(clippy::too_many_arguments)]
    async fn start_work_in_progress(
        labels: Vec<(String, String)>,
        offset: Offset,
        msg: JetstreamMessage,
        mut ack_rx: oneshot::Receiver<ReadAck>,
        tick: Duration,
        _permit: OwnedSemaphorePermit, // permit to release after acking the offsets.
        cancel_token: CancellationToken,
        message_processing_start: Instant,
    ) {
        let start = Instant::now();
        let mut interval = time::interval_at(start + tick, tick);

        loop {
            let wip = async {
                interval.tick().await;
                let ack_result = msg.ack_with(AckKind::Progress).await;
                if let Err(e) = ack_result {
                    error!(
                        ?e,
                        ?offset,
                        "Failed to send InProgress Ack to Jetstream for message"
                    );
                }
            };

            let ack = tokio::select! {
                ack = &mut ack_rx => ack,
                _ = wip => continue,
            };

            let ack = ack.unwrap_or_else(|e| {
                error!(
                    ?e,
                    ?offset,
                    "Received error while waiting for Ack oneshot channel"
                );
                ReadAck::Nak
            });

            match ack {
                ReadAck::Ack => {
                    let ack_start = Instant::now();
                    Self::invoke_ack_with_retry(&msg, AckKind::Ack, &cancel_token, offset.clone())
                        .await;

                    pipeline_metrics()
                        .forwarder
                        .ack_processing_time
                        .get_or_create(&labels)
                        .observe(ack_start.elapsed().as_micros() as f64);

                    pipeline_metrics()
                        .forwarder
                        .ack_total
                        .get_or_create(&labels)
                        .inc();
                    pipeline_metrics()
                        .forwarder
                        .e2e_time
                        .get_or_create(&labels)
                        .observe(message_processing_start.elapsed().as_micros() as f64);
                    return;
                }
                ReadAck::Nak => {
                    Self::invoke_ack_with_retry(
                        &msg,
                        AckKind::Nak(None),
                        &cancel_token,
                        offset.clone(),
                    )
                    .await;
                    warn!(?offset, "Sent Nak to Jetstream for message");
                    return;
                }
            }
        }
    }

    // invokes the ack with infinite retries until the cancellation token is cancelled.
    async fn invoke_ack_with_retry(
        msg: &JetstreamMessage,
        ack_kind: AckKind,
        cancel_token: &CancellationToken,
        offset: Offset,
    ) {
        let interval = fixed::Interval::from_millis(ACK_RETRY_INTERVAL).take(ACK_RETRY_ATTEMPTS);
        let _ = Retry::retry(
            interval,
            async || {
                let result = match ack_kind {
                    AckKind::Ack => msg.double_ack().await, // double ack is used for exactly once semantics
                    _ => msg.ack_with(ack_kind).await,
                };

                if result.is_err() && cancel_token.is_cancelled() {
                    error!(
                        ?result,
                        ?offset,
                        "Cancellation token received, stopping the {:?} retry loop",
                        ack_kind
                    );
                    return Ok(());
                }

                result.map_err(|e| {
                    Error::Connection(format!(
                        "Failed to send {:?} to Jetstream: {:?}",
                        ack_kind, e
                    ))
                })
            },
            |_: &Error| true,
        )
        .await;
    }

    pub(crate) async fn pending(&mut self) -> Result<Option<usize>> {
        let x = self.consumer.info().await.map_err(|e| {
            Error::ISB(format!(
                "Failed to get consumer info for stream {}: {}",
                self.stream.name, e
            ))
        })?;
        Ok(Some(x.num_pending as usize + x.num_ack_pending))
    }

    pub(crate) fn name(&self) -> &'static str {
        self.stream.name
    }
}

impl fmt::Display for JetStreamReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JetstreamReader {{ stream_name: {}, partition_idx: {}, config: {:?} }}",
            self.stream, self.stream.partition, self.config
        )
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
        };
        let tracker = TrackerHandle::new(None, None);
        let js_reader = JetStreamReader::new(ISBReaderConfig {
            vertex_type: "Map".to_string(),
            stream: stream.clone(),
            js_ctx: context.clone(),
            config: buf_reader_config,
            tracker_handle: tracker.clone(),
            batch_size: 500,
            read_timeout: Duration::from_millis(100),
            watermark_handle: None,
            isb_config: None,
        })
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
        let tracker_handle = TrackerHandle::new(None, None);

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
        };
        let js_reader = JetStreamReader::new(ISBReaderConfig {
            vertex_type: "Map".to_string(),
            stream: js_stream.clone(),
            js_ctx: context.clone(),
            config: buf_reader_config,
            tracker_handle: tracker_handle.clone(),
            batch_size: 1,
            read_timeout: Duration::from_millis(100),
            watermark_handle: None,
            isb_config: None,
        })
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
            compression: crate::config::pipeline::isb_config::Compression {
                compress_type: CompressionType::Gzip,
            },
        };

        let buf_reader_config = BufferReaderConfig {
            streams: vec![],
            wip_ack_interval: Duration::from_millis(5),
        };
        let tracker = TrackerHandle::new(None, None);
        let js_reader = JetStreamReader::new(ISBReaderConfig {
            vertex_type: "Map".to_string(),
            stream: stream.clone(),
            js_ctx: context.clone(),
            config: buf_reader_config,
            tracker_handle: tracker.clone(),
            batch_size: 500,
            read_timeout: Duration::from_millis(100),
            watermark_handle: None,
            isb_config: Some(isb_config.clone()),
        })
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
        use crate::config::pipeline::isb_config::CompressionType;
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
            let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
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
            let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
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
            let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
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
