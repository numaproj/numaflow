use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_nats::jetstream::Context;
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::context::{PublishAckFuture, PublishErrorKind};
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::publish::PublishAck;
use async_nats::jetstream::stream::RetentionPolicy::Limits;
use bytes::BytesMut;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::Result;
use crate::config::pipeline::isb::{BufferWriterConfig, CompressionType, Stream};
use crate::error::Error;
use crate::message::Message;
use crate::metrics::{
    jetstream_isb_error_metrics_labels, jetstream_isb_metrics_labels, pipeline_metrics,
};
use crate::pipeline::isb::compression;

/// Type alias for metric labels
type MetricLabels = Arc<Vec<(String, String)>>;

/// Error types specific to JetStreamWriter operations
#[derive(Debug, Clone)]
pub(crate) enum WriteError {
    /// Buffer is full, cannot write (retryable)
    BufferFull,
    /// Publish operation failed (retryable)
    PublishFailed(String),
}

/// Buffer information for a JetStream stream.
#[derive(Debug)]
struct BufferInfo {
    soft_usage: f64,
    solid_usage: f64,
    num_pending: u64,
    num_ack_pending: usize,
}

const DEFAULT_RETRY_INTERVAL_MILLIS: u64 = 10;
const DEFAULT_REFRESH_INTERVAL_SECS: u64 = 1;

/// Lightweight JetStream Writer for a single stream.
/// Handles core JetStream operations: async write (returning PAF), blocking write (returning PublishAck),
/// and buffer fullness tracking for its stream.
///
/// Error handling and shutdown: All failures are infinitely retried until the message is successfully
/// written. The cancellation token is used to short-circuit the retries during shutdown.
#[derive(Clone)]
pub(crate) struct JetStreamWriter {
    stream: Stream,
    js_ctx: Context,
    compression_type: Option<CompressionType>,
    is_full: Arc<AtomicBool>,
    writer_config: BufferWriterConfig,
    /// Cached metric labels to avoid repeated allocations
    buffer_labels: MetricLabels,
}

impl JetStreamWriter {
    /// Creates a new JetStreamWriter for a single stream and spawns a background task
    /// to monitor buffer fullness.
    pub(crate) async fn new(
        stream: Stream,
        js_ctx: Context,
        writer_config: BufferWriterConfig,
        compression_type: Option<CompressionType>,
        cln_token: CancellationToken,
    ) -> Result<Self> {
        let is_full = Arc::new(AtomicBool::new(false));

        // Build metric labels once during initialization
        let buffer_labels = Arc::new(jetstream_isb_metrics_labels(stream.name));

        let js_writer = Self {
            stream,
            js_ctx,
            compression_type,
            is_full: Arc::clone(&is_full),
            writer_config,
            buffer_labels,
        };

        // Spawn background task to monitor this stream's fullness
        tokio::spawn({
            let stream = js_writer
                .js_ctx
                .get_stream(js_writer.stream.name)
                .await
                .map_err(|_| Error::ISB("Failed to get stream".to_string()))?;

            let consumer: PullConsumer = js_writer
                .js_ctx
                .get_consumer_from_stream(js_writer.stream.name, js_writer.stream.name)
                .await
                .map_err(|e| Error::ISB(format!("Failed to get the consumer {e}")))?;

            let is_full = Arc::clone(&js_writer.is_full);
            let writer_config = js_writer.writer_config.clone();
            let buffer_labels = Arc::clone(&js_writer.buffer_labels);
            let stream_name = js_writer.stream.name;

            async move {
                Self::check_stream_status(
                    stream,
                    consumer,
                    is_full,
                    writer_config,
                    buffer_labels,
                    stream_name,
                    cln_token,
                )
                .await;
            }
        });

        Ok(js_writer)
    }

    /// Returns the stream name.
    #[allow(dead_code)]
    pub(crate) fn name(&self) -> &'static str {
        self.stream.name
    }

    /// Returns whether this stream is full.
    pub(crate) fn is_full(&self) -> bool {
        self.is_full.load(Ordering::Relaxed)
    }

    /// Writes the message to the JetStream ISB and returns a PublishAckFuture.
    /// Returns an error if the buffer is full or if the operation is cancelled.
    /// The orchestrator is responsible for retry logic and handling buffer full strategies.
    pub(crate) async fn async_write(
        &self,
        message: Message,
    ) -> std::result::Result<PublishAckFuture, WriteError> {
        // Check if buffer is full
        if self.is_full() {
            pipeline_metrics()
                .jetstream_isb
                .isfull_total
                .get_or_create(&self.buffer_labels)
                .inc();
            return Err(WriteError::BufferFull);
        }

        // message id will be used for deduplication
        let id = message.id.to_string();

        // Compress the message value if compression is enabled
        let mut message = message;
        if let Some(compression_type) = self.compression_type {
            message.value = bytes::Bytes::from(
                compression::compress(compression_type, &message.value)
                    .map_err(|e| WriteError::PublishFailed(format!("Compression failed: {}", e)))?,
            );
        }

        let payload: BytesMut = message
            .try_into()
            .expect("message serialization should not fail");

        // Try to publish
        match self
            .js_ctx
            .send_publish(
                self.stream.name,
                PublishMessage::build()
                    .payload(payload.freeze())
                    .message_id(&id),
            )
            .await
        {
            Ok(paf) => Ok(paf),
            Err(e) => {
                pipeline_metrics()
                    .jetstream_isb
                    .write_error_total
                    .get_or_create(&jetstream_isb_error_metrics_labels(
                        self.stream.name,
                        e.kind().to_string(),
                    ))
                    .inc();
                if let PublishErrorKind::TimedOut = e.kind() {
                    pipeline_metrics()
                        .jetstream_isb
                        .write_timeout_total
                        .get_or_create(&jetstream_isb_error_metrics_labels(
                            self.stream.name,
                            e.kind().to_string(),
                        ))
                        .inc();
                }
                // Return publish error - orchestrator will decide whether to retry
                Err(WriteError::PublishFailed(e.to_string()))
            }
        }
    }

    /// Writes the message to the JetStream ISB and returns the PublishAck. It will do
    /// infinite retries until the message gets published successfully. If it returns
    /// an error it means it is fatal non-retryable error.
    pub(crate) async fn blocking_write(
        &self,
        message: Message,
        cln_token: CancellationToken,
    ) -> Result<PublishAck> {
        let start_time = Instant::now();

        // Compress the message value if compression is enabled
        let mut message = message;
        message.value = match self.compression_type {
            Some(compression_type) => bytes::Bytes::from(
                compression::compress(compression_type, &message.value)
                    .map_err(|e| Error::ISB(format!("Compression failed: {}", e)))?,
            ),
            None => message.value,
        };

        let payload: BytesMut = message
            .try_into()
            .expect("message serialization should not fail");

        loop {
            match self
                .js_ctx
                .publish(self.stream.name, payload.clone().freeze())
                .await
            {
                Ok(paf) => match paf.await {
                    Ok(ack) => {
                        debug!(
                            elapsed_ms = start_time.elapsed().as_millis(),
                            "Blocking write successful in",
                        );
                        pipeline_metrics()
                            .jetstream_isb
                            .write_time_total
                            .get_or_create(&self.buffer_labels)
                            .observe(start_time.elapsed().as_micros() as f64);
                        return Ok(ack);
                    }
                    Err(e) => {
                        error!(?e, "awaiting publish ack failed, retrying");
                        sleep(Duration::from_millis(10)).await;
                    }
                },
                Err(e) => {
                    pipeline_metrics()
                        .jetstream_isb
                        .write_error_total
                        .get_or_create(&self.buffer_labels)
                        .inc();
                    error!(?e, "publishing failed, retrying");
                    sleep(Duration::from_millis(DEFAULT_RETRY_INTERVAL_MILLIS)).await;
                }
            }

            if cln_token.is_cancelled() {
                return Err(Error::Cancelled());
            }
        }
    }

    /// Checks the buffer usage metrics (soft and solid usage) and pending metrics for a stream.
    /// If the usage is greater than the bufferUsageLimit, it sets the is_full flag to true.
    async fn check_stream_status(
        mut stream: async_nats::jetstream::stream::Stream,
        mut consumer: PullConsumer,
        is_full: Arc<AtomicBool>,
        writer_config: BufferWriterConfig,
        buffer_labels: MetricLabels,
        stream_name: &'static str,
        cln_token: CancellationToken,
    ) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(DEFAULT_REFRESH_INTERVAL_SECS));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match Self::fetch_buffer_info(
                        &mut stream,
                        &mut consumer,
                        writer_config.max_length
                    ).await {
                        Ok(buffer_info) => {
                            if buffer_info.solid_usage >= writer_config.usage_limit
                                && buffer_info.soft_usage >= writer_config.usage_limit
                            {
                                is_full.store(true, Ordering::Relaxed);
                            } else {
                                is_full.store(false, Ordering::Relaxed);
                            }
                            pipeline_metrics().jetstream_isb.buffer_soft_usage
                                .get_or_create(&buffer_labels).set(buffer_info.soft_usage);
                            pipeline_metrics().jetstream_isb.buffer_solid_usage
                                .get_or_create(&buffer_labels).set(buffer_info.solid_usage);
                            pipeline_metrics().jetstream_isb.buffer_pending
                                .get_or_create(&buffer_labels).set(buffer_info.num_pending as i64);
                            pipeline_metrics().jetstream_isb.buffer_ack_pending
                                .get_or_create(&buffer_labels).set(buffer_info.num_ack_pending as i64);
                        }
                        Err(e) => {
                            error!(?e, "Failed to fetch buffer info for stream {}, updating isFull to true", stream_name);
                            pipeline_metrics().jetstream_isb.isfull_error_total
                                .get_or_create(&jetstream_isb_error_metrics_labels(stream_name, e.to_string())).inc();
                            is_full.store(true, Ordering::Relaxed);
                        }
                    }
                }
                _ = cln_token.cancelled() => {
                    return;
                }
            }
        }
    }

    /// Fetches the buffer usage metrics (soft and solid usage) and pending metrics for the given stream.
    ///
    /// Soft Usage:
    /// Formula: (NumPending + NumAckPending) / maxLength
    /// - NumPending: The number of pending messages.
    /// - NumAckPending: The number of messages that are in processing state(yet to be acked).
    /// - maxLength: The maximum length of the buffer.
    ///
    /// Solid Usage:
    /// Formula:
    /// - If the stream's retention policy is LimitsPolicy: solidUsage = softUsage
    /// - Otherwise: solidUsage = State.Msgs / maxLength
    /// - State.Msgs: The total number of messages in the stream.
    /// - maxLength: The maximum length of the buffer.
    async fn fetch_buffer_info(
        stream: &mut async_nats::jetstream::stream::Stream,
        consumer: &mut PullConsumer,
        max_length: usize,
    ) -> Result<BufferInfo> {
        let stream_info = stream
            .info()
            .await
            .map_err(|e| Error::ISB(format!("Failed to get the stream info {e}")))?;

        let consumer_info = consumer
            .info()
            .await
            .map_err(|e| Error::ISB(format!("Failed to get the consumer info {e}")))?;

        let soft_usage = (consumer_info.num_pending as f64 + consumer_info.num_ack_pending as f64)
            / max_length as f64;
        let solid_usage = if stream_info.config.retention == Limits {
            soft_usage
        } else {
            stream_info.state.messages as f64 / max_length as f64
        };

        Ok(BufferInfo {
            soft_usage,
            solid_usage,
            num_pending: consumer_info.num_pending,
            num_ack_pending: consumer_info.num_ack_pending,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{IntOffset, MessageID, Offset};
    use async_nats::jetstream;
    use async_nats::jetstream::consumer::{self, Config};
    use async_nats::jetstream::stream;
    use chrono::Utc;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_fetch_buffer_info() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test-buffer-info", "temp", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let mut js_stream = context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                max_messages: 100,
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name,
            )
            .await
            .unwrap();

        let mut consumer: PullConsumer = context
            .get_consumer_from_stream(stream.name, stream.name)
            .await
            .map_err(|e| Error::ISB(format!("Failed to get the consumer {e}")))
            .expect("failed to create consumer");

        let buffer_info = JetStreamWriter::fetch_buffer_info(&mut js_stream, &mut consumer, 100)
            .await
            .unwrap();

        assert_eq!(buffer_info.num_pending, 0);
        assert_eq!(buffer_info.num_ack_pending, 0);
        assert!(
            buffer_info.soft_usage >= 0.0 && buffer_info.soft_usage <= 1.0,
            "soft_usage should be between 0 and 1"
        );
        assert!(
            buffer_info.solid_usage >= 0.0 && buffer_info.solid_usage <= 1.0,
            "solid_usage should be between 0 and 1"
        );
        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_async_write_buffer_full() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let cln_token = CancellationToken::new();

        let stream = Stream::new("test-write-full", "temp", 0);
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                max_messages: 5,
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name,
            )
            .await
            .unwrap();

        let writer_config = BufferWriterConfig {
            streams: vec![stream.clone()],
            max_length: 5,
            usage_limit: 0.5,
            buffer_full_strategy:
                crate::config::pipeline::isb::BufferFullStrategy::RetryUntilSuccess,
            ..Default::default()
        };

        let writer = JetStreamWriter::new(
            stream.clone(),
            context.clone(),
            writer_config,
            None,
            cln_token.clone(),
        )
        .await
        .unwrap();

        // Fill the buffer to trigger is_full
        for i in 0..4 {
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: None,
                value: format!("message {}", i).as_bytes().to_vec().into(),
                offset: Offset::Int(IntOffset::new(i, 0)),
                event_time: Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: format!("offset_{}", i).into(),
                    index: i as i32,
                },
                ..Default::default()
            };
            context
                .publish(stream.name, message.try_into().unwrap())
                .await
                .unwrap()
                .await
                .unwrap();
        }

        // Wait for background task to update is_full
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Try to write when buffer is full
        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key_test".to_string()]),
            tags: None,
            value: "test message".as_bytes().to_vec().into(),
            offset: Offset::Int(IntOffset::new(100, 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "offset_test".to_string().into(),
                index: 100,
            },
            ..Default::default()
        };

        let result = writer.async_write(message).await;
        assert!(matches!(result, Err(WriteError::BufferFull)));

        context.delete_stream(stream.name).await.unwrap();
    }
}
