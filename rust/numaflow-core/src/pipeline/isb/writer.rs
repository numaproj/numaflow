use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, sleep};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::Result;
use crate::config::pipeline::isb::{BufferFullStrategy, Stream};
use crate::config::pipeline::{ToVertexConfig, VertexType};
use crate::error::Error;
use crate::message::{Message, Offset};
use crate::metrics::{
    PIPELINE_PARTITION_NAME_LABEL, pipeline_drop_metric_labels, pipeline_metric_labels,
    pipeline_metrics,
};
use crate::pipeline::isb::error::ISBError;
use crate::pipeline::isb::{ISBWriter, WriteError, WriteResult};
use crate::shared::forward;
use crate::typ::NumaflowTypeConfig;
use crate::watermark::WatermarkHandle;

const DEFAULT_RETRY_INTERVAL_MILLIS: u64 = 10;

/// Type alias for metric labels
type MetricLabels = Arc<Vec<(String, String)>>;
/// Type alias for stream metric labels map
type StreamMetricLabelsMap = Arc<HashMap<&'static str, MetricLabels>>;

/// Result of a successful async write operation to a stream.
/// Contains the pending write (PAF) that needs to be resolved.
struct PendingWriteResult<C: NumaflowTypeConfig> {
    stream: Stream,
    paf: <C::ISBWriter as ISBWriter>::PendingWrite,
    write_start: Instant,
}

/// Components needed to create an ISBWriterOrchestrator.
#[derive(Clone)]
pub(crate) struct ISBWriterOrchestratorComponents<C: NumaflowTypeConfig> {
    pub config: Vec<ToVertexConfig>,
    pub writers: HashMap<&'static str, C::ISBWriter>,
    pub paf_concurrency: usize,
    pub watermark_handle: Option<WatermarkHandle>,
    pub vertex_type: VertexType,
}

/// ISBWriterOrchestrator orchestrates writing to multiple ISB streams.
/// It manages multiple ISBWriters (one per stream), handles message routing,
/// watermark publishing, and tracker operations.
#[derive(Clone)]
pub(crate) struct ISBWriterOrchestrator<C: NumaflowTypeConfig> {
    config: Arc<Vec<ToVertexConfig>>,
    /// HashMap: stream_name -> ISBWriter
    writers: Arc<HashMap<&'static str, C::ISBWriter>>,
    watermark_handle: Option<WatermarkHandle>,
    sem: Arc<Semaphore>,
    paf_concurrency: usize,
    vertex_type: VertexType,
    /// Cached metric labels per stream to avoid repeated allocations
    /// HashMap: stream_name -> labels
    stream_metric_labels: StreamMetricLabelsMap,
}

impl<C: NumaflowTypeConfig> ISBWriterOrchestrator<C> {
    /// Creates a new ISBWriterOrchestrator from pre-created ISBWriters.
    /// The ISBWriters are created upstream and passed in, making this more testable
    /// and decoupled from ISB implementation details.
    pub(crate) fn new(components: ISBWriterOrchestratorComponents<C>) -> Self {
        // Build metric labels for each stream once during initialization
        let mut stream_metric_labels = HashMap::new();
        for stream_name in components.writers.keys() {
            let mut labels = pipeline_metric_labels(components.vertex_type.as_str()).clone();
            labels.push((
                PIPELINE_PARTITION_NAME_LABEL.to_string(),
                (*stream_name).to_string(),
            ));
            stream_metric_labels.insert(*stream_name, Arc::new(labels));
        }

        Self {
            config: Arc::new(components.config),
            writers: Arc::new(components.writers),
            watermark_handle: components.watermark_handle,
            sem: Arc::new(Semaphore::new(components.paf_concurrency)),
            paf_concurrency: components.paf_concurrency,
            vertex_type: components.vertex_type,
            stream_metric_labels: Arc::new(stream_metric_labels),
        }
    }

    /// Starts reading messages from the stream and writes them to Jetstream ISB.
    pub(crate) async fn streaming_write(
        self,
        messages_stream: ReceiverStream<Message>,
        cln_token: CancellationToken,
    ) -> Result<JoinHandle<Result<()>>> {
        let handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            let mut messages_stream = messages_stream;

            while let Some(message) = messages_stream.next().await {
                self.write_to_isb(message, cln_token.clone())
                    .await
                    .inspect_err(|e| {
                        error!(?e, "Failed to process message");
                        cln_token.cancel();
                    })?;
            }

            // Wait for all the PAF resolvers to complete before returning
            self.wait_for_paf_resolvers().await?;

            Ok(())
        });
        Ok(handle)
    }

    /// Writes a single message to the ISB. It will keep retrying until it succeeds or is cancelled.
    /// Writes are ordered only if PAF concurrency is 1 because during retries we cannot guarantee order.
    /// This calls `write_to_stream` internally once it has figured out the target streams. This Write
    /// is like a `flap-map` operation. It could end in 0, 1, or more writes based on conditions.
    async fn write_to_isb(&self, message: Message, cln_token: CancellationToken) -> Result<()> {
        // Handle dropped messages
        if message.dropped() {
            // Increment metric for user-initiated drops via DROP tag
            pipeline_metrics()
                .forwarder
                .udf_drop_total
                .get_or_create(pipeline_metric_labels(self.vertex_type.as_str()))
                .inc();
            return Ok(());
        }

        // Route and write to appropriate streams
        let write_results = self
            .route_and_write_message(&message, cln_token.clone())
            .await;

        // Resolve PAFs and finalize
        self.resolve_and_finalize(write_results, message, cln_token)
            .await
    }

    /// Routes a message to appropriate streams and writes to each.
    /// Returns a list of PendingWriteResults (one per successful write) with PAFs to be resolved.
    async fn route_and_write_message(
        &self,
        message: &Message,
        cln_token: CancellationToken,
    ) -> Vec<PendingWriteResult<C>> {
        let mut results = vec![];

        for vertex in &*self.config {
            // Check whether we need to write this message to downstream vertex
            if !forward::should_forward(message.tags.clone(), vertex.conditions.clone()) {
                continue;
            }

            // Determine target stream based on partitioning
            let stream = Self::determine_target_stream(message, vertex);

            // Write to the stream with retry logic
            if let Some(write_result) = self
                .write_to_stream(
                    message,
                    &stream,
                    vertex.writer_config.buffer_full_strategy.clone(),
                    cln_token.clone(),
                )
                .await
            {
                results.push(write_result);
            }
        }

        // Handle empty results (conditional forwarding not met or all writes failed)
        if results.is_empty() {
            debug!(
                tags = ?message.tags,
                "message will be dropped because conditional forwarding rules are not met or buffer is full"
            );
            self.publish_stream_drop_metric("n/a", "forwarding-rules-not-met", 0);
        }

        results
    }

    /// Determines the target stream for a message based on vertex configuration.
    fn determine_target_stream(message: &Message, vertex: &ToVertexConfig) -> Stream {
        // If the to_vertex is a reduce vertex, use the keys as the shuffle key
        let shuffle_key = match vertex.to_vertex_type {
            VertexType::MapUDF | VertexType::Sink | VertexType::Source => {
                String::from_utf8_lossy(&message.id.offset).to_string()
            }
            VertexType::ReduceUDF => message.keys.join(":"),
        };

        // Determine partition
        let partition = forward::determine_partition(shuffle_key, vertex.partitions);

        // Get the stream for this partition
        vertex
            .writer_config
            .streams
            .get(partition as usize)
            .expect("stream should be present")
            .clone()
    }

    /// Writes a message to a single stream with retry logic.
    /// Returns None if the message should be dropped (DiscardLatest + buffer full or cancelled).
    /// On success, returns a PendingWriteResult containing the PAF that needs to be resolved.
    async fn write_to_stream(
        &self,
        message: &Message,
        stream: &Stream,
        buffer_full_strategy: BufferFullStrategy,
        cln_token: CancellationToken,
    ) -> Option<PendingWriteResult<C>> {
        let writer = self
            .writers
            .get(stream.name)
            .expect("writer should exist for stream");
        let write_start = Instant::now();
        let mut log_counter = 500u16;

        loop {
            // Use the trait's async_write method which returns immediately with a PAF
            match writer.async_write(message.clone()).await {
                Ok(paf) => {
                    return Some(PendingWriteResult {
                        stream: stream.clone(),
                        paf,
                        write_start,
                    });
                }
                Err(WriteError::BufferFull) => {
                    // Throttled logging for buffer full
                    if log_counter >= 500 {
                        warn!(stream = ?stream, "stream is full (throttled logging)");
                        log_counter = 0;
                    }
                    log_counter += 1;

                    // Handle buffer full based on strategy
                    match buffer_full_strategy {
                        BufferFullStrategy::DiscardLatest => {
                            self.publish_stream_drop_metric(
                                stream.name,
                                "buffer-full",
                                message.value.len(),
                            );
                            return None;
                        }
                        BufferFullStrategy::RetryUntilSuccess => {
                            // Continue retrying
                        }
                    }
                }
                Err(WriteError::WriteFailed(e)) => {
                    // Write failed, continue retrying
                    error!(?e, "Publishing failed, retrying");
                }
            }

            if cln_token.is_cancelled() {
                error!("Shutdown signal received, exiting write loop");
                return None;
            }

            // Sleep to avoid busy looping
            sleep(Duration::from_millis(DEFAULT_RETRY_INTERVAL_MILLIS)).await;
        }
    }

    /// Waits for all PAF resolvers to complete.
    async fn wait_for_paf_resolvers(&self) -> Result<()> {
        let _ = Arc::clone(&self.sem)
            .acquire_many_owned(self.paf_concurrency as u32)
            .await
            .expect("Failed to acquire semaphore permit");
        Ok(())
    }

    /// Publishes drop metrics for a specific stream.
    fn publish_stream_drop_metric(&self, stream_name: &str, reason: &str, bytes: usize) {
        pipeline_metrics()
            .forwarder
            .drop_total
            .get_or_create(&pipeline_drop_metric_labels(
                self.vertex_type.as_str(),
                stream_name,
                reason,
            ))
            .inc();
        pipeline_metrics()
            .forwarder
            .drop_bytes_total
            .get_or_create(&pipeline_drop_metric_labels(
                self.vertex_type.as_str(),
                stream_name,
                reason,
            ))
            .inc_by(bytes as u64);
    }

    /// Publishes write metrics for a successful write.
    fn publish_write_metrics(
        &self,
        partition_name: &str,
        message: &Message,
        write_processing_start: Instant,
    ) {
        let labels = self
            .stream_metric_labels
            .get(partition_name)
            .expect("labels should exist for stream");

        pipeline_metrics()
            .forwarder
            .write_total
            .get_or_create(labels)
            .inc();
        pipeline_metrics()
            .forwarder
            .write_bytes_total
            .get_or_create(labels)
            .inc_by(message.value.len() as u64);
        pipeline_metrics()
            .forwarder
            .write_processing_time
            .get_or_create(labels)
            .observe(write_processing_start.elapsed().as_micros() as f64);

        // jetstream write time histogram metric
        pipeline_metrics()
            .jetstream_isb
            .write_time_total
            .get_or_create(&crate::metrics::jetstream_isb_metrics_labels(
                partition_name,
            ))
            .observe(write_processing_start.elapsed().as_micros() as f64);
    }

    /// Resolves PAFs and finalizes the message processing.
    /// Spawns a background task to resolve all PAFs, publish watermarks, and update tracker.
    async fn resolve_and_finalize(
        &self,
        write_results: Vec<PendingWriteResult<C>>,
        message: Message,
        cln_token: CancellationToken,
    ) -> Result<()> {
        let permit = Arc::clone(&self.sem).acquire_owned().await.map_err(|_e| {
            Error::ISB(ISBError::Other(
                "Failed to acquire semaphore permit".to_string(),
            ))
        });

        let mut this = self.clone();
        tokio::spawn(async move {
            let _permit = permit;

            let n = write_results.len();
            // Resolve all PAFs
            let resolved_offsets = this
                .resolve_all_pafs(write_results, &message, cln_token)
                .await;

            // If any of the writes failed, NAK the message so it can be retried
            if resolved_offsets.len() != n {
                message
                    .ack_handle
                    .as_ref()
                    .expect("ack handle should be present")
                    .is_failed
                    .store(true, Ordering::Relaxed);
                warn!(
                    expected = n,
                    actual = resolved_offsets.len(),
                    "Some writes failed during PAF resolution, message will be NAK'd"
                );
                return;
            }

            // Publish watermarks for successful writes
            this.publish_watermarks_for_offsets(resolved_offsets, &message)
                .await;
        });

        Ok(())
    }

    /// Resolves all PAFs and returns the offsets for successful writes.
    async fn resolve_all_pafs(
        &self,
        write_results: Vec<PendingWriteResult<C>>,
        message: &Message,
        cln_token: CancellationToken,
    ) -> Vec<(Stream, Offset)> {
        let mut offsets = Vec::new();

        for write_result in write_results {
            let writer = self
                .writers
                .get(write_result.stream.name)
                .expect("writer should exist for stream");

            // Try to resolve the PAF using the trait's resolve method
            let resolve_result = match writer.resolve(write_result.paf).await {
                Ok(result) => Ok(result),
                Err(e) => {
                    error!(
                        ?e, stream = ?write_result.stream,
                        "Failed to resolve the future, trying write with retry",
                    );
                    Self::publish_paf_error_metrics(write_result.stream.name, &e);
                    // Fallback to write with retry loop
                    self.write_with_retry(writer, message.clone(), cln_token.clone())
                        .await
                }
            };

            // Handle the resolve result
            if let Some(offset) =
                self.handle_paf_result(resolve_result, &write_result.stream, message)
            {
                // Publish write metrics for successful write
                self.publish_write_metrics(
                    write_result.stream.name,
                    message,
                    write_result.write_start,
                );
                offsets.push((write_result.stream.clone(), offset));
            }
        }

        offsets
    }

    /// Writes a message using the write method with retry logic.
    /// Retries until success or cancellation.
    async fn write_with_retry(
        &self,
        writer: &C::ISBWriter,
        message: Message,
        cln_token: CancellationToken,
    ) -> std::result::Result<WriteResult, WriteError> {
        loop {
            match writer.write(message.clone()).await {
                Ok(result) => return Ok(result),
                Err(WriteError::BufferFull) => {
                    // Buffer is full, wait and retry
                    debug!("Buffer full during write retry, waiting before next attempt");
                }
                Err(WriteError::WriteFailed(ref e)) => {
                    error!(?e, "Write failed during retry, will retry");
                }
            }

            if cln_token.is_cancelled() {
                return Err(WriteError::WriteFailed("Cancelled".to_string()));
            }

            // Sleep to avoid busy looping
            sleep(Duration::from_millis(DEFAULT_RETRY_INTERVAL_MILLIS)).await;
        }
    }

    /// Handles the result of a PAF resolution or write.
    /// Returns Some(offset) if successful, None if failed.
    /// Also handles duplicate detection and publishes appropriate metrics.
    fn handle_paf_result(
        &self,
        result: std::result::Result<WriteResult, WriteError>,
        stream: &Stream,
        message: &Message,
    ) -> Option<Offset> {
        match result {
            Ok(write_result) => {
                if write_result.is_duplicate {
                    warn!(
                        message_id = ?message.id,
                        stream = ?stream,
                        "Duplicate message detected"
                    );
                    self.publish_stream_drop_metric(
                        stream.name,
                        "duplicate-id",
                        message.value.len(),
                    );
                }
                Some(write_result.offset)
            }
            Err(e) => {
                error!(?e, stream = ?stream, "Write/resolve failed");
                self.publish_stream_drop_metric(stream.name, "write-failed", message.value.len());
                None
            }
        }
    }

    /// Publishes error metrics for PAF resolution failures.
    fn publish_paf_error_metrics(stream_name: &str, e: &WriteError) {
        let error_kind = match e {
            WriteError::BufferFull => "buffer_full",
            WriteError::WriteFailed(_) => "write_failed",
        };
        pipeline_metrics()
            .jetstream_isb
            .write_error_total
            .get_or_create(&crate::metrics::jetstream_isb_error_metrics_labels(
                stream_name,
                error_kind.to_string(),
            ))
            .inc();
    }

    /// Publishes watermarks for all resolved offsets.
    async fn publish_watermarks_for_offsets(
        &mut self,
        offsets: Vec<(Stream, Offset)>,
        message: &Message,
    ) {
        let Some(watermark_handle) = self.watermark_handle.as_mut() else {
            return;
        };

        for (stream, offset) in offsets {
            match watermark_handle {
                WatermarkHandle::ISB(handle) => {
                    handle.publish_watermark(stream, offset).await;
                }
                WatermarkHandle::Source(handle) => {
                    let input_partition = match &message.offset {
                        Offset::Int(offset) => offset.partition_idx,
                        Offset::String(offset) => offset.partition_idx,
                    };
                    handle
                        .publish_source_isb_watermark(stream, offset, input_partition)
                        .await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::pipeline::isb::BufferWriterConfig;
    use crate::message::{AckHandle, IntOffset, Message, MessageID, Offset, ReadAck};
    use crate::pipeline::isb::jetstream::js_writer::JetStreamWriter;
    use crate::typ::WithoutRateLimiter;
    use async_nats::jetstream;
    use async_nats::jetstream::consumer::{self, Config, Consumer};
    use async_nats::jetstream::stream;
    use chrono::Utc;
    use numaflow_models::models::{ForwardConditions, TagConditions};
    use tokio::sync::mpsc;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_streaming_write() {
        let cln_token = CancellationToken::new();
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test-streaming", "temp", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
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
            ..Default::default()
        };

        // Create JetStreamWriter
        let mut writers = HashMap::new();
        writers.insert(
            stream.name,
            JetStreamWriter::new(
                stream.clone(),
                context.clone(),
                writer_config.clone(),
                None,
                cln_token.clone(),
            )
            .await
            .unwrap(),
        );

        let writer_components: ISBWriterOrchestratorComponents<WithoutRateLimiter> =
            ISBWriterOrchestratorComponents {
                config: vec![ToVertexConfig {
                    name: "test-vertex",
                    partitions: 1,
                    writer_config,
                    conditions: None,
                    to_vertex_type: VertexType::Sink,
                }],
                writers,
                paf_concurrency: 100,
                watermark_handle: None,
                vertex_type: VertexType::Source,
            };
        let writer = ISBWriterOrchestrator::<WithoutRateLimiter>::new(writer_components);

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = ReceiverStream::new(rx);

        let write_handle = writer
            .streaming_write(messages_stream, cln_token.clone())
            .await
            .unwrap();

        let mut ack_rxs = vec![];
        // Send messages
        for i in 0..10 {
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
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
                ack_handle: Some(Arc::new(AckHandle::new(ack_tx))),
                ..Default::default()
            };
            ack_rxs.push(ack_rx);
            tx.send(message).await.unwrap();
        }
        drop(tx);

        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }

        write_handle.await.unwrap().unwrap();
        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_streaming_write_with_cancellation() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let cln_token = CancellationToken::new();

        let stream = Stream::new("test-streaming-cancel", "temp", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
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
            ..Default::default()
        };

        // Create JetStreamWriter
        let mut writers = HashMap::new();
        writers.insert(
            stream.name,
            JetStreamWriter::new(
                stream.clone(),
                context.clone(),
                writer_config.clone(),
                None,
                cln_token.clone(),
            )
            .await
            .unwrap(),
        );

        let writer_components: ISBWriterOrchestratorComponents<WithoutRateLimiter> =
            ISBWriterOrchestratorComponents {
                config: vec![ToVertexConfig {
                    name: "test-vertex",
                    partitions: 1,
                    writer_config,
                    conditions: None,
                    to_vertex_type: VertexType::Sink,
                }],
                writers,
                paf_concurrency: 100,
                watermark_handle: None,
                vertex_type: VertexType::Source,
            };
        let writer = ISBWriterOrchestrator::<WithoutRateLimiter>::new(writer_components);

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = ReceiverStream::new(rx);

        let write_handle = writer
            .streaming_write(messages_stream, cln_token.clone())
            .await
            .unwrap();

        let mut ack_rxs = vec![];
        // Send some messages
        for i in 0..5 {
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
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
                ack_handle: Some(Arc::new(AckHandle::new(ack_tx))),
                ..Default::default()
            };
            ack_rxs.push(ack_rx);
            tx.send(message).await.unwrap();
        }

        // Cancel after sending some messages
        cln_token.cancel();
        drop(tx);

        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }

        write_handle.await.unwrap().unwrap();
        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_streaming_write_multiple_streams_vertices() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let cln_token = CancellationToken::new();

        // Create multiple streams for different vertices
        let vertex1_streams = vec![
            Stream::new("test-multi-v1-0", "vertex1", 0),
            Stream::new("test-multi-v1-1", "vertex1", 1),
        ];
        let vertex2_streams = vec![Stream::new("test-multi-v2-0", "vertex2", 0)];
        let vertex3_streams = vec![Stream::new("test-multi-v3-0", "vertex3", 0)];

        let (_, _) = create_streams_and_consumers(&context, &vertex1_streams).await;
        let (_, _) = create_streams_and_consumers(&context, &vertex2_streams).await;
        let (_, _) = create_streams_and_consumers(&context, &vertex3_streams).await;

        let vertex1_writer_config = BufferWriterConfig {
            streams: vertex1_streams.clone(),
            ..Default::default()
        };
        let vertex2_writer_config = BufferWriterConfig {
            streams: vertex2_streams.clone(),
            ..Default::default()
        };
        let vertex3_writer_config = BufferWriterConfig {
            streams: vertex3_streams.clone(),
            ..Default::default()
        };

        // Create JetStreamWriters for all streams
        let mut writers = HashMap::new();
        for stream in &vertex1_streams {
            writers.insert(
                stream.name,
                JetStreamWriter::new(
                    stream.clone(),
                    context.clone(),
                    vertex1_writer_config.clone(),
                    None,
                    cln_token.clone(),
                )
                .await
                .unwrap(),
            );
        }
        for stream in &vertex2_streams {
            writers.insert(
                stream.name,
                JetStreamWriter::new(
                    stream.clone(),
                    context.clone(),
                    vertex2_writer_config.clone(),
                    None,
                    cln_token.clone(),
                )
                .await
                .unwrap(),
            );
        }
        for stream in &vertex3_streams {
            writers.insert(
                stream.name,
                JetStreamWriter::new(
                    stream.clone(),
                    context.clone(),
                    vertex3_writer_config.clone(),
                    None,
                    cln_token.clone(),
                )
                .await
                .unwrap(),
            );
        }

        let writer_components: ISBWriterOrchestratorComponents<WithoutRateLimiter> =
            ISBWriterOrchestratorComponents {
                config: vec![
                    ToVertexConfig {
                        name: "vertex1",
                        partitions: 2,
                        writer_config: vertex1_writer_config,
                        conditions: Some(Box::new(ForwardConditions {
                            tags: Box::new(TagConditions {
                                operator: Some("or".to_string()),
                                values: vec!["tag1".to_string()],
                            }),
                        })),
                        to_vertex_type: VertexType::Sink,
                    },
                    ToVertexConfig {
                        name: "vertex2",
                        partitions: 1,
                        writer_config: vertex2_writer_config,
                        conditions: Some(Box::new(ForwardConditions {
                            tags: Box::new(TagConditions {
                                operator: Some("or".to_string()),
                                values: vec!["tag2".to_string()],
                            }),
                        })),
                        to_vertex_type: VertexType::Sink,
                    },
                    ToVertexConfig {
                        name: "vertex3",
                        partitions: 1,
                        writer_config: vertex3_writer_config,
                        conditions: None, // No conditions, always forward
                        to_vertex_type: VertexType::Sink,
                    },
                ],
                writers,
                paf_concurrency: 100,
                watermark_handle: None,
                vertex_type: VertexType::Source,
            };
        let writer = ISBWriterOrchestrator::<WithoutRateLimiter>::new(writer_components);

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = ReceiverStream::new(rx);

        let write_handle = writer
            .streaming_write(messages_stream, cln_token.clone())
            .await
            .unwrap();

        let mut ack_rxs = vec![];
        // Send messages with different tags
        for i in 0..10 {
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            let tags = if i % 3 == 0 {
                Some(Arc::from(vec!["tag1".to_string()]))
            } else if i % 3 == 1 {
                Some(Arc::from(vec!["tag2".to_string()]))
            } else {
                None
            };

            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags,
                value: format!("message {}", i).as_bytes().to_vec().into(),
                offset: Offset::Int(IntOffset::new(i, 0)),
                event_time: Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: format!("offset_{}", i).into(),
                    index: i as i32,
                },
                ack_handle: Some(Arc::new(AckHandle::new(ack_tx))),
                ..Default::default()
            };
            ack_rxs.push(ack_rx);
            tx.send(message).await.unwrap();
        }
        drop(tx);

        // make sure all messages are acked
        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }

        write_handle.await.unwrap().unwrap();
        for stream_name in vertex1_streams
            .iter()
            .chain(&vertex2_streams)
            .chain(&vertex3_streams)
        {
            context.delete_stream(stream_name.name).await.unwrap();
        }
    }

    async fn create_streams_and_consumers(
        context: &jetstream::Context,
        stream_names: &[Stream],
    ) -> (Vec<stream::Stream>, Vec<Consumer<Config>>) {
        let mut streams = Vec::new();
        let mut consumers = Vec::new();

        for stream_name in stream_names {
            let _ = context.delete_stream(stream_name.name).await;
            let stream = context
                .get_or_create_stream(stream::Config {
                    name: stream_name.name.to_string(),
                    subjects: vec![stream_name.name.to_string()],
                    ..Default::default()
                })
                .await
                .unwrap();
            streams.push(stream);

            let consumer = context
                .create_consumer_on_stream(
                    Config {
                        name: Some(stream_name.name.to_string()),
                        ack_policy: consumer::AckPolicy::Explicit,
                        ..Default::default()
                    },
                    stream_name.name,
                )
                .await
                .unwrap();
            consumers.push(consumer);
        }

        (streams, consumers)
    }
}

// SimpleBuffer Integration Tests
// These tests use SimpleBuffer to test ISBWriterOrchestrator without a distributed ISB.
#[cfg(test)]
mod simplebuffer_tests {
    use super::*;
    use crate::config::pipeline::isb::BufferWriterConfig;
    use crate::message::{AckHandle, IntOffset, MessageID, ReadAck};
    use crate::pipeline::isb::simplebuffer::{SimpleBufferAdapter, WithSimpleBuffer};
    use bytes::Bytes;
    use chrono::Utc;
    use numaflow_testing::simplebuffer::SimpleBuffer;
    use tokio::sync::mpsc;

    /// Helper to create a test message with an ack handle
    fn create_test_message(
        id: i64,
        value: &str,
        tags: Option<Vec<&str>>,
    ) -> (Message, tokio::sync::oneshot::Receiver<ReadAck>) {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec![format!("key-{}", id)]),
            tags: tags.map(|t| Arc::from(t.into_iter().map(String::from).collect::<Vec<_>>())),
            value: Bytes::from(value.to_string()),
            offset: Offset::Int(IntOffset::new(id, 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "test".into(),
                index: id as i32,
                offset: format!("offset-{}", id).into(),
            },
            headers: Arc::new(HashMap::new()),
            metadata: None,
            is_late: false,
            ack_handle: Some(Arc::new(AckHandle::new(ack_tx))),
        };
        (message, ack_rx)
    }

    /// Helper to create ISBWriterOrchestrator with a single SimpleBuffer
    fn create_single_stream_orchestrator(
        adapter: &SimpleBufferAdapter,
        stream_name: &'static str,
        buffer_full_strategy: BufferFullStrategy,
        paf_concurrency: usize,
    ) -> (ISBWriterOrchestrator<WithSimpleBuffer>, CancellationToken) {
        let stream = Stream::new(stream_name, "test-vertex", 0);
        let cancel = CancellationToken::new();

        let writer_config = BufferWriterConfig {
            streams: vec![stream.clone()],
            buffer_full_strategy,
            ..Default::default()
        };

        let mut writers = HashMap::new();
        writers.insert(stream_name, adapter.writer());

        let components = ISBWriterOrchestratorComponents {
            config: vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config,
                conditions: None,
                to_vertex_type: VertexType::Sink,
            }],
            writers,
            paf_concurrency,
            watermark_handle: None,
            vertex_type: VertexType::Source,
        };

        let orchestrator = ISBWriterOrchestrator::<WithSimpleBuffer>::new(components);
        (orchestrator, cancel)
    }

    // ==================== Happy Path Tests ====================

    /// Test: Single message write completes successfully
    #[tokio::test]
    async fn test_streaming_write_single_message() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        let (orchestrator, cancel) = create_single_stream_orchestrator(
            &adapter,
            "test-buffer",
            BufferFullStrategy::RetryUntilSuccess,
            10,
        );

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = ReceiverStream::new(rx);

        let handle = orchestrator
            .streaming_write(messages_stream, cancel.clone())
            .await
            .unwrap();

        let (msg, ack_rx) = create_test_message(1, "hello", None);
        tx.send(msg).await.unwrap();
        drop(tx);

        // Wait for ack
        let ack = ack_rx.await.unwrap();
        assert_eq!(ack, ReadAck::Ack);

        handle.await.unwrap().unwrap();

        // Verify message was written to buffer
        assert_eq!(adapter.pending_count(), 1);
    }

    /// Test: Multiple messages write completes successfully
    #[tokio::test]
    async fn test_streaming_write_multiple_messages() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        let (orchestrator, cancel) = create_single_stream_orchestrator(
            &adapter,
            "test-buffer",
            BufferFullStrategy::RetryUntilSuccess,
            10,
        );

        let (tx, rx) = mpsc::channel(20);
        let messages_stream = ReceiverStream::new(rx);

        let handle = orchestrator
            .streaming_write(messages_stream, cancel.clone())
            .await
            .unwrap();

        let mut ack_rxs = vec![];
        for i in 0..10 {
            let (msg, ack_rx) = create_test_message(i, &format!("message-{}", i), None);
            ack_rxs.push(ack_rx);
            tx.send(msg).await.unwrap();
        }
        drop(tx);

        // All messages should be acked
        for ack_rx in ack_rxs {
            let ack = ack_rx.await.unwrap();
            assert_eq!(ack, ReadAck::Ack);
        }

        handle.await.unwrap().unwrap();
        assert_eq!(adapter.pending_count(), 10);
    }

    // ==================== Buffer Full Tests ====================

    /// Test: Buffer full with DiscardLatest strategy drops message
    #[tokio::test]
    async fn test_buffer_full_with_discard_strategy() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        // Force buffer full
        adapter.error_injector().set_buffer_full(true);

        let (orchestrator, cancel) = create_single_stream_orchestrator(
            &adapter,
            "test-buffer",
            BufferFullStrategy::DiscardLatest,
            10,
        );

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        let handle = orchestrator
            .streaming_write(messages_stream, cancel.clone())
            .await
            .unwrap();

        let (msg, ack_rx) = create_test_message(1, "hello", None);
        tx.send(msg).await.unwrap();
        drop(tx);

        // Message should still be acked (dropped messages are considered processed)
        let ack = ack_rx.await.unwrap();
        assert_eq!(ack, ReadAck::Ack);

        handle.await.unwrap().unwrap();

        // No messages should be in buffer (was dropped)
        assert_eq!(adapter.pending_count(), 0);
    }

    /// Test: Buffer full with RetryUntilSuccess strategy retries until buffer available
    #[tokio::test]
    async fn test_buffer_full_with_retry_strategy() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        // Force buffer full initially
        adapter.error_injector().set_buffer_full(true);

        let (orchestrator, cancel) = create_single_stream_orchestrator(
            &adapter,
            "test-buffer",
            BufferFullStrategy::RetryUntilSuccess,
            10,
        );

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = ReceiverStream::new(rx);

        let handle = orchestrator
            .streaming_write(messages_stream, cancel.clone())
            .await
            .unwrap();

        let (msg, ack_rx) = create_test_message(1, "hello", None);
        tx.send(msg).await.unwrap();

        // Let it retry a few times, then unblock (this sleep is to simulate some retries, won't
        // affect the test outcome)
        sleep(Duration::from_millis(50)).await;
        adapter.error_injector().set_buffer_full(false);

        drop(tx);

        // Should eventually succeed
        let ack = tokio::time::timeout(Duration::from_secs(2), ack_rx)
            .await
            .expect("Should receive ack")
            .unwrap();
        assert_eq!(ack, ReadAck::Ack);

        handle.await.unwrap().unwrap();
        // So pending_count() == 1 is correct - one message is in the buffer, and no consumer has
        // read it.
        assert_eq!(adapter.pending_count(), 1);
    }

    // ==================== Write Failure Tests ====================

    /// Test: Write failures are retried until success
    #[tokio::test]
    async fn test_write_failed_retries_until_success() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        // Fail first 3 writes
        adapter.error_injector().fail_writes(3);

        let (orchestrator, cancel) = create_single_stream_orchestrator(
            &adapter,
            "test-buffer",
            BufferFullStrategy::RetryUntilSuccess,
            10,
        );

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = ReceiverStream::new(rx);

        let handle = orchestrator
            .streaming_write(messages_stream, cancel.clone())
            .await
            .unwrap();

        let (msg, ack_rx) = create_test_message(1, "hello", None);
        tx.send(msg).await.unwrap();
        drop(tx);

        // Should eventually succeed after retries
        let ack = tokio::time::timeout(Duration::from_secs(2), ack_rx)
            .await
            .expect("Should receive ack")
            .unwrap();
        assert_eq!(ack, ReadAck::Ack);

        handle.await.unwrap().unwrap();
        assert_eq!(adapter.pending_count(), 1);
    }

    /// Test: Write failures with cancellation stops retry loop
    #[tokio::test]
    async fn test_write_failed_with_cancellation() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        // Force buffer full forever (will keep retrying)
        adapter.error_injector().set_buffer_full(true);

        let (orchestrator, cancel) = create_single_stream_orchestrator(
            &adapter,
            "test-buffer",
            BufferFullStrategy::RetryUntilSuccess,
            10,
        );

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = ReceiverStream::new(rx);

        let handle = orchestrator
            .streaming_write(messages_stream, cancel.clone())
            .await
            .unwrap();

        let (msg, ack_rx) = create_test_message(1, "hello", None);
        tx.send(msg).await.unwrap();

        // Let it try a few times (doesn't affect test outcome)
        sleep(Duration::from_millis(50)).await;

        // Cancel to stop retries
        cancel.cancel();
        drop(tx);

        // Message should be acked (we exit the loop on cancel)
        let ack = tokio::time::timeout(Duration::from_secs(2), ack_rx)
            .await
            .expect("Should receive ack")
            .unwrap();
        assert_eq!(ack, ReadAck::Ack);

        handle.await.unwrap().unwrap();
        assert_eq!(adapter.pending_count(), 0);
    }

    // ==================== PAF Resolution Failure Tests ====================

    /// Test: PAF resolution failure triggers write_with_retry fallback
    #[tokio::test]
    async fn test_paf_resolution_failure_triggers_write_retry() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        // Fail first resolve, but write should succeed on retry
        adapter.error_injector().fail_resolves(1);

        let (orchestrator, cancel) = create_single_stream_orchestrator(
            &adapter,
            "test-buffer",
            BufferFullStrategy::RetryUntilSuccess,
            10,
        );

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = ReceiverStream::new(rx);

        let handle = orchestrator
            .streaming_write(messages_stream, cancel.clone())
            .await
            .unwrap();

        let (msg, ack_rx) = create_test_message(1, "hello", None);
        tx.send(msg).await.unwrap();
        drop(tx);

        // Should succeed via fallback write
        let ack = tokio::time::timeout(Duration::from_secs(2), ack_rx)
            .await
            .expect("Should receive ack")
            .unwrap();
        assert_eq!(ack, ReadAck::Ack);

        handle.await.unwrap().unwrap();
        assert_eq!(adapter.pending_count(), 1);
    }

    /// Test: When resolve fails but is still retrying when cancellation happens,
    /// the message still gets processed correctly.
    ///
    /// This tests that when PAF resolution fails and triggers write_with_retry,
    /// if the retry eventually succeeds (as duplicate detection), the message is ACK'd.
    /// Note: Testing the NAK path where write_with_retry fails is non-trivial with
    /// SimpleBuffer because async_write and write share the same error injection.
    #[tokio::test]
    async fn test_paf_resolution_failure_with_retry_succeeds_as_duplicate() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        // Fail only the first resolve - this triggers write_with_retry fallback
        // The fallback write will succeed because the message is already in the buffer
        // (written by async_write), so it will be detected as a duplicate.
        adapter.error_injector().fail_resolves(1);

        let (orchestrator, cancel) = create_single_stream_orchestrator(
            &adapter,
            "test-buffer",
            BufferFullStrategy::RetryUntilSuccess,
            10,
        );

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = ReceiverStream::new(rx);

        let handle = orchestrator
            .streaming_write(messages_stream, cancel.clone())
            .await
            .unwrap();

        let (msg, ack_rx) = create_test_message(1, "hello", None);
        tx.send(msg).await.unwrap();

        // Message should succeed via duplicate detection in fallback write
        let ack = tokio::time::timeout(Duration::from_secs(2), ack_rx)
            .await
            .expect("Timeout future should not error out");
        assert!(ack.is_ok(), "Should receive ack before timeout");
        assert_eq!(ack.expect("Should receive an ack"), ReadAck::Ack);

        cancel.cancel();
        drop(tx);

        handle.await.unwrap().unwrap();
        assert_eq!(adapter.pending_count(), 1);
    }

    // ==================== Dropped Message Tests ====================

    /// Test: Messages with DROP tag are not written to buffer
    #[tokio::test]
    async fn test_dropped_message_not_written() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        let (orchestrator, cancel) = create_single_stream_orchestrator(
            &adapter,
            "test-buffer",
            BufferFullStrategy::RetryUntilSuccess,
            10,
        );

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = ReceiverStream::new(rx);

        let handle = orchestrator
            .streaming_write(messages_stream, cancel.clone())
            .await
            .unwrap();

        // Send a message with DROP tag
        let (msg, ack_rx) = create_test_message(1, "hello", Some(vec!["U+005C__DROP__"]));
        tx.send(msg).await.unwrap();
        drop(tx);

        // Should be acked (drop is considered successful processing)
        let ack = ack_rx.await.unwrap();
        assert_eq!(ack, ReadAck::Ack);

        handle.await.unwrap().unwrap();

        // Message should NOT be in buffer
        assert_eq!(adapter.pending_count(), 0);
    }

    // ==================== Duplicate Detection Tests ====================

    /// Test: Duplicate messages are detected
    #[tokio::test]
    async fn test_duplicate_message_detection() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        let (orchestrator, cancel) = create_single_stream_orchestrator(
            &adapter,
            "test-buffer",
            BufferFullStrategy::RetryUntilSuccess,
            10,
        );

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = ReceiverStream::new(rx);

        let handle = orchestrator
            .streaming_write(messages_stream, cancel.clone())
            .await
            .unwrap();

        // Send the same message twice (same ID)
        let (msg1, ack_rx1) = create_test_message(1, "hello", None);
        let (msg2, ack_rx2) = create_test_message(1, "hello", None); // Same ID!

        tx.send(msg1).await.unwrap();
        // Wait for first to complete
        let ack1 = ack_rx1.await.unwrap();
        assert_eq!(ack1, ReadAck::Ack);

        tx.send(msg2).await.unwrap();
        drop(tx);

        // Second should also be acked (duplicate is still successful)
        let ack2 = ack_rx2.await.unwrap();
        assert_eq!(ack2, ReadAck::Ack);

        handle.await.unwrap().unwrap();

        // Only 1 message should be in buffer (duplicate was detected)
        assert_eq!(adapter.pending_count(), 1);
    }

    // ==================== Graceful Shutdown Tests ====================

    /// Test: wait_for_paf_resolvers blocks until all PAF resolutions complete
    #[tokio::test]
    async fn test_graceful_shutdown_waits_for_paf_resolvers() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        // Add resolve latency to make the wait observable
        adapter.error_injector().set_resolve_latency(10);

        let (orchestrator, cancel) = create_single_stream_orchestrator(
            &adapter,
            "test-buffer",
            BufferFullStrategy::RetryUntilSuccess,
            10,
        );

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = ReceiverStream::new(rx);

        let handle = orchestrator
            .streaming_write(messages_stream, cancel.clone())
            .await
            .unwrap();

        let mut ack_rxs = vec![];
        for i in 0..3 {
            let (msg, ack_rx) = create_test_message(i, &format!("message-{}", i), None);
            ack_rxs.push(ack_rx);
            tx.send(msg).await.unwrap();
        }
        drop(tx); // Close the channel to signal end of stream

        // Wait for all acks
        for ack_rx in ack_rxs {
            let ack = tokio::time::timeout(Duration::from_secs(5), ack_rx)
                .await
                .expect("Should receive ack")
                .unwrap();
            assert_eq!(ack, ReadAck::Ack);
        }

        // Handle should complete cleanly (wait_for_paf_resolvers ensures all done)
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("Handle should complete")
            .unwrap()
            .unwrap();

        assert_eq!(adapter.pending_count(), 3);
    }

    /// Test: Multiple messages with paf_concurrency=1 are processed sequentially,
    /// and the semaphore properly gates concurrent PAF resolutions.
    #[tokio::test]
    async fn test_paf_concurrency_one_processes_sequentially() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        // Add resolve latency to make timing observable
        adapter.error_injector().set_resolve_latency(10);

        let (orchestrator, cancel) = create_single_stream_orchestrator(
            &adapter,
            "test-buffer",
            BufferFullStrategy::RetryUntilSuccess,
            1, // paf_concurrency = 1
        );

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = ReceiverStream::new(rx);

        let handle = orchestrator
            .streaming_write(messages_stream, cancel.clone())
            .await
            .unwrap();

        let start = std::time::Instant::now();

        // Send 3 messages
        let mut ack_rxs = vec![];
        for i in 0..3 {
            let (msg, ack_rx) = create_test_message(i, &format!("msg-{}", i), None);
            ack_rxs.push(ack_rx);
            tx.send(msg).await.unwrap();
        }
        drop(tx);

        // All should complete
        for ack_rx in ack_rxs {
            let ack = tokio::time::timeout(Duration::from_secs(5), ack_rx)
                .await
                .expect("Should receive ack")
                .unwrap();
            assert_eq!(ack, ReadAck::Ack);
        }

        handle.await.unwrap().unwrap();

        // With paf_concurrency=1 and 10ms resolve latency per message,
        // 3 messages should take at least 30ms (sequential processing)
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_millis() >= 30,
            "Expected sequential processing (>=30ms), but took {}ms",
            elapsed.as_millis()
        );

        assert_eq!(adapter.pending_count(), 3);
    }

    // ==================== Multi-Stream Tests ====================

    /// Helper to create ISBWriterOrchestrator with multiple downstream vertices.
    /// Each vertex has its own stream with independent error injection.
    /// This allows testing scenarios where a message is written to multiple streams
    /// (one per downstream vertex).
    fn create_multi_vertex_orchestrator(
        adapters: Vec<(&'static str, &SimpleBufferAdapter)>,
        buffer_full_strategy: BufferFullStrategy,
        paf_concurrency: usize,
    ) -> (ISBWriterOrchestrator<WithSimpleBuffer>, CancellationToken) {
        let cancel = CancellationToken::new();

        let mut writers = HashMap::new();
        let mut vertex_configs = vec![];

        for (idx, (stream_name, adapter)) in adapters.iter().enumerate() {
            let vertex_name: &'static str = Box::leak(format!("vertex-{}", idx).into_boxed_str());
            let stream = Stream::new(stream_name, vertex_name, 0);
            writers.insert(*stream_name, adapter.writer());

            let writer_config = BufferWriterConfig {
                streams: vec![stream],
                buffer_full_strategy: buffer_full_strategy.clone(),
                ..Default::default()
            };

            vertex_configs.push(ToVertexConfig {
                name: Box::leak(format!("vertex-{}", idx).into_boxed_str()),
                partitions: 1,
                writer_config,
                conditions: None,
                to_vertex_type: VertexType::Sink,
            });
        }

        let components = ISBWriterOrchestratorComponents {
            config: vertex_configs,
            writers,
            paf_concurrency,
            watermark_handle: None,
            vertex_type: VertexType::Source,
        };

        let orchestrator = ISBWriterOrchestrator::<WithSimpleBuffer>::new(components);
        (orchestrator, cancel)
    }

    /// Test: When writing to multiple downstream vertices and one vertex's PAF resolution fails
    /// (and retry also fails due to cancellation), the message should be NAK'd.
    ///
    /// This tests the `resolved_offsets.len() != n` path in resolve_and_finalize.
    #[tokio::test]
    async fn test_multi_vertex_partial_failure_naks_message() {
        // Create two separate buffers with independent error injection
        let adapter1 = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "stream-1"));
        let adapter2 = SimpleBufferAdapter::new(SimpleBuffer::new(100, 1, "stream-2"));

        // Stream 2 configuration:
        // 1. Initial write succeeds (skip 1 write before failing)
        // 2. Resolve fails (fail_resolves(1))
        // 3. Retry writes fail (fail all writes after skipping the first one)
        // 4. Cancellation will cause write_with_retry to exit with error
        adapter2.error_injector().fail_resolves(1);
        adapter2
            .error_injector()
            .skip_writes_then_fail(1, usize::MAX);

        let (orchestrator, cancel) = create_multi_vertex_orchestrator(
            vec![("stream-1", &adapter1), ("stream-2", &adapter2)],
            BufferFullStrategy::RetryUntilSuccess,
            10,
        );

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = ReceiverStream::new(rx);

        let handle = orchestrator
            .streaming_write(messages_stream, cancel.clone())
            .await
            .unwrap();

        let (msg, ack_rx) = create_test_message(1, "hello", None);
        tx.send(msg).await.unwrap();

        // Cancel after a short delay to allow the initial write and resolve to happen,
        // then cause the retry loop to exit with error
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            // Wait for the resolve to fail and retry to start
            sleep(Duration::from_millis(50)).await;
            cancel_clone.cancel();
        });

        // The message should be NAK'd because stream-2's retry fails due to cancellation
        let ack = tokio::time::timeout(Duration::from_secs(2), ack_rx)
            .await
            .expect("Should receive ack response")
            .unwrap();
        assert_eq!(ack, ReadAck::Nak);

        drop(tx);

        handle.await.unwrap().unwrap();

        // Stream 1 should have the message (write succeeded)
        assert_eq!(adapter1.pending_count(), 1);
        // Stream 2 should also have the message from initial write (before resolve failed)
        assert_eq!(adapter2.pending_count(), 1);
    }

    /// Test: Semaphore permit is properly released even when PAF resolution fails,
    /// allowing subsequent messages to be processed.
    #[tokio::test]
    async fn test_semaphore_released_on_paf_failure() {
        let adapter1 = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "stream-1"));
        let adapter2 = SimpleBufferAdapter::new(SimpleBuffer::new(100, 1, "stream-2"));

        // First message: stream-2 will fail resolve, then retry will fail due to cancellation
        // skip_writes_then_fail: skip 1 write (initial succeeds), then fail all subsequent (retry fails)
        adapter2.error_injector().fail_resolves(1);
        adapter2
            .error_injector()
            .skip_writes_then_fail(1, usize::MAX);

        // Use paf_concurrency = 1 to ensure sequential processing
        let (orchestrator, cancel) = create_multi_vertex_orchestrator(
            vec![("stream-1", &adapter1), ("stream-2", &adapter2)],
            BufferFullStrategy::RetryUntilSuccess,
            1,
        );

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = ReceiverStream::new(rx);

        let handle = orchestrator
            .streaming_write(messages_stream, cancel.clone())
            .await
            .unwrap();

        // Spawn a task to cancel after a short delay
        // This allows initial write and resolve to happen, then causes retry loop to exit
        let cancel_inner = cancel.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            cancel_inner.cancel();
        });

        // Send first message (will fail on stream-2)
        let (msg1, ack_rx1) = create_test_message(1, "first", None);
        tx.send(msg1).await.unwrap();

        // First message should be NAK'd
        let ack1 = tokio::time::timeout(Duration::from_secs(2), ack_rx1)
            .await
            .expect("Should receive ack response for msg1")
            .unwrap();
        assert_eq!(ack1, ReadAck::Nak);

        drop(tx);
        handle.await.unwrap().unwrap();

        // The key assertion: even though the first message failed,
        // the semaphore was released (permit dropped when spawn task completed).
        // We verify this by checking that the orchestrator completed successfully
        // (wait_for_paf_resolvers was able to acquire all permits).
    }
}
