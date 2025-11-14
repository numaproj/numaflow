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
use crate::message::{IntOffset, Message, Offset};
use crate::metrics::{
    PIPELINE_PARTITION_NAME_LABEL, pipeline_drop_metric_labels, pipeline_metric_labels,
    pipeline_metrics,
};
use crate::pipeline::isb::jetstream::js_writer::{JetStreamWriter, WriteError};
use crate::shared::forward;
use crate::watermark::WatermarkHandle;

const DEFAULT_RETRY_INTERVAL_MILLIS: u64 = 10;

/// Type alias for metric labels
type MetricLabels = Arc<Vec<(String, String)>>;
/// Type alias for stream metric labels map
type StreamMetricLabelsMap = Arc<HashMap<&'static str, MetricLabels>>;

/// Result of a successful write operation to a stream.
struct WriteResult {
    stream: Stream,
    paf: async_nats::jetstream::context::PublishAckFuture,
    write_start: Instant,
}

/// Components needed to create an ISBWriter.
#[derive(Clone)]
pub(crate) struct ISBWriterComponents {
    pub config: Vec<ToVertexConfig>,
    pub writers: HashMap<&'static str, JetStreamWriter>,
    pub paf_concurrency: usize,
    pub watermark_handle: Option<WatermarkHandle>,
    pub vertex_type: VertexType,
}

/// ISBWriter orchestrates writing to multiple JetStream streams.
/// It manages multiple JetStreamWriters (one per stream), handles message routing,
/// PAF resolution, watermark publishing, and tracker operations.
#[derive(Clone)]
pub(crate) struct ISBWriter {
    config: Arc<Vec<ToVertexConfig>>,
    /// HashMap: stream_name -> JetStreamWriter
    writers: Arc<HashMap<&'static str, JetStreamWriter>>,
    watermark_handle: Option<WatermarkHandle>,
    sem: Arc<Semaphore>,
    paf_concurrency: usize,
    vertex_type: VertexType,
    /// Cached metric labels per stream to avoid repeated allocations
    /// HashMap: stream_name -> labels
    stream_metric_labels: StreamMetricLabelsMap,
}

impl ISBWriter {
    /// Creates a new ISBWriter from pre-created JetStreamWriters.
    /// The JetStreamWriters are created upstream and passed in, making this more testable
    /// and decoupled from JetStream implementation details.
    pub(crate) fn new(components: ISBWriterComponents) -> Self {
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
    /// Returns a list of WriteResults (one per successful write).
    async fn route_and_write_message(
        &self,
        message: &Message,
        cln_token: CancellationToken,
    ) -> Vec<WriteResult> {
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
    async fn write_to_stream(
        &self,
        message: &Message,
        stream: &Stream,
        buffer_full_strategy: BufferFullStrategy,
        cln_token: CancellationToken,
    ) -> Option<WriteResult> {
        let writer = self
            .writers
            .get(stream.name)
            .expect("writer should exist for stream");
        let write_start = Instant::now();
        let mut log_counter = 500u16;

        loop {
            match writer.async_write(message.clone()).await {
                Ok(paf) => {
                    return Some(WriteResult {
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
                            return None; // Don't retry
                        }
                        BufferFullStrategy::RetryUntilSuccess => {
                            // Continue retrying
                        }
                    }
                }
                Err(WriteError::PublishFailed(e)) => {
                    // Continue retrying
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
        write_results: Vec<WriteResult>,
        message: Message,
        cln_token: CancellationToken,
    ) -> Result<()> {
        let permit = Arc::clone(&self.sem)
            .acquire_owned()
            .await
            .map_err(|_e| Error::ISB("Failed to acquire semaphore permit".to_string()));

        let mut this = self.clone();
        tokio::spawn(async move {
            let _permit = permit;

            let n = write_results.len();
            // Resolve all PAFs
            let resolved_offsets = this
                .resolve_all_pafs(write_results, &message, cln_token)
                .await;

            // If any of the writes failed, NAK the message
            if resolved_offsets.len() != n {
                message
                    .ack_handle
                    .as_ref()
                    .expect("ack handle should be present")
                    .is_failed
                    .store(true, Ordering::Relaxed);
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
        write_results: Vec<WriteResult>,
        message: &Message,
        cln_token: CancellationToken,
    ) -> Vec<(Stream, Offset)> {
        let mut offsets = Vec::new();

        for write_result in write_results {
            let writer = self
                .writers
                .get(write_result.stream.name)
                .expect("writer should exist for stream");

            // Try to resolve the PAF
            let paf_result = match write_result.paf.await {
                Ok(ack) => Ok(ack),
                Err(e) => {
                    error!(
                        ?e, stream = ?write_result.stream,
                        "Failed to resolve the future, trying blocking write",
                    );
                    Self::publish_paf_error_metrics(write_result.stream.name, &e);
                    writer
                        .blocking_write(message.clone(), cln_token.clone())
                        .await
                }
            };

            // Handle the ack result
            if let Some(offset) = self
                .handle_paf_result(paf_result, &write_result.stream, message)
                .await
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

    /// Handles the result of an ack operation.
    /// Returns Some(offset) if successful, None if failed.
    async fn handle_paf_result(
        &self,
        ack_result: Result<async_nats::jetstream::publish::PublishAck>,
        stream: &Stream,
        message: &Message,
    ) -> Option<Offset> {
        match ack_result {
            Ok(ack) => {
                if ack.duplicate {
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
                Some(Offset::Int(IntOffset::new(
                    ack.sequence as i64,
                    stream.partition,
                )))
            }
            Err(e) => {
                error!(?e, stream = ?stream, "Blocking write failed");
                None
            }
        }
    }

    /// Publishes error metrics for PAF resolution failures.
    fn publish_paf_error_metrics(
        stream_name: &str,
        e: &async_nats::jetstream::context::PublishError,
    ) {
        pipeline_metrics()
            .jetstream_isb
            .write_error_total
            .get_or_create(&crate::metrics::jetstream_isb_error_metrics_labels(
                stream_name,
                e.kind().to_string(),
            ))
            .inc();
        if let async_nats::jetstream::context::PublishErrorKind::TimedOut = e.kind() {
            pipeline_metrics()
                .jetstream_isb
                .write_timeout_total
                .get_or_create(&crate::metrics::jetstream_isb_error_metrics_labels(
                    stream_name,
                    e.kind().to_string(),
                ))
                .inc();
        }
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

        let writer_components = ISBWriterComponents {
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
        let writer = ISBWriter::new(writer_components);

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

        let writer_components = ISBWriterComponents {
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
        let writer = ISBWriter::new(writer_components);

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

        let writer_components = ISBWriterComponents {
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
        let writer = ISBWriter::new(writer_components);

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
