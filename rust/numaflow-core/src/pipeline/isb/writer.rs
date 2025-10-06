use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, sleep};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::Result;
use crate::config::pipeline::isb::{BufferFullStrategy, ISBConfig, Stream};
use crate::config::pipeline::{ToVertexConfig, VertexType};
use crate::error::Error;
use crate::message::{IntOffset, Message, Offset};
use crate::metrics::{
    PIPELINE_PARTITION_NAME_LABEL, pipeline_drop_metric_labels, pipeline_metric_labels,
    pipeline_metrics,
};
use crate::pipeline::isb::jetstream::writer::{JetStreamWriter, WriteError};
use crate::shared::forward;
use crate::tracker::TrackerHandle;
use crate::watermark::WatermarkHandle;

const DEFAULT_RETRY_INTERVAL_MILLIS: u64 = 10;

/// Components needed to create an ISBWriter.
#[derive(Clone)]
pub(crate) struct ISBWriterComponents {
    pub config: Vec<ToVertexConfig>,
    pub js_ctx: async_nats::jetstream::Context,
    pub paf_concurrency: usize,
    pub tracker_handle: TrackerHandle,
    pub cancel_token: CancellationToken,
    pub watermark_handle: Option<WatermarkHandle>,
    pub vertex_type: VertexType,
    pub isb_config: Option<ISBConfig>,
}

impl ISBWriterComponents {
    /// Create ISBWriterComponents from minimal components and pipeline context
    pub fn new(
        watermark_handle: Option<WatermarkHandle>,
        context: &crate::pipeline::PipelineContext<'_>,
    ) -> Self {
        Self {
            config: context.config.to_vertex_config.clone(),
            js_ctx: context.js_context.clone(),
            paf_concurrency: context.config.writer_concurrency,
            tracker_handle: context.tracker_handle.clone(),
            cancel_token: context.cln_token.clone(),
            watermark_handle,
            vertex_type: context.config.vertex_type,
            isb_config: context.config.isb_config.clone(),
        }
    }
}

/// ISBWriter orchestrates writing to multiple JetStream streams.
/// It manages multiple JetStreamWriters (one per stream), handles message routing,
/// PAF resolution, watermark publishing, and tracker operations.
#[derive(Clone)]
pub(crate) struct ISBWriter {
    config: Arc<Vec<ToVertexConfig>>,
    /// HashMap: stream_name -> JetStreamWriter
    writers: Arc<HashMap<&'static str, JetStreamWriter>>,
    tracker_handle: TrackerHandle,
    watermark_handle: Option<WatermarkHandle>,
    sem: Arc<Semaphore>,
    paf_concurrency: usize,
    vertex_type: VertexType,
}

impl ISBWriter {
    /// Creates a new ISBWriter with multiple JetStreamWriters (one per stream).
    pub(crate) fn new(components: ISBWriterComponents) -> Self {
        // Create one JetStreamWriter per stream
        let mut writers = HashMap::new();
        for vertex_config in &components.config {
            for stream in &vertex_config.writer_config.streams {
                let writer = JetStreamWriter::new(
                    stream.clone(),
                    components.js_ctx.clone(),
                    vertex_config.writer_config.clone(),
                    components
                        .isb_config
                        .as_ref()
                        .map(|c| c.compression.compress_type),
                    components.cancel_token.clone(),
                );
                writers.insert(stream.name, writer);
            }
        }

        Self {
            config: Arc::new(components.config),
            writers: Arc::new(writers),
            tracker_handle: components.tracker_handle,
            watermark_handle: components.watermark_handle,
            sem: Arc::new(Semaphore::new(components.paf_concurrency)),
            paf_concurrency: components.paf_concurrency,
            vertex_type: components.vertex_type,
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
                let write_processing_start = Instant::now();
                // if message needs to be dropped, ack and continue
                if message.dropped() {
                    // delete the entry from tracker
                    self.tracker_handle
                        .delete(message.offset)
                        .await
                        .expect("Failed to delete offset from tracker");
                    pipeline_metrics()
                        .forwarder
                        .udf_drop_total
                        .get_or_create(&pipeline_drop_metric_labels(
                            self.vertex_type.as_str(),
                            "n/a",
                            "to_drop",
                        ))
                        .inc();
                    continue;
                }

                // Write message to appropriate streams and collect PAFs
                let pafs = self
                    .write_message(message.clone(), write_processing_start, cln_token.clone())
                    .await;

                // pafs is empty means message should not be written to any stream, so we can delete
                // and continue. The `to_drop()` case is already handled above.
                // NOTE: PAFs can be empty during following scenarios:
                //  1. Conditional forwarding conditions are not met.
                //  2. Buffer is full and strategy is DiscardLatest.
                if pafs.is_empty() {
                    debug!(
                        tags = ?message.tags,
                        "message will be dropped because conditional forwarding rules are not met or buffer is full"
                    );

                    pipeline_metrics()
                        .forwarder
                        .udf_drop_total
                        .get_or_create(&pipeline_drop_metric_labels(
                            self.vertex_type.as_str(),
                            "n/a",
                            "forwarding-rules-not-met",
                        ))
                        .inc();

                    // delete the entry from tracker
                    self.tracker_handle
                        .delete(message.offset)
                        .await
                        .expect("Failed to delete offset from tracker");
                    continue;
                }

                self.resolve_pafs(pafs, message, cln_token.clone())
                    .await
                    .inspect_err(|e| {
                        error!(?e, "Failed to resolve PAFs");
                        cln_token.cancel();
                    })?;
            }

            // wait for all the paf resolvers to complete before returning
            let _ = Arc::clone(&self.sem)
                .acquire_many_owned(self.paf_concurrency as u32)
                .await
                .expect("Failed to acquire semaphore permit");

            Ok(())
        });
        Ok(handle)
    }

    /// Writes a message to the appropriate streams based on routing logic.
    /// Returns a list of PAFs (one per stream written to).
    async fn write_message(
        &self,
        message: Message,
        write_processing_start: Instant,
        cln_token: CancellationToken,
    ) -> Vec<(
        Stream,
        Instant,
        async_nats::jetstream::context::PublishAckFuture,
    )> {
        let mut pafs = vec![];

        for vertex in &*self.config {
            // check whether we need to write to this downstream vertex
            if !forward::should_forward(message.tags.clone(), vertex.conditions.clone()) {
                continue;
            }

            // if the to_vertex is a reduce vertex, we should use the keys as the shuffle key
            let shuffle_key = match vertex.to_vertex_type {
                VertexType::MapUDF | VertexType::Sink | VertexType::Source => {
                    String::from_utf8_lossy(&message.id.offset).to_string()
                }
                VertexType::ReduceUDF => message.keys.join(":"),
            };

            // check to which partition the message should be written
            let partition = forward::determine_partition(shuffle_key, vertex.partitions);

            // write the message to the corresponding stream
            let stream = vertex
                .writer_config
                .streams
                .get(partition as usize)
                .expect("stream should be present")
                .clone();

            // Get the appropriate JetStreamWriter for this stream
            let writer = self
                .writers
                .get(stream.name)
                .expect("writer should exist for stream");

            // Try to write with retry logic based on buffer full strategy
            let paf = self
                .write_with_retry(
                    writer,
                    message.clone(),
                    &stream,
                    vertex.writer_config.buffer_full_strategy.clone(),
                    cln_token.clone(),
                )
                .await;

            if let Some(paf) = paf {
                pafs.push((stream, write_processing_start, paf));
            }
        }

        pafs
    }

    /// Writes a message with retry logic based on buffer full strategy.
    /// Returns None if the message should be dropped (DiscardLatest + buffer full or cancelled).
    async fn write_with_retry(
        &self,
        writer: &JetStreamWriter,
        message: Message,
        stream: &Stream,
        buffer_full_strategy: BufferFullStrategy,
        cln_token: CancellationToken,
    ) -> Option<async_nats::jetstream::context::PublishAckFuture> {
        let mut log_counter = 500u16;

        loop {
            match writer.async_write(message.clone()).await {
                Ok(paf) => return Some(paf),
                Err(WriteError::BufferFull) => {
                    if log_counter >= 500 {
                        warn!(stream = ?stream, "stream is full (throttled logging)");
                        log_counter = 0;
                    }
                    log_counter += 1;

                    match buffer_full_strategy {
                        BufferFullStrategy::DiscardLatest => {
                            let msg_bytes = message.value.len();
                            // delete the entry from tracker
                            self.tracker_handle
                                .delete(message.offset.clone())
                                .await
                                .expect("Failed to delete offset from tracker");
                            // increment drop metric
                            pipeline_metrics()
                                .forwarder
                                .drop_total
                                .get_or_create(&pipeline_drop_metric_labels(
                                    self.vertex_type.as_str(),
                                    stream.name,
                                    "buffer-full",
                                ))
                                .inc();
                            pipeline_metrics()
                                .forwarder
                                .drop_bytes_total
                                .get_or_create(&pipeline_drop_metric_labels(
                                    self.vertex_type.as_str(),
                                    stream.name,
                                    "buffer-full",
                                ))
                                .inc_by(msg_bytes as u64);
                            return None;
                        }
                        BufferFullStrategy::RetryUntilSuccess => {
                            // Continue retrying
                        }
                    }
                }
                Err(WriteError::PublishFailed(e)) => {
                    error!(?e, "Publishing failed, retrying");
                    // Continue retrying
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

    fn send_write_metrics(
        partition_name: &str,
        vertex_type: &str,
        message: Message,
        write_processing_start: Instant,
    ) {
        let mut labels = pipeline_metric_labels(vertex_type).clone();
        labels.push((
            PIPELINE_PARTITION_NAME_LABEL.to_string(),
            partition_name.to_string(),
        ));
        pipeline_metrics()
            .forwarder
            .write_total
            .get_or_create(&labels)
            .inc();
        pipeline_metrics()
            .forwarder
            .write_bytes_total
            .get_or_create(&labels)
            .inc_by(message.value.len() as u64);
        pipeline_metrics()
            .forwarder
            .write_processing_time
            .get_or_create(&labels)
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

    /// resolve_pafs resolves the PAFs for the given result. It will try to resolve the PAFs
    /// asynchronously, if it fails it will do a blocking write to resolve the PAFs.
    /// At any point in time, we will only have X PAF resolvers running, this will help us create a
    /// natural backpressure.
    async fn resolve_pafs(
        &self,
        pafs: Vec<(
            Stream,
            Instant,
            async_nats::jetstream::context::PublishAckFuture,
        )>,
        message: Message,
        cln_token: CancellationToken,
    ) -> Result<()> {
        let permit = Arc::clone(&self.sem)
            .acquire_owned()
            .await
            .map_err(|_e| Error::ISB("Failed to acquire semaphore permit".to_string()))?;

        let this = self.clone();

        tokio::spawn(async move {
            let _permit = permit;
            let mut offsets = Vec::new();

            // resolve the pafs
            for (stream, write_processing_start, paf) in pafs {
                let writer = this
                    .writers
                    .get(stream.name)
                    .expect("writer should exist for stream");

                let ack = match paf.await {
                    Ok(ack) => {
                        Self::send_write_metrics(
                            stream.name,
                            this.vertex_type.as_str(),
                            message.clone(),
                            write_processing_start,
                        );
                        Ok(ack)
                    }
                    Err(e) => {
                        error!(
                            ?e, stream = ?stream,
                            "Failed to resolve the future trying blocking write",
                        );
                        pipeline_metrics()
                            .jetstream_isb
                            .write_error_total
                            .get_or_create(&crate::metrics::jetstream_isb_error_metrics_labels(
                                stream.name,
                                e.kind().to_string(),
                            ))
                            .inc();
                        if let async_nats::jetstream::context::PublishErrorKind::TimedOut = e.kind()
                        {
                            pipeline_metrics()
                                .jetstream_isb
                                .write_timeout_total
                                .get_or_create(&crate::metrics::jetstream_isb_error_metrics_labels(
                                    stream.name,
                                    e.kind().to_string(),
                                ))
                                .inc();
                        }
                        writer
                            .blocking_write(message.clone(), cln_token.clone())
                            .await
                    }
                };

                match ack {
                    Ok(ack) => {
                        if ack.duplicate {
                            warn!(
                                message_id = ?message.id,
                                ?stream,
                                ?ack,
                                "Duplicate message detected"
                            );
                            // Increment drop metric for duplicate messages
                            pipeline_metrics()
                                .forwarder
                                .drop_total
                                .get_or_create(&pipeline_drop_metric_labels(
                                    this.vertex_type.as_str(),
                                    stream.name,
                                    "duplicate-id",
                                ))
                                .inc();
                            pipeline_metrics()
                                .forwarder
                                .drop_bytes_total
                                .get_or_create(&pipeline_drop_metric_labels(
                                    this.vertex_type.as_str(),
                                    stream.name,
                                    "duplicate-id",
                                ))
                                .inc_by(message.value.len() as u64);
                        }
                        offsets.push((
                            stream.clone(),
                            Offset::Int(IntOffset::new(ack.sequence as i64, stream.partition)),
                        ));
                    }
                    Err(e) => {
                        error!(?e, stream = ?stream, "Blocking write failed");
                        // Since we failed to write to the stream, we need to send a NAK to the reader
                        this.tracker_handle
                            .discard(message.offset.clone())
                            .await
                            .expect("Failed to discard offset from the tracker");
                        return;
                    }
                }
            }

            if let Some(mut watermark_handle) = this.watermark_handle.clone() {
                // now the pafs have resolved, lets use the offsets to send watermark
                for (stream, offset) in offsets {
                    Self::publish_watermark(&mut watermark_handle, stream, offset, &message).await;
                }
            }

            // Now that the PAF is resolved, we can delete the entry from the tracker which will send
            // an ACK to the reader.
            this.tracker_handle
                .delete(message.offset.clone())
                .await
                .expect("Failed to delete offset from tracker");
        });

        Ok(())
    }

    /// publishes the watermark for the given stream and offset
    async fn publish_watermark(
        watermark_handle: &mut WatermarkHandle,
        stream: Stream,
        offset: Offset,
        message: &Message,
    ) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::pipeline::isb::BufferWriterConfig;
    use crate::message::{IntOffset, Message, MessageID, Offset, ReadAck};
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
        let tracker_handle = TrackerHandle::new(None);

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

        let writer_components = ISBWriterComponents {
            config: vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![stream.clone()],
                    ..Default::default()
                },
                conditions: None,
                to_vertex_type: VertexType::Sink,
            }],
            js_ctx: context.clone(),
            paf_concurrency: 100,
            tracker_handle: tracker_handle.clone(),
            cancel_token: cln_token.clone(),
            watermark_handle: None,
            vertex_type: VertexType::Source,
            isb_config: None,
        };
        let writer = ISBWriter::new(writer_components);

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

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
                ..Default::default()
            };
            ack_rxs.push(ack_rx);
            tracker_handle.insert(&message, ack_tx).await.unwrap();
            tx.send(message).await.unwrap();
        }
        drop(tx);

        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }

        write_handle.await.unwrap().unwrap();

        // make sure all messages are acked
        assert!(tracker_handle.is_empty().await.unwrap());
        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_streaming_write_with_cancellation() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let tracker_handle = TrackerHandle::new(None);
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

        let writer_components = ISBWriterComponents {
            config: vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![stream.clone()],
                    ..Default::default()
                },
                conditions: None,
                to_vertex_type: VertexType::Sink,
            }],
            js_ctx: context.clone(),
            paf_concurrency: 100,
            tracker_handle: tracker_handle.clone(),
            cancel_token: cln_token.clone(),
            watermark_handle: None,
            vertex_type: VertexType::Source,
            isb_config: None,
        };
        let writer = ISBWriter::new(writer_components);

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

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
                ..Default::default()
            };
            ack_rxs.push(ack_rx);
            tracker_handle.insert(&message, ack_tx).await.unwrap();
            tx.send(message).await.unwrap();
        }

        // Cancel after sending some messages
        cln_token.cancel();
        drop(tx);

        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }

        write_handle.await.unwrap().unwrap();

        // make sure all messages are acked
        assert!(tracker_handle.is_empty().await.unwrap());
        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_streaming_write_multiple_streams_vertices() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let tracker_handle = TrackerHandle::new(None);
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

        let writer_components = ISBWriterComponents {
            config: vec![
                ToVertexConfig {
                    name: "vertex1",
                    partitions: 2,
                    writer_config: BufferWriterConfig {
                        streams: vertex1_streams.clone(),
                        ..Default::default()
                    },
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
                    writer_config: BufferWriterConfig {
                        streams: vertex2_streams.clone(),
                        ..Default::default()
                    },
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
                    writer_config: BufferWriterConfig {
                        streams: vertex3_streams.clone(),
                        ..Default::default()
                    },
                    conditions: None, // No conditions, always forward
                    to_vertex_type: VertexType::Sink,
                },
            ],
            js_ctx: context.clone(),
            paf_concurrency: 100,
            tracker_handle: tracker_handle.clone(),
            cancel_token: cln_token.clone(),
            watermark_handle: None,
            vertex_type: VertexType::Source,
            isb_config: None,
        };
        let writer = ISBWriter::new(writer_components);

        let (tx, rx) = mpsc::channel(10);
        let messages_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

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
                ..Default::default()
            };
            ack_rxs.push(ack_rx);
            tracker_handle.insert(&message, ack_tx).await.unwrap();
            tx.send(message).await.unwrap();
        }
        drop(tx);

        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }

        write_handle.await.unwrap().unwrap();

        // make sure all messages are acked
        assert!(tracker_handle.is_empty().await.unwrap());

        for stream_name in vertex1_streams
            .iter()
            .chain(&vertex2_streams)
            .chain(&vertex3_streams)
        {
            context.delete_stream(stream_name.name).await.unwrap();
        }
    }

    async fn create_streams_and_consumers(
        context: &async_nats::jetstream::Context,
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
