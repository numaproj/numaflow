use std::collections::{BTreeSet, HashMap};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_nats::jetstream::Context;
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::context::{Publish, PublishAckFuture, PublishErrorKind};
use async_nats::jetstream::publish::PublishAck;
use async_nats::jetstream::stream::RetentionPolicy::Limits;
use bytes::{Bytes, BytesMut};
use flate2::Compression;
use flate2::write::GzEncoder;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::Result;
use crate::config::pipeline::isb::{BufferFullStrategy, CompressionType, ISBConfig, Stream};
use crate::config::pipeline::{ToVertexConfig, VertexType};
use crate::error::Error;

use crate::message::{IntOffset, Message, Offset};
use crate::metrics::{
    PIPELINE_PARTITION_NAME_LABEL, jetstream_isb_error_metrics_labels,
    jetstream_isb_metrics_labels, pipeline_drop_metric_labels, pipeline_metric_labels,
    pipeline_metrics,
};
use crate::shared::forward;
use crate::tracker::TrackerHandle;
use crate::watermark::WatermarkHandle;

#[derive(Debug)]
pub struct BufferInfo {
    pub soft_usage: f64,
    pub solid_usage: f64,
    pub num_pending: u64,
    pub num_ack_pending: usize,
}
/// Components needed to create a JetstreamWriter, with reduced duplication
#[derive(Clone)]
pub(crate) struct ISBWriterComponents {
    pub config: Vec<ToVertexConfig>,
    pub js_ctx: Context,
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

const DEFAULT_RETRY_INTERVAL_MILLIS: u64 = 10;
const DEFAULT_REFRESH_INTERVAL_SECS: u64 = 1;

/// Writes to JetStream ISB. Exposes both write and blocking methods to write messages.
/// It accepts a cancellation token to stop infinite retries during shutdown.
/// JetstreamWriter is one to many mapping of streams to write messages to. It also
/// maintains the buffer usage metrics and pending metrics for each stream.
///
/// Error handling and shutdown: Unlike udf components we will not have non retryable
/// errors here all the failures are infinitely retried until the message is successfully
/// written. The cancellation token is used to short-circuit the retries during shutdown.
/// We will always drain the stream from which we are reading messages.
#[derive(Clone)]
pub(crate) struct JetstreamWriter {
    config: Arc<Vec<ToVertexConfig>>,
    js_ctx: Context,
    /// HashMap of streams (a vertex can write to any immediate downstream) and a bool to represent
    /// whether the corresponding stream is full.
    is_full: HashMap<&'static str, Arc<AtomicBool>>,
    tracker_handle: TrackerHandle,
    sem: Arc<Semaphore>,
    watermark_handle: Option<WatermarkHandle>,
    paf_concurrency: usize,
    vertex_type: VertexType,
    compression_type: Option<CompressionType>,
}

impl JetstreamWriter {
    /// Creates a JetStream Writer and a background task to make sure the Write futures (PAFs) are
    /// successful. Batch Size determines the maximum pending futures.
    pub(crate) fn new(writer_components: ISBWriterComponents) -> Self {
        let to_vertex_streams = writer_components
            .config
            .iter()
            .flat_map(|c| c.writer_config.streams.clone())
            .collect::<Vec<Stream>>();

        let is_full = to_vertex_streams
            .iter()
            .map(|stream| (stream.name, Arc::new(AtomicBool::new(false))))
            .collect::<HashMap<_, _>>();

        let this = Self {
            config: Arc::new(writer_components.config),
            js_ctx: writer_components.js_ctx,
            is_full,
            tracker_handle: writer_components.tracker_handle,
            sem: Arc::new(Semaphore::new(writer_components.paf_concurrency)),
            watermark_handle: writer_components.watermark_handle,
            paf_concurrency: writer_components.paf_concurrency,
            vertex_type: writer_components.vertex_type,
            compression_type: writer_components
                .isb_config
                .map(|c| c.compression.compress_type),
        };

        // spawn a task for checking whether buffer is_full
        tokio::task::spawn({
            let mut this = this.clone();
            async move {
                this.check_stream_status(writer_components.cancel_token)
                    .await;
            }
        });

        this
    }

    /// Checks the buffer usage metrics (soft and solid usage) and pending metrics for each stream in the streams vector.
    /// If the usage is greater than the bufferUsageLimit, it sets the is_full flag to true.
    async fn check_stream_status(&mut self, cln_token: CancellationToken) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(DEFAULT_REFRESH_INTERVAL_SECS));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    for config in &*self.config {
                        for stream in &config.writer_config.streams {
                            let stream = stream.name;
                            let buffer_labels = jetstream_isb_metrics_labels(stream);
                            match Self::fetch_buffer_info(self.js_ctx.clone(), stream, config.writer_config.max_length).await {
                                Ok(buffer_info) => {
                                    if buffer_info.solid_usage >= config.writer_config.usage_limit && buffer_info.soft_usage >= config.writer_config.usage_limit {
                                        if let Some(is_full) = self.is_full.get(stream) {
                                            is_full.store(true, Ordering::Relaxed);
                                        }
                                    } else if let Some(is_full) = self.is_full.get(stream) {
                                        is_full.store(false, Ordering::Relaxed);
                                    }
                                    pipeline_metrics().jetstream_isb.buffer_soft_usage.get_or_create(&buffer_labels).set(buffer_info.soft_usage);
                                    pipeline_metrics().jetstream_isb.buffer_solid_usage.get_or_create(&buffer_labels).set(buffer_info.solid_usage);
                                    pipeline_metrics().jetstream_isb.buffer_pending.get_or_create(&buffer_labels).set(buffer_info.num_pending as i64);
                                    pipeline_metrics().jetstream_isb.buffer_ack_pending.get_or_create(&buffer_labels).set(buffer_info.num_ack_pending as i64);
                                }
                                Err(e) => {
                                    error!(?e, "Failed to fetch buffer info for stream {}, updating isFull to true", stream);
                                    pipeline_metrics().jetstream_isb.isfull_error_total.get_or_create(&jetstream_isb_error_metrics_labels(stream, e.to_string())).inc();
                                    if let Some(is_full) = self.is_full.get(stream) {
                                        is_full.store(true, Ordering::Relaxed);
                                    }
                                }
                            }
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
        js_ctx: Context,
        stream_name: &str,
        max_length: usize,
    ) -> Result<BufferInfo> {
        let mut stream = js_ctx
            .get_stream(stream_name)
            .await
            .map_err(|_| Error::ISB("Failed to get stream".to_string()))?;

        let stream_info = stream
            .info()
            .await
            .map_err(|e| Error::ISB(format!("Failed to get the stream info {e}")))?;

        let mut consumer: PullConsumer = js_ctx
            .get_consumer_from_stream(stream_name, stream_name)
            .await
            .map_err(|e| Error::ISB(format!("Failed to get the consumer {e}")))?;

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

    /// Starts reading messages from the stream and writes them to Jetstream ISB.
    pub(crate) async fn streaming_write(
        self,
        messages_stream: ReceiverStream<Message>,
        cln_token: CancellationToken,
    ) -> Result<JoinHandle<Result<()>>> {
        let handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            let mut this = self;
            let mut messages_stream = messages_stream;

            // Pre-compute all downstream streams we might write to
            let all_streams: BTreeSet<Stream> = this
                .config
                .iter()
                .flat_map(|c| c.writer_config.streams.clone())
                .collect();

            // track which streams received at least one message within the current idle interval
            let mut idle_streams = all_streams.clone();

            // Fixed idle tick; on each tick, if only a subset of streams were written, publish idle WM for the rest
            let mut idle_tick = tokio::time::interval(Duration::from_millis(1000));
            idle_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    maybe_msg = messages_stream.next() => {
                        let Some(message) = maybe_msg else { break; };

                        let write_processing_start = Instant::now();
                        // if message needs to be dropped, ack and continue
                        if message.dropped() {
                            // delete the entry from tracker
                            this.tracker_handle
                                .delete(message.offset)
                                .await
                                .expect("Failed to delete offset from tracker");
                            pipeline_metrics()
                                .forwarder
                                .udf_drop_total
                                .get_or_create(&pipeline_drop_metric_labels(
                                    this.vertex_type.as_str(),
                                    "n/a",
                                    "to_drop",
                                ))
                                .inc();
                            continue;
                        }

                        let mut message = message;
                        // if compression is enabled, then compress and send
                        message.value = Self::compress(this.compression_type, message.value)?;

                        // List of PAFs(one message can be written to multiple streams)
                        let mut pafs = vec![];
                        for vertex in &*this.config {
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

                            if let Some(paf) = this
                                .write(
                                    stream.clone(),
                                    message.clone(),
                                    vertex.writer_config.buffer_full_strategy.clone(),
                                    cln_token.clone(),
                                )
                                .await
                            {
                                idle_streams.remove(&stream);
                                pafs.push((stream, write_processing_start, paf));
                            }
                        }

                        // pafs is empty means message should not be written to any stream, so we can delete
                        // and continue. The `to_drop()` case is already handled above.
                        // NOTE: PAFs can be empty during following scenarios:
                        //  1. Conditional forwarding conditions are not met.
                        if pafs.is_empty() {
                            debug!(
                                tags = ?message.tags,
                                "message will be dropped because conditional forwarding rules are not met"
                            );

                            pipeline_metrics()
                                .forwarder
                                .udf_drop_total
                                .get_or_create(&pipeline_drop_metric_labels(
                                    this.vertex_type.as_str(),
                                    "n/a",
                                    "forwarding-rules-not-met",
                                ))
                                .inc();

                            // delete the entry from tracker
                            this.tracker_handle
                                .delete(message.offset)
                                .await
                                .expect("Failed to delete offset from tracker");
                            continue;
                        }

                        this.resolve_pafs(pafs, message, cln_token.clone())
                            .await
                            .inspect_err(|e| {
                                error!(?e, "Failed to resolve PAFs");
                                cln_token.cancel();
                            })?;
                    }
                    _ = idle_tick.tick() => {
                        // Only invoke when we have partial streams (some written, some not). If none written, skip.
                        if idle_streams.len() < all_streams.len() {
                            if let Some(wm_handle) = this.watermark_handle.as_mut() {
                                JetstreamWriter::publish_idle_watermark(wm_handle, Some(idle_streams.iter().cloned().collect())).await;
                            }
                            // reset interval state
                            idle_streams = all_streams.clone();
                        }
                    }
                }
            }

            // wait for all the paf resolvers to complete before returning
            let _ = Arc::clone(&this.sem)
                .acquire_many_owned(this.paf_concurrency as u32)
                .await
                .expect("Failed to acquire semaphore permit");

            Ok(())
        });
        Ok(handle)
    }

    /// Compress the message body based on the compression type.
    fn compress(compression_type: Option<CompressionType>, message: Bytes) -> Result<Bytes> {
        match compression_type {
            Some(CompressionType::Gzip) => {
                let mut encoder = GzEncoder::new(vec![], Compression::default());
                encoder.write_all(message.as_ref()).map_err(|e| {
                    Error::ISB(format!("Failed to compress message (write_all): {e}"))
                })?;
                Ok(Bytes::from(encoder.finish().map_err(|e| {
                    Error::ISB(format!("Failed to compress message (finish): {e}"))
                })?))
            }
            Some(CompressionType::Zstd) => {
                // 3 is default if you specify 0
                let mut encoder = zstd::Encoder::new(vec![], 3)
                    .map_err(|e| Error::ISB(format!("Failed to create zstd encoder: {e:?}")))?;
                encoder.write_all(message.as_ref()).map_err(|e| {
                    Error::ISB(format!("Failed to compress message (write_all): {e}"))
                })?;
                Ok(Bytes::from(encoder.finish().map_err(|e| {
                    Error::ISB(format!(
                        "Failed to flush compressed message (encoder_shutdown): {e}"
                    ))
                })?))
            }
            Some(CompressionType::LZ4) => {
                let mut encoder = lz4::EncoderBuilder::new()
                    .build(vec![])
                    .map_err(|e| Error::ISB(format!("Failed to create lz4 encoder: {e:?}")))?;
                encoder.write_all(message.as_ref()).map_err(|e| {
                    Error::ISB(format!("Failed to compress message (write_all): {e}"))
                })?;
                let (compressed, result) = encoder.finish();
                if let Err(e) = result {
                    return Err(Error::ISB(format!(
                        "Failed to flush compressed message (encoder_shutdown): {e}"
                    )));
                };
                Ok(Bytes::from(compressed))
            }
            None | Some(CompressionType::None) => Ok(message),
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
            .get_or_create(&jetstream_isb_metrics_labels(partition_name))
            .observe(write_processing_start.elapsed().as_micros() as f64);
    }

    /// Returns true if the stream is full.
    fn is_stream_full(&self, stream: &Stream) -> Option<bool> {
        self.is_full
            .get(stream.name)
            .map(|is_full| is_full.load(Ordering::Relaxed))
    }

    /// Drops the message by deleting the offset from the tracker.
    async fn drop_message_on_full(&self, stream_name: &str, offset: Offset, msg_bytes: usize) {
        // delete the entry from tracker
        self.tracker_handle
            .delete(offset)
            .await
            .expect("Failed to delete offset from tracker");

        // increment drop metrics when buffer is full and strategy is DiscardLatest
        pipeline_metrics()
            .forwarder
            .drop_total
            .get_or_create(&pipeline_drop_metric_labels(
                self.vertex_type.as_str(),
                stream_name,
                "buffer-full",
            ))
            .inc();
        pipeline_metrics()
            .forwarder
            .drop_bytes_total
            .get_or_create(&pipeline_drop_metric_labels(
                self.vertex_type.as_str(),
                stream_name,
                "buffer-full",
            ))
            .inc_by(msg_bytes as u64);
    }

    /// Publishes the message to the stream once.
    async fn publish_once(
        &self,
        stream_name: &'static str,
        id: &str,
        payload: Bytes,
    ) -> std::result::Result<PublishAckFuture, async_nats::jetstream::context::PublishError> {
        self.js_ctx
            .send_publish(
                stream_name,
                Publish::build().payload(payload).message_id(id),
            )
            .await
    }

    /// Records the publish error metrics.
    fn record_publish_error_metrics(
        stream_name: &str,
        e: async_nats::jetstream::context::PublishError,
    ) {
        pipeline_metrics()
            .jetstream_isb
            .write_error_total
            .get_or_create(&jetstream_isb_error_metrics_labels(
                stream_name,
                e.kind().to_string(),
            ))
            .inc();
        if let PublishErrorKind::TimedOut = e.kind() {
            pipeline_metrics()
                .jetstream_isb
                .write_timeout_total
                .get_or_create(&jetstream_isb_error_metrics_labels(
                    stream_name,
                    e.kind().to_string(),
                ))
                .inc();
        }
    }

    /// Writes the message to the JetStream ISB and returns a future which can be
    /// awaited to get the PublishAck. It will do infinite retries until the message
    /// gets published successfully. If it returns an error it means it is fatal error
    pub(super) async fn write(
        &self,
        stream: Stream,
        message: Message,
        on_full: BufferFullStrategy,
        cln_token: CancellationToken,
    ) -> Option<PublishAckFuture> {
        let mut log_counter = 500u16;
        let offset = message.offset.clone();

        // message id will be used for deduplication
        let id = message.id.to_string();
        let msg_bytes = message.value.len();

        // Serialize once and reuse a cheap cloneable Bytes across retries
        let payload_mut: BytesMut = message
            .try_into()
            .expect("message serialization should not fail");
        let payload: Bytes = payload_mut.freeze();

        while !cln_token.is_cancelled() {
            match self.is_stream_full(&stream) {
                Some(true) => {
                    pipeline_metrics()
                        .jetstream_isb
                        .isfull_total
                        .get_or_create(&jetstream_isb_metrics_labels(stream.name))
                        .inc();
                    if log_counter >= 500 {
                        warn!(?stream, "stream is full (throttled logging)");
                        log_counter = 0;
                    }
                    log_counter += 1;
                    match on_full {
                        BufferFullStrategy::DiscardLatest => {
                            self.drop_message_on_full(stream.name, offset.clone(), msg_bytes)
                                .await;
                            return None;
                        }
                        BufferFullStrategy::RetryUntilSuccess => {}
                    }
                }
                Some(false) => match self.publish_once(stream.name, &id, payload.clone()).await {
                    Ok(ack_fut) => return Some(ack_fut),
                    Err(e) => {
                        error!(?e, "publishing failed, retrying");
                        Self::record_publish_error_metrics(stream.name, e);
                    }
                },
                None => {
                    error!("Stream {} not found in is_full map", stream);
                    return None;
                }
            }

            // sleep to avoid busy looping
            sleep(Duration::from_millis(DEFAULT_RETRY_INTERVAL_MILLIS)).await;
        }

        error!("Shutdown signal received, exiting write loop");
        None
    }

    /// resolve_pafs resolves the PAFs for the given result. It will try to resolve the PAFs
    /// asynchronously, if it fails it will do a blocking write to resolve the PAFs.
    /// At any point in time, we will only have X PAF resolvers running, this will help us create a
    /// natural backpressure.
    pub(super) async fn resolve_pafs(
        &self,
        pafs: Vec<(Stream, Instant, PublishAckFuture)>,
        message: Message,
        cln_token: CancellationToken,
    ) -> Result<()> {
        let permit = Arc::clone(&self.sem)
            .acquire_owned()
            .await
            .map_err(|_e| Error::ISB("Failed to acquire semaphore permit".to_string()))?;

        let mut this = self.clone();

        tokio::spawn(async move {
            let _permit = permit;
            let mut offsets = Vec::new();

            // resolve the pafs
            for (stream, write_processing_start, paf) in pafs {
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
                            .get_or_create(&jetstream_isb_error_metrics_labels(
                                stream.name,
                                e.kind().to_string(),
                            ))
                            .inc();
                        if let PublishErrorKind::TimedOut = e.kind() {
                            pipeline_metrics()
                                .jetstream_isb
                                .write_timeout_total
                                .get_or_create(&jetstream_isb_error_metrics_labels(
                                    stream.name,
                                    e.kind().to_string(),
                                ))
                                .inc();
                        }
                        this.blocking_write(stream.clone(), message.clone(), cln_token.clone())
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

            if let Some(watermark_handle) = this.watermark_handle.as_mut() {
                // now the pafs have resolved, lets use the offsets to send watermark
                for (stream, offset) in offsets {
                    JetstreamWriter::publish_watermark(watermark_handle, stream, offset, &message)
                        .await;
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

    /// Publishes idle watermark for the given set of streams
    async fn publish_idle_watermark(
        watermark_handle: &mut WatermarkHandle,
        streams: Option<Vec<Stream>>,
    ) {
        if let WatermarkHandle::ISB(handle) = watermark_handle {
            handle.publish_idle_watermark(streams).await;
        }
    }

    /// Writes the message to the JetStream ISB and returns the PublishAck. It will do
    /// infinite retries until the message gets published successfully. If it returns
    /// an error it means it is fatal non-retryable error.
    pub(crate) async fn blocking_write(
        &self,
        stream: Stream,
        message: Message,
        cln_token: CancellationToken,
    ) -> Result<PublishAck> {
        let start_time = Instant::now();
        let message_length = message.value.len();
        let payload: BytesMut = message
            .try_into()
            .expect("message serialization should not fail");

        loop {
            match self
                .js_ctx
                .publish(stream.name, payload.clone().freeze())
                .await
            {
                Ok(paf) => match paf.await {
                    Ok(ack) => {
                        if ack.duplicate {
                            warn!(?ack, "Duplicate message detected, ignoring");
                            // Increment drop metric for duplicate messages
                            pipeline_metrics()
                                .forwarder
                                .drop_total
                                .get_or_create(&pipeline_drop_metric_labels(
                                    self.vertex_type.as_str(),
                                    stream.name,
                                    "duplicate-id",
                                ))
                                .inc();
                            pipeline_metrics()
                                .forwarder
                                .drop_bytes_total
                                .get_or_create(&pipeline_drop_metric_labels(
                                    self.vertex_type.as_str(),
                                    stream.name,
                                    "duplicate-id",
                                ))
                                .inc_by(message_length as u64);
                        }
                        debug!(
                            elapsed_ms = start_time.elapsed().as_millis(),
                            "Blocking write successful in",
                        );
                        pipeline_metrics()
                            .jetstream_isb
                            .write_time_total
                            .get_or_create(&jetstream_isb_metrics_labels(stream.name))
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
                        .get_or_create(&jetstream_isb_metrics_labels(stream.name))
                        .inc();
                    error!(?e, "publishing failed, retrying");
                    sleep(Duration::from_millis(DEFAULT_RETRY_INTERVAL_MILLIS)).await;
                }
            }

            if cln_token.is_cancelled() {
                return Err(Error::ISB("Shutdown signal received".to_string()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use async_nats::jetstream;
    use async_nats::jetstream::consumer::{Config, Consumer};
    use async_nats::jetstream::{consumer, stream};
    use bytes::Bytes;
    use chrono::Utc;
    use numaflow_models::models::ForwardConditions;
    use numaflow_models::models::TagConditions;

    use super::*;
    use crate::config::pipeline::isb::BufferWriterConfig;
    use crate::message::{Message, MessageID, ReadAck};

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_async_write() {
        let tracker_handle = TrackerHandle::new(None);
        let cln_token = CancellationToken::new();
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test-async", "temp", 0);
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
            tracker_handle,
            cancel_token: cln_token.clone(),
            watermark_handle: None,
            vertex_type: VertexType::Source,
            isb_config: None,
        };
        let writer = JetstreamWriter::new(writer_components);

        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key_0".to_string()]),
            tags: None,
            value: "message 0".as_bytes().to_vec().into(),
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "offset_0".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        let paf = writer
            .write(
                stream.clone(),
                message,
                BufferFullStrategy::RetryUntilSuccess,
                cln_token.clone(),
            )
            .await;
        assert!(paf.unwrap().await.is_ok());

        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_sync_write() {
        let cln_token = CancellationToken::new();
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test-sync", "temp", 0);
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

        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key_0".to_string()]),
            tags: None,
            value: "message 0".as_bytes().to_vec().into(),
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "offset_0".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

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
            tracker_handle: TrackerHandle::new(None),
            cancel_token: cln_token.clone(),
            watermark_handle: None,
            vertex_type: VertexType::Source,
            isb_config: None,
        };
        let writer = JetstreamWriter::new(writer_components);

        let result = writer
            .blocking_write(stream.clone(), message, cln_token.clone())
            .await;
        assert!(result.is_ok());

        let publish_ack = result.unwrap();
        assert_eq!(publish_ack.stream, stream.name);

        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_write_with_cancellation() {
        let tracker_handle = TrackerHandle::new(None);
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test-cancellation", "temp", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
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
                Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name.to_string(),
            )
            .await
            .unwrap();

        let cancel_token = CancellationToken::new();

        let writer_components = ISBWriterComponents {
            config: vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![stream.clone()],
                    ..Default::default()
                },
                conditions: None,
                to_vertex_type: VertexType::MapUDF,
            }],
            js_ctx: context.clone(),
            paf_concurrency: 100,
            tracker_handle,
            cancel_token: cancel_token.clone(),
            watermark_handle: None,
            vertex_type: VertexType::MapUDF,
            isb_config: None,
        };
        let writer = JetstreamWriter::new(writer_components);

        let mut result_receivers = Vec::new();
        // Publish 10 messages successfully
        for i in 0..10 {
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: None,
                value: format!("message {}", i).as_bytes().to_vec().into(),
                offset: Offset::Int(IntOffset::new(i, 0)),
                event_time: Utc::now(),
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: format!("offset_{}", i).into(),
                    index: i as i32,
                },
                ..Default::default()
            };
            let paf = writer
                .write(
                    stream.clone(),
                    message,
                    BufferFullStrategy::RetryUntilSuccess,
                    cancel_token.clone(),
                )
                .await;
            result_receivers.push(paf);
        }

        // Attempt to publish a message which has a payload size greater than the max_message_size
        // so that it fails and sync write will be attempted and it will be blocked
        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key_11".to_string()]),
            tags: None,
            value: vec![0; 1025].into(),
            offset: Offset::Int(IntOffset::new(11, 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "offset_11".to_string().into(),
                index: 11,
            },
            ..Default::default()
        };
        let paf = writer
            .write(
                stream.clone(),
                message,
                BufferFullStrategy::RetryUntilSuccess,
                cancel_token.clone(),
            )
            .await;
        result_receivers.push(paf);

        // Cancel the token to exit the retry loop
        cancel_token.cancel();

        // Check the results
        for (i, receiver) in result_receivers.into_iter().enumerate() {
            if i < 10 {
                assert!(
                    receiver.unwrap().await.is_ok(),
                    "Message {} should be published successfully",
                    i
                );
            } else {
                assert!(
                    receiver.unwrap().await.is_err(),
                    "Message 11 should fail with cancellation error"
                );
            }
        }

        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_fetch_buffer_info() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_fetch_buffer_info", "temp", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                max_messages: 1000,
                max_message_size: 1024,
                max_messages_per_subject: 1000,
                retention: Limits, // Set retention policy to Limits for solid usage
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
                stream.name.to_string(),
            )
            .await;

        let _consumer = context
            .create_consumer_on_stream(
                Config {
                    name: Some(stream.name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream.name.to_string(),
            )
            .await
            .unwrap();

        let max_length = 100;

        // Publish messages to fill the buffer
        for _ in 0..80 {
            context
                .publish(stream.name, Bytes::from("test message"))
                .await
                .unwrap();
        }

        // Fetch buffer info
        let buffer_info =
            JetstreamWriter::fetch_buffer_info(context.clone(), stream.name, max_length)
                .await
                .unwrap();

        // Verify the buffer info metrics
        assert_eq!(buffer_info.soft_usage, 0.8);
        assert_eq!(buffer_info.solid_usage, 0.8);
        assert_eq!(buffer_info.num_pending, 80);

        // Clean up
        context
            .delete_consumer_from_stream(stream.name, stream.name)
            .await
            .unwrap();
        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_check_stream_status() {
        let tracker_handle = TrackerHandle::new(None);
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_check_stream_status", "temp", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.to_string()],
                max_messages: 1000,
                max_message_size: 1024,
                max_messages_per_subject: 1000,
                retention: Limits, // Set retention policy to Limits for solid usage
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
                stream.name.to_string(),
            )
            .await
            .unwrap();

        let cancel_token = CancellationToken::new();
        let writer_components = ISBWriterComponents {
            config: vec![ToVertexConfig {
                name: "test-vertex",
                partitions: 1,
                writer_config: BufferWriterConfig {
                    streams: vec![stream.clone()],
                    max_length: 100,
                    ..Default::default()
                },
                conditions: None,
                to_vertex_type: VertexType::Sink,
            }],
            js_ctx: context.clone(),
            paf_concurrency: 100,
            tracker_handle,
            cancel_token: cancel_token.clone(),
            watermark_handle: None,
            vertex_type: VertexType::Source,
            isb_config: None,
        };
        let writer = JetstreamWriter::new(writer_components);

        let mut js_writer = writer.clone();
        // Simulate the stream status check
        tokio::spawn(async move {
            js_writer.check_stream_status(cancel_token.clone()).await;
        });

        // Publish messages to fill the buffer, since max_length is 100, we need to publish 80 messages
        for _ in 0..80 {
            context
                .publish(stream.name, Bytes::from("test message"))
                .await
                .unwrap();
        }

        let start_time = Instant::now();
        while !writer
            .is_full
            .get(stream.name)
            .map(|is_full| is_full.load(Ordering::Relaxed))
            .unwrap()
            && start_time.elapsed().as_millis() < 1000
        {
            sleep(Duration::from_millis(5)).await;
        }

        // Verify the is_full flag
        assert!(
            writer
                .is_full
                .get(stream.name)
                .map(|is_full| is_full.load(Ordering::Relaxed))
                .unwrap(),
            "Buffer should be full after publishing messages"
        );

        // Clean up
        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_streaming_write() {
        let cln_token = CancellationToken::new();
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let tracker_handle = TrackerHandle::new(None);

        let stream = Stream::new("test_publish_messages", "temp", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.into()],
                max_messages: 1000,
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
                    max_length: 1000,
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
        let writer = JetstreamWriter::new(writer_components);

        let (messages_tx, messages_rx) = tokio::sync::mpsc::channel(500);
        let mut ack_rxs = vec![];
        // Publish 500 messages
        for i in 0..500 {
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
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            tracker_handle.insert(&message, ack_tx).await.unwrap();
            ack_rxs.push(ack_rx);
            messages_tx.send(message).await.unwrap();
        }
        drop(messages_tx);

        let receiver_stream = ReceiverStream::new(messages_rx);
        let _handle = writer
            .streaming_write(receiver_stream, cln_token.clone())
            .await
            .unwrap();

        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }
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

        let stream = Stream::new("test_publish_cancellation", "temp", 0);
        // Delete stream if it exists
        let _ = context.delete_stream(stream.name).await;
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream.name.to_string(),
                subjects: vec![stream.name.into()],
                max_message_size: 1024,
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

        let cancel_token = CancellationToken::new();
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
            cancel_token: cancel_token.clone(),
            watermark_handle: None,
            vertex_type: VertexType::Source,
            isb_config: None,
        };
        let writer = JetstreamWriter::new(writer_components);

        let (tx, rx) = tokio::sync::mpsc::channel(500);
        let mut ack_rxs = vec![];
        // Publish 100 messages successfully
        for i in 0..100 {
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
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            tracker_handle.insert(&message, ack_tx).await.unwrap();
            ack_rxs.push(ack_rx);
            tx.send(message).await.unwrap();
        }

        let receiver_stream = ReceiverStream::new(rx);
        let _handle = writer
            .streaming_write(receiver_stream, cancel_token.clone())
            .await
            .unwrap();

        // Attempt to publish the 101st message, which should get stuck in the retry loop
        // because the max message size is set to 1024
        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key_101".to_string()]),
            tags: None,
            value: vec![0; 1025].into(),
            offset: Offset::Int(IntOffset::new(101, 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "offset_101".to_string().into(),
                index: 101,
            },
            ..Default::default()
        };
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        tracker_handle.insert(&message, ack_tx).await.unwrap();
        ack_rxs.push(ack_rx);
        tx.send(message).await.unwrap();
        drop(tx);

        // Cancel the token to exit the retry loop
        cancel_token.cancel();
        // Check the results
        for (i, receiver) in ack_rxs.into_iter().enumerate() {
            let result = receiver.await.unwrap();
            if i < 100 {
                assert_eq!(result, ReadAck::Ack);
            } else {
                assert_eq!(result, ReadAck::Nak);
            }
        }

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

        let vertex1_streams = vec![
            Stream::new("vertex1-0", "temp", 0),
            Stream::new("vertex1-1", "temp", 0),
        ];
        let vertex2_streams = vec![
            Stream::new("vertex2-0", "temp", 0),
            Stream::new("vertex2-1", "temp", 0),
        ];
        let vertex3_streams = vec![
            Stream::new("vertex3-0", "temp", 0),
            Stream::new("vertex3-1", "temp", 0),
        ];

        let (_, consumers1) = create_streams_and_consumers(&context, &vertex1_streams).await;
        let (_, consumers2) = create_streams_and_consumers(&context, &vertex2_streams).await;
        let (_, consumers3) = create_streams_and_consumers(&context, &vertex3_streams).await;

        let writer_components = ISBWriterComponents {
            config: vec![
                ToVertexConfig {
                    name: "vertex1",
                    partitions: 2,
                    writer_config: BufferWriterConfig {
                        streams: vertex1_streams.clone(),
                        ..Default::default()
                    },
                    conditions: Some(Box::new(ForwardConditions::new(TagConditions {
                        operator: Some("and".to_string()),
                        values: vec!["tag1".to_string(), "tag2".to_string()],
                    }))),
                    to_vertex_type: VertexType::Sink,
                },
                ToVertexConfig {
                    name: "vertex2",
                    partitions: 2,
                    writer_config: BufferWriterConfig {
                        streams: vertex2_streams.clone(),
                        ..Default::default()
                    },
                    conditions: Some(Box::new(ForwardConditions::new(TagConditions {
                        operator: Some("or".to_string()),
                        values: vec!["tag2".to_string()],
                    }))),
                    to_vertex_type: VertexType::Sink,
                },
                ToVertexConfig {
                    name: "vertex3",
                    partitions: 2,
                    writer_config: BufferWriterConfig {
                        streams: vertex3_streams.clone(),
                        ..Default::default()
                    },
                    conditions: Some(Box::new(ForwardConditions::new(TagConditions {
                        operator: Some("not".to_string()),
                        values: vec!["tag1".to_string()],
                    }))),
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
        let writer = JetstreamWriter::new(writer_components);

        let (messages_tx, messages_rx) = tokio::sync::mpsc::channel(500);
        let mut ack_rxs = vec![];
        for i in 0..10 {
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: Some(Arc::from(vec!["tag1".to_string(), "tag2".to_string()])),
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
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            tracker_handle.insert(&message, ack_tx).await.unwrap();
            ack_rxs.push(ack_rx);
            messages_tx.send(message).await.unwrap();
        }
        drop(messages_tx);

        let receiver_stream = ReceiverStream::new(messages_rx);
        let _handle = writer
            .streaming_write(receiver_stream, cln_token.clone())
            .await
            .unwrap();

        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }

        // since its and operation and both the tags match all 10 messages should be written
        // messages will be distributed based on the message id but the total message count
        // should be 10
        let mut write_count = 0;
        for mut consumer in consumers1 {
            write_count += consumer.info().await.unwrap().num_pending;
        }
        assert_eq!(write_count, 10);

        // since its or operation and one of the tags match all 10 messages should be written
        write_count = 0;
        for mut consumer in consumers2 {
            write_count += consumer.info().await.unwrap().num_pending;
        }
        assert_eq!(write_count, 10);

        // since it's a not operation, and none of the tags match, no messages should be written
        write_count = 0;
        for mut consumer in consumers3 {
            write_count += consumer.info().await.unwrap().num_pending;
        }
        assert_eq!(write_count, 0);

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
        context: &Context,
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

    // Unit tests for the compress function
    mod compress_tests {
        use super::*;
        use crate::config::pipeline::isb::CompressionType;
        use flate2::read::GzDecoder;
        use lz4::Decoder;
        use std::io::Read;
        use zstd::Decoder as ZstdDecoder;

        #[test]
        fn test_compress_none_compression() {
            let test_data = Bytes::from("Hello, World!");
            let result = JetstreamWriter::compress(None, test_data.clone()).unwrap();
            assert_eq!(result, test_data);
        }

        #[test]
        fn test_compress_none_compression_type() {
            let test_data = Bytes::from("Hello, World!");
            let result =
                JetstreamWriter::compress(Some(CompressionType::None), test_data.clone()).unwrap();
            assert_eq!(result, test_data);
        }

        #[test]
        fn test_compress_gzip() {
            let test_data =
                Bytes::from("Hello, World! This is a test message for gzip compression.");
            let result =
                JetstreamWriter::compress(Some(CompressionType::Gzip), test_data.clone()).unwrap();

            // Verify the result is different from original (compressed)
            assert_ne!(result, test_data);

            // Verify we can decompress it back to original
            let mut decoder = GzDecoder::new(result.as_ref());
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed).unwrap();
            assert_eq!(decompressed, test_data.as_ref());
        }

        #[test]
        fn test_compress_zstd() {
            let test_data =
                Bytes::from("Hello, World! This is a test message for zstd compression.");
            let result =
                JetstreamWriter::compress(Some(CompressionType::Zstd), test_data.clone()).unwrap();

            // Verify the result is different from original (compressed)
            assert_ne!(result, test_data);

            // Verify we can decompress it back to original
            let mut decoder = ZstdDecoder::new(result.as_ref()).unwrap();
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed).unwrap();
            assert_eq!(decompressed, test_data.as_ref());
        }

        #[test]
        fn test_compress_lz4() {
            let test_data =
                Bytes::from("Hello, World! This is a test message for lz4 compression.");
            let result =
                JetstreamWriter::compress(Some(CompressionType::LZ4), test_data.clone()).unwrap();

            // Verify the result is different from original (compressed)
            assert_ne!(result, test_data);

            // Verify we can decompress it back to original
            let mut decoder = Decoder::new(result.as_ref()).unwrap();
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed).unwrap();
            assert_eq!(decompressed, test_data.as_ref());
        }
    }
}
