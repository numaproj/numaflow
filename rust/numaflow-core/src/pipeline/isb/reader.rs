use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::Result;
use crate::config::get_vertex_name;
use crate::config::pipeline::VertexType::ReduceUDF;
use crate::config::pipeline::isb::{BufferReaderConfig, ISBConfig, Stream};
use crate::error::Error;
use crate::message::{IntOffset, Message, MessageType, Offset, ReadAck};
use crate::metrics::{
    PIPELINE_PARTITION_NAME_LABEL, jetstream_isb_error_metrics_labels,
    jetstream_isb_metrics_labels, pipeline_metric_labels, pipeline_metrics,
};
use crate::tracker::TrackerHandle;
use crate::typ::NumaflowTypeConfig;
use crate::watermark::isb::ISBWatermarkHandle;

use crate::watermark::wmb::WMB;
use async_nats::jetstream::Context;
use backoff::retry::Retry;
use backoff::strategy::fixed;
use numaflow_throttling::RateLimiter;
use tracing::{error, warn};

const ACK_RETRY_INTERVAL: u64 = 100; // ms
const ACK_RETRY_ATTEMPTS: usize = usize::MAX;

// Alias the thin JS reader as JSReader to avoid name clashes
use crate::pipeline::isb::jetstream::reader::JetStreamReader as JSReader;

// Compact container to avoid clippy::too_many_arguments in wip_loop
struct WipParams {
    stream_name: &'static str,
    labels: Vec<(String, String)>,
    jsr: JSReader,
    offset: Offset,
    ack_rx: oneshot::Receiver<ReadAck>,
    tick: Duration,
    permit: tokio::sync::OwnedSemaphorePermit,
    cancel: CancellationToken,
    message_processing_start: Instant,
}

/// Orchestrator that drives fetching, WIP, ack/nack, watermark/idle handling over a thin JS reader.
#[derive(Clone)]
pub(crate) struct ISBReader<C: NumaflowTypeConfig> {
    vertex_type: String,
    stream: Stream,
    cfg: BufferReaderConfig,
    batch_size: usize,
    read_timeout: Duration,
    tracker: TrackerHandle,
    watermark: Option<ISBWatermarkHandle>,
    inner: JSReader,
    rate_limiter: Option<C::RateLimiter>,
}

impl<C: NumaflowTypeConfig> ISBReader<C> {
    pub(crate) async fn new(
        components: ISBReaderComponents,
        inner: JSReader,
        rate_limiter: Option<C::RateLimiter>,
    ) -> Result<Self> {
        Ok(Self {
            vertex_type: components.vertex_type,
            stream: components.stream,
            cfg: components.config,
            batch_size: components.batch_size,
            read_timeout: components.read_timeout,
            tracker: components.tracker_handle,
            watermark: components.watermark_handle,
            inner,
            rate_limiter,
        })
    }

    /// Start a background loop that fetches messages, starts WIP, and forwards core messages to a channel.
    pub(crate) async fn streaming_read(
        mut self,
        cancel: CancellationToken,
    ) -> Result<(ReceiverStream<Message>, JoinHandle<Result<()>>)> {
        let max_ack_pending = self.cfg.max_ack_pending;
        let batch_size = std::cmp::min(self.batch_size, max_ack_pending);
        let mut labels = pipeline_metric_labels(&self.vertex_type).clone();
        labels.push((
            PIPELINE_PARTITION_NAME_LABEL.to_string(),
            self.stream.name.to_string(),
        ));
        let (tx, rx) = mpsc::channel(batch_size);
        let handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            let semaphore = Arc::new(Semaphore::new(max_ack_pending));
            loop {
                if cancel.is_cancelled() {
                    break;
                }

                // Acquire permits up-front to cap inflight
                let mut permits = Arc::clone(&semaphore)
                    .acquire_many_owned(batch_size as u32)
                    .await
                    .map_err(|e| Error::ISB(format!("Failed to acquire semaphore permit: {e}")))?;

                let start = Instant::now();

                // Apply rate limiting if configured to determine effective batch
                let (effective_batch_size, token_epoch) = match &self.rate_limiter {
                    Some(rl) => {
                        let cur_epoch = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .expect("Time went backwards beyond unix epoch")
                            .as_secs();
                        let acquired = rl
                            .acquire_n(Some(batch_size), Some(Duration::from_secs(1)))
                            .await;
                        let eff = std::cmp::min(acquired, batch_size);
                        (eff, cur_epoch)
                    }
                    None => (batch_size, 0),
                };

                let mut batch = if effective_batch_size == 0 {
                    Vec::new()
                } else {
                    match self
                        .inner
                        .fetch(effective_batch_size, self.read_timeout)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            pipeline_metrics()
                                .jetstream_isb
                                .read_error_total
                                .get_or_create(&jetstream_isb_error_metrics_labels(
                                    self.stream.name,
                                    e.to_string(),
                                ))
                                .inc();
                            warn!(?e, stream=?self.stream, "Failed to get message batch from Jetstream (ignoring, will be retried)");
                            Vec::new()
                        }
                    }
                };
                pipeline_metrics()
                    .jetstream_isb
                    .read_time_total
                    .get_or_create(&jetstream_isb_metrics_labels(self.stream.name))
                    .observe(start.elapsed().as_micros() as f64);

                // Deposit unused tokens back if any
                if let Some(rl) = &self.rate_limiter {
                    rl.deposit_unused(
                        effective_batch_size.saturating_sub(batch.len()),
                        token_epoch,
                    )
                    .await;
                }

                // Idle handling (publish reduce WMB when truly idle)
                if let Some(wm) = self.watermark.as_mut() {
                    if batch.is_empty() {
                        let idle_wmb = wm.fetch_head_idle_wmb(self.stream.partition).await;
                        match idle_wmb {
                            Some(wmb) => {
                                self.tracker
                                    .set_idle_offset(self.stream.partition, Some(wmb.offset))
                                    .await?;
                                Self::create_and_write_wmb_message_for_reduce(
                                    &self.vertex_type,
                                    self.stream.partition,
                                    wmb,
                                    &tx,
                                )
                                .await?;
                            }
                            None => {
                                self.tracker
                                    .set_idle_offset(self.stream.partition, None)
                                    .await?;
                            }
                        }
                    } else {
                        self.tracker
                            .set_idle_offset(self.stream.partition, None)
                            .await?;
                    }
                }

                // For each message: enrich WM, start WIP, send to channel
                for mut message in batch.drain(..) {
                    if let MessageType::WMB = message.typ {
                        // Ack and skip JS WMB control messages
                        self.inner.ack(&[message.offset.clone()]).await?;
                        continue;
                    }

                    // Fetch watermark per message (orchestrator-level concern)
                    if let Some(wm) = self.watermark.as_mut() {
                        let watermark = wm.fetch_watermark(message.offset.clone()).await;
                        message.watermark = Some(watermark);
                    }

                    // Update per-message read metrics
                    Self::publish_read_metrics(&labels, &message);

                    // Insert into tracker and spawn WIP loop
                    let (ack_tx, ack_rx) = oneshot::channel();
                    self.tracker.insert(&message, ack_tx).await?;

                    let params = WipParams {
                        stream_name: self.stream.name,
                        labels: labels.clone(),
                        jsr: self.inner.clone(),
                        offset: message.offset.clone(),
                        ack_rx,
                        tick: self.cfg.wip_ack_interval,
                        permit: permits.split(1).expect("Failed to split permit"),
                        cancel: cancel.clone(),
                        message_processing_start: start,
                    };
                    tokio::spawn(async move {
                        Self::wip_loop(params).await;
                    });

                    if tx.send(message).await.is_err() {
                        break;
                    }
                }

                // Update per-batch processing time metric
                pipeline_metrics()
                    .forwarder
                    .read_processing_time
                    .get_or_create(&labels)
                    .observe(start.elapsed().as_micros() as f64);
            }
            // Drain: wait for inflight to finish
            let _ = Arc::clone(&semaphore)
                .acquire_many_owned(max_ack_pending as u32)
                .await;

            // Shutdown rate limiter if configured
            if let Some(rl) = &self.rate_limiter {
                rl.shutdown()
                    .await
                    .map_err(|e| Error::ISB(format!("Failed to shutdown rate limiter: {e}")))?;
            }

            Ok(())
        });
        Ok((ReceiverStream::new(rx), handle))
    }
    pub(crate) async fn pending(&mut self) -> Result<Option<usize>> {
        self.inner.pending().await
    }

    pub(crate) fn name(&mut self) -> &'static str {
        self.inner.name()
    }

    // Periodically mark WIP until ack/nack received, then perform final ack/nack and publish metrics.
    async fn wip_loop(mut params: WipParams) {
        let mut interval = time::interval(params.tick);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let _ = params.jsr.mark_wip(std::slice::from_ref(&params.offset)).await;
                },
                res = &mut params.ack_rx => {
                    match res.unwrap_or(ReadAck::Nak) {
                        ReadAck::Ack => {
                            let ack_start = Instant::now();
                            Self::ack_with_retry(params.jsr.clone(), params.offset.clone(), params.cancel.clone()).await;
                            Self::publish_ack_metrics(
                                params.stream_name,
                                &params.labels,
                                ack_start,
                                params.message_processing_start,
                            );
                        },
                        ReadAck::Nak => {
                            Self::nak_with_retry(params.jsr.clone(), params.offset.clone(), params.cancel.clone()).await;
                        },
                    }
                    break;
                }
            }
        }
    }

    async fn ack_with_retry(jsr: JSReader, offset: Offset, cancel: CancellationToken) {
        let interval = fixed::Interval::from_millis(ACK_RETRY_INTERVAL).take(ACK_RETRY_ATTEMPTS);
        let _ = Retry::new(
            interval,
            async || {
                jsr.ack(std::slice::from_ref(&offset))
                    .await
                    .map_err(|e| Error::ISB(format!("Failed to send Ack to JetStream: {e}")))
            },
            |e: &Error| {
                if cancel.is_cancelled() {
                    error!(
                        ?e,
                        ?offset,
                        "Cancellation received, stopping Ack retry loop"
                    );
                    return false;
                }
                warn!(?e, ?offset, "Ack to JetStream failed, retrying...");
                true
            },
        )
        .await;
    }

    async fn nak_with_retry(jsr: JSReader, offset: Offset, cancel: CancellationToken) {
        let interval = fixed::Interval::from_millis(ACK_RETRY_INTERVAL).take(ACK_RETRY_ATTEMPTS);
        let _ = Retry::new(
            interval,
            async || {
                jsr.nack(std::slice::from_ref(&offset))
                    .await
                    .map_err(|e| Error::ISB(format!("Failed to send Nak to JetStream: {e}")))
            },
            |e: &Error| {
                if cancel.is_cancelled() {
                    error!(
                        ?e,
                        ?offset,
                        "Cancellation received, stopping Nak retry loop"
                    );
                    return false;
                }
                warn!(?e, ?offset, "Nak to JetStream failed, retrying...");
                true
            },
        )
        .await;
    }

    async fn create_and_write_wmb_message_for_reduce(
        vertex_type: &str,
        partition: u16,
        idle_wmb: WMB,
        tx: &mpsc::Sender<Message>,
    ) -> Result<()> {
        if vertex_type != ReduceUDF.as_str() {
            return Ok(());
        }
        let idle_watermark = chrono::DateTime::from_timestamp_millis(idle_wmb.watermark)
            .expect("Failed to create watermark from WMB");
        let msg = Message {
            typ: MessageType::WMB,
            watermark: Some(idle_watermark),
            offset: Offset::Int(IntOffset::new(idle_wmb.offset, partition)),
            event_time: idle_watermark,
            id: crate::message::MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: idle_wmb.offset.to_string().into(),
                index: 0,
            },
            ..Default::default()
        };
        tx.send(msg)
            .await
            .map_err(|_| Error::ISB("Failed to send wmb message to channel".to_string()))
    }

    fn publish_ack_metrics(
        stream_name: &'static str,
        labels: &[(String, String)],
        ack_start: Instant,
        message_processing_start: Instant,
    ) {
        let labels_vec = labels.to_vec();
        pipeline_metrics()
            .jetstream_isb
            .ack_time_total
            .get_or_create(&jetstream_isb_metrics_labels(stream_name))
            .observe(ack_start.elapsed().as_micros() as f64);
        pipeline_metrics()
            .forwarder
            .ack_processing_time
            .get_or_create(&labels_vec)
            .observe(ack_start.elapsed().as_micros() as f64);
        pipeline_metrics()
            .forwarder
            .ack_total
            .get_or_create(&labels_vec)
            .inc();
        pipeline_metrics()
            .forwarder
            .e2e_time
            .get_or_create(&labels_vec)
            .observe(message_processing_start.elapsed().as_micros() as f64);
    }

    fn publish_read_metrics(labels: &[(String, String)], message: &Message) {
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
}

/// Components needed to create a JetStreamReader.
#[derive(Clone)]
pub(crate) struct ISBReaderComponents {
    pub vertex_type: String,
    pub stream: Stream,
    pub js_ctx: Context,
    pub config: BufferReaderConfig,
    pub tracker_handle: TrackerHandle,
    pub batch_size: usize,
    pub read_timeout: Duration,
    pub watermark_handle: Option<ISBWatermarkHandle>,
    pub isb_config: Option<ISBConfig>,
    pub cln_token: CancellationToken,
}

impl ISBReaderComponents {
    pub fn new(
        stream: Stream,
        reader_config: BufferReaderConfig,
        watermark_handle: Option<ISBWatermarkHandle>,
        context: &crate::pipeline::PipelineContext<'_>,
    ) -> Self {
        Self {
            vertex_type: context.config.vertex_type.to_string(),
            stream,
            js_ctx: context.js_context.clone(),
            config: reader_config,
            tracker_handle: context.tracker_handle.clone(),
            batch_size: context.config.batch_size,
            read_timeout: context.config.read_timeout,
            watermark_handle,
            isb_config: context.config.isb_config.clone(),
            cln_token: context.cln_token.clone(),
        }
    }
}
