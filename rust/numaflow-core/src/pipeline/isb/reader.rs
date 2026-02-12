//! ISBReaderOrchestrator is responsible for reading messages from ISB, assigning watermark to the messages and
//! starts tracking them using the tracker and also listens for ack/nack from the tracker and performs
//! the ack/nack to the ISB.

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
use crate::message::{AckHandle, IntOffset, Message, MessageType, Offset, ReadAck};
use crate::metrics::{
    PIPELINE_PARTITION_NAME_LABEL, jetstream_isb_error_metrics_labels,
    jetstream_isb_metrics_labels, pipeline_metric_labels, pipeline_metrics,
};
use crate::pipeline::isb::ISBReader;
use crate::pipeline::isb::error::ISBError;
use crate::tracker::Tracker;
use crate::typ::NumaflowTypeConfig;
use crate::watermark::isb::ISBWatermarkHandle;

use crate::watermark::wmb::WMB;
use backoff::retry::Retry;
use backoff::strategy::fixed;
use numaflow_throttling::RateLimiter;
use tracing::{error, info, warn};

const ACK_RETRY_INTERVAL: u64 = 100; // ms
const ACK_RETRY_ATTEMPTS: usize = usize::MAX;

/// Type alias for metric labels
type MetricLabels = Arc<Vec<(String, String)>>;

/// ISBReaderOrchestrator component which reads messages from ISB, assigns watermark to the messages and starts
/// tracking them using the tracker and also listens for ack/nack from the tracker and performs the
/// ack/nack to the ISB.
#[derive(Clone)]
pub(crate) struct ISBReaderOrchestrator<C: NumaflowTypeConfig> {
    vertex_type: String,
    stream: Stream,
    cfg: BufferReaderConfig,
    batch_size: usize,
    read_timeout: Duration,
    tracker: Tracker,
    watermark: Option<ISBWatermarkHandle>,
    reader: C::ISBReader,
    rate_limiter: Option<C::RateLimiter>,
    /// Cached metric labels to avoid repeated allocations
    metric_labels: MetricLabels,
}

impl<C: NumaflowTypeConfig> ISBReaderOrchestrator<C> {
    pub(crate) async fn new(
        components: ISBReaderComponents,
        reader: C::ISBReader,
        rate_limiter: Option<C::RateLimiter>,
    ) -> Result<Self> {
        // Build metric labels once during initialization
        let mut labels = pipeline_metric_labels(&components.vertex_type).clone();
        labels.push((
            PIPELINE_PARTITION_NAME_LABEL.to_string(),
            components.stream.name.to_string(),
        ));
        let metric_labels = Arc::new(labels);

        Ok(Self {
            vertex_type: components.vertex_type,
            stream: components.stream,
            cfg: components.config,
            batch_size: components.batch_size,
            read_timeout: components.read_timeout,
            tracker: components.tracker,
            watermark: components.watermark_handle,
            reader,
            rate_limiter,
            metric_labels,
        })
    }

    /// Streaming read from ISB, returns a ReceiverStream and a JoinHandle for monitoring errors.
    pub(crate) async fn streaming_read(
        mut self,
        cancel: CancellationToken,
    ) -> Result<(ReceiverStream<Message>, JoinHandle<Result<()>>)> {
        let max_ack_pending = self.cfg.max_ack_pending;
        let batch_size = std::cmp::min(self.batch_size, max_ack_pending);
        let (tx, rx) = mpsc::channel(batch_size);

        let handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            let semaphore = Arc::new(Semaphore::new(max_ack_pending));

            loop {
                // stop reading if the token is cancelled. cancel is only honored here since it is
                // the first block in the chain.
                if cancel.is_cancelled() {
                    break;
                }

                // Acquire permits up-front to cap inflight messages
                let mut permits = Arc::clone(&semaphore)
                    .acquire_many_owned(batch_size as u32)
                    .await
                    .map_err(|e| {
                        Error::ISB(crate::pipeline::isb::error::ISBError::Other(format!(
                            "Failed to acquire semaphore permit: {e}"
                        )))
                    })?;

                let start = Instant::now();
                // Apply rate limiting and fetch message batch
                let batch = self.apply_rate_limiting_and_fetch(batch_size).await;

                pipeline_metrics()
                    .jetstream_isb
                    .read_time_total
                    .get_or_create(&jetstream_isb_metrics_labels(self.stream.name))
                    .observe(start.elapsed().as_micros() as f64);

                // Handle idle watermarks
                self.handle_idle_watermarks(batch.is_empty(), &tx).await?;

                // Process each message in the batch
                self.process_message_batch(batch, &tx, &mut permits, cancel.clone(), start)
                    .await?;

                pipeline_metrics()
                    .forwarder
                    .read_processing_time
                    .get_or_create(&self.metric_labels)
                    .observe(start.elapsed().as_micros() as f64);
            }

            // Cleanup on shutdown
            self.cleanup_on_shutdown(semaphore, max_ack_pending).await
        });

        Ok((ReceiverStream::new(rx), handle))
    }

    pub(crate) async fn pending(&mut self) -> Result<Option<usize>> {
        self.reader.pending().await
    }

    pub(crate) fn name(&mut self) -> &'static str {
        self.reader.name()
    }

    /// Periodically mark WIP until ack/nack received, then perform final ack/nack and publish metrics.
    async fn wip_loop(mut params: WipParams<C>) {
        // Create interval for WIP if supported by the reader
        let mut wip_interval = params.tick.map(time::interval);

        loop {
            tokio::select! {
                // Only tick if WIP is supported
                _ = async {
                    if let Some(ref mut interval) = wip_interval {
                        interval.tick().await
                    } else {
                        // If no WIP support, wait forever (never fires)
                        std::future::pending::<tokio::time::Instant>().await
                    }
                } => {
                    // mark_wip will be called only if interval returns
                    let _ = params.reader.mark_wip(&params.offset).await;
                },
                res = &mut params.ack_rx => {
                    match res.unwrap_or(ReadAck::Nak) {
                        ReadAck::Ack => {
                            let ack_start = Instant::now();
                            if let Err(e) = Self::ack_with_retry(&params.reader, &params.offset, &params.cancel).await {
                                error!(?e, ?params.offset, "Failed to ack message after retries");
                            } else {
                                Self::publish_ack_metrics(
                                    params.stream_name,
                                    &params.labels,
                                    ack_start,
                                    params.message_processing_start,
                                );
                            }
                        },
                        ReadAck::Nak => {
                            info!(?params.offset, "Nak received for offset");
                            if let Err(e) = Self::nak_with_retry(&params.reader, &params.offset, &params.cancel).await {
                                error!(?e, ?params.offset, "Failed to nack message after retries");
                            }
                        },
                    }
                    params.tracker.delete(&params.offset).await.expect("Failed to remove offset from tracker");
                    break;
                }
            }
        }
    }

    /// invokes the ack with infinite retries until the cancellation token is cancelled.
    async fn ack_with_retry(
        reader: &C::ISBReader,
        offset: &Offset,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let interval = fixed::Interval::from_millis(ACK_RETRY_INTERVAL).take(ACK_RETRY_ATTEMPTS);
        Retry::new(
            interval,
            async || reader.ack(offset).await,
            |e: &Error| {
                if cancel.is_cancelled() {
                    error!(
                        ?e,
                        ?offset,
                        "Cancellation received, stopping Ack retry loop"
                    );
                    return false;
                }
                // Don't retry on OffsetNotFound - it won't succeed on retry
                if matches!(e, Error::ISB(ISBError::OffsetNotFound(_))) {
                    error!(?e, ?offset, "Offset not found, stopping Ack retry loop");
                    return false;
                }
                warn!(?e, ?offset, "Ack failed, retrying...");
                true
            },
        )
        .await
    }

    /// invokes the nack with infinite retries until the cancellation token is cancelled.
    async fn nak_with_retry(
        reader: &C::ISBReader,
        offset: &Offset,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let interval = fixed::Interval::from_millis(ACK_RETRY_INTERVAL).take(ACK_RETRY_ATTEMPTS);
        let result = Retry::new(
            interval,
            async || reader.nack(offset).await,
            |e: &Error| {
                if cancel.is_cancelled() {
                    error!(
                        ?e,
                        ?offset,
                        "Cancellation received, stopping Nak retry loop"
                    );
                    return false;
                }
                // Don't retry on OffsetNotFound - it won't succeed on retry
                if matches!(e, Error::ISB(ISBError::OffsetNotFound(_))) {
                    error!(?e, ?offset, "Offset not found, stopping Nak retry loop");
                    return false;
                }
                warn!(?e, ?offset, "Nak failed, retrying...");
                true
            },
        )
        .await;

        if result.is_ok() {
            info!(?offset, "Nak sent for offset");
        }
        result
    }

    /// Creates and writes a WMB message for reduce vertex when it is idle.
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
        tx.send(msg).await.map_err(|_| {
            Error::ISB(crate::pipeline::isb::error::ISBError::Other(
                "Failed to send wmb message to channel".to_string(),
            ))
        })
    }

    fn publish_ack_metrics(
        stream_name: &'static str,
        labels: &MetricLabels,
        ack_start: Instant,
        message_processing_start: Instant,
    ) {
        pipeline_metrics()
            .jetstream_isb
            .ack_time_total
            .get_or_create(&jetstream_isb_metrics_labels(stream_name))
            .observe(ack_start.elapsed().as_micros() as f64);
        pipeline_metrics()
            .forwarder
            .ack_processing_time
            .get_or_create(labels)
            .observe(ack_start.elapsed().as_micros() as f64);
        pipeline_metrics()
            .forwarder
            .ack_total
            .get_or_create(labels)
            .inc();
        pipeline_metrics()
            .forwarder
            .e2e_time
            .get_or_create(labels)
            .observe(message_processing_start.elapsed().as_micros() as f64);
    }

    fn publish_read_metrics(labels: &MetricLabels, message: &Message) {
        let message_bytes = message.value.len();
        pipeline_metrics()
            .forwarder
            .read_total
            .get_or_create(labels)
            .inc();
        pipeline_metrics()
            .forwarder
            .data_read_total
            .get_or_create(labels)
            .inc();
        pipeline_metrics()
            .forwarder
            .read_bytes_total
            .get_or_create(labels)
            .inc_by(message_bytes as u64);
        pipeline_metrics()
            .forwarder
            .data_read_bytes_total
            .get_or_create(labels)
            .inc_by(message_bytes as u64);
    }

    /// Applies rate limiting and fetches the message batch.
    async fn apply_rate_limiting_and_fetch(&mut self, batch_size: usize) -> Vec<Message> {
        // Apply rate limiting if configured to determine effective batch size
        let effective_batch_size = match &self.rate_limiter {
            Some(rl) => {
                let acquired = rl
                    .acquire_n(Some(batch_size), Some(Duration::from_secs(1)))
                    .await;
                std::cmp::min(acquired, batch_size)
            }
            None => batch_size,
        };

        // Fetch message batch
        let batch = if effective_batch_size == 0 {
            // if throttled
            Vec::new()
        } else {
            match self
                .reader
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
                    warn!(?e, stream=?self.stream, "Failed to get message batch from ISB (ignoring, will be retried)");
                    Vec::new()
                }
            }
        };

        // Deposit unused tokens back if any
        if let Some(rl) = &self.rate_limiter {
            rl.deposit_unused(batch_size.saturating_sub(batch.len()))
                .await;
        }

        batch
    }

    /// Handles idle watermarks for the given batch.
    async fn handle_idle_watermarks(
        &mut self,
        batch_is_empty: bool,
        tx: &mpsc::Sender<Message>,
    ) -> Result<()> {
        if batch_is_empty {
            if let Some(wm) = self.watermark.as_mut() {
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
                            tx,
                        )
                        .await?;
                    }
                    None => {
                        self.tracker
                            .set_idle_offset(self.stream.partition, None)
                            .await?;
                    }
                }
            }
        } else {
            self.tracker
                .set_idle_offset(self.stream.partition, None)
                .await?;
        }
        Ok(())
    }

    /// Processes a batch of messages, enriches them with watermarks, starts message tracking, and
    /// sends them to the downstream channel.
    async fn process_message_batch(
        &mut self,
        mut batch: Vec<Message>,
        tx: &mpsc::Sender<Message>,
        permits: &mut tokio::sync::OwnedSemaphorePermit,
        cancel: CancellationToken,
        processing_start: Instant,
    ) -> Result<()> {
        for mut message in batch.drain(..) {
            // Skip WMB control messages
            if let MessageType::WMB = message.typ {
                self.reader.ack(&message.offset).await?;
                continue;
            }

            // Enrich message with watermark
            if let Some(wm) = self.watermark.as_mut() {
                let watermark = wm.fetch_watermark(message.offset.clone()).await;
                message.watermark = Some(watermark);
            }

            // Publish read metrics
            Self::publish_read_metrics(&self.metric_labels, &message);

            let (ack_tx, ack_rx) = oneshot::channel();
            message.ack_handle = Some(Arc::new(AckHandle::new(ack_tx)));

            // Start message tracking and WIP loop
            self.start_message_tracking(
                &message,
                permits.split(1).expect("Failed to split permit"),
                cancel.clone(),
                processing_start,
                ack_rx,
            )
            .await?;

            // Send message to channel
            if tx.send(message).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    /// Starts tracking the message by adding it to the tracker and spawning a task to periodically
    /// mark WIP until ack/nack is received.
    async fn start_message_tracking(
        &self,
        message: &Message,
        permit: tokio::sync::OwnedSemaphorePermit,
        cancel: CancellationToken,
        processing_start: Instant,
        ack_rx: oneshot::Receiver<ReadAck>,
    ) -> Result<()> {
        self.tracker.insert(message).await?;
        let params = WipParams {
            stream_name: self.stream.name,
            labels: Arc::clone(&self.metric_labels),
            reader: self.reader.clone(),
            offset: message.offset.clone(),
            ack_rx,
            tick: self.reader.wip_ack_interval(),
            _permit: permit,
            cancel,
            message_processing_start: processing_start,
            tracker: self.tracker.clone(),
        };

        tokio::spawn(async move {
            Self::wip_loop(params).await;
        });

        Ok(())
    }

    /// Wait until all inflight messages are acked/nacked before shutting down and shutdown the rate
    /// limiter if configured.
    async fn cleanup_on_shutdown(
        &self,
        semaphore: Arc<Semaphore>,
        max_ack_pending: usize,
    ) -> Result<()> {
        info!(
            "ISBReaderOrchestrator is shutting down (pending={}), waiting for inflight messages to be acked/nacked",
            max_ack_pending - semaphore.available_permits()
        );
        // Wait for inflight messages to finish
        let _ = semaphore.acquire_many_owned(max_ack_pending as u32).await;

        // Shutdown rate limiter if configured
        if let Some(rl) = &self.rate_limiter {
            info!("ISBReaderOrchestrator is shutting down, shutting down rate limiter");
            rl.shutdown().await.map_err(|e| {
                Error::ISB(crate::pipeline::isb::error::ISBError::Other(format!(
                    "Failed to shutdown rate limiter: {e}"
                )))
            })?;
        }

        info!("ISBReaderOrchestrator cleanup on shutdown completed.");

        Ok(())
    }
}

struct WipParams<C: NumaflowTypeConfig> {
    stream_name: &'static str,
    labels: Arc<Vec<(String, String)>>,
    reader: C::ISBReader,
    offset: Offset,
    ack_rx: oneshot::Receiver<ReadAck>,
    tick: Option<Duration>,
    _permit: tokio::sync::OwnedSemaphorePermit, // drop guard
    cancel: CancellationToken,
    message_processing_start: Instant,
    tracker: Tracker,
}

/// Components needed to create an ISB reader.
///
/// This struct holds all the configuration and runtime components needed
/// to create an ISB reader. It is ISB-agnostic - the actual reader creation
/// is handled by the ISBFactory.
#[derive(Clone)]
pub(crate) struct ISBReaderComponents {
    pub vertex_type: String,
    pub stream: Stream,
    pub config: BufferReaderConfig,
    pub tracker: Tracker,
    pub batch_size: usize,
    pub read_timeout: Duration,
    pub watermark_handle: Option<ISBWatermarkHandle>,
    pub isb_config: Option<ISBConfig>,
    pub cln_token: CancellationToken,
}

impl ISBReaderComponents {
    pub fn new<C, F>(
        stream: Stream,
        reader_config: BufferReaderConfig,
        watermark_handle: Option<ISBWatermarkHandle>,
        context: &crate::pipeline::PipelineContext<'_, C, F>,
    ) -> Self
    where
        C: crate::typ::NumaflowTypeConfig,
        F: crate::pipeline::isb::ISBFactory<Reader = C::ISBReader, Writer = C::ISBWriter>,
    {
        Self {
            vertex_type: context.config.vertex_type.to_string(),
            stream,
            config: reader_config,
            tracker: context.tracker.clone(),
            batch_size: context.config.batch_size,
            read_timeout: context.config.read_timeout,
            watermark_handle,
            isb_config: context.config.isb_config.clone(),
            cln_token: context.cln_token.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;

    use super::*;
    use crate::config::pipeline::isb::{BufferReaderConfig, CompressionType};
    use crate::message::{Message, MessageID};
    use crate::pipeline::isb::error::ISBError;
    use crate::pipeline::isb::jetstream::js_reader::JetStreamReader;
    use crate::pipeline::isb::reader::{ISBReaderComponents, ISBReaderOrchestrator};
    use crate::tracker::Tracker;
    use async_nats::jetstream;
    use async_nats::jetstream::consumer::PullConsumer;
    use async_nats::jetstream::{consumer, stream};
    use bytes::{Bytes, BytesMut};
    use chrono::Utc;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use tokio::time::sleep;
    use tokio_stream::StreamExt;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn simple_permit_test() {
        use tokio::sync::Semaphore;

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
        let tracker = Tracker::new(None, CancellationToken::new());

        let js_reader = JetStreamReader::new(stream.clone(), context.clone(), None)
            .await
            .unwrap();

        let isb_reader_components = ISBReaderComponents {
            vertex_type: "Map".to_string(),
            stream: stream.clone(),
            config: buf_reader_config,
            tracker: tracker.clone(),
            batch_size: 500,
            read_timeout: Duration::from_millis(100),
            watermark_handle: None,
            isb_config: None,
            cln_token: CancellationToken::new(),
        };

        let isb_reader: ISBReaderOrchestrator<crate::typ::WithoutRateLimiter> =
            ISBReaderOrchestrator::new(isb_reader_components, js_reader, None)
                .await
                .unwrap();

        let reader_cancel_token = CancellationToken::new();
        let (mut js_reader_rx, js_reader_task) = isb_reader
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
        drop(buffer);

        reader_cancel_token.cancel();
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
        let tracker = Tracker::new(None, CancellationToken::new());

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

        let js_reader = JetStreamReader::new(js_stream.clone(), context.clone(), None)
            .await
            .unwrap();

        let isb_reader_components = ISBReaderComponents {
            vertex_type: "Map".to_string(),
            stream: js_stream.clone(),
            config: buf_reader_config,
            tracker: tracker.clone(),
            batch_size: 1,
            read_timeout: Duration::from_millis(100),
            watermark_handle: None,
            isb_config: None,
            cln_token: CancellationToken::new(),
        };

        let isb_reader: ISBReaderOrchestrator<crate::typ::WithoutRateLimiter> =
            ISBReaderOrchestrator::new(isb_reader_components, js_reader, None)
                .await
                .unwrap();

        let reader_cancel_token = CancellationToken::new();
        let (mut js_reader_rx, js_reader_task) = isb_reader
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

        // wait until the tracker becomes empty, don't wait more than 1 second
        tokio::time::timeout(Duration::from_secs(1), async {
            while !tracker.is_empty().await.unwrap() {
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
        let tracker = Tracker::new(None, CancellationToken::new());

        let js_reader =
            JetStreamReader::new(stream.clone(), context.clone(), Some(isb_config.clone()))
                .await
                .unwrap();

        let isb_reader_components = ISBReaderComponents {
            vertex_type: "Map".to_string(),
            stream: stream.clone(),
            config: buf_reader_config,
            tracker: tracker.clone(),
            batch_size: 500,
            read_timeout: Duration::from_millis(100),
            watermark_handle: None,
            isb_config: Some(isb_config.clone()),
            cln_token: CancellationToken::new(),
        };

        let isb_reader: ISBReaderOrchestrator<crate::typ::WithoutRateLimiter> =
            ISBReaderOrchestrator::new(isb_reader_components, js_reader, None)
                .await
                .unwrap();

        let reader_cancel_token = CancellationToken::new();
        let (mut js_reader_rx, js_reader_task) = isb_reader
            .streaming_read(reader_cancel_token.clone())
            .await
            .unwrap();

        let mut compressed = GzEncoder::new(Vec::new(), Compression::default());
        compressed
            .write_all(Bytes::new().as_ref())
            .map_err(|e| {
                Error::ISB(ISBError::Other(format!(
                    "Failed to compress message (write_all): {}",
                    e
                )))
            })
            .unwrap();

        let body = Bytes::from(
            compressed
                .finish()
                .map_err(|e| {
                    Error::ISB(ISBError::Other(format!(
                        "Failed to compress message (finish): {}",
                        e
                    )))
                })
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

        drop(received_message);

        reader_cancel_token.cancel();
        js_reader_task.await.unwrap().unwrap();
        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_nack_with_retry_success() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_nack_retry_success", "test", 0);
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

        let mut js_reader = JetStreamReader::new(stream.clone(), context.clone(), None)
            .await
            .unwrap();

        // Publish a message
        let message = Message {
            keys: Arc::from(vec!["test-key".to_string()]),
            value: Bytes::from("test message"),
            offset: Offset::Int(IntOffset::new(1, 0)),
            event_time: Utc::now(),
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "offset_1".into(),
                index: 0,
            },
            ..Default::default()
        };

        let message_bytes: BytesMut = message.try_into().unwrap();
        context
            .publish(stream.name, message_bytes.into())
            .await
            .unwrap();

        // Fetch the message to populate offset2jsmsg
        let messages = js_reader
            .fetch(1, Duration::from_millis(1000))
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);

        let offset = messages
            .first()
            .expect("Expected at least one message")
            .offset
            .clone();
        let cancel_token = CancellationToken::new();

        // Test nack_with_retry - should succeed
        let result = ISBReaderOrchestrator::<crate::typ::WithoutRateLimiter>::nak_with_retry(
            &js_reader,
            &offset,
            &cancel_token,
        )
        .await;
        assert!(result.is_ok(), "nack_with_retry should succeed");

        // Verify message is back in pending
        let pending = js_reader.pending().await.unwrap();
        assert_eq!(pending, Some(1));

        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_nack_with_retry_missing_offset() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_nack_retry_missing", "test", 0);
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

        let js_reader = JetStreamReader::new(stream.clone(), context.clone(), None)
            .await
            .unwrap();

        // Try to nack an offset that doesn't exist
        let missing_offset = Offset::Int(IntOffset::new(999, 0));
        let cancel_token = CancellationToken::new();

        // Test nack_with_retry - should fail with OffsetNotFound
        let result = ISBReaderOrchestrator::<crate::typ::WithoutRateLimiter>::nak_with_retry(
            &js_reader,
            &missing_offset,
            &cancel_token,
        )
        .await;
        assert!(
            result.is_err(),
            "nack_with_retry should fail for missing offset"
        );

        if let Err(Error::ISB(ISBError::OffsetNotFound(msg))) = result {
            assert!(msg.contains("999"));
        } else {
            panic!("Expected ISBError::OffsetNotFound");
        }

        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_ack_with_retry_success() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_ack_retry_success", "test", 0);
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

        let mut js_reader = JetStreamReader::new(stream.clone(), context.clone(), None)
            .await
            .unwrap();

        // Publish a message
        let message = Message {
            keys: Arc::from(vec!["test-key".to_string()]),
            value: Bytes::from("test message"),
            offset: Offset::Int(IntOffset::new(1, 0)),
            event_time: Utc::now(),
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "offset_1".into(),
                index: 0,
            },
            ..Default::default()
        };

        let message_bytes: BytesMut = message.try_into().unwrap();
        context
            .publish(stream.name, message_bytes.into())
            .await
            .unwrap();

        // Fetch the message to populate offset2jsmsg
        let messages = js_reader
            .fetch(1, Duration::from_millis(1000))
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);

        let offset = messages
            .first()
            .expect("Expected at least one message")
            .offset
            .clone();
        let cancel_token = CancellationToken::new();

        // Test ack_with_retry - should succeed
        let result = ISBReaderOrchestrator::<crate::typ::WithoutRateLimiter>::ack_with_retry(
            &js_reader,
            &offset,
            &cancel_token,
        )
        .await;
        assert!(result.is_ok(), "ack_with_retry should succeed");

        // Verify message is acked
        let pending = js_reader.pending().await.unwrap();
        assert_eq!(pending, Some(0));

        context.delete_stream(stream.name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_ack_with_retry_missing_offset() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream = Stream::new("test_ack_retry_missing", "test", 0);
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

        let js_reader = JetStreamReader::new(stream.clone(), context.clone(), None)
            .await
            .unwrap();

        // Try to ack an offset that doesn't exist
        let missing_offset = Offset::Int(IntOffset::new(999, 0));
        let cancel_token = CancellationToken::new();

        // Test ack_with_retry - should fail with OffsetNotFound
        let result = ISBReaderOrchestrator::<crate::typ::WithoutRateLimiter>::ack_with_retry(
            &js_reader,
            &missing_offset,
            &cancel_token,
        )
        .await;
        assert!(
            result.is_err(),
            "ack_with_retry should fail for missing offset"
        );

        if let Err(Error::ISB(ISBError::OffsetNotFound(msg))) = result {
            assert!(msg.contains("999"));
        } else {
            panic!("Expected ISBError::OffsetNotFound");
        }

        context.delete_stream(stream.name).await.unwrap();
    }
}

// SimpleBuffer Integration Tests
// These tests use SimpleBuffer to test ISBReaderOrchestrator without a distributed ISB.
#[cfg(test)]
mod simplebuffer_tests {
    use super::*;
    use crate::pipeline::isb::simplebuffer::{SimpleBufferAdapter, WithSimpleBuffer};
    use numaflow_testing::simplebuffer::SimpleBuffer;
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;

    use crate::message::MessageID;
    use bytes::Bytes;
    use chrono::Utc;
    use tokio::time::sleep;
    use tokio_stream::StreamExt;
    use tokio_util::sync::CancellationToken;

    /// Helper to create ISBReaderOrchestrator with SimpleBuffer
    async fn create_orchestrator(
        adapter: &SimpleBufferAdapter,
        batch_size: usize,
        max_ack_pending: usize,
    ) -> (
        ISBReaderOrchestrator<WithSimpleBuffer>,
        Tracker,
        CancellationToken,
    ) {
        let stream = Stream::new("test-stream", "test", 0);
        let cancel = CancellationToken::new();
        let tracker = Tracker::new(None, cancel.clone());

        let buf_reader_config = BufferReaderConfig {
            streams: vec![],
            wip_ack_interval: Duration::from_millis(10),
            max_ack_pending,
        };

        let components = ISBReaderComponents {
            vertex_type: "Map".to_string(),
            stream,
            config: buf_reader_config,
            tracker: tracker.clone(),
            batch_size,
            read_timeout: Duration::from_millis(50),
            watermark_handle: None,
            isb_config: None,
            cln_token: cancel.clone(),
        };

        let orchestrator: ISBReaderOrchestrator<WithSimpleBuffer> =
            ISBReaderOrchestrator::new(components, adapter.reader(), None)
                .await
                .unwrap();

        (orchestrator, tracker, cancel)
    }

    /// Helper to write messages to the buffer
    async fn write_messages(adapter: &SimpleBufferAdapter, count: usize) {
        let writer = adapter.writer();
        for i in 0..count {
            use crate::pipeline::isb::ISBWriter;
            let msg = Message {
                typ: Default::default(),
                keys: Arc::new([format!("key-{}", i)]),
                tags: None,
                value: Bytes::from(format!("payload-{}", i)),
                offset: Offset::Int(IntOffset::new(i as i64, 0)),
                event_time: Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "test".into(),
                    index: i as i32,
                    offset: format!("{}", i).into(),
                },
                headers: Arc::new(HashMap::new()),
                metadata: None,
                is_late: false,
                ack_handle: None,
            };
            writer.write(msg).await.expect("write should succeed");
        }
    }

    /// Test: streaming_read happy path - read messages, ack them, verify completion
    #[tokio::test]
    async fn test_streaming_read_and_ack_flow() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        write_messages(&adapter, 5).await;

        let (orchestrator, tracker, cancel) = create_orchestrator(&adapter, 10, 100).await;

        let (mut rx, handle) = orchestrator.streaming_read(cancel.clone()).await.unwrap();

        // Read all 5 messages
        let mut received = Vec::new();
        for _ in 0..5 {
            if let Some(msg) = rx.next().await {
                received.push(msg);
            }
        }
        assert_eq!(received.len(), 5, "Should receive all 5 messages");

        // Ack all messages by dropping them (default behavior is ack on drop)
        drop(received);

        // Wait for tracker to become empty (all acks processed)
        tokio::time::timeout(Duration::from_secs(2), async {
            while !tracker.is_empty().await.unwrap() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Tracker should become empty after acks");

        // Cancel and verify clean shutdown
        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    /// Test: fetch errors are handled gracefully and reading continues
    #[tokio::test]
    async fn test_fetch_error_recovery() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        write_messages(&adapter, 3).await;

        // Inject 2 fetch failures
        adapter.error_injector().fail_fetches(2);

        let (orchestrator, _tracker, cancel) = create_orchestrator(&adapter, 10, 100).await;

        let (mut rx, handle) = orchestrator.streaming_read(cancel.clone()).await.unwrap();

        // Despite fetch failures, we should eventually get messages
        let mut received = Vec::new();
        let timeout_result = tokio::time::timeout(Duration::from_secs(2), async {
            while received.len() < 3 {
                if let Some(msg) = rx.next().await {
                    received.push(msg);
                }
            }
        })
        .await;

        assert!(
            timeout_result.is_ok(),
            "Should recover from fetch errors and receive messages"
        );
        assert_eq!(received.len(), 3);

        // Ack all messages by dropping
        drop(received);

        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    /// Test: ack retries on transient failures and eventually succeeds
    /// Note: We only test ack retries here because nacks cause message redelivery,
    /// which creates an infinite fetch loop with the streaming_read.
    #[tokio::test]
    async fn test_ack_retry_on_transient_failures() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        write_messages(&adapter, 2).await;

        let (orchestrator, tracker, cancel) = create_orchestrator(&adapter, 10, 100).await;

        let (mut rx, handle) = orchestrator.streaming_read(cancel.clone()).await.unwrap();

        // Read both messages
        let msg1 = rx.next().await.expect("Should receive first message");
        let msg2 = rx.next().await.expect("Should receive second message");

        // Inject ack failures - will retry and succeed after 2 failures
        // With ACK_RETRY_INTERVAL=100ms, 2 failures = ~200ms of retries per message
        adapter.error_injector().fail_acks(4); // 2 failures per message * 2 messages
        drop(msg1);
        drop(msg2);

        // Wait for tracker to process both (ack retries should succeed)
        // Allow 5 seconds: 4 retries * 100ms = 400ms + buffer
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            while !tracker.is_empty().await.unwrap() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        // Cancel before checking result to ensure cleanup
        cancel.cancel();

        // Check the result
        result.expect("Tracker should become empty after retried acks");

        // Wait for handle with a timeout to avoid hanging
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    /// Test: ack/nack stops immediately on OffsetNotFound (non-retryable error)
    #[tokio::test]
    async fn test_ack_nack_stops_on_offset_not_found() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        let reader = adapter.reader();
        let cancel = CancellationToken::new();

        // Test ack with non-existent offset
        let missing_offset = Offset::Int(IntOffset::new(999, 0));
        let ack_result = ISBReaderOrchestrator::<WithSimpleBuffer>::ack_with_retry(
            &reader,
            &missing_offset,
            &cancel,
        )
        .await;

        assert!(ack_result.is_err());
        assert!(
            matches!(ack_result, Err(Error::ISB(ISBError::OffsetNotFound(_)))),
            "Should get OffsetNotFound error for ack"
        );

        // Test nack with non-existent offset
        let nack_result = ISBReaderOrchestrator::<WithSimpleBuffer>::nak_with_retry(
            &reader,
            &missing_offset,
            &cancel,
        )
        .await;

        assert!(nack_result.is_err());
        assert!(
            matches!(nack_result, Err(Error::ISB(ISBError::OffsetNotFound(_)))),
            "Should get OffsetNotFound error for nack"
        );
    }

    /// Test: cancellation stops ack/nack retry loops
    #[tokio::test]
    async fn test_cancellation_stops_retry_loops() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        write_messages(&adapter, 1).await;

        let mut reader = adapter.reader();
        use crate::pipeline::isb::ISBReader;

        // Fetch the message to get a valid offset
        let messages = reader.fetch(1, Duration::from_millis(100)).await.unwrap();
        let offset = messages
            .first()
            .expect("should have at least one message")
            .offset
            .clone();

        // Inject infinite ack failures
        adapter.error_injector().fail_acks(1000);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        // Spawn ack_with_retry in background
        let ack_handle = tokio::spawn(async move {
            ISBReaderOrchestrator::<WithSimpleBuffer>::ack_with_retry(
                &reader,
                &offset,
                &cancel_clone,
            )
            .await
        });

        // Give it time to start retrying
        sleep(Duration::from_millis(50)).await;

        // Cancel should stop the retry loop
        cancel.cancel();

        // Should complete quickly after cancellation
        let result = tokio::time::timeout(Duration::from_secs(1), ack_handle)
            .await
            .expect("ack_with_retry should complete after cancellation")
            .unwrap();

        // Result should be an error (cancelled during retry)
        assert!(result.is_err(), "Should error after cancellation");
    }

    /// Test: WIP failures are ignored and loop continues
    #[tokio::test]
    async fn test_wip_failures_are_ignored() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        write_messages(&adapter, 1).await;

        // Inject WIP failures - these should be ignored
        adapter.error_injector().fail_wip_acks(5);

        let (orchestrator, tracker, cancel) = create_orchestrator(&adapter, 10, 100).await;

        let (mut rx, handle) = orchestrator.streaming_read(cancel.clone()).await.unwrap();

        // Read message
        let msg = rx.next().await.expect("Should receive message");

        // Wait a bit for WIP to be attempted (and fail)
        sleep(Duration::from_millis(50)).await;

        // Ack by dropping msg (is_failed defaults to false)
        drop(msg);

        // Tracker should become empty
        tokio::time::timeout(Duration::from_secs(2), async {
            while !tracker.is_empty().await.unwrap() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Tracker should become empty despite WIP failures");

        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    /// Test: nack causes message redelivery
    /// When a message is nacked, it goes back to Pending state in the buffer
    /// and the streaming_read loop will refetch it.
    #[tokio::test]
    async fn test_nack_causes_redelivery() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        write_messages(&adapter, 1).await;

        let (orchestrator, tracker, cancel) = create_orchestrator(&adapter, 1, 100).await;

        let (mut rx, handle) = orchestrator.streaming_read(cancel.clone()).await.unwrap();

        // Read message first time
        let msg1 = rx.next().await.expect("Should receive message first time");
        let payload1 = msg1.value.clone();

        // Nack it by setting is_failed and dropping
        if let Some(h) = &msg1.ack_handle {
            h.is_failed.store(true, Ordering::Relaxed);
        }
        drop(msg1);

        // After nacking, the message goes back to Pending state and will be refetched.
        // The streaming_read loop immediately fetches it again, so wait for redelivery.
        let msg2 = tokio::time::timeout(Duration::from_secs(1), rx.next())
            .await
            .expect("Should receive redelivered message")
            .expect("Stream should not end");

        assert_eq!(
            msg2.value, payload1,
            "Redelivered message should have same payload"
        );

        // Ack it this time by dropping (is_failed defaults to false)
        drop(msg2);

        // Wait for final ack
        let result = tokio::time::timeout(Duration::from_secs(1), async {
            while !tracker.is_empty().await.unwrap() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        cancel.cancel();
        result.expect("Tracker should become empty after final ack");
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    /// Test: cleanup waits for inflight messages before shutdown
    #[tokio::test]
    async fn test_cleanup_waits_for_inflight() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        write_messages(&adapter, 3).await;

        let (orchestrator, tracker, cancel) = create_orchestrator(&adapter, 10, 100).await;

        let (mut rx, handle) = orchestrator.streaming_read(cancel.clone()).await.unwrap();

        // Read all messages but don't ack yet
        let mut messages = Vec::new();
        for _ in 0..3 {
            if let Some(msg) = rx.next().await {
                messages.push(msg);
            }
        }
        assert_eq!(messages.len(), 3);

        // Cancel while messages are inflight
        cancel.cancel();

        // Ack messages after cancellation by dropping them
        drop(messages);

        // Handle should complete (cleanup waits for inflight)
        let result = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("Handle should complete after inflight messages are acked");

        assert!(result.unwrap().is_ok(), "Shutdown should be clean");
        assert!(tracker.is_empty().await.unwrap(), "Tracker should be empty");
    }

    /// Test: semaphore limits inflight messages (backpressure)
    /// Note: We use batch_size=1 to ensure permits are acquired one at a time.
    /// With larger batch sizes, the loop would wait for all batch_size permits at once,
    /// which would prevent incremental permit release from unblocking the next fetch.
    #[tokio::test]
    async fn test_semaphore_backpressure() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        write_messages(&adapter, 10).await;

        // Set max_ack_pending to 3 with batch_size=1 - only 3 messages can be inflight
        // batch_size=1 means we acquire 1 permit per fetch iteration
        let (orchestrator, _tracker, cancel) = create_orchestrator(&adapter, 1, 3).await;

        let (mut rx, handle) = orchestrator.streaming_read(cancel.clone()).await.unwrap();

        // Read 3 messages (should work)
        let mut inflight = Vec::new();
        for _ in 0..3 {
            let msg = tokio::time::timeout(Duration::from_millis(500), rx.next())
                .await
                .expect("Should receive message within timeout")
                .expect("Stream should not end");
            inflight.push(msg);
        }

        // Try to read 4th message - should block because semaphore is exhausted
        let fourth = tokio::time::timeout(Duration::from_millis(100), rx.next()).await;
        assert!(
            fourth.is_err(),
            "4th message should block due to backpressure"
        );

        // Ack first message to free a permit by taking it out and dropping
        let first_msg = inflight.remove(0);
        drop(first_msg);

        // Small delay to allow the wip_loop to complete ack and release permit
        sleep(Duration::from_millis(100)).await;

        // Now 4th message should come through
        let fourth = tokio::time::timeout(Duration::from_millis(500), rx.next())
            .await
            .expect("Should receive 4th message after ack")
            .expect("Stream should not end");
        inflight.push(fourth);

        // Cleanup - ack remaining by dropping
        drop(inflight);

        cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    /// Test: multiple concurrent ack operations complete successfully
    /// Note: This test only uses acks (not nacks) because nacks cause messages
    /// to be redelivered, which would create an infinite fetch loop.
    #[tokio::test]
    async fn test_concurrent_ack_operations() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        write_messages(&adapter, 10).await;

        let (orchestrator, tracker, cancel) = create_orchestrator(&adapter, 10, 100).await;

        let (mut rx, handle) = orchestrator.streaming_read(cancel.clone()).await.unwrap();

        // Read all messages
        let mut messages = Vec::new();
        for _ in 0..10 {
            if let Some(msg) = rx.next().await {
                messages.push(msg);
            }
        }
        assert_eq!(messages.len(), 10);

        // Drop all messages to trigger ack (is_failed defaults to false)
        drop(messages);

        // Wait for all ack operations to complete
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            while !tracker.is_empty().await.unwrap() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        cancel.cancel();
        result.expect("All ack operations should complete");
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }
}
