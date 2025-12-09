//! ISBReader is responsible for reading messages from ISB, assigning watermark to the messages and
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
use crate::tracker::Tracker;
use crate::typ::NumaflowTypeConfig;
use crate::watermark::isb::ISBWatermarkHandle;

use crate::pipeline::isb::jetstream::js_reader::JetStreamReader;
use crate::watermark::wmb::WMB;
use async_nats::jetstream::Context;
use backoff::retry::Retry;
use backoff::strategy::fixed;
use numaflow_throttling::RateLimiter;
use tracing::{error, info, warn};

const ACK_RETRY_INTERVAL: u64 = 100; // ms
const ACK_RETRY_ATTEMPTS: usize = usize::MAX;

/// Type alias for metric labels
type MetricLabels = Arc<Vec<(String, String)>>;

/// ISBReader component which reads messages from ISB, assigns watermark to the messages and starts
/// tracking them using the tracker and also listens for ack/nack from the tracker and performs the
/// ack/nack to the ISB.
#[derive(Clone)]
pub(crate) struct ISBReader<C: NumaflowTypeConfig> {
    vertex_type: String,
    stream: Stream,
    cfg: BufferReaderConfig,
    batch_size: usize,
    read_timeout: Duration,
    tracker: Tracker,
    watermark: Option<ISBWatermarkHandle>,
    js_reader: JetStreamReader,
    rate_limiter: Option<C::RateLimiter>,
    /// Cached metric labels to avoid repeated allocations
    metric_labels: MetricLabels,
}

impl<C: NumaflowTypeConfig> ISBReader<C> {
    pub(crate) async fn new(
        components: ISBReaderComponents,
        js_reader: JetStreamReader,
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
            js_reader,
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
                    .map_err(|e| Error::ISB(format!("Failed to acquire semaphore permit: {e}")))?;

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
        self.js_reader.pending().await
    }

    pub(crate) fn name(&mut self) -> &'static str {
        self.js_reader.name()
    }

    /// Periodically mark WIP until ack/nack received, then perform final ack/nack and publish metrics.
    async fn wip_loop(mut params: WipParams) {
        let mut interval = time::interval(params.tick);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let _ = params.jsr.mark_wip(&params.offset).await;
                },
                res = &mut params.ack_rx => {
                    match res.unwrap_or(ReadAck::Nak) {
                        ReadAck::Ack => {
                            let ack_start = Instant::now();
                            Self::ack_with_retry(&params.jsr, &params.offset, &params.cancel).await;
                            Self::publish_ack_metrics(
                                params.stream_name,
                                &params.labels,
                                ack_start,
                                params.message_processing_start,
                            );
                        },
                        ReadAck::Nak => {
                            info!(?params.offset, "Nak received for offset");
                            Self::nak_with_retry(&params.jsr, &params.offset, &params.cancel).await;
                        },
                    }
                    params.tracker.delete(&params.offset).await.expect("Failed to remove offset from tracker");
                    break;
                }
            }
        }
    }

    /// invokes the ack with infinite retries until the cancellation token is cancelled.
    async fn ack_with_retry(jsr: &JetStreamReader, offset: &Offset, cancel: &CancellationToken) {
        let interval = fixed::Interval::from_millis(ACK_RETRY_INTERVAL).take(ACK_RETRY_ATTEMPTS);
        let _ = Retry::new(
            interval,
            async || {
                jsr.ack(offset)
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

    /// invokes the nack with infinite retries until the cancellation token is cancelled.
    async fn nak_with_retry(jsr: &JetStreamReader, offset: &Offset, cancel: &CancellationToken) {
        let interval = fixed::Interval::from_millis(ACK_RETRY_INTERVAL).take(ACK_RETRY_ATTEMPTS);
        let _ = Retry::new(
            interval,
            async || {
                let nack_result = jsr
                    .nack(offset)
                    .await
                    .map_err(|e| Error::ISB(format!("Failed to send Nak to JetStream: {e}")));
                info!(?offset, "Nak sent for offset");
                nack_result
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
        tx.send(msg)
            .await
            .map_err(|_| Error::ISB("Failed to send wmb message to channel".to_string()))
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
                .js_reader
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
                self.js_reader.ack(&message.offset).await?;
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
            jsr: self.js_reader.clone(),
            offset: message.offset.clone(),
            ack_rx,
            tick: self.js_reader.get_wip_ack_interval(),
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
            "ISBReader is shutting down (pending={}), waiting for inflight messages to be acked/nacked",
            max_ack_pending - semaphore.available_permits()
        );
        // Wait for inflight messages to finish
        let _ = semaphore.acquire_many_owned(max_ack_pending as u32).await;

        // Shutdown rate limiter if configured
        if let Some(rl) = &self.rate_limiter {
            info!("ISBReader is shutting down, shutting down rate limiter");
            rl.shutdown()
                .await
                .map_err(|e| Error::ISB(format!("Failed to shutdown rate limiter: {e}")))?;
        }

        info!("ISBReader cleanup on shutdown completed.");

        Ok(())
    }
}

struct WipParams {
    stream_name: &'static str,
    labels: Arc<Vec<(String, String)>>,
    jsr: JetStreamReader,
    offset: Offset,
    ack_rx: oneshot::Receiver<ReadAck>,
    tick: Duration,
    _permit: tokio::sync::OwnedSemaphorePermit, // drop guard
    cancel: CancellationToken,
    message_processing_start: Instant,
    tracker: Tracker,
}

/// Components needed to create a JetStreamReader.
#[derive(Clone)]
pub(crate) struct ISBReaderComponents {
    pub vertex_type: String,
    pub stream: Stream,
    pub js_ctx: Context,
    pub config: BufferReaderConfig,
    pub tracker: Tracker,
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
    use crate::pipeline::isb::reader::{ISBReader, ISBReaderComponents};
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
            js_ctx: context.clone(),
            config: buf_reader_config,
            tracker: tracker.clone(),
            batch_size: 500,
            read_timeout: Duration::from_millis(100),
            watermark_handle: None,
            isb_config: None,
            cln_token: CancellationToken::new(),
        };

        let isb_reader: ISBReader<crate::typ::WithoutRateLimiter> =
            ISBReader::new(isb_reader_components, js_reader, None)
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
            js_ctx: context.clone(),
            config: buf_reader_config,
            tracker: tracker.clone(),
            batch_size: 1,
            read_timeout: Duration::from_millis(100),
            watermark_handle: None,
            isb_config: None,
            cln_token: CancellationToken::new(),
        };

        let isb_reader: ISBReader<crate::typ::WithoutRateLimiter> =
            ISBReader::new(isb_reader_components, js_reader, None)
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
            js_ctx: context.clone(),
            config: buf_reader_config,
            tracker: tracker.clone(),
            batch_size: 500,
            read_timeout: Duration::from_millis(100),
            watermark_handle: None,
            isb_config: Some(isb_config.clone()),
            cln_token: CancellationToken::new(),
        };

        let isb_reader: ISBReader<crate::typ::WithoutRateLimiter> =
            ISBReader::new(isb_reader_components, js_reader, None)
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

        drop(received_message);

        reader_cancel_token.cancel();
        js_reader_task.await.unwrap().unwrap();
        context.delete_stream(stream.name).await.unwrap();
    }
}
