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
use crate::config::pipeline::isb::{BufferReaderConfig, Stream};
use crate::error::Error;
use crate::message::{IntOffset, Message, MessageType, Offset, ReadAck};
use crate::tracker::TrackerHandle;
use crate::typ::NumaflowTypeConfig;
use crate::watermark::isb::ISBWatermarkHandle;
use crate::watermark::wmb::WMB;
use backoff::retry::Retry;
use backoff::strategy::fixed;
use numaflow_throttling::RateLimiter;
use tracing::{error, warn};

const ACK_RETRY_INTERVAL: u64 = 100; // ms
const ACK_RETRY_ATTEMPTS: usize = usize::MAX;

// Alias the thin JS reader as JSReader to avoid name clashes
use crate::pipeline::isb::jetstream::reader::{ISBReaderComponents, Reader as JSReader};

/// Orchestrator that drives fetching, WIP, ack/nack, watermark/idle handling over a thin JS reader.
pub(crate) struct Reader<C: NumaflowTypeConfig> {
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

impl<C: NumaflowTypeConfig> Reader<C> {
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
                    self.inner
                        .fetch(effective_batch_size, self.read_timeout)
                        .await?
                };

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

                    // Insert into tracker and spawn WIP loop
                    let (ack_tx, ack_rx) = oneshot::channel();
                    self.tracker.insert(&message, ack_tx).await?;

                    let permit = permits.split(1).expect("Failed to split permit");
                    let jsr = self.inner.clone();
                    let offset = message.offset.clone();
                    let tick = self.cfg.wip_ack_interval;
                    let cancel_clone = cancel.clone();

                    tokio::spawn(async move {
                        Self::wip_loop(jsr, offset, ack_rx, tick, permit, cancel_clone, start)
                            .await;
                    });

                    if tx.send(message).await.is_err() {
                        break;
                    }
                }
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

    // Periodically mark WIP until ack/nack received, then perform final ack/nack.
    async fn wip_loop(
        jsr: JSReader,
        offset: Offset,
        mut ack_rx: oneshot::Receiver<ReadAck>,
        tick: Duration,
        _permit: tokio::sync::OwnedSemaphorePermit,
        cancel: CancellationToken,
        _started: Instant,
    ) {
        let mut interval = time::interval(tick);
        loop {
            tokio::select! {
                _ = interval.tick() => { let _ = jsr.mark_wip(std::slice::from_ref(&offset)).await; },
                res = &mut ack_rx => {
                    match res.unwrap_or(ReadAck::Nak) {
                        ReadAck::Ack => { Self::ack_with_retry(jsr.clone(), offset.clone(), cancel.clone()).await; },
                        ReadAck::Nak => { Self::nak_with_retry(jsr.clone(), offset.clone(), cancel.clone()).await; },
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
}
