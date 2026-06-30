//! [Source] vertex is responsible for reliable reading data from an unbounded source into Numaflow
//! and also assigning [Watermark].
//!
//! [Source]: https://numaflow.numaproj.io/user-guide/sources/overview/
//! [Watermark]: https://numaflow.numaproj.io/core-concepts/watermarks/

use crate::config::pipeline::VERTEX_TYPE_SOURCE;
use crate::config::{get_vertex_name, is_mono_vertex};
use crate::error::{Error, Result};
use crate::message::{MessageHandle, NackOffset, ReadAck};
use crate::metrics::{
    PIPELINE_PARTITION_NAME_LABEL, SOURCE_PARTITION_NAME_LABEL, monovertex_metrics,
    mvtx_forward_metric_labels, pipeline_drop_metric_labels, pipeline_metric_labels,
    pipeline_metrics,
};
use crate::monovertex::bypass_router::MvtxBypassRouter;
use crate::shared::otel;
use crate::source::http::CoreHttpSource;
use crate::tracker::Tracker;
use crate::{
    message::{Message, Offset},
    reader::LagReader,
};
use backoff::retry::Retry;
use backoff::strategy::fixed;
use numaflow_kafka::source::KafkaSource;
use numaflow_nats::jetstream::JetstreamSource;
use numaflow_nats::nats::NatsSource;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pulsar::source::PulsarSource;
use numaflow_sqs::source::SqsSource;
use numaflow_throttling::RateLimiter;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::warn;
use tracing::{error, info};

/// [User-Defined Source] extends Numaflow to add custom sources supported outside the builtins.
///
/// [User-Defined Source]: https://numaflow.numaproj.io/user-guide/sources/user-defined-sources/
pub(crate) mod user_defined;

/// [Generator] is a builtin to generate data for load testing and other internal use-cases.
///
/// [Generator]: https://numaflow.numaproj.io/user-guide/sources/generator/
pub(crate) mod generator;

/// [Pulsar] is a builtin to ingest data from a Pulsar topic
///
/// [Pulsar]: https://numaflow.numaproj.io/user-guide/sources/pulsar/
pub(crate) mod pulsar;

pub(crate) mod jetstream;
pub(crate) mod nats;

pub(crate) mod sqs;

pub(crate) mod http;
pub(crate) mod kafka;
#[cfg(test)]
pub(crate) mod test_utils;

use crate::transformer::Transformer;
use crate::watermark::source::{SourceWatermarkEntry, SourceWatermarkHandle};

const ACK_RETRY_INTERVAL: u64 = 100;
const ACK_RETRY_ATTEMPTS: usize = usize::MAX;
const NACK_RETRY_ATTEMPTS: usize = 1200; // 100ms apart, total=2 minutes

/// Represents the partition information returned by a source.
/// Contains both the active partitions being processed and optionally the total number of partitions.
#[derive(Debug, Clone, Default)]
pub(crate) struct SourcePartitions {
    /// The list of active partitions being processed by this source instance.
    pub active_partitions: Vec<u16>,
    /// The total number of partitions in the source (if known).
    /// This is used for watermark stability to know when all processors have reported.
    /// Sources that don't support partitions (e.g., generator, HTTP) should set this to None.
    pub total_partitions: Option<u32>,
}

impl SourcePartitions {
    /// Creates a new SourcePartitions with the given active partitions and optional total partitions.
    /// Sources that don't support partitions should pass `None` for `total_partitions`.
    pub fn new(active_partitions: Vec<u16>, total_partitions: Option<u32>) -> Self {
        Self {
            active_partitions,
            total_partitions,
        }
    }
}

/// Set of Read related items that has to be implemented to become a Source.
/// Uses `trait_variant::make` to generate an object-safe `SourceReader` trait with `Send` bound.
#[allow(dead_code)]
#[trait_variant::make(SourceReader: Send)]
pub(crate) trait LocalSourceReader {
    /// Name of the source.
    fn name(&self) -> &'static str;

    /// Read messages from the source. Returns None when the stream has ended.
    async fn read(&mut self) -> Option<Result<Vec<Message>>>;

    /// Returns partition information for this source, including active partitions
    /// and optionally the total number of partitions.
    async fn partitions(&mut self) -> Result<SourcePartitions>;
}

/// Set of Ack related items that has to be implemented to become a Source.
/// Uses `trait_variant::make` to generate an object-safe `SourceAcker` trait with `Send` bound.
#[allow(dead_code)]
#[trait_variant::make(SourceAcker: Send)]
pub(crate) trait LocalSourceAcker {
    /// acknowledge an offset. The implementor might choose to do it in an asynchronous way.
    async fn ack(&mut self, offsets: Vec<Offset>) -> Result<()>;

    /// negatively acknowledge an offset. The implementor might choose to do it in an asynchronous way.
    /// For sources that don't support nack, this should be a no-op.
    async fn nack(&mut self, offsets: Vec<NackOffset>) -> Result<()>;
}

pub(crate) enum SourceType {
    UserDefinedSource(
        Box<user_defined::UserDefinedSourceRead>,
        Box<user_defined::UserDefinedSourceAck>,
        user_defined::UserDefinedSourceLagReader,
    ),
    Generator(
        generator::GeneratorRead,
        generator::GeneratorAck,
        generator::GeneratorLagReader,
    ),
    Pulsar(PulsarSource),
    Sqs(SqsSource),
    Jetstream(JetstreamSource),
    Kafka(KafkaSource),
    Http(CoreHttpSource),
    Nats(NatsSource),
}

enum ActorMessage {
    #[allow(dead_code)]
    Name {
        respond_to: oneshot::Sender<&'static str>,
    },
    Read {
        respond_to: oneshot::Sender<Option<Result<Vec<Message>>>>,
    },
    Ack {
        respond_to: oneshot::Sender<Result<()>>,
        offsets: Vec<Offset>,
    },
    Nack {
        respond_to: oneshot::Sender<Result<()>>,
        offsets: Vec<NackOffset>,
    },
    Pending {
        respond_to: oneshot::Sender<Result<Option<usize>>>,
    },
    Partitions {
        respond_to: oneshot::Sender<Result<SourcePartitions>>,
    },
}

struct SourceActor<R, A, L> {
    receiver: mpsc::Receiver<ActorMessage>,
    reader: R,
    acker: A,
    lag_reader: L,
}

impl<R, A, L> SourceActor<R, A, L>
where
    R: SourceReader,
    A: SourceAcker,
    L: LagReader,
{
    fn new(receiver: mpsc::Receiver<ActorMessage>, reader: R, acker: A, lag_reader: L) -> Self {
        Self {
            receiver,
            reader,
            acker,
            lag_reader,
        }
    }

    async fn handle_message(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::Name { respond_to } => {
                let name = self.reader.name();
                let _ = respond_to.send(name);
            }
            ActorMessage::Read { respond_to } => {
                let msgs = self.reader.read().await;
                let _ = respond_to.send(msgs);
            }
            ActorMessage::Ack {
                respond_to,
                offsets,
            } => {
                let ack = self.acker.ack(offsets).await;
                let _ = respond_to.send(ack);
            }
            ActorMessage::Nack {
                respond_to,
                offsets,
            } => {
                let nack = self.acker.nack(offsets).await;
                let _ = respond_to.send(nack);
            }
            ActorMessage::Pending { respond_to } => {
                let pending = self.lag_reader.pending().await;
                let _ = respond_to.send(pending);
            }
            ActorMessage::Partitions { respond_to } => {
                let partitions = self.reader.partitions().await;
                let _ = respond_to.send(partitions);
            }
        }
    }

    async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }
}

/// Source is used to read, ack, and get the pending messages count from the source.
/// Source is responsible for invoking the transformer.
///
/// Error handling and shutdown: Source will stop reading messages from the source when the
/// cancellation token is cancelled(any downstream critical error). There can be critical
/// non retryable errors in source as well(udsource crashing etc.), we will drop the downstream
/// tokio stream to signal the shutdown to the downstream components and wait for all the inflight
/// messages to be acked before shutting down the source.
#[derive(Clone)]
pub(crate) struct Source<C: crate::typ::NumaflowTypeConfig> {
    read_batch_size: usize,
    /// Cap on in-flight (read-but-not-acked) messages from this source. Sourced from
    /// `Limits.Concurrency` on the vertex/MonoVertex spec.
    concurrency: usize,
    sender: mpsc::Sender<ActorMessage>,
    tracker: Tracker,
    read_ahead: bool,
    /// When true, the source uses per-message in-flight permits (one per read-but-unacked message)
    /// bounded by `concurrency`, and acks each message individually as its downstream disposition
    /// resolves — out of order. When false (default), the existing one-batch-in-flight + batched-ack
    /// behavior is used unchanged.
    streaming: bool,
    /// Transformer handler for transforming messages from Source.
    transformer: Option<Transformer>,
    watermark_handle: Option<SourceWatermarkHandle>,
    health_checker: Option<SourceClient<Channel>>,
    rate_limiter: Option<C::RateLimiter>,
}

/// Interval for refreshing source partitions (10 seconds)
const PARTITION_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// Decrements the per-input remaining-output count and ends the dispatch span once it
/// reaches zero. `remaining` is `None` when tracing is disabled, in which case this is a no-op.
fn decrement_remaining_dispatch(
    remaining: Option<&mut HashMap<Offset, usize>>,
    source_trace: &mut otel::SourceTraceState,
    offset: &Offset,
) {
    let Some(rem) = remaining else { return };
    if let Some(count) = rem.get_mut(offset) {
        *count -= 1;
        if *count == 0 {
            rem.remove(offset);
            source_trace.end(offset);
        }
    }
}

impl<C: crate::typ::NumaflowTypeConfig> Source<C> {
    /// Create a new StreamingSource. It starts the read and ack actors in the background.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        batch_size: usize,
        concurrency: usize,
        src_type: SourceType,
        tracker: Tracker,
        read_ahead: bool,
        transformer: Option<Transformer>,
        watermark_handle: Option<SourceWatermarkHandle>,
        cln_token: CancellationToken,
        rate_limiter: Option<C::RateLimiter>,
        streaming: bool,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(batch_size);
        let mut health_checker = None;
        match src_type {
            SourceType::UserDefinedSource(reader, acker, lag_reader) => {
                health_checker = Some(reader.get_source_client());
                tokio::spawn(async move {
                    let actor = SourceActor::new(receiver, *reader, *acker, lag_reader);
                    actor.run().await;
                });
            }
            SourceType::Generator(reader, acker, lag_reader) => {
                tokio::spawn(async move {
                    let actor = SourceActor::new(receiver, reader, acker, lag_reader);
                    actor.run().await;
                });
            }
            SourceType::Pulsar(pulsar_source) => {
                tokio::spawn(async move {
                    let actor = SourceActor::new(
                        receiver,
                        pulsar_source.clone(),
                        pulsar_source.clone(),
                        pulsar_source,
                    );
                    actor.run().await;
                });
            }
            SourceType::Sqs(sqs_source) => {
                tokio::spawn(async move {
                    let actor = SourceActor::new(
                        receiver,
                        sqs_source.clone(),
                        sqs_source.clone(),
                        sqs_source,
                    );
                    actor.run().await;
                });
            }
            SourceType::Jetstream(jetstream) => {
                tokio::spawn(async move {
                    let actor =
                        SourceActor::new(receiver, jetstream.clone(), jetstream.clone(), jetstream);
                    actor.run().await;
                });
            }
            SourceType::Nats(nats) => {
                tokio::spawn(async move {
                    let actor = SourceActor::new(receiver, nats.clone(), nats.clone(), nats);
                    actor.run().await;
                });
            }
            SourceType::Kafka(kafka) => {
                tokio::spawn(async move {
                    let actor = SourceActor::new(receiver, kafka.clone(), kafka.clone(), kafka);
                    actor.run().await;
                });
            }
            SourceType::Http(http_source) => {
                tokio::spawn(async move {
                    let actor = SourceActor::new(
                        receiver,
                        http_source.clone(),
                        http_source.clone(),
                        http_source,
                    );
                    actor.run().await;
                });
            }
        };

        // Start a background task to periodically refresh partitions from the source.
        // This handles dynamic partition changes (e.g., Kafka partition rebalancing).
        // The interval ticks immediately on first call, so this also initializes the active partitions.
        if let Some(wm_handle) = watermark_handle.clone() {
            let source_sender = sender.clone();
            tokio::spawn(async move {
                let mut interval_ticker = tokio::time::interval(PARTITION_REFRESH_INTERVAL);
                loop {
                    tokio::select! {
                        _ = interval_ticker.tick() => {
                            if let Ok(partitions) = Self::partitions(source_sender.clone()).await {
                                wm_handle
                                    .initialize_active_partitions(
                                        partitions.active_partitions,
                                        partitions.total_partitions,
                                    )
                                    .await;
                            }
                        }
                        _ = cln_token.cancelled() => {
                            break;
                        }
                    }
                }
            });
        }

        Self {
            read_batch_size: batch_size,
            concurrency,
            sender,
            tracker,
            read_ahead,
            streaming,
            transformer,
            watermark_handle,
            health_checker,
            rate_limiter,
        }
    }

    /// read messages from the source by communicating with the read actor.
    async fn read(source_handle: mpsc::Sender<ActorMessage>) -> Option<Result<Vec<Message>>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Read { respond_to: sender };
        // Ignore send errors. If send fails, so does the recv.await below. There's no reason
        // to check for the same failure twice.
        let _ = source_handle.send(msg).await;
        receiver
            .await
            .map_err(|e| Error::ActorPatternRecv(e.to_string()))
            .unwrap_or(Some(Err(Error::ActorPatternRecv(
                "Channel closed".to_string(),
            ))))
    }

    /// ack the offsets by communicating with the ack actor.
    async fn ack(source_handle: mpsc::Sender<ActorMessage>, offsets: Vec<Offset>) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Ack {
            respond_to: sender,
            offsets,
        };
        // Ignore send errors. If send fails, so does the recv.await below. There's no reason
        // to check for the same failure twice.
        let _ = source_handle.send(msg).await;
        receiver
            .await
            .map_err(|e| Error::ActorPatternRecv(e.to_string()))?
    }

    /// nack the offsets by communicating with the nack actor.
    async fn nack(
        source_handle: mpsc::Sender<ActorMessage>,
        offsets: Vec<NackOffset>,
    ) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Nack {
            respond_to: sender,
            offsets,
        };
        // Ignore send errors. If send fails, so does the recv.await below. There's no reason
        // to check for the same failure twice.
        let _ = source_handle.send(msg).await;
        receiver
            .await
            .map_err(|e| Error::ActorPatternRecv(e.to_string()))?
    }

    /// get the pending messages count by communicating with the pending actor.
    pub(crate) async fn pending(&self) -> Result<Option<usize>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Pending { respond_to: sender };
        // Ignore send errors. If send fails, so does the recv.await below. There's no reason
        // to check for the same failure twice.
        let _ = self.sender.send(msg).await;
        receiver
            .await
            .map_err(|e| Error::ActorPatternRecv(e.to_string()))?
    }

    /// get the source partitions from which the source is reading from.
    async fn partitions(source_handle: mpsc::Sender<ActorMessage>) -> Result<SourcePartitions> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Partitions { respond_to: sender };
        // Ignore send errors. If send fails, so does the recv.await below. There's no reason
        // to check for the same failure twice.
        let _ = source_handle.send(msg).await;
        receiver
            .await
            .map_err(|e| Error::ActorPatternRecv(e.to_string()))?
    }

    /// Starts streaming messages from the source. It returns a stream of MessageHandles and
    /// a handle to the spawned task.
    pub(crate) fn streaming_read(
        mut self,
        cln_token: CancellationToken,
        bypass_router: Option<MvtxBypassRouter>,
    ) -> Result<(ReceiverStream<MessageHandle>, JoinHandle<Result<()>>)> {
        let (messages_tx, messages_rx) = mpsc::channel(2 * self.read_batch_size);

        let mut pipeline_labels = pipeline_metric_labels(VERTEX_TYPE_SOURCE).clone();
        pipeline_labels.push((
            PIPELINE_PARTITION_NAME_LABEL.to_string(),
            get_vertex_name().to_string(),
        ));
        let mvtx_labels = mvtx_forward_metric_labels();

        info!(
            ?self.read_batch_size,
            ?self.concurrency,
            ?self.read_ahead,
            streaming = self.streaming,
            "Started streaming source"
        );

        let handle = tokio::spawn(async move {
            if self.streaming {
                if self.read_ahead {
                    warn!(
                        "streaming=true supersedes read_ahead=true; read_ahead is ignored in streaming mode"
                    );
                }
                self.streaming_source(
                    pipeline_labels,
                    mvtx_labels,
                    bypass_router,
                    messages_tx,
                    cln_token,
                )
                .await
            } else {
                // The semaphore caps the number of in-flight ack tasks. With read-ahead disabled, we
                // allow exactly one batch in flight (sequential processing). With read-ahead enabled,
                // we allow up to `concurrency` in-flight messages, divided by `read_batch_size` because
                // we do batch acking. We always allow at least one task so a `concurrency` smaller than
                // `read_batch_size` still makes progress.
                let max_ack_tasks = match &self.read_ahead {
                    true => std::cmp::max(1, self.concurrency / self.read_batch_size.max(1)),
                    false => 1,
                };

                let semaphore = Arc::new(Semaphore::new(max_ack_tasks));
                let mut result = Ok(());
                loop {
                    // Acquire the semaphore permit before reading the next batch to make
                    // sure we are not reading ahead and all the inflight messages are acked.
                    let _permit = Arc::clone(&semaphore)
                        .acquire_owned()
                        .await
                        .expect("acquiring permit should not fail");

                    // Apply rate limiting before reading if configured.
                    // In source, we rate limit the `read` method invocations,
                    // and not the number of messages read. It just removes a single token per read.
                    // To throttle the number of messages read, make sure that `read_batch_size` is set to
                    // appropriate value.
                    if let Some(ref rate_limiter) = self.rate_limiter
                        && rate_limiter
                            .acquire_n(Some(1), Some(Duration::from_secs(1)))
                            .await
                            == 0
                    {
                        continue;
                    }

                    let read_start_time = Instant::now();
                    let mut messages = match Self::read(self.sender.clone()).await {
                        Some(Ok(messages)) => messages,
                        None => {
                            info!("Source returned None (end of stream). Stopping the source.");
                            break;
                        }
                        Some(Err(e)) => {
                            error!("Error while reading messages: {:?}", e);
                            result = Err(e);
                            break;
                        }
                    };

                    let msgs_len = messages.len();
                    let read_time = read_start_time.elapsed().as_micros() as f64;
                    Self::record_batch_read_metrics(
                        &pipeline_labels,
                        mvtx_labels,
                        read_time,
                        msgs_len,
                        false,
                    );

                    let mut msg_handles = vec![];
                    let mut ack_batch = Vec::with_capacity(msgs_len);

                    // Tracing: hold per-message `source.dispatch` span contexts keyed
                    // by source offset. Spans are created before tracker insert and closed only
                    // after the last downstream message for that source offset is bypassed/sent.
                    // This keeps the span honest even when the transformer fans one input message
                    // out into multiple outputs.
                    //
                    // Any messages whose dispatch spans are still in the map at end-of-iteration
                    // (e.g., transformer error that breaks the outer loop) have their spans
                    // closed by the RAII guard when the map is dropped.
                    //
                    let mut source_trace =
                        otel::SourceTraceState::new(msgs_len, self.transformer.is_some());

                    for mut message in messages.drain(..) {
                        Self::record_partition_read_metrics(
                            &pipeline_labels,
                            mvtx_labels,
                            message.offset.partition_idx(),
                            message.value.len(),
                        );

                        // Insert into the tracker first. A duplicate in-flight delivery (same
                        // offset already being processed in this pod) is ignored and not forwarded
                        // to downstream. The original copy already in the tracker drives the
                        // source-side ack/nack for that offset.
                        match self.tracker.insert(&message).await {
                            Ok(()) => {}
                            Err(Error::DuplicateInflight(_)) => {
                                warn!(
                                    offset = ?message.offset,
                                    "duplicate delivery from source, dropping"
                                );
                                Self::record_duplicate_drop(mvtx_labels, message.value.len());
                                continue;
                            }
                            Err(e) => return Err(e),
                        }

                        // - Create `numaflow.vertex.process` root span for this message's full lifecycle.
                        //   Parent: upstream trace context from message headers (W3C or B3), if present.
                        //   The span is stored in MessageHandle's AckHandle and dropped on ack, giving
                        //   accurate duration.
                        // - Create a topology-specific `source.dispatch` child span covering per-message
                        //   source-stage work (tracker insert + optional `source.transform` child span +
                        //   watermark + downstream bypass/send). It is closed after the last message for
                        //   this source offset leaves the source stage, or by the RAII guard on error.
                        //   Note: this span measures the per-message source-stage dispatch work, NOT
                        //   source read latency.
                        // - Inject `vertex.process` context into sys_metadata["tracing"] so that map
                        //   and sink become siblings of `source.dispatch` under `vertex.process`.
                        let platform_span = source_trace.start_message(&mut message);

                        let (ack_tx, ack_rx) = oneshot::channel();
                        // store the ack receiver in the batch to invoke ack later.
                        ack_batch.push((message.offset.clone(), ack_rx));
                        let msg_handle = MessageHandle::new(message, ack_tx);
                        msg_handle.set_pipeline_span(platform_span);
                        msg_handles.push(msg_handle);
                    }

                    // start a background task to invoke ack on the source for the offsets that are acked.
                    // if read ahead is disabled, acquire the semaphore permit before invoking ack so that
                    // we wait for all the inflight messages to be acked before reading the next batch.
                    tokio::spawn({
                        let sender = self.sender.clone();
                        let tracker = self.tracker.clone();
                        let cln_token = cln_token.clone();
                        async move {
                            let result = Self::invoke_ack(
                                read_start_time,
                                sender,
                                ack_batch,
                                _permit,
                                tracker,
                                cln_token.clone(),
                            )
                            .await;

                            if let Err(e) = result {
                                error!(
                                    ?e,
                                    "Non retryable error while invoking ack, stopping the source forwarder"
                                );
                                // This cancels the source forwarder, which will stop the source.
                                cln_token.cancel();
                            }
                        }
                    });

                    // transform the batch if the transformer is present, this need not
                    // be streaming because transformation should be fast operation.
                    // transform_batch accepts MessageHandles and returns MessageHandles with ack
                    // tracking preserved — flatmap outputs share the parent's ack handle.
                    // Move the read-only transform parents out of SourceTraceState; the dispatch span
                    // contexts remain owned by SourceTraceState for lifecycle cleanup.
                    let dispatch_parent_contexts = source_trace.take_transform_parents();
                    let mut msg_handles = match self.transformer.as_mut() {
                        None => msg_handles,
                        Some(transformer) => match transformer
                            .transform_batch(
                                msg_handles,
                                cln_token.clone(),
                                dispatch_parent_contexts.as_ref(),
                            )
                            .await
                        {
                            Ok(handles) => handles,
                            Err(e) => {
                                error!(
                                    ?e,
                                    "Error while transforming messages, sending nack to the batch"
                                );
                                // handles dropped without mark_success, causing NAK
                                result = Err(e);
                                break;
                            }
                        },
                    };

                    // Per-input-offset countdown driving `source.dispatch` end once the last
                    // downstream message for that input is bypassed or sent. Only built when
                    // tracing is on; otherwise SourceTraceState skips bookkeeping that would
                    // do no useful work.
                    let mut remaining_dispatches = source_trace
                        .remaining_dispatches(msg_handles.iter().map(|m| &m.message().offset));

                    // If a source input produced no downstream messages (for example, the transformer
                    // filtered it out), close its dispatch span now so it does not stay open for the
                    // rest of the batch's watermark/send work.
                    if let Some(ref rem) = remaining_dispatches {
                        source_trace.end_without_outputs(rem);
                    }

                    if let Some(watermark_handle) = self.watermark_handle.as_mut() {
                        let entries: Vec<SourceWatermarkEntry> =
                            msg_handles.iter().map(SourceWatermarkEntry::from).collect();
                        watermark_handle
                            .generate_and_publish_source_watermark(&entries)
                            .await;

                        let watermark = watermark_handle.fetch_source_watermark().await;
                        // set is_late on messages that arrived after the watermark
                        for msg_handle in msg_handles.iter_mut() {
                            if msg_handle.message().event_time < watermark {
                                msg_handle.message_mut().is_late = true;
                            }
                        }
                    }

                    // write the messages to downstream as MessageHandles.
                    for read_message in msg_handles.into_iter() {
                        let offset = read_message.message().offset.clone();
                        // Try to bypass the message. If bypassed, try_bypass takes ownership and returns None.
                        // If not bypassed, it returns Some(read_message) for us to send downstream.
                        let maybe_read_message = if let Some(ref bypass_router) = bypass_router {
                            match bypass_router
                                .try_bypass(read_message)
                                .await
                                .expect("failed to send message to bypass channel")
                            {
                                Some(msg) => msg,
                                None => {
                                    decrement_remaining_dispatch(
                                        remaining_dispatches.as_mut(),
                                        &mut source_trace,
                                        &offset,
                                    );
                                    continue;
                                }
                            }
                        } else {
                            read_message
                        };

                        messages_tx
                            .send(maybe_read_message)
                            .await
                            .expect("send should not fail");

                        decrement_remaining_dispatch(
                            remaining_dispatches.as_mut(),
                            &mut source_trace,
                            &offset,
                        );
                    }
                    // source_trace drops here — any remaining dispatch spans (shouldn't happen on success path)
                    // get closed by the RAII Drop impl.
                }

                info!(status=?result, "Source stopped, waiting for inflight messages to be acked/nacked");
                // wait for all the ack tasks to be completed before stopping the source, since we give
                // a permit for each ack task all the permits should be released when the ack tasks are
                // done, we can verify this by trying to acquire the permit for max_ack_tasks.
                let _permit = Arc::clone(&semaphore)
                    .acquire_many_owned(max_ack_tasks as u32)
                    .await
                    .expect("acquiring permit should not fail");
                info!("All inflight messages are acked/nacked. Source stopped.");

                // Shutdown rate limiter if configured
                if let Some(ref rate_limiter) = self.rate_limiter {
                    rate_limiter.shutdown().await.map_err(|e| {
                        Error::Source(format!("Failed to shutdown rate limiter: {e}"))
                    })?;
                }

                result
            } // end non-streaming branch
        });
        Ok((ReceiverStream::new(messages_rx), handle))
    }

    async fn streaming_source(
        mut self,
        pipeline_labels: Vec<(String, String)>,
        mvtx_labels: &Vec<(String, String)>,
        bypass_router: Option<MvtxBypassRouter>,
        messages_tx: Sender<MessageHandle>,
        cln_token: CancellationToken,
    ) -> Result<()> {
        // Acquire-after-read model:
        //   1. Call read() to get a full batch (up to read_batch_size messages).
        //   2. For EACH returned message, block-acquire ONE permit before inserting
        //      it into the tracker and dispatching it downstream.
        //   3. Each message is dispatched inline (one at a time) so that backpressure
        //      from permit acquisition does not prevent already-tracked messages from
        //      reaching downstream consumers
        //   4. The permit travels with that message to its per-message ack task and
        //      is released only when the message is acked or nak'd.
        //
        // Transient read buffer: at most one read_batch_size worth of messages can
        // be waiting on permit acquisition at any time.
        let semaphore = Arc::new(Semaphore::new(self.concurrency));
        let mut result = Ok(());

        'outer: loop {
            // Apply rate limiting before reading if configured.
            if let Some(ref rate_limiter) = self.rate_limiter
                && rate_limiter
                    .acquire_n(Some(1), Some(Duration::from_secs(1)))
                    .await
                    == 0
            {
                continue;
            }

            let read_start_time = Instant::now();
            let mut messages = match Self::read(self.sender.clone()).await {
                Some(Ok(messages)) => messages,
                None => {
                    info!("Source returned None (end of stream). Stopping the source.");
                    break 'outer;
                }
                Some(Err(e)) => {
                    error!("Error while reading messages: {:?}", e);
                    result = Err(e);
                    break 'outer;
                }
            };

            let msgs_len = messages.len();

            let read_time = read_start_time.elapsed().as_micros() as f64;
            // streaming=true: read_time and read_batch_size are skipped on the mvtx
            // path (batch semantics are meaningless per-message). Pipeline path unchanged.
            // TODO: track per message metrics
            Self::record_batch_read_metrics(
                &pipeline_labels,
                mvtx_labels,
                read_time,
                msgs_len,
                true,
            );

            for mut message in messages.drain(..) {
                // Pre-compute per-source-offset trace context (same as non-streaming).
                let mut source_trace = otel::SourceTraceState::new(1, self.transformer.is_some());

                Self::record_partition_read_metrics(
                    &pipeline_labels,
                    mvtx_labels,
                    message.offset.partition_idx(),
                    message.value.len(),
                );

                // Block-acquire one permit per message BEFORE inserting into the
                // tracker. This is the sole backpressure point.
                // When `concurrency` messages are already in flight, this await
                // blocks until an ack/nak task releases a permit.
                //
                // Cancellation: select against cln_token so a cancel while
                // waiting on a stalled permit acquisition does not hang forever.
                // Messages already read but not yet permitted are dropped here
                // (never tracker-inserted) and will be redelivered by the source.
                let permit = tokio::select! {
                    biased;
                    _ = cln_token.cancelled() => {
                        // Loop is stopping; remaining messages in this batch are
                        // dropped without tracker insertion → redelivered by source.
                        break;
                    }
                    p = Arc::clone(&semaphore).acquire_owned() => {
                        p.expect("semaphore acquire should not fail")
                    }
                };

                // Insert into the tracker. A duplicate in-flight delivery (same
                // offset already being processed) is dropped; its permit is
                // released immediately so it doesn't leak or skew the drain.
                match self.tracker.insert(&message).await {
                    Ok(()) => {}
                    Err(Error::DuplicateInflight(_)) => {
                        warn!(
                            offset = ?message.offset,
                            "duplicate delivery from source, dropping"
                        );
                        Self::record_duplicate_drop(mvtx_labels, message.value.len());
                        // Drop the just-acquired permit to keep accounting exact.
                        drop(permit);
                        continue;
                    }
                    Err(e) => {
                        // Non-duplicate tracker error; drop permit and abort.
                        drop(permit);
                        result = Err(e);
                        break 'outer;
                    }
                }

                let platform_span = source_trace.start_message(&mut message);

                let (ack_tx, ack_rx) = oneshot::channel();
                let msg_handle = MessageHandle::new(message, ack_tx);
                msg_handle.set_pipeline_span(platform_span);

                // Spawn one ack task per message. The task owns one in-flight
                // permit; dropping the permit on completion frees one slot.
                tokio::spawn({
                    let sender = self.sender.clone();
                    let tracker = self.tracker.clone();
                    let cln_token = cln_token.clone();
                    let offset = msg_handle.message().offset.clone();
                    async move {
                        if let Err(e) = Self::invoke_ack_single(
                            read_start_time,
                            sender,
                            offset,
                            ack_rx,
                            permit,
                            tracker,
                            cln_token.clone(),
                        )
                        .await
                        {
                            error!(
                                ?e,
                                "Non retryable error in per-message ack, stopping forwarder"
                            );
                            cln_token.cancel();
                        }
                    }
                });

                // Each message is dispatched to downstream immediately after its
                // permit is acquired and ack task is spawned.
                //
                // transform_batch is called with a single-element Vec (per-message transform).
                // This trades batch transformer throughput for per-message forward latency,
                // which is acceptable in streaming mode.
                //
                // Capture the input offset before msg_handle is consumed by transform_batch.
                // This is needed to close the dispatch span if the transformer filters the
                // message to zero outputs (see guard below).
                let input_offset = msg_handle.message().offset.clone();
                let dispatch_parent_contexts = source_trace.take_transform_parents();
                let mut transformed_handles = match self.transformer.as_mut() {
                    None => vec![msg_handle],
                    Some(transformer) => {
                        match transformer
                            .transform_batch(
                                vec![msg_handle],
                                cln_token.clone(),
                                dispatch_parent_contexts.as_ref(),
                            )
                            .await
                        {
                            Ok(handles) => handles,
                            Err(e) => {
                                error!(?e, "Error transforming message, sending nack");
                                result = Err(e);
                                break 'outer;
                            }
                        }
                    }
                };

                // If the transformer filtered this message to zero outputs, close its
                // dispatch span now. Without this, the span stays open until source_trace
                // drops at the end of this loop iteration, inflating its duration by all
                // subsequent per-message work in the same batch.
                if transformed_handles.is_empty() {
                    source_trace.end(&input_offset);
                }

                if let Some(watermark_handle) = self.watermark_handle.as_mut() {
                    let entries: Vec<SourceWatermarkEntry> = transformed_handles
                        .iter()
                        .map(SourceWatermarkEntry::from)
                        .collect();
                    watermark_handle
                        .generate_and_publish_source_watermark(&entries)
                        .await;

                    let watermark = watermark_handle.fetch_source_watermark().await;
                    // set is_late on messages that arrived after the watermark
                    for msg_handle in transformed_handles.iter_mut() {
                        if msg_handle.message().event_time < watermark {
                            msg_handle.message_mut().is_late = true;
                        }
                    }
                }

                // Bypass and downstream send inline for each output handle.
                for read_message in transformed_handles.into_iter() {
                    let offset = read_message.message().offset.clone();
                    let maybe_read_message = if let Some(ref bypass_router) = bypass_router {
                        match bypass_router
                            .try_bypass(read_message)
                            .await
                            .expect("failed to send message to bypass channel")
                        {
                            Some(msg) => msg,
                            None => {
                                source_trace.end(&offset);
                                continue;
                            }
                        }
                    } else {
                        read_message
                    };

                    messages_tx
                        .send(maybe_read_message)
                        .await
                        .expect("send should not fail");

                    source_trace.end(&offset);
                }
            }
            // source_trace drops here; RAII closes any remaining dispatch spans
            // (e.g., messages dropped mid-loop on cancellation).

            // If cln_token was cancelled mid-batch, stop the outer loop now.
            if cln_token.is_cancelled() {
                break 'outer;
            }
        }

        // Wait for every in-flight per-message ack task to complete. Each task
        // releases its permit on ack/nak
        info!(
            status = ?result,
            "Source stopped (streaming), waiting for all in-flight messages to ack/nack"
        );
        let _drain_permit = Arc::clone(&semaphore)
            .acquire_many_owned(self.concurrency as u32)
            .await
            .expect("acquiring drain permits should not fail");
        info!("All in-flight messages acked/nacked. Streaming source stopped.");

        // Shutdown rate limiter if configured.
        if let Some(ref rate_limiter) = self.rate_limiter {
            rate_limiter
                .shutdown()
                .await
                .map_err(|e| Error::Source(format!("Failed to shutdown rate limiter: {e}")))?;
        }

        result
    }
    /// Per-message ack for the streaming path. Awaits a single message's oneshot, issues
    /// ack or nack for that one offset, then releases the in-flight permit. This runs as an
    /// independent background task per message, enabling out-of-order acknowledgement.
    ///
    /// Mirrors `invoke_ack` for a single `(offset, oneshot::Receiver<ReadAck>)` pair.
    async fn invoke_ack_single(
        e2e_start_time: Instant,
        source_handle: mpsc::Sender<ActorMessage>,
        offset: Offset,
        ack_rx: oneshot::Receiver<ReadAck>,
        permit: OwnedSemaphorePermit, // one permit per in-flight message; released on completion.
        tracker: Tracker,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        let mut nack_option = None;
        let disposition = match ack_rx.await {
            Ok(ReadAck::Ack) => true,
            Ok(ReadAck::Nak(option)) => {
                warn!(?offset, "Nak received for offset (streaming)");
                nack_option = option;
                false
            }
            Err(e) => {
                error!(?offset, err=?e, "Error receiving ack for offset (streaming)");
                // Treat recv-error the same as Nak: delete from tracker so it does not
                // accumulate, then release the permit. No retryable error is returned here
                // (matches current behavior at the recv-error branch in invoke_ack).
                tracker
                    .delete(&offset)
                    .await
                    .expect("Failed to delete offset from tracker");
                // Drop permit explicitly before returning so drain barrier sees it.
                drop(permit);
                return Ok(());
            }
        };

        // Delete from tracker exactly once for this offset.
        tracker
            .delete(&offset)
            .await
            .expect("Failed to delete offset from tracker");

        let start = Instant::now();
        let ack_result = if disposition {
            Self::ack_with_retry(source_handle, vec![offset], &cancel_token).await
        } else {
            Self::nack_with_retry(
                source_handle,
                vec![NackOffset {
                    offset,
                    option: nack_option,
                }],
                &cancel_token,
            )
            .await
        };

        // streaming=true: ack_time and e2e_time are skipped on the mvtx path;
        // ack_total still increments (n=1 per message — valid per-message counter).
        Self::send_ack_metrics(e2e_start_time, 1, start, true);

        // Drop the permit here so the drain barrier sees one fewer in-flight message.
        // The drop happens after ack_with_retry so we never signal "done" before the
        // source SDK has actually received the ack.
        drop(permit);

        ack_result
    }

    /// Listens to the oneshot receivers and invokes ack/nack on the source for the offsets.
    async fn invoke_ack(
        e2e_start_time: Instant,
        source_handle: mpsc::Sender<ActorMessage>,
        ack_rx_batch: Vec<(Offset, oneshot::Receiver<ReadAck>)>,
        _permit: OwnedSemaphorePermit, // permit to release after acking the offsets.
        tracker: Tracker,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        let n = ack_rx_batch.len();
        let mut offsets_to_ack = Vec::with_capacity(n);
        let mut offsets_to_nack = Vec::with_capacity(n);

        for (offset, oneshot_rx) in ack_rx_batch {
            match oneshot_rx.await {
                Ok(ReadAck::Ack) => {
                    offsets_to_ack.push(offset.clone());
                }
                Ok(ReadAck::Nak(option)) => {
                    warn!(?offset, "Nak received for offset");
                    offsets_to_nack.push(NackOffset {
                        offset: offset.clone(),
                        option,
                    });
                }
                Err(e) => {
                    error!(?offset, err=?e, "Error receiving ack for offset");
                }
            }
            tracker
                .delete(&offset)
                .await
                .expect("Failed to delete offset from tracker");
        }

        let start = Instant::now();
        if !offsets_to_ack.is_empty() {
            Self::ack_with_retry(source_handle.clone(), offsets_to_ack, &cancel_token).await?;
        }
        if !offsets_to_nack.is_empty() {
            // Group offsets by their nack options so each distinct option set results in a
            // single backend nack call; offsets sharing the same (or no) options are batched.
            Self::nack_with_retry(source_handle.clone(), offsets_to_nack, &cancel_token).await?
        }
        // Non-streaming path: record all batch-granular metrics (streaming=false).
        Self::send_ack_metrics(e2e_start_time, n, start, false);

        Ok(())
    }

    /// Invokes ack with infinite retries until the cancellation token is cancelled.
    async fn ack_with_retry(
        source_handle: mpsc::Sender<ActorMessage>,
        offsets: Vec<Offset>,
        cancel_token: &CancellationToken,
    ) -> Result<()> {
        let interval = fixed::Interval::from_millis(ACK_RETRY_INTERVAL).take(ACK_RETRY_ATTEMPTS);
        Retry::new(
            interval,
            async || Self::ack(source_handle.clone(), offsets.clone()).await,
            |error: &Error| {
                error!(?error, "Failed to send ack to source, retrying...");
                // Don't retry non-retryable errors
                if matches!(error, Error::NonRetryable(_)) {
                    error!(?error, "Non retryable error, stopping the ack retry loop");
                    return false;
                }
                // Don't retry if cancelled
                if cancel_token.is_cancelled() {
                    error!(
                        ?error,
                        "Cancellation token received, stopping the ack retry loop"
                    );
                    return false;
                }
                true // Retry for all other errors
            },
        )
        .await
    }

    /// Invokes nack with infinite retries until the cancellation token is cancelled.
    async fn nack_with_retry(
        source_handle: mpsc::Sender<ActorMessage>,
        offsets: Vec<NackOffset>,
        cancel_token: &CancellationToken,
    ) -> Result<()> {
        // In practice, this retry should exit early since it is invoked during ISB/map/sink errors, which results in CancellationToken cancellation
        let interval = fixed::Interval::from_millis(ACK_RETRY_INTERVAL).take(NACK_RETRY_ATTEMPTS);
        let _ = Retry::new(
            interval,
            async || Self::nack(source_handle.clone(), offsets.clone()).await,
            |error: &Error| {
                error!(?error, "Failed to send nack to source, retrying...");
                // Don't retry non-retryable errors
                if matches!(error, Error::NonRetryable(_)) {
                    error!(?error, "Non retryable error, stopping the NACK retry loop");
                    return false;
                }
                if cancel_token.is_cancelled() {
                    error!(
                        ?error,
                        "Cancellation token received, stopping the NACK retry loop"
                    );
                    return false;
                }
                true
            },
        )
        .await;
        Ok(())
    }

    /// Record per-batch read metrics (read_time, read_batch_size).
    /// These metrics are recorded once per batch read operation.
    ///
    /// `streaming`: when `true` and `is_mono_vertex()`, the batch-granular `read_time` and
    /// `read_batch_size` metrics are skipped — their batch semantics are meaningless for
    /// per-message streaming. The pipeline label path is always recorded unchanged.
    fn record_batch_read_metrics(
        pipeline_labels: &Vec<(String, String)>,
        mvtx_labels: &Vec<(String, String)>,
        read_time: f64,
        batch_size: usize,
        streaming: bool,
    ) {
        if is_mono_vertex() {
            // Gate batch-granular metrics off under streaming. read_time would measure
            // the duration of a read that returns a full batch regardless of how many
            // messages are actually in flight, and read_batch_size would pin to ~1
            // (meaningless for per-message throughput monitoring).
            if !streaming {
                monovertex_metrics()
                    .read_time
                    .get_or_create(mvtx_labels)
                    .observe(read_time);
                monovertex_metrics()
                    .read_batch_size
                    .get_or_create(mvtx_labels)
                    .set(batch_size as i64);
            }
        } else {
            pipeline_metrics()
                .forwarder
                .read_batch_size
                .get_or_create(pipeline_labels)
                .set(batch_size as i64);
            pipeline_metrics()
                .forwarder
                .read_processing_time
                .get_or_create(pipeline_labels)
                .observe(read_time);
        }
    }

    /// Record per-partition read metrics (read_total, data_read_total, read_bytes_total, etc.).
    /// These metrics are recorded for each message, grouped by partition.
    fn record_partition_read_metrics(
        pipeline_labels: &[(String, String)],
        mvtx_labels: &[(String, String)],
        partition_idx: u16,
        bytes: usize,
    ) {
        if is_mono_vertex() {
            let mut labels = mvtx_labels.to_owned();
            labels.push((
                SOURCE_PARTITION_NAME_LABEL.to_string(),
                partition_idx.to_string(),
            ));
            monovertex_metrics().read_total.get_or_create(&labels).inc();
        } else {
            let mut labels = pipeline_labels.to_owned();
            labels.push((
                SOURCE_PARTITION_NAME_LABEL.to_string(),
                partition_idx.to_string(),
            ));
            pipeline_metrics()
                .forwarder
                .read_total
                .get_or_create(&labels)
                .inc();
            pipeline_metrics()
                .forwarder
                .data_read_total
                .get_or_create(&labels)
                .inc();
            pipeline_metrics()
                .forwarder
                .read_bytes_total
                .get_or_create(&labels)
                .inc_by(bytes as u64);
            pipeline_metrics()
                .forwarder
                .data_read_bytes_total
                .get_or_create(&labels)
                .inc_by(bytes as u64);
        }
    }
    /// Records a duplicate drop on the source read path. The source forwarder runs in
    /// both monovertex and pipeline modes, so increment whichever counter family
    /// matches the current mode. Monovertex has a single `dropped_total` without a
    /// reason label; pipeline mode carries `reason="duplicate"` on `forwarder.drop_total`.
    fn record_duplicate_drop(mvtx_labels: &[(String, String)], bytes: usize) {
        if is_mono_vertex() {
            monovertex_metrics()
                .dropped_total
                .get_or_create(&mvtx_labels.to_owned())
                .inc();
        } else {
            let labels =
                pipeline_drop_metric_labels(VERTEX_TYPE_SOURCE, get_vertex_name(), "duplicate");
            pipeline_metrics()
                .forwarder
                .drop_total
                .get_or_create(&labels)
                .inc();
            pipeline_metrics()
                .forwarder
                .drop_bytes_total
                .get_or_create(&labels)
                .inc_by(bytes as u64);
        }
    }

    /// `streaming`: when `true` and `is_mono_vertex()`, `ack_time` and `e2e_time` observes
    /// are skipped — these batch-granular histograms are meaningless per-message (one sample
    /// per ack call would misrepresent the aggregate). `ack_total` always increments (n=1
    /// per message in the streaming path) since it is a valid per-message counter. The
    /// pipeline label path is always recorded unchanged.
    fn send_ack_metrics(e2e_start_time: Instant, n: usize, start: Instant, streaming: bool) {
        if is_mono_vertex() {
            let mvtx_labels = mvtx_forward_metric_labels();

            // Gate batch-granular timing histograms off under streaming.
            if !streaming {
                monovertex_metrics()
                    .ack_time
                    .get_or_create(mvtx_labels)
                    .observe(start.elapsed().as_micros() as f64);
                monovertex_metrics()
                    .e2e_time
                    .get_or_create(mvtx_labels)
                    .observe(e2e_start_time.elapsed().as_micros() as f64);
            }

            // ack_total is a per-message counter — always valid, never gated.
            monovertex_metrics()
                .ack_total
                .get_or_create(mvtx_labels)
                .inc_by(n as u64);
        } else {
            let mut pipeline_labels = pipeline_metric_labels(VERTEX_TYPE_SOURCE).clone();
            pipeline_labels.push((
                PIPELINE_PARTITION_NAME_LABEL.to_string(),
                get_vertex_name().to_string(),
            ));
            pipeline_metrics()
                .forwarder
                .ack_processing_time
                .get_or_create(&pipeline_labels)
                .observe(start.elapsed().as_micros() as f64);

            pipeline_metrics()
                .forwarder
                .ack_total
                .get_or_create(&pipeline_labels)
                .inc_by(n as u64);
            pipeline_metrics()
                .forwarder
                .e2e_time
                .get_or_create(&pipeline_labels)
                .observe(e2e_start_time.elapsed().as_micros() as f64);
        }
    }

    pub(crate) async fn ready(&mut self) -> bool {
        let source_ready = if let Some(client) = &mut self.health_checker {
            let request = tonic::Request::new(());
            match client.is_ready(request).await {
                Ok(response) => response.into_inner().ready,
                Err(e) => {
                    error!("Source is not ready: {:?}", e);
                    false
                }
            }
        } else {
            true
        };

        let transformer_ready = if let Some(client) = &mut self.transformer {
            client.ready().await
        } else {
            true
        };

        source_ready && transformer_ready
    }
}

#[cfg(test)]
mod tests {
    use crate::mark_success;
    use crate::message::{IntOffset as CoreIntOffset, Offset as CoreOffset};
    use crate::shared::grpc::create_rpc_channel;
    use crate::shared::otel::SourceDispatchSpans;
    use crate::source::user_defined::new_source;
    use crate::source::{Source, SourceType};
    use crate::tracker::Tracker;
    use chrono::Utc;
    use numaflow::shared::ServerExtras;
    use numaflow::source;
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow_pb::clients::source::source_client::SourceClient;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::mpsc::Sender;
    use tokio_stream::StreamExt;
    use tokio_util::sync::CancellationToken;

    struct SimpleSource {
        num: usize,
        sent_count: AtomicUsize,
        yet_to_ack: std::sync::RwLock<HashSet<String>>,
        nacked: std::sync::RwLock<HashSet<String>>,
    }

    impl SimpleSource {
        fn new(num: usize) -> Self {
            Self {
                num,
                sent_count: AtomicUsize::new(0),
                yet_to_ack: std::sync::RwLock::new(HashSet::new()),
                nacked: std::sync::RwLock::new(HashSet::new()),
            }
        }

        async fn create_message(&self, offset: String) -> Message {
            Message {
                value: b"hello".to_vec(),
                event_time: Utc::now(),
                offset: Offset {
                    offset: offset.clone().into_bytes(),
                    partition_id: 0,
                },
                keys: vec![],
                headers: Default::default(),
                user_metadata: None,
            }
        }
    }

    #[tonic::async_trait]
    impl source::Sourcer for SimpleSource {
        async fn read(&self, request: SourceReadRequest, transmitter: Sender<Message>) {
            let event_time = Utc::now();
            let mut message_offsets = Vec::with_capacity(request.count);

            // if there are nacked message send them first and remove them from the nacked set
            // and return early
            let nacked = self.nacked.read().unwrap().clone();
            if !nacked.is_empty() {
                for offset in nacked {
                    transmitter
                        .send(self.create_message(offset).await)
                        .await
                        .unwrap();
                    self.sent_count.fetch_add(1, Ordering::SeqCst);
                }

                // clear the nacked set
                self.nacked.write().unwrap().clear();
                return;
            }

            for i in 0..request.count {
                if self.sent_count.load(Ordering::SeqCst) >= self.num {
                    return;
                }

                let offset = format!("{}-{}", event_time.timestamp_nanos_opt().unwrap(), i);
                transmitter
                    .send(self.create_message(offset.clone()).await)
                    .await
                    .unwrap();
                message_offsets.push(offset);
                self.sent_count.fetch_add(1, Ordering::SeqCst);
            }
            self.yet_to_ack.write().unwrap().extend(message_offsets);
        }

        async fn ack(&self, offsets: Vec<Offset>) {
            for offset in offsets {
                let offset = &String::from_utf8(offset.offset).unwrap();
                self.yet_to_ack.write().unwrap().remove(offset);
            }
        }

        async fn nack(
            &self,
            offsets: Vec<Offset>,
            _nack_options: Option<numaflow::shared::NackOptions>,
        ) {
            for offset in offsets {
                self.yet_to_ack
                    .write()
                    .unwrap()
                    .remove(&String::from_utf8(offset.offset.clone()).unwrap());
                self.nacked
                    .write()
                    .unwrap()
                    .insert(String::from_utf8(offset.offset).unwrap());
                self.sent_count.fetch_sub(1, Ordering::SeqCst);
            }
        }

        async fn pending(&self) -> Option<usize> {
            Some(self.yet_to_ack.read().unwrap().len() + self.nacked.read().unwrap().len())
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![1, 2])
        }
    }

    fn core_offset(offset: i64) -> CoreOffset {
        CoreOffset::Int(CoreIntOffset::new(offset, 0))
    }

    #[test]
    fn source_dispatch_spans_end_without_outputs_removes_filtered_offsets() {
        let kept = core_offset(1);
        let filtered = core_offset(2);
        let mut spans = SourceDispatchSpans::new();
        spans.insert_for_test(kept.clone(), opentelemetry::Context::new());
        spans.insert_for_test(filtered.clone(), opentelemetry::Context::new());

        let output_counts = HashMap::from([(kept.clone(), 1)]);
        spans.end_without_outputs(&output_counts);

        assert!(spans.contains(&kept));
        assert!(!spans.contains(&filtered));
    }

    #[test]
    fn source_dispatch_spans_end_is_idempotent_for_unknown_offsets() {
        let known = core_offset(1);
        let unknown = core_offset(2);
        let mut spans = SourceDispatchSpans::new();
        spans.insert_for_test(known.clone(), opentelemetry::Context::new());

        spans.end(&unknown);
        assert!(spans.contains(&known));

        spans.end(&known);
        spans.end(&known);
        assert!(spans.is_empty());
    }

    #[test]
    fn source_dispatch_spans_drop_with_remaining_spans_is_safe() {
        let mut spans = SourceDispatchSpans::new();
        spans.insert_for_test(core_offset(1), opentelemetry::Context::new());
    }

    #[tokio::test]
    async fn test_source() {
        // start the server
        let cln_token = CancellationToken::new();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("source.sock");
        let server_info_file = tmp_dir.path().join("source-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let server_handle = tokio::spawn(async move {
            // a simple source which generates total of 100 messages
            source::Server::new(SimpleSource::new(100))
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .unwrap()
        });

        // wait for the server to start
        // TODO: flaky
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = SourceClient::new(create_rpc_channel(sock_file).await.unwrap());

        let (src_read, src_ack, lag_reader) = new_source(
            client,
            5,
            Duration::from_millis(1000),
            cln_token.clone(),
            true,
        )
        .await
        .map_err(|e| panic!("failed to create source reader: {:?}", e))
        .unwrap();

        let tracker = Tracker::new(None, CancellationToken::new());
        // Concurrency is large enough to comfortably hold 50 unacked messages plus read-ahead
        // batches; matches the previous behavior that depended on the now-removed 10000-message
        // MAX_ACK_PENDING constant.
        let source: Source<crate::typ::WithoutRateLimiter> = Source::new(
            5,
            10000,
            SourceType::UserDefinedSource(Box::new(src_read), Box::new(src_ack), lag_reader),
            tracker.clone(),
            true,
            None,
            None,
            cln_token.clone(),
            None,
            false, // non-streaming path (existing test)
        )
        .await;

        let sender = source.sender.clone();

        let (mut stream, handle) = source
            .clone()
            .streaming_read(cln_token.clone(), None)
            .unwrap();
        let mut offsets = vec![];
        let mut messages = Vec::with_capacity(50);
        // we should read all the 100 messages
        for i in 0..100 {
            let message = stream.next().await.unwrap();
            assert_eq!(message.message.value, "hello".as_bytes());
            offsets.push(message.message.offset.clone());

            // store last 50 messages; ACK the first 50 explicitly.
            if i >= 50 {
                messages.push(message);
            } else {
                mark_success!(message);
            }
        }

        // wait for upto 1s with 10ms sleep between each check to make sure the pending becomes 50
        let x = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let pending = source.pending().await.unwrap();
                if pending == Some(50) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        assert!(x.is_ok(), "Timeout occurred before pending became 50");

        let source_partitions =
            Source::<crate::typ::WithoutRateLimiter>::partitions(sender.clone())
                .await
                .unwrap();
        assert_eq!(source_partitions.active_partitions, vec![1, 2]);

        // Drop messages without calling mark_success() to cause NAK
        drop(messages);

        // read should return 50 nacked messages
        for _ in 0..50 {
            let message = stream.next().await.unwrap();
            assert_eq!(message.message.value, "hello".as_bytes());
            // Mark as success so they get ACK'd (pending goes to 0)
            mark_success!(message);
        }

        // pending should be 0 now
        let x = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let pending = source.pending().await.unwrap();
                if pending == Some(0) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        assert!(x.is_ok(), "Timeout occurred before pending became 0");

        drop(source);
        drop(sender);

        cln_token.cancel();
        let _ = handle.await.unwrap();
        let _ = shutdown_tx.send(());
        server_handle.await.unwrap();
    }

    /// UD-source that emits a fixed batch on the first read containing a duplicated
    /// offset, then nothing more. Records acks and nacks so the test can assert which
    /// offsets resolved how.
    struct DuplicateSource {
        served: AtomicUsize,
        acked: std::sync::RwLock<Vec<String>>,
        nacked: std::sync::RwLock<Vec<String>>,
    }

    impl DuplicateSource {
        fn new() -> Self {
            Self {
                served: AtomicUsize::new(0),
                acked: std::sync::RwLock::new(Vec::new()),
                nacked: std::sync::RwLock::new(Vec::new()),
            }
        }

        fn message(offset_bytes: &[u8]) -> Message {
            Message {
                value: b"hi".to_vec(),
                event_time: Utc::now(),
                offset: Offset {
                    offset: offset_bytes.to_vec(),
                    partition_id: 0,
                },
                keys: vec![],
                headers: Default::default(),
                user_metadata: None,
            }
        }
    }

    #[tonic::async_trait]
    impl source::Sourcer for DuplicateSource {
        async fn read(&self, _req: SourceReadRequest, transmitter: Sender<Message>) {
            if self.served.fetch_add(1, Ordering::SeqCst) > 0 {
                // Subsequent reads return nothing so the source quiesces.
                return;
            }
            // Same offset twice (duplicate) plus one unique offset.
            transmitter.send(Self::message(b"dup")).await.unwrap();
            transmitter.send(Self::message(b"dup")).await.unwrap();
            transmitter.send(Self::message(b"unique")).await.unwrap();
        }

        async fn ack(&self, offsets: Vec<Offset>) {
            for off in offsets {
                self.acked
                    .write()
                    .unwrap()
                    .push(String::from_utf8(off.offset).unwrap());
            }
        }

        async fn nack(
            &self,
            offsets: Vec<Offset>,
            _nack_options: Option<numaflow::shared::NackOptions>,
        ) {
            for off in offsets {
                self.nacked
                    .write()
                    .unwrap()
                    .push(String::from_utf8(off.offset).unwrap());
            }
        }

        async fn pending(&self) -> Option<usize> {
            Some(0)
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![0])
        }
    }

    /// When a source batch contains two messages with the same offset, only the first
    /// is forwarded downstream. The duplicate is dropped without any ack/nack against
    /// the source SDK — the original copy already in the tracker owns the source-side
    /// resolution for that offset.
    #[tokio::test]
    async fn duplicate_inflight_from_source_is_dropped() {
        let cln_token = CancellationToken::new();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("dup-source.sock");
        let server_info_file = tmp_dir.path().join("dup-source-server-info");

        let server_socket = sock_file.clone();
        let server_info = server_info_file.clone();
        let server_handle = tokio::spawn(async move {
            source::Server::new(DuplicateSource::new())
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .unwrap()
        });

        // Give the server time to bind.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = SourceClient::new(create_rpc_channel(sock_file).await.unwrap());
        let (src_read, src_ack, lag_reader) = new_source(
            client,
            5,
            Duration::from_millis(1000),
            cln_token.clone(),
            true,
        )
        .await
        .expect("new_source should succeed");

        let tracker = Tracker::new(None, CancellationToken::new());
        let source: Source<crate::typ::WithoutRateLimiter> = Source::new(
            5,
            5,
            SourceType::UserDefinedSource(Box::new(src_read), Box::new(src_ack), lag_reader),
            tracker.clone(),
            true,
            None,
            None,
            cln_token.clone(),
            None,
            false, // non-streaming path (existing test)
        )
        .await;

        let (mut stream, handle) = source
            .clone()
            .streaming_read(cln_token.clone(), None)
            .unwrap();

        // Each distinct offset should be forwarded exactly once.
        let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("first message")
            .expect("stream open");
        let second = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("second message")
            .expect("stream open");

        // Each distinct offset should be forwarded exactly once.
        assert_ne!(
            first.message.offset, second.message.offset,
            "duplicate offset must not be forwarded twice; got {} twice",
            first.message.offset
        );

        // A third message should NOT arrive (duplicate was filtered).
        let third = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
        assert!(
            third.is_err(),
            "no third message expected, got {:?}",
            third.ok().flatten().map(|m| m.message.offset.to_string())
        );

        // Ack the forwarded copies so the source SDK sees a clean resolution for each
        // distinct offset.
        mark_success!(first);
        mark_success!(second);

        // Wait for the ack pipeline to flush.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if tracker.is_empty().await.unwrap() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("tracker should drain");

        drop(source);
        cln_token.cancel();
        let _ = handle.await.unwrap();
        let _ = shutdown_tx.send(());
        server_handle.await.unwrap();
    }

    // ─── Streaming-path tests ────────────────────────────────────────────────
    //
    // These tests cover the acquire-after-read model (009) using a simple
    // in-process generator source (no gRPC server needed).

    /// Build a generator-backed Source with `streaming = true`.
    async fn make_streaming_source(
        batch_size: usize,
        concurrency: usize,
        num_messages: usize,
        tracker: Tracker,
        cln_token: CancellationToken,
    ) -> Source<crate::typ::WithoutRateLimiter> {
        use crate::config::components::source::GeneratorConfig;
        let cfg = GeneratorConfig {
            rpu: num_messages,
            content: bytes::Bytes::from_static(b"payload"),
            duration: std::time::Duration::from_millis(10),
            value: None,
            key_count: 0,
            msg_size_bytes: 8,
            jitter: std::time::Duration::ZERO,
        };
        let (reader, acker, lag) =
            crate::source::generator::new_generator(cfg, batch_size, cln_token.clone()).unwrap();
        Source::new(
            batch_size,
            concurrency,
            SourceType::Generator(reader, acker, lag),
            tracker,
            false,
            None,
            None,
            cln_token,
            None,
            true, // streaming = true
        )
        .await
    }

    /// Streaming: in-flight count is capped at `concurrency`.
    ///
    /// We set `concurrency = 3` and `batch_size = 10`. After reading 3 messages
    /// without acking them, the 4th permit acquisition must block (concurrency cap).
    /// Once we ack one message, one more message can be admitted. No message is
    /// dropped or skipped.
    #[tokio::test]
    async fn streaming_inflight_bounded_by_concurrency() {
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        // concurrency=3, batch_size=10, 20 total messages
        let source = make_streaming_source(10, 3, 20, tracker.clone(), cln_token.clone()).await;

        let (mut stream, handle) = source.streaming_read(cln_token.clone(), None).unwrap();

        // Receive exactly 3 messages without acking — the 4th should not arrive
        // while these are held (all permits consumed).
        let msg1 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("msg1 timeout")
            .expect("stream open");
        let msg2 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("msg2 timeout")
            .expect("stream open");
        let msg3 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("msg3 timeout")
            .expect("stream open");

        // With concurrency=3 and all 3 permits held, the 4th message should not
        // be forwarded downstream within 200ms (acquire_owned will block).
        let no_fourth = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
        assert!(
            no_fourth.is_err(),
            "4th message must not arrive while concurrency=3 slots are all held"
        );

        // Ack one — now one permit is released; the 4th message should arrive.
        mark_success!(msg1);
        let msg4 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("msg4 timeout after acking msg1")
            .expect("stream open");

        // Ack the rest.
        mark_success!(msg2);
        mark_success!(msg3);
        mark_success!(msg4);

        cln_token.cancel();
        let _ = handle.await.unwrap();
    }

    /// Streaming: per-message out-of-order ack releases permits individually.
    ///
    /// We read exactly 2 messages with concurrency=2 (no more can be in flight).
    /// Ack msg2 BEFORE msg1 (out of order). Both should succeed individually, and
    /// after cancellation + drain, the tracker should be empty — no deadlock on
    /// out-of-order resolution.
    #[tokio::test]
    async fn streaming_per_message_out_of_order_ack() {
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        // concurrency=2, batch_size=2: exactly 2 messages in flight at a time.
        let source = make_streaming_source(2, 2, 10, tracker.clone(), cln_token.clone()).await;

        let (mut stream, handle) = source.streaming_read(cln_token.clone(), None).unwrap();

        let msg1 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("msg1")
            .expect("stream open");
        let msg2 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("msg2")
            .expect("stream open");

        // Cancel before acking so no new messages are admitted after we ack.
        // (The already-in-flight msg1 and msg2 will still be acked.)
        cln_token.cancel();

        // Ack msg2 BEFORE msg1 (out of order) — per-message ack means each oneshot
        // fires independently; msg1's ack task does not wait for msg2's.
        mark_success!(msg2);
        mark_success!(msg1);

        // Handle must return Ok (drain completes once both permits are released).
        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("handle should complete")
            .expect("handle join");
        assert!(result.is_ok(), "handle returned error: {:?}", result);

        // Tracker must be empty (both messages acked, no new ones admitted after cancel).
        assert!(
            tracker.is_empty().await.unwrap(),
            "tracker should be empty after out-of-order ack"
        );
    }

    /// Streaming duplicate-inflight: the duplicate drops its permit immediately
    /// and does not stall/panic the drain.
    ///
    /// Uses `DuplicateSource` (already defined above). With streaming=true, the
    /// duplicate's permit must be released so the drain can complete without
    /// hanging. The one unique message and the first copy of "dup" are acked;
    /// the duplicate copy is silently dropped.
    #[tokio::test]
    async fn streaming_duplicate_inflight_releases_permit_and_drains() {
        let cln_token = CancellationToken::new();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("streaming-dup-source.sock");
        let server_info_file = tmp_dir.path().join("streaming-dup-source-server-info");

        let server_socket = sock_file.clone();
        let server_info = server_info_file.clone();
        let server_handle = tokio::spawn(async move {
            source::Server::new(DuplicateSource::new())
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = SourceClient::new(create_rpc_channel(sock_file).await.unwrap());
        let (src_read, src_ack, lag_reader) = new_source(
            client,
            5,
            Duration::from_millis(1000),
            cln_token.clone(),
            true,
        )
        .await
        .expect("new_source");

        let tracker = Tracker::new(None, cln_token.clone());
        // concurrency=5 so permits are plentiful; the test is about drain correctness
        // not about backpressure.
        let source: Source<crate::typ::WithoutRateLimiter> = Source::new(
            5,
            5,
            SourceType::UserDefinedSource(Box::new(src_read), Box::new(src_ack), lag_reader),
            tracker.clone(),
            true,
            None,
            None,
            cln_token.clone(),
            None,
            true, // streaming = true
        )
        .await;

        let (mut stream, handle) = source.streaming_read(cln_token.clone(), None).unwrap();

        // DuplicateSource emits ["dup", "dup", "unique"] — only 2 distinct offsets
        // forwarded downstream ("dup" once + "unique").
        let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("first")
            .expect("stream open");
        let second = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("second")
            .expect("stream open");

        // The two forwarded offsets must be distinct.
        assert_ne!(
            first.message.offset, second.message.offset,
            "duplicate must not be forwarded twice"
        );

        // No third message (the duplicate was dropped).
        let third = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
        assert!(third.is_err(), "no third message expected");

        mark_success!(first);
        mark_success!(second);

        // Tracker must drain (no leaked permit, no hang).
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if tracker.is_empty().await.unwrap() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("tracker should drain — no permit leak from duplicate");

        cln_token.cancel();
        let _ = handle.await.unwrap();
        let _ = shutdown_tx.send(());
        server_handle.await.unwrap();
    }

    /// Streaming cancellation: already-read-but-not-yet-permitted messages are
    /// dropped (never tracker-inserted) and eligible for redelivery; the drain
    /// barrier completes without hanging; handle returns Ok.
    ///
    /// We set concurrency=1 so the first message fills the only permit. Then we
    /// cancel while the second message (already read into the batch) is waiting on
    /// permit acquisition. It must be dropped (tracker stays at 1 entry for the
    /// in-flight first message). After we ack the first message the drain completes.
    #[tokio::test]
    async fn streaming_cancellation_drops_unpermitted_messages() {
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        // concurrency=1, batch_size=5: 1 message in-flight, rest blocked on permit.
        let source = make_streaming_source(5, 1, 10, tracker.clone(), cln_token.clone()).await;

        let (mut stream, handle) = source.streaming_read(cln_token.clone(), None).unwrap();

        // Read one message; this fills the sole permit.
        let msg1 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("msg1")
            .expect("stream open");

        // The source should have tracker size = 1 (msg1 in flight).
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !tracker.is_empty().await.unwrap() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("tracker should have 1 entry");

        // Cancel while msg1 permit is held. The read loop should unblock, break
        // out of permit acquisition, and stop.
        cln_token.cancel();

        // Ack msg1 — its permit is released; the drain barrier can complete.
        mark_success!(msg1);

        // Handle must return Ok within a generous timeout (no hang).
        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("handle should complete after cancel + ack")
            .expect("handle join");
        assert!(result.is_ok(), "handle returned error: {:?}", result);

        // Tracker must be empty (msg1 was acked, no other messages were inserted).
        assert!(
            tracker.is_empty().await.unwrap(),
            "tracker should be empty after drain"
        );
    }

    /// Streaming: dropping a MessageHandle without ack/nak triggers the recv-error
    /// branch in invoke_ack_single (oneshot sender dropped) — the tracker entry is
    /// deleted and the permit released so the drain completes (no hang/leak).
    #[tokio::test]
    async fn streaming_dropped_handle_releases_permit_via_recv_error() {
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        let source = make_streaming_source(1, 1, 10, tracker.clone(), cln_token.clone()).await;
        let (mut stream, handle) = source.streaming_read(cln_token.clone(), None).unwrap();

        let msg1 = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("msg1")
            .expect("stream open");

        cln_token.cancel();
        drop(msg1); // no ack/nak → ack_rx.await returns Err → recv-error branch

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("handle completes")
            .expect("join");
        assert!(result.is_ok());
        assert!(
            tracker.is_empty().await.unwrap(),
            "tracker empty after dropped handle"
        );
    }

    /// 1->1 passthrough source transformer used to exercise the streaming
    /// transformer branch (Some(transformer) => Ok) in `streaming_source`.
    struct PassthroughTransformer;

    #[tonic::async_trait]
    impl numaflow::sourcetransform::SourceTransformer for PassthroughTransformer {
        async fn transform(
            &self,
            input: numaflow::sourcetransform::SourceTransformRequest,
        ) -> Vec<numaflow::sourcetransform::Message> {
            vec![
                numaflow::sourcetransform::Message::new(input.value, Utc::now())
                    .with_keys(input.keys),
            ]
        }
    }

    /// Streaming with a transformer present: exercises the
    /// `Some(transformer) => transform_batch(...)` Ok branch of `streaming_source`.
    /// A 1->1 passthrough transformer forwards each message downstream; we confirm
    /// a transformed message arrives and the source drains cleanly on shutdown.
    #[tokio::test]
    async fn streaming_with_transformer_forwards_messages() {
        use crate::config::components::source::GeneratorConfig;
        use crate::transformer::Transformer;
        use numaflow::sourcetransform;
        use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;

        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());

        // Start a UD source-transformer server (1->1 passthrough).
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("streaming-transformer.sock");
        let server_info_file = tmp_dir.path().join("streaming-transformer-server-info");
        let server_socket = sock_file.clone();
        let server_info = server_info_file.clone();
        let server_handle = tokio::spawn(async move {
            sourcetransform::Server::new(PassthroughTransformer)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = SourceTransformClient::new(create_rpc_channel(sock_file).await.unwrap());
        let transformer =
            Transformer::new(10, 10, Duration::from_secs(10), client, tracker.clone())
                .await
                .unwrap();

        // Generator source (10 msgs), streaming=true, WITH the transformer.
        let cfg = GeneratorConfig {
            rpu: 10,
            content: bytes::Bytes::from_static(b"payload"),
            duration: Duration::from_millis(10),
            value: None,
            key_count: 0,
            msg_size_bytes: 8,
            jitter: Duration::ZERO,
        };
        let (reader, acker, lag) =
            crate::source::generator::new_generator(cfg, 5, cln_token.clone()).unwrap();
        // concurrency=1, batch_size=5: only one message is in flight at a time; the next
        // is blocked on permit acquisition, so a cancel cleanly stops admission and the
        // source never sends into a closed channel (mirrors the streaming_cancellation test).
        let source: Source<crate::typ::WithoutRateLimiter> = Source::new(
            5,
            1,
            SourceType::Generator(reader, acker, lag),
            tracker.clone(),
            false,
            Some(transformer),
            None,
            cln_token.clone(),
            None,
            true, // streaming = true
        )
        .await;

        let (mut stream, handle) = source.streaming_read(cln_token.clone(), None).unwrap();

        // A transformed message must arrive (the transformer Ok branch produced
        // output and it was forwarded downstream).
        let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("transformed message timeout")
            .expect("stream open");

        // Cancel BEFORE acking so no further message is admitted (the next one is blocked
        // on permit acquisition and the cancel breaks it); then ack the one in hand so the
        // drain barrier completes. The stream is kept alive — never dropped mid-send.
        cln_token.cancel();
        mark_success!(msg);
        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("handle should complete")
            .expect("handle join");
        assert!(result.is_ok(), "handle returned error: {:?}", result);

        let _ = shutdown_tx.send(());
        let _ = server_handle.await;
    }

    /// Streaming with a bypass router present: exercises the
    /// `if let Some(ref bypass_router) => try_bypass(...)` branch of `streaming_source`.
    /// The bypass condition requires a tag the generator never sets, so no message is
    /// bypassed — every message goes through `try_bypass` and is forwarded downstream
    /// (the `Ok(Some(msg))` arm), then sent on the channel.
    #[tokio::test]
    async fn streaming_with_bypass_router_forwards_unmatched_messages() {
        use crate::config::monovertex::BypassConditions;
        use crate::monovertex::bypass_router::{BypassRouterConfig, MvtxBypassRouter};
        use crate::sinker::sink::{SinkClientType, SinkWriterBuilder};
        use numaflow_models::models::{ForwardConditions, TagConditions};

        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());

        // Log sink writer backs the bypass receiver. It is never exercised here
        // because no message matches the bypass condition.
        let sink_writer =
            SinkWriterBuilder::new(10, Duration::from_millis(100), SinkClientType::Log)
                .build()
                .await
                .unwrap();

        // Sink bypass condition requiring a tag the generator never sets → nothing
        // is bypassed; messages flow through try_bypass and are forwarded.
        let conditions = BypassConditions {
            sink: Some(Box::new(ForwardConditions::new(TagConditions {
                values: vec!["never-matches".to_string()],
                operator: Some("or".to_string()),
            }))),
            fallback: None,
            on_success: None,
        };
        let config = BypassRouterConfig::new(conditions, 10, Duration::from_millis(1000));
        let (router, _router_handle) =
            MvtxBypassRouter::initialize(config, sink_writer, cln_token.clone()).await;

        // concurrency=1, batch_size=5: one message in flight; cancel cleanly stops
        // admission and the source never sends into a closed channel.
        let source = make_streaming_source(5, 1, 10, tracker.clone(), cln_token.clone()).await;
        let (mut stream, handle) = source
            .streaming_read(cln_token.clone(), Some(router))
            .unwrap();

        // Generator messages carry no tags → not bypassed → forwarded downstream.
        let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("forwarded message timeout")
            .expect("stream open");

        // Cancel before acking (next message is blocked on permit; cancel breaks it),
        // then ack the one in hand so the drain completes. Stream kept alive.
        cln_token.cancel();
        mark_success!(msg);
        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("handle should complete")
            .expect("handle join");
        assert!(result.is_ok(), "handle returned error: {:?}", result);
    }
}
