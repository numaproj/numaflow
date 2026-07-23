use crate::config::monovertex::BypassConditions;
use crate::config::pipeline::VERTEX_TYPE_SINK;
use crate::config::{get_vertex_name, is_mono_vertex};
use crate::error::Error;
use crate::message::{Message, MessageHandle, MessageID, NackOptions};
use crate::metrics::{
    PIPELINE_PARTITION_NAME_LABEL, monovertex_metrics, mvtx_forward_metric_labels,
    pipeline_drop_metric_labels, pipeline_metric_labels, pipeline_metrics,
};
use crate::shared::otel;
use crate::sinker::actor::{SinkActorMessage, SinkActorResponse};
use crate::sinker::builder::HealthCheckClients;
// Re-export SinkWriterBuilder for external use
pub(crate) use crate::sinker::builder::SinkWriterBuilder;
use crate::{Result, mark_failed_batch};
use crate::{mark_failed, mark_success_batch};
use numaflow_kafka::sink::KafkaSink;
use numaflow_pb::clients::sink::Status::{Failure, Fallback, OnSuccess, Serve, Success};
use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::sink::sink_response;
use numaflow_pulsar::sink::Sink as PulsarSink;
use numaflow_sqs::sink::SqsSink;
use serve::{ServingStore, StoreEntry};
use serving::{DEFAULT_ID_HEADER, DEFAULT_POD_HASH_KEY};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::{pin, time};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{error, info};

/// A [Blackhole] sink which reads but never writes to anywhere, semantic equivalent of `/dev/null`.
///
/// [Blackhole]: https://numaflow.numaproj.io/user-guide/sinks/blackhole/
pub(super) mod blackhole;

/// [log] sink prints out the read messages on to stdout.
///
/// [Log]: https://numaflow.numaproj.io/user-guide/sinks/log/
pub(super) mod log;

/// Serving [ServingStore] to store the result of the serving pipeline. It also contains the builtin [serve::ServeSink]
/// to write to the serving store.
pub(crate) mod serve;

mod kafka;
mod pulsar;
mod sqs;

/// [User-Defined Sink] extends Numaflow to add custom sources supported outside the builtins.
///
/// [User-Defined Sink]: https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/
#[path = "sink/user_defined.rs"]
pub(crate) mod user_defined;

/// Set of items to be implemented be a Numaflow Sink.
///
/// [Sink]: https://numaflow.numaproj.io/user-guide/sinks/overview/
#[trait_variant::make(Sink: Send)]
#[allow(dead_code)]
pub(crate) trait LocalSink {
    /// Write the messages to the Sink.
    async fn sink(&mut self, messages: Vec<Message>) -> Result<Vec<ResponseFromSink>>;
}

pub(crate) enum SinkClientType {
    Log,
    Blackhole,
    Serve,
    UserDefined(
        Box<SinkClient<Channel>>,
        Option<user_defined::ReconnectConfig>,
    ),
    Sqs(SqsSink),
    Kafka(KafkaSink),
    Pulsar(Box<PulsarSink>),
}

/// Aims to contain messages that require special handling,
/// eg: nack with options, for them to be carried back to the callee
///
/// This struct is introduced here to allow for extensibility
/// in case we need to implement ack with options in the future
#[derive(Default)]
pub(crate) struct ProcessedSinkBatch {
    // Messages that are to be nacked back to source
    pub(crate) nacked: Vec<Message>,
}

impl ProcessedSinkBatch {
    pub(crate) fn new() -> Self {
        ProcessedSinkBatch::default()
    }

    pub(crate) fn with_nacked(mut self, nacked: Vec<Message>) -> Self {
        self.nacked.extend(nacked);
        self
    }

    pub(crate) fn merge_with(mut self, other: Self) -> Self {
        self.nacked.extend(other.nacked);
        self
    }
}

impl From<SinkActorResponse> for ProcessedSinkBatch {
    fn from(value: SinkActorResponse) -> Self {
        ProcessedSinkBatch::new().with_nacked(value.nacked)
    }
}

/// SinkWriter is a writer that writes messages to the Sink.
///
/// Error handling and shutdown: There can be non-retryable errors(udsink panics etc.), in that case we will
/// cancel the token to indicate the upstream will not send any more messages to the sink, we drain any inflight
/// messages that are in input stream and nack them using the tracker, when the upstream stops sending
/// messages the input stream will be closed, and we will stop the component.
#[derive(Clone)]
pub(crate) struct SinkWriter {
    batch_size: usize,
    chunk_timeout: Duration,
    sink_handle: mpsc::Sender<SinkActorMessage>,
    fb_sink_handle: Option<mpsc::Sender<SinkActorMessage>>,
    on_success_sink_handle: Option<mpsc::Sender<SinkActorMessage>>,
    shutting_down_on_err: bool,
    final_result: Result<()>,
    serving_store: Option<ServingStore>,
    health_check_clients: HealthCheckClients,
    bypass_conditions: Option<BypassConditions>,
}

impl SinkWriter {
    /// Create a new SinkWriter instance.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        batch_size: usize,
        chunk_timeout: Duration,
        sink_handle: mpsc::Sender<SinkActorMessage>,
        fb_sink_handle: Option<mpsc::Sender<SinkActorMessage>>,
        on_success_sink_handle: Option<mpsc::Sender<SinkActorMessage>>,
        serving_store: Option<ServingStore>,
        health_check_clients: HealthCheckClients,
        bypass_conditions: Option<BypassConditions>,
    ) -> Self {
        Self {
            batch_size,
            chunk_timeout,
            sink_handle,
            fb_sink_handle,
            on_success_sink_handle,
            shutting_down_on_err: false,
            final_result: Ok(()),
            serving_store,
            health_check_clients,
            bypass_conditions,
        }
    }

    /// Sink the messages to the Primary Sink.
    async fn write_to_primary_sink(
        &self,
        messages: Vec<Message>,
        cancel: CancellationToken,
    ) -> Result<SinkActorResponse> {
        let (tx, rx) = oneshot::channel();
        let msg = SinkActorMessage {
            messages,
            respond_to: tx,
            cancel,
        };
        let _ = self.sink_handle.send(msg).await;
        rx.await
            .map_err(|e| Error::ActorPatternRecv(e.to_string()))?
    }

    /// Sink the messages to the Fallback Sink.
    async fn write_to_fb_sink(
        &self,
        messages: Vec<Message>,
        cancel: CancellationToken,
    ) -> Result<SinkActorResponse> {
        if self.fb_sink_handle.is_none() {
            return Err(Error::FbSink(
                "Response contains fallback messages but no fallback sink is configured. \
                Please update the spec to configure fallback sink https://numaflow.numaproj.io/user-guide/sinks/fallback/ ".to_string(),
            ));
        }

        let (tx, rx) = oneshot::channel();
        let msg = SinkActorMessage {
            messages,
            respond_to: tx,
            cancel,
        };
        let _ = self.fb_sink_handle.as_ref().unwrap().send(msg).await;
        rx.await
            .map_err(|e| Error::ActorPatternRecv(e.to_string()))?
    }

    /// Sink the messages to the OnSuccess Sink.
    async fn write_to_on_success_sink(
        &self,
        messages: Vec<Message>,
        cancel: CancellationToken,
    ) -> Result<SinkActorResponse> {
        if self.on_success_sink_handle.is_none() {
            return Err(Error::OsSink(
                "Response contains OnSuccess messages but no OnSuccess sink is configured. \
                Please update the spec to configure on-success sink https://numaflow.numaproj.io/user-guide/sinks/on-success/ ".to_string(),
            ));
        }

        let (tx, rx) = oneshot::channel();
        let msg = SinkActorMessage {
            messages,
            respond_to: tx,
            cancel,
        };
        let _ = self
            .on_success_sink_handle
            .as_ref()
            .unwrap()
            .send(msg)
            .await;
        rx.await
            .map_err(|e| Error::ActorPatternRecv(e.to_string()))?
    }

    /// Streaming write the messages to the Sink, it will keep writing messages until the stream is
    /// closed or the cancellation token is triggered.
    pub(crate) async fn streaming_write(
        mut self,
        messages_stream: ReceiverStream<MessageHandle>,
        cln_token: CancellationToken,
    ) -> Result<JoinHandle<Result<()>>> {
        Ok(tokio::spawn({
            async move {
                info!(?self.batch_size, ?self.chunk_timeout, "Starting sink writer");

                // Combine chunking and timeout into a stream
                let chunk_stream =
                    messages_stream.chunks_timeout(self.batch_size, self.chunk_timeout);
                pin!(chunk_stream);

                // Main processing loop
                while let Some(read_batch) = chunk_stream.next().await {
                    // We are in shutting down mode, NAK all messages
                    if self.shutting_down_on_err {
                        for msg in read_batch {
                            msg.mark_failed(self.final_result.as_ref().unwrap_err(), None);
                        }
                        continue;
                    }

                    let messages = read_batch.iter().map(|msg| msg.message().clone()).collect();
                    match self.process_batch(messages, cln_token.clone()).await {
                        Ok(processed_messages) => {
                            // Batch processed successfully

                            let (acked_handles, nacked_handles) =
                                Self::split_batch_handles(read_batch, processed_messages);

                            for nacked_handle in nacked_handles {
                                let opts = nacked_handle.message.nack_options.clone();
                                mark_failed!(nacked_handle, "message nacked", opts);
                            }
                            mark_success_batch!(acked_handles);
                        }
                        Err(e) => {
                            if matches!(e, Error::UdfRedrive(_)) {
                                mark_failed_batch!(read_batch, &e);
                                error!(?e, "redrivable sink error");
                                continue;
                            }
                            mark_failed_batch!(read_batch, &e);
                            // Critical error, cancel upstream and initiate shutdown
                            error!(?e, "Error writing to sink, initiating shutdown.");
                            cln_token.cancel();
                            self.final_result = Err(e);
                            self.shutting_down_on_err = true;
                        }
                    }
                }

                // finalize
                self.final_result
            }
        }))
    }

    /// processed_sink_batch already contains nacked messages,
    pub(crate) fn split_batch_handles(
        read_batch: Vec<MessageHandle>,
        processed_sink_batch: ProcessedSinkBatch,
    ) -> (Vec<MessageHandle>, Vec<MessageHandle>) {
        let mut read_map: HashMap<MessageID, MessageHandle> = read_batch
            .into_iter()
            .map(|msg| (msg.message.id.clone(), msg))
            .collect();

        // Messages the sink nacked: pull each handle out of the read map and carry over the
        // nack options that came from downstream (udsink, fb_udsink, ons_udsink).
        let nacked_handles: Vec<MessageHandle> = processed_sink_batch
            .nacked
            .into_iter()
            .map(|msg| {
                let mut handle = read_map.remove(&msg.id).expect(
                    "nacked message not found in message handle batch read into sink. \
                There is a duplicate in msg handle batch.",
                );
                handle.message.nack_options = msg.nack_options;
                handle
            })
            .collect();

        // Everything left in the map is acked.
        let acked_handles = read_map.into_values().collect();

        (acked_handles, nacked_handles)
    }

    /// Processes a batch of message handles: handles bypass, dropped messages, and writes to sink.
    /// On success, all messages are ACK'd. On error, messages are NAK'd (dropped without ack).
    async fn process_batch(
        &mut self,
        messages: Vec<Message>,
        cln_token: CancellationToken,
    ) -> Result<ProcessedSinkBatch> {
        let (nacked, to_process): (Vec<_>, Vec<_>) =
            messages.into_iter().partition(|read_msg| read_msg.nacked());

        // If bypass conditions exist for primary sink, ack and skip
        if let Some(conditions) = &self.bypass_conditions
            && let Some(ref _sink) = conditions.sink
        {
            return Ok(ProcessedSinkBatch::new().with_nacked(nacked));
        }

        // Separate dropped messages from messages to process
        let (dropped, to_process): (Vec<_>, Vec<_>) = to_process
            .into_iter()
            .partition(|read_msg| read_msg.dropped());

        let dropped_count = dropped.len();

        // If all messages were dropped, we're done
        if to_process.is_empty() {
            send_drop_metrics(is_mono_vertex(), dropped_count);
            return Ok(ProcessedSinkBatch::new().with_nacked(nacked));
        }

        // Perform the write operation
        let written_messages = self
            .write_to_sink(to_process, cln_token.clone())
            .await?
            .with_nacked(nacked);

        send_drop_metrics(is_mono_vertex(), dropped_count);
        // TODO: add nacked metrics

        Ok(written_messages)
    }

    /// Write the messages to the Sink.
    /// Invokes the primary sink actor, handles fallback messages, serving messages, and errors.
    pub(crate) async fn write_to_sink(
        &mut self,
        mut messages: Vec<Message>,
        cln_token: CancellationToken,
    ) -> Result<ProcessedSinkBatch> {
        if messages.is_empty() {
            return Ok(ProcessedSinkBatch::new());
        }

        let mut processed_fallback_msgs: Option<ProcessedSinkBatch> = None;
        let mut processed_on_success_msgs: Option<ProcessedSinkBatch> = None;
        let mut processed_serving_msgs: Option<ProcessedSinkBatch> = None;
        let write_start_time = time::Instant::now();
        let messages_count = messages.len();
        let messages_size: usize = messages.iter().map(|msg| msg.value.len()).sum();

        // Tracing: per-message primary sink stage spans, scoped to the primary sink actor call.
        // When no OTel layer is registered, `inject_stage_span` returns non-recording spans and
        // the sys_metadata copy-on-write is skipped — no need to gate this call site.
        let mut response = {
            let _stage_spans = otel::inject_stage_spans!(
                messages.iter_mut(),
                otel::TraceTopology::current(),
                otel::TraceStage::Sink(otel::SinkStage::Primary),
            );
            self.write_to_primary_sink(messages, cln_token.clone())
                .await?
        };

        // Strip tracing_udf from response messages so fallback/on_success/serving sinks don't
        // inherit the primary sink stage span as a parent. No-op when no key was injected.
        response
            .fallback
            .iter_mut()
            .for_each(|m| m.strip_tracing_udf());
        response
            .on_success
            .iter_mut()
            .for_each(|m| m.strip_tracing_udf());
        response
            .serving
            .iter_mut()
            .for_each(|m| m.strip_tracing_udf());

        if !response.failed.is_empty() {
            error!(
                "Failed to write messages after retries: {:?}",
                response.failed
            );
            Self::send_error_metrics(is_mono_vertex());
            return Err(Error::Sink(
                "Failed to write messages after retries".to_string(),
            ));
        }

        let fallback_messages_count = response.fallback.len();
        let fallback_messages_size: usize =
            response.fallback.iter().map(|msg| msg.value.len()).sum();
        let dropped_messages_count = response.dropped.len();
        let dropped_messages_size: usize = response.dropped.iter().map(|msg| msg.value.len()).sum();

        // If there are fallback messages, write them to the fallback sink
        if !response.fallback.is_empty() {
            processed_fallback_msgs = Some(
                self.write_to_fallback(response.fallback, cln_token.clone())
                    .await?,
            );
        }

        // If there are on_success messages, write them to the on_success sink
        if !response.on_success.is_empty() {
            processed_on_success_msgs = Some(
                self.write_to_on_success(response.on_success, cln_token.clone())
                    .await?,
            );
        }

        // If there are serving messages, write them to the serving store
        if !response.serving.is_empty() {
            processed_serving_msgs = Some(self.write_to_serving_store(response.serving).await?);
        }

        let processed_primary_messages = [
            processed_fallback_msgs,
            processed_on_success_msgs,
            processed_serving_msgs,
        ]
        .into_iter()
        .flatten()
        .fold(
            ProcessedSinkBatch::new().with_nacked(response.nacked),
            ProcessedSinkBatch::merge_with,
        );

        Self::send_metrics(
            messages_count,
            messages_size,
            fallback_messages_count,
            fallback_messages_size,
            dropped_messages_count,
            dropped_messages_size,
            write_start_time,
        );

        Ok(processed_primary_messages)
    }

    /// Write messages to the OnSuccess Sink.
    pub(crate) async fn write_to_on_success(
        &mut self,
        mut messages: Vec<Message>,
        cln_token: CancellationToken,
    ) -> Result<ProcessedSinkBatch> {
        if messages.is_empty() {
            return Ok(ProcessedSinkBatch::new());
        }

        let on_success_sink_start = time::Instant::now();
        let messages_count = messages.len();
        let messages_size: usize = messages.iter().map(|msg| msg.value.len()).sum();

        // Tracing: per-message on-success sink stage spans, scoped to the on-success sink
        // actor call. Only emit when an on-success sink is configured; the noop-tracer path
        // handles the "no OTel layer" case.
        let on_success_response = {
            let _stage_spans = self.on_success_sink_handle.is_some().then(|| {
                otel::inject_stage_spans!(
                    messages.iter_mut(),
                    otel::TraceTopology::current(),
                    otel::TraceStage::Sink(otel::SinkStage::OnSuccess),
                )
            });
            self.write_to_on_success_sink(messages, cln_token.clone())
                .await?
        };

        // Check if fallback returned fallback or serving status (not allowed)
        if !on_success_response.fallback.is_empty() {
            return Err(Error::FbSink(
                "OnSuccess response contains fallback messages. \
                    Specifying fallback status in OnSuccess response is not allowed."
                    .to_string(),
            ));
        }
        if !on_success_response.serving.is_empty() {
            return Err(Error::FbSink(
                "OnSuccess response contains serving messages. \
                    Specifying serving status in OnSuccess response is not allowed."
                    .to_string(),
            ));
        }

        if !on_success_response.failed.is_empty() {
            return Err(Error::OsSink(
                "Failed to write messages to on_success sink after retries".to_string(),
            ));
        }

        Self::send_ons_sink_metrics(messages_count, messages_size, on_success_sink_start);

        Ok(on_success_response.into())
    }

    /// Write the messages to the Fallback Sink.
    pub(crate) async fn write_to_fallback(
        &mut self,
        mut messages: Vec<Message>,
        cln_token: CancellationToken,
    ) -> Result<ProcessedSinkBatch> {
        if messages.is_empty() {
            return Ok(ProcessedSinkBatch::new());
        }

        let fallback_sink_start = time::Instant::now();
        let messages_count = messages.len();
        let messages_size: usize = messages.iter().map(|msg| msg.value.len()).sum();

        // Tracing: per-message fallback sink stage spans, scoped to the fallback sink actor
        // call. Only emit when a fallback sink is configured.
        let fb_response = {
            let _stage_spans = self.fb_sink_handle.is_some().then(|| {
                otel::inject_stage_spans!(
                    messages.iter_mut(),
                    otel::TraceTopology::current(),
                    otel::TraceStage::Sink(otel::SinkStage::Fallback),
                )
            });
            self.write_to_fb_sink(messages, cln_token.clone()).await?
        };

        // Check if fallback returned fallback or serving status (not allowed)
        if !fb_response.fallback.is_empty() {
            return Err(Error::FbSink(
                "Response in fallback sink contains fallback messages. \
                    Specifying fallback status in fallback sink is not allowed."
                    .to_string(),
            ));
        }
        if !fb_response.serving.is_empty() {
            return Err(Error::FbSink(
                "Response in fallback sink contains serving messages. \
                    Specifying serving status in fallback response is not allowed."
                    .to_string(),
            ));
        }
        if !fb_response.failed.is_empty() {
            return Err(Error::FbSink(
                "Failed to write messages to fallback sink after retries".to_string(),
            ));
        }

        Self::send_fb_sink_metrics(messages_count, messages_size, fallback_sink_start);

        Ok(fb_response.into())
    }

    /// Writes the serving messages to the serving store
    async fn write_to_serving_store(
        &mut self,
        messages: Vec<Message>,
    ) -> Result<ProcessedSinkBatch> {
        let Some(serving_store) = &mut self.serving_store else {
            return Err(Error::Sink(
                "Response contains serving messages but no serving store is configured. \
                Please consider updating spec to configure serving store."
                    .to_string(),
            ));
        };

        // convert Message to StoreEntry
        let mut payloads = Vec::with_capacity(messages.len());
        for msg in messages.clone() {
            let id = msg
                .headers
                .get(DEFAULT_ID_HEADER)
                .ok_or(Error::Sink("Missing numaflow id header".to_string()))?
                .clone();
            let pod_hash = msg
                .headers
                .get(DEFAULT_POD_HASH_KEY)
                .ok_or(Error::Sink("Missing pod replica header".to_string()))?
                .clone();
            payloads.push(StoreEntry {
                pod_hash,
                id,
                value: msg.value.clone(),
            });
        }

        // push to corresponding store
        match serving_store {
            ServingStore::UserDefined(ud_store) => {
                ud_store.put_datum(get_vertex_name(), payloads).await?;
            }
            ServingStore::Nats(nats_store) => {
                nats_store.put_datum(get_vertex_name(), payloads).await?;
            }
        }

        Ok(ProcessedSinkBatch::new())
    }

    /// Check if the Sink is ready to accept messages.
    pub(crate) async fn ready(&mut self) -> bool {
        self.health_check_clients.ready().await
    }

    /// Send metrics for sink
    fn send_metrics(
        messages_count: usize,
        messages_size: usize,
        fallback_messages_count: usize,
        fallback_messages_size: usize,
        dropped_messages_count: usize,
        dropped_messages_size: usize,
        write_start_time: time::Instant,
    ) {
        if is_mono_vertex() {
            monovertex_metrics()
                .sink
                .time
                .get_or_create(mvtx_forward_metric_labels())
                .observe(write_start_time.elapsed().as_micros() as f64);
            monovertex_metrics()
                .sink
                .write_total
                .get_or_create(mvtx_forward_metric_labels())
                .inc_by((messages_count - fallback_messages_count - dropped_messages_count) as u64);

            if dropped_messages_count > 0 {
                monovertex_metrics()
                    .sink
                    .dropped_total
                    .get_or_create(mvtx_forward_metric_labels())
                    .inc_by(dropped_messages_count as u64);
            }
        } else {
            let mut labels = pipeline_metric_labels(VERTEX_TYPE_SINK).clone();
            labels.push((
                PIPELINE_PARTITION_NAME_LABEL.to_string(),
                get_vertex_name().to_string(),
            ));

            pipeline_metrics()
                .forwarder
                .write_total
                .get_or_create(&labels)
                .inc_by((messages_count - fallback_messages_count - dropped_messages_count) as u64);
            pipeline_metrics()
                .forwarder
                .write_bytes_total
                .get_or_create(&labels)
                .inc_by((messages_size - fallback_messages_size - dropped_messages_size) as u64);
            pipeline_metrics()
                .forwarder
                .write_processing_time
                .get_or_create(&labels)
                .observe(write_start_time.elapsed().as_micros() as f64);

            if dropped_messages_count > 0 {
                pipeline_metrics()
                    .forwarder
                    .drop_total
                    .get_or_create(&pipeline_drop_metric_labels(
                        VERTEX_TYPE_SINK,
                        get_vertex_name(),
                        "Retries exhausted in the Sink",
                    ))
                    .inc_by(dropped_messages_count as u64);
            }
        }
    }

    /// Send metrics for fallback sink
    fn send_fb_sink_metrics(
        messages_count: usize,
        messages_size: usize,
        fallback_sink_start: time::Instant,
    ) {
        if is_mono_vertex() {
            monovertex_metrics()
                .fb_sink
                .write_total
                .get_or_create(mvtx_forward_metric_labels())
                .inc_by(messages_count as u64);
            monovertex_metrics()
                .fb_sink
                .time
                .get_or_create(mvtx_forward_metric_labels())
                .observe(fallback_sink_start.elapsed().as_micros() as f64);
        } else {
            let mut labels = pipeline_metric_labels(VERTEX_TYPE_SINK).clone();
            labels.push((
                PIPELINE_PARTITION_NAME_LABEL.to_string(),
                get_vertex_name().to_string(),
            ));
            pipeline_metrics()
                .sink_forwarder
                .fbsink_write_total
                .get_or_create(&labels)
                .inc_by(messages_count as u64);
            pipeline_metrics()
                .sink_forwarder
                .fbsink_write_bytes_total
                .get_or_create(&labels)
                .inc_by(messages_size as u64);

            pipeline_metrics()
                .sink_forwarder
                .fbsink_write_processing_time
                .get_or_create(&labels)
                .observe(fallback_sink_start.elapsed().as_micros() as f64);
        }
    }

    /// Send metrics for onSuccess sink
    fn send_ons_sink_metrics(
        messages_count: usize,
        messages_size: usize,
        ons_sink_start: time::Instant,
    ) {
        if is_mono_vertex() {
            monovertex_metrics()
                .ons_sink
                .write_total
                .get_or_create(mvtx_forward_metric_labels())
                .inc_by(messages_count as u64);
            monovertex_metrics()
                .ons_sink
                .time
                .get_or_create(mvtx_forward_metric_labels())
                .observe(ons_sink_start.elapsed().as_micros() as f64);
        } else {
            let mut labels = pipeline_metric_labels(VERTEX_TYPE_SINK).clone();
            labels.push((
                PIPELINE_PARTITION_NAME_LABEL.to_string(),
                get_vertex_name().to_string(),
            ));
            pipeline_metrics()
                .sink_forwarder
                .onsuccess_sink_write_total
                .get_or_create(&labels)
                .inc_by(messages_count as u64);
            pipeline_metrics()
                .sink_forwarder
                .onsuccess_sink_write_bytes_total
                .get_or_create(&labels)
                .inc_by(messages_size as u64);

            pipeline_metrics()
                .sink_forwarder
                .onsuccess_sink_write_processing_time
                .get_or_create(&labels)
                .observe(ons_sink_start.elapsed().as_micros() as f64);
        }
    }

    /// Send metrics for errors
    fn send_error_metrics(is_mono_vertex: bool) {
        if is_mono_vertex {
            monovertex_metrics()
                .sink
                .write_errors_total
                .get_or_create(mvtx_forward_metric_labels())
                .inc();
        } else {
            let mut labels = pipeline_metric_labels(VERTEX_TYPE_SINK).clone();
            labels.push((
                PIPELINE_PARTITION_NAME_LABEL.to_string(),
                get_vertex_name().to_string(),
            ));
            pipeline_metrics()
                .forwarder
                .write_error_total
                .get_or_create(&labels)
                .inc_by(1);
        }
    }
}

/// Sends count of messages marked for explicit drop by the user.
/// For MonoVertex these drops originate in the map UDF (or its bypass router),
/// so they are accounted under the UDF drop counter.
/// Currently pub(crate) to allow usage by the bypass_router.
pub(crate) fn send_drop_metrics(is_mono_vertex: bool, dropped_messages_count: usize) {
    if is_mono_vertex {
        monovertex_metrics()
            .udf
            .dropped_total
            .get_or_create(mvtx_forward_metric_labels())
            .inc_by(dropped_messages_count as u64);
    } else {
        // The reason here is different from the one used when
        // messages are dropped after 3 failed retries
        pipeline_metrics()
            .forwarder
            .drop_total
            .get_or_create(&pipeline_drop_metric_labels(
                VERTEX_TYPE_SINK,
                get_vertex_name(),
                "Dropped Upstream",
            ))
            .inc_by(dropped_messages_count as u64);
    }
}

/// Sink's status for each [Message] written to Sink.
#[derive(PartialEq, Debug)]
pub(crate) enum ResponseStatusFromSink {
    /// Successfully wrote to the Sink.
    Success,
    /// Failed with error message.
    Failed(String),
    /// Write to FallBack Sink.
    Fallback,
    /// Write to serving store.
    Serve(Option<Vec<u8>>),
    OnSuccess(Option<sink_response::result::Message>),
    // TODO: add options
    Nack(Option<NackOptions>),
}

/// Sink will give a response per [Message].
#[derive(Debug, PartialEq)]
pub(crate) struct ResponseFromSink {
    /// Unique id per [Message]. We need to track per [Message] status.
    pub(crate) id: String,
    /// Status of the "sink" operation per [Message].
    pub(crate) status: ResponseStatusFromSink,
}

impl From<sink_response::Result> for ResponseFromSink {
    fn from(value: sink_response::Result) -> Self {
        let status = match value.status() {
            Success => ResponseStatusFromSink::Success,
            Failure => ResponseStatusFromSink::Failed(value.err_msg),
            Fallback => ResponseStatusFromSink::Fallback,
            Serve => ResponseStatusFromSink::Serve(value.serve_response),
            OnSuccess => ResponseStatusFromSink::OnSuccess(value.on_success_msg),
            numaflow_pb::clients::sink::Status::Nack => {
                ResponseStatusFromSink::Nack(value.nack_options.map(Into::into))
            }
        };
        Self {
            id: value.id,
            status,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::pipeline::NatsStoreConfig;
    use crate::message::{IntOffset, Message, MessageHandle, MessageID, Offset, ReadAck};
    use crate::metadata::{KeyValueGroup, Metadata};
    use crate::shared::grpc::create_rpc_channel;
    use crate::sinker::sink::serve::nats::NatsServingStore;
    use crate::tracker::Tracker;
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;
    use chrono::{TimeZone, Utc};
    use numaflow::shared::ServerExtras;
    use numaflow::sink;
    use numaflow_pb::clients::sink::{SinkRequest, SinkResponse};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Once;
    use tokio::sync::mpsc::Receiver;
    use tokio::time::{Duration, sleep};
    use tokio_util::sync::CancellationToken;

    struct SimpleSink;
    #[tonic::async_trait]
    impl sink::Sinker for SimpleSink {
        async fn sink(&self, mut input: Receiver<sink::SinkRequest>) -> Vec<sink::Response> {
            let mut responses: Vec<sink::Response> = Vec::new();
            while let Some(datum) = input.recv().await {
                if datum.keys.first().unwrap() == "fallback" {
                    responses.push(sink::Response::fallback(datum.id));
                } else if datum.keys.first().unwrap() == "error" {
                    responses.push(sink::Response::failure(
                        datum.id,
                        "simple error".to_string(),
                    ));
                } else if datum.keys.first().unwrap() == "serve" {
                    responses.push(sink::Response::serve(datum.id, "serve-response".into()));
                } else if datum.keys.first().unwrap() == "onSuccess" {
                    let on_success_msg = sink::Message {
                        value: "ons-message".into(),
                        keys: None,
                        user_metadata: None,
                    };
                    responses.push(sink::Response::on_success(datum.id, Some(on_success_msg)));
                } else {
                    responses.push(sink::Response::ok(datum.id));
                }
            }
            responses
        }
    }

    fn init_test_propagator() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            opentelemetry::global::set_text_map_propagator(
                opentelemetry_sdk::propagation::TraceContextPropagator::new(),
            );
            // Without a real tracer provider, the global tracer is a noop and spans aren't
            // recording — `inject_stage_span` will skip the sys_metadata write. These tests
            // verify the write side-effect, so install an SDK provider with no exporter.
            opentelemetry::global::set_tracer_provider(
                opentelemetry_sdk::trace::SdkTracerProvider::builder().build(),
            );
        });
    }

    fn test_message_with_metadata(offset: i64) -> Message {
        Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key".to_string()]),
            tags: None,
            value: format!("message {offset}").as_bytes().to_vec().into(),
            offset: Offset::Int(IntOffset::new(offset, 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: format!("offset_{offset}").into(),
                index: offset as i32,
            },
            metadata: Some(Arc::new(Metadata::default())),
            ..Default::default()
        }
    }

    #[test]
    fn inject_stage_span_adds_tracing_udf_metadata() {
        init_test_propagator();
        let mut messages = vec![test_message_with_metadata(1), test_message_with_metadata(2)];

        let _stage_spans = otel::inject_stage_spans!(
            enabled,
            messages.iter_mut(),
            otel::TraceTopology::MonoVertex,
            otel::TraceStage::Sink(otel::SinkStage::Primary),
        );

        for message in messages {
            let metadata = message.metadata.expect("metadata should exist");
            assert!(
                metadata
                    .sys_metadata
                    .contains_key(otel::TRACING_UDF_METADATA_KEY)
            );
        }
    }

    #[test]
    fn inject_stage_span_preserves_unrelated_metadata() {
        init_test_propagator();
        let mut message = test_message_with_metadata(1);
        Arc::make_mut(message.metadata.as_mut().expect("metadata should exist"))
            .sys_metadata
            .insert(
                "unrelated".to_string(),
                KeyValueGroup {
                    key_value: HashMap::from([("k".to_string(), "v".into())]),
                },
            );
        let mut messages = [message];

        let _stage_spans = otel::inject_stage_spans!(
            enabled,
            messages.iter_mut(),
            otel::TraceTopology::MonoVertex,
            otel::TraceStage::Sink(otel::SinkStage::Fallback),
        );

        let metadata = messages
            .first()
            .and_then(|msg| msg.metadata.as_deref())
            .expect("metadata should exist");
        assert!(metadata.sys_metadata.contains_key("unrelated"));
        assert!(
            metadata
                .sys_metadata
                .contains_key(otel::TRACING_UDF_METADATA_KEY)
        );
    }

    #[tokio::test]
    async fn test_write() {
        let cln_token = CancellationToken::new();
        let mut sink_writer =
            SinkWriterBuilder::new(10, Duration::from_secs(1), SinkClientType::Log)
                .build()
                .await
                .unwrap();

        let messages: Vec<Message> = (0..5)
            .map(|i| Message {
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
            })
            .collect();

        let result = sink_writer.write_to_sink(messages.clone(), cln_token).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_streaming_write() {
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        let sink_writer =
            SinkWriterBuilder::new(10, Duration::from_millis(100), SinkClientType::Log)
                .build()
                .await
                .unwrap();

        let mut ack_rxs = vec![];
        let messages: Vec<MessageHandle> = (0..10)
            .map(|i| {
                let (ack_tx, ack_rx) = oneshot::channel();
                ack_rxs.push(ack_rx);
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
                MessageHandle::new(message, ack_tx)
            })
            .collect();

        let (tx, rx) = mpsc::channel(10);
        for msg in messages {
            let _ = tx.send(msg).await;
        }

        drop(tx);

        let handle = sink_writer
            .streaming_write(ReceiverStream::new(rx), cln_token)
            .await
            .unwrap();

        let _ = handle.await.unwrap();
        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }
        // check if the tracker is empty
        assert!(tracker.is_empty().await.unwrap());
    }

    #[tokio::test]
    async fn test_streaming_write_redrivable_error_nacks_batch() {
        let (sink_tx, mut sink_rx) = mpsc::channel::<SinkActorMessage>(1);
        tokio::spawn(async move {
            while let Some(msg) = sink_rx.recv().await {
                let _ = msg.respond_to.send(Err(Error::UdfRedrive(Box::new(
                    tonic::Status::unavailable("sink reconnecting"),
                ))));
            }
        });

        let sink_writer = SinkWriter::new(
            10,
            Duration::from_millis(100),
            sink_tx,
            None,
            None,
            None,
            HealthCheckClients {
                sink_client: None,
                fb_sink_client: None,
                store_client: None,
                on_success_sink_client: None,
            },
            None,
        );

        let mut ack_rxs = vec![];
        let messages: Vec<MessageHandle> = (0..3)
            .map(|i| {
                let (ack_tx, ack_rx) = oneshot::channel();
                ack_rxs.push(ack_rx);
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
                MessageHandle::new(message, ack_tx)
            })
            .collect();

        let (tx, rx) = mpsc::channel(3);
        for msg in messages {
            let _ = tx.send(msg).await;
        }
        drop(tx);

        let handle = sink_writer
            .streaming_write(ReceiverStream::new(rx), CancellationToken::new())
            .await
            .unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_ok());
        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Nak(None));
        }
    }

    #[tokio::test]
    async fn test_streaming_write_error() {
        let tracker = Tracker::new(None, CancellationToken::new());
        // start the server
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sink.sock");
        let server_info_file = tmp_dir.path().join("sink-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();

        let _server_handle = tokio::spawn(async move {
            sink::Server::new(SimpleSink)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("failed to start sink server");
        });

        // wait for the server to start
        sleep(Duration::from_millis(100)).await;

        let sink_writer = SinkWriterBuilder::new(
            10,
            Duration::from_millis(100),
            SinkClientType::UserDefined(
                Box::new(SinkClient::new(
                    create_rpc_channel(sock_file).await.unwrap(),
                )),
                None,
            ),
        )
        .build()
        .await
        .unwrap();

        let mut ack_rxs = vec![];
        let messages: Vec<MessageHandle> = (0..10)
            .map(|i| {
                let (ack_tx, ack_rx) = oneshot::channel();
                ack_rxs.push(ack_rx);
                let message = Message {
                    typ: Default::default(),
                    keys: Arc::from(vec!["error".to_string()]),
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
                MessageHandle::new(message, ack_tx)
            })
            .collect();

        let (tx, rx) = mpsc::channel(10);
        for msg in messages {
            let _ = tx.send(msg).await;
        }
        drop(tx);

        let cln_token = CancellationToken::new();
        let handle = sink_writer
            .streaming_write(ReceiverStream::new(rx), cln_token.clone())
            .await
            .unwrap();

        // cancel the token after 1 second to exit from the retry loop
        tokio::spawn(async move {
            sleep(Duration::from_secs(1)).await;
            cln_token.cancel();
        });

        let _ = handle.await.unwrap();
        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Nak(None));
        }

        // check if the tracker is empty
        assert!(tracker.is_empty().await.unwrap());
    }

    #[tokio::test]
    async fn test_fallback_write() {
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());

        // start the server
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sink.sock");
        let server_info_file = tmp_dir.path().join("sink-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();

        let _server_handle = tokio::spawn(async move {
            sink::Server::new(SimpleSink)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("failed to start sink server");
        });

        // wait for the server to start
        sleep(Duration::from_millis(100)).await;

        let sink_writer = SinkWriterBuilder::new(
            10,
            Duration::from_millis(100),
            SinkClientType::UserDefined(
                Box::new(SinkClient::new(
                    create_rpc_channel(sock_file).await.unwrap(),
                )),
                None,
            ),
        )
        .fb_sink_client(SinkClientType::Log)
        .build()
        .await
        .unwrap();

        let mut ack_rxs = vec![];
        let messages: Vec<MessageHandle> = (0..20)
            .map(|i| {
                let (ack_tx, ack_rx) = oneshot::channel();
                ack_rxs.push(ack_rx);
                let message = Message {
                    typ: Default::default(),
                    keys: Arc::from(vec!["fallback".to_string()]),
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
                MessageHandle::new(message, ack_tx)
            })
            .collect();

        let (tx, rx) = mpsc::channel(20);
        for msg in messages {
            let _ = tx.send(msg).await;
        }

        drop(tx);
        let handle = sink_writer
            .streaming_write(ReceiverStream::new(rx), cln_token)
            .await
            .unwrap();

        let _ = handle.await.unwrap();
        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }

        // check if the tracker is empty
        assert!(tracker.is_empty().await.unwrap());
    }

    #[tokio::test]
    async fn test_on_success_write() {
        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());

        // start the server
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sink.sock");
        let server_info_file = tmp_dir.path().join("sink-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();

        let _server_handle = tokio::spawn(async move {
            sink::Server::new(SimpleSink)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("failed to start sink server");
        });

        // wait for the server to start
        sleep(Duration::from_millis(100)).await;

        let sink_writer = SinkWriterBuilder::new(
            10,
            Duration::from_millis(100),
            SinkClientType::UserDefined(
                Box::new(SinkClient::new(
                    create_rpc_channel(sock_file).await.unwrap(),
                )),
                None,
            ),
        )
        .on_success_sink_client(SinkClientType::Log)
        .build()
        .await
        .unwrap();

        let mut ack_rxs = vec![];
        let messages: Vec<MessageHandle> = (0..20)
            .map(|i| {
                let (ack_tx, ack_rx) = oneshot::channel();
                ack_rxs.push(ack_rx);
                let message = Message {
                    typ: Default::default(),
                    keys: Arc::from(vec!["onSuccess".to_string()]),
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
                MessageHandle::new(message, ack_tx)
            })
            .collect();

        let (tx, rx) = mpsc::channel(20);
        for msg in messages {
            let _ = tx.send(msg).await;
        }

        drop(tx);
        let handle = sink_writer
            .streaming_write(ReceiverStream::new(rx), cln_token)
            .await
            .unwrap();

        let _ = handle.await.unwrap();
        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }

        // check if the tracker is empty
        assert!(tracker.is_empty().await.unwrap());
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_serving_write() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let serving_store = "test_serving_write";

        // Delete bucket so that re-running the test won't fail
        let _ = context.delete_key_value(serving_store).await;

        let kv_store = context
            .create_key_value(Config {
                bucket: serving_store.to_string(),
                description: "test_description".to_string(),
                history: 15,
                ..Default::default()
            })
            .await
            .unwrap();

        let tracker = Tracker::new(None, CancellationToken::new());
        let serving_store = ServingStore::Nats(Box::new(
            NatsServingStore::new(
                context.clone(),
                NatsStoreConfig {
                    rs_store_name: serving_store.to_string(),
                },
            )
            .await
            .unwrap(),
        ));

        // start the server
        let (_shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sink.sock");
        let server_info_file = tmp_dir.path().join("sink-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();

        let _server_handle = tokio::spawn(async move {
            sink::Server::new(SimpleSink)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("failed to start sink server");
        });

        // wait for the server to start
        sleep(Duration::from_millis(100)).await;

        let sink_writer = SinkWriterBuilder::new(
            10,
            Duration::from_millis(100),
            SinkClientType::UserDefined(
                Box::new(SinkClient::new(
                    create_rpc_channel(sock_file).await.unwrap(),
                )),
                None,
            ),
        )
        .serving_store(serving_store.clone())
        .build()
        .await
        .unwrap();

        let mut ack_rxs = vec![];
        let read_messages: Vec<MessageHandle> = (0..10)
            .map(|i| {
                let mut headers = HashMap::new();
                headers.insert(DEFAULT_ID_HEADER.to_string(), format!("id_{}", i));
                headers.insert(DEFAULT_POD_HASH_KEY.to_string(), "abcd".to_string());
                let (ack_tx, ack_rx) = oneshot::channel();
                ack_rxs.push(ack_rx);
                let message = Message {
                    typ: Default::default(),
                    keys: Arc::from(vec!["serve".to_string()]),
                    tags: None,
                    value: vec![1, 2, 3].into(),
                    offset: Offset::Int(IntOffset::new(i, 0)),
                    event_time: Default::default(),
                    watermark: None,
                    id: MessageID {
                        vertex_name: "vertex".to_string().into(),
                        offset: "123".to_string().into(),
                        index: i as i32,
                    },
                    headers: Arc::new(headers),
                    ..Default::default()
                };
                MessageHandle::new(message, ack_tx)
            })
            .collect();

        let (tx, rx) = mpsc::channel(10);
        for msg in read_messages {
            let _ = tx.send(msg).await;
        }

        drop(tx);
        let cln_token = CancellationToken::new();
        let handle = sink_writer
            .streaming_write(ReceiverStream::new(rx), cln_token.clone())
            .await
            .unwrap();

        let _ = handle.await.unwrap();
        for ack_rx in ack_rxs {
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Ack);
        }

        // check if the tracker is empty
        assert!(tracker.is_empty().await.unwrap());

        let keys: Vec<_> = kv_store.keys().await.unwrap().collect().await;
        assert_eq!(keys.len(), 10);
    }

    #[test]
    fn test_message_to_sink_request() {
        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".to_string()]),
            tags: None,
            value: vec![1, 2, 3].into(),
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: Utc.timestamp_opt(1627846261, 0).unwrap(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex".to_string().into(),
                offset: "123".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        let request: SinkRequest = message.into();
        assert!(request.request.is_some());
    }

    #[test]
    fn test_sink_response_to_response_from_sink() {
        let sink_response = SinkResponse {
            results: vec![sink_response::Result {
                id: "123".to_string(),
                status: Success as i32,
                err_msg: "".to_string(),
                serve_response: None,
                on_success_msg: None,
                nack_options: None,
            }],
            handshake: None,
            status: None,
        };

        let results: Vec<ResponseFromSink> = sink_response
            .results
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        assert!(!results.is_empty());

        assert_eq!(results.first().unwrap().id, "123");
        assert_eq!(
            results.first().unwrap().status,
            ResponseStatusFromSink::Success
        );
    }

    #[test]
    fn test_strip_tracing_udf_removes_stage_context() {
        let mut sys_metadata = HashMap::new();
        sys_metadata.insert(
            otel::TRACING_UDF_METADATA_KEY.to_string(),
            KeyValueGroup {
                key_value: HashMap::new(),
            },
        );

        let mut message = Message {
            metadata: Some(Arc::new(Metadata {
                previous_vertex: String::new(),
                sys_metadata,
                user_metadata: HashMap::new(),
            })),
            ..Default::default()
        };

        message.strip_tracing_udf();

        let metadata = message.metadata.expect("metadata should still exist");
        assert!(
            !metadata
                .sys_metadata
                .contains_key(otel::TRACING_UDF_METADATA_KEY),
            "tracing_udf should be removed from downstream messages"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_send_error_metrics_mono_vertex() {
        let before = monovertex_metrics()
            .sink
            .write_errors_total
            .get_or_create(mvtx_forward_metric_labels())
            .get();

        SinkWriter::send_error_metrics(true);

        let after = monovertex_metrics()
            .sink
            .write_errors_total
            .get_or_create(mvtx_forward_metric_labels())
            .get();

        assert_eq!(
            after,
            before + 1,
            "monovertex sink write_errors_total should be incremented by 1"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_send_error_metrics_pipeline() {
        let mut labels = pipeline_metric_labels(VERTEX_TYPE_SINK).clone();
        labels.push((
            PIPELINE_PARTITION_NAME_LABEL.to_string(),
            get_vertex_name().to_string(),
        ));

        let before = pipeline_metrics()
            .forwarder
            .write_error_total
            .get_or_create(&labels)
            .get();

        SinkWriter::send_error_metrics(false);

        let after = pipeline_metrics()
            .forwarder
            .write_error_total
            .get_or_create(&labels)
            .get();

        assert_eq!(
            after,
            before + 1,
            "pipeline write_error_total should be incremented by 1"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_send_drop_metrics_mono_vertex() {
        let before = monovertex_metrics()
            .udf
            .dropped_total
            .get_or_create(mvtx_forward_metric_labels())
            .get();

        send_drop_metrics(true, 5);

        let after = monovertex_metrics()
            .udf
            .dropped_total
            .get_or_create(mvtx_forward_metric_labels())
            .get();

        assert_eq!(
            after,
            before + 5,
            "monovertex udf dropped_total should be incremented by 5"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_send_drop_metrics_pipeline() {
        let before = pipeline_metrics()
            .forwarder
            .drop_total
            .get_or_create(&pipeline_drop_metric_labels(
                VERTEX_TYPE_SINK,
                get_vertex_name(),
                "Dropped Upstream",
            ))
            .get();

        send_drop_metrics(false, 3);

        let after = pipeline_metrics()
            .forwarder
            .drop_total
            .get_or_create(&pipeline_drop_metric_labels(
                VERTEX_TYPE_SINK,
                get_vertex_name(),
                "Dropped Upstream",
            ))
            .get();

        assert_eq!(
            after,
            before + 3,
            "pipeline drop_total should be incremented by 3"
        );
    }

    // Build a MessageHandle whose message has the given tags + optional nack options.
    fn sink_handle(
        id: i64,
        tags: Option<Vec<&str>>,
        nack_options: Option<crate::message::NackOptions>,
    ) -> (MessageHandle, oneshot::Receiver<ReadAck>) {
        use crate::message::{IntOffset, MessageID, Offset};
        let (ack_tx, ack_rx) = oneshot::channel();
        let message = Message {
            keys: Arc::from(vec![format!("key-{id}")]),
            tags: tags.map(|t| Arc::from(t.into_iter().map(String::from).collect::<Vec<_>>())),
            value: format!("v-{id}").into_bytes().into(),
            offset: Offset::Int(IntOffset::new(id, 0)),
            event_time: Utc::now(),
            id: MessageID {
                vertex_name: "v".to_string().into(),
                offset: format!("o-{id}").into(),
                index: id as i32,
            },
            nack_options,
            ..Default::default()
        };
        (MessageHandle::new(message, ack_tx), ack_rx)
    }

    #[tokio::test]
    async fn test_streaming_write_input_tagged_nack() {
        use crate::message::NackOptions;
        let cln = CancellationToken::new();
        let sink_writer =
            SinkWriterBuilder::new(10, Duration::from_millis(100), SinkClientType::Blackhole)
                .build()
                .await
                .unwrap();

        let opts = NackOptions {
            delay: Some(1000),
            max_deliveries: None,
            reason: Some("upstream nack".to_string()),
            ..Default::default()
        };
        let (h_ok, rx_ok) = sink_handle(0, None, None);
        let (h_nack, rx_nack) = sink_handle(1, Some(vec!["U+005C__NACK__"]), Some(opts.clone()));
        let (h_drop, rx_drop) = sink_handle(2, Some(vec!["U+005C__DROP__"]), None);

        let (tx, rx) = mpsc::channel(10);
        for h in [h_ok, h_nack, h_drop] {
            tx.send(h).await.unwrap();
        }
        drop(tx);

        let handle = sink_writer
            .streaming_write(ReceiverStream::new(rx), cln)
            .await
            .unwrap();
        handle.await.unwrap().unwrap();

        assert_eq!(rx_ok.await.unwrap(), ReadAck::Ack);
        assert_eq!(rx_drop.await.unwrap(), ReadAck::Ack);
        assert_eq!(rx_nack.await.unwrap(), ReadAck::Nak(Some(opts)));
    }

    #[tokio::test]
    async fn test_streaming_write_all_dropped_preserves_nack_options() {
        // Regression: a batch with only dropped + nacked (nothing to write) must
        // still carry the nacked options, not collapse to Nak(None).
        use crate::message::NackOptions;
        let cln = CancellationToken::new();
        let sink_writer =
            SinkWriterBuilder::new(10, Duration::from_millis(100), SinkClientType::Blackhole)
                .build()
                .await
                .unwrap();

        let opts = NackOptions {
            delay: Some(2500),
            max_deliveries: Some(4),
            reason: Some("all-dropped batch".to_string()),
            ..Default::default()
        };
        let (h_drop, rx_drop) = sink_handle(0, Some(vec!["U+005C__DROP__"]), None);
        let (h_nack, rx_nack) = sink_handle(1, Some(vec!["U+005C__NACK__"]), Some(opts.clone()));

        let (tx, rx) = mpsc::channel(10);
        for h in [h_drop, h_nack] {
            tx.send(h).await.unwrap();
        }
        drop(tx);

        let handle = sink_writer
            .streaming_write(ReceiverStream::new(rx), cln)
            .await
            .unwrap();
        handle.await.unwrap().unwrap();

        assert_eq!(rx_drop.await.unwrap(), ReadAck::Ack);
        assert_eq!(rx_nack.await.unwrap(), ReadAck::Nak(Some(opts)));
    }

    #[test]
    fn test_sink_response_nack_status_conversion() {
        use crate::message::NackOptions;
        use numaflow_pb::clients::sink::Status;
        use numaflow_pb::common::nack_options::NackOptions as PbNackOptions;

        // NACK + options -> Nack(Some(..))
        let r = sink_response::Result {
            id: "id-1".to_string(),
            status: Status::Nack as i32,
            err_msg: String::new(),
            serve_response: None,
            on_success_msg: None,
            nack_options: Some(PbNackOptions {
                reason: Some("rate limited".to_string()),
                max_deliveries: None,
                delay: Some(3000),
                ..Default::default()
            }),
        };
        let resp: ResponseFromSink = r.into();
        assert_eq!(resp.id, "id-1");
        assert_eq!(
            resp.status,
            ResponseStatusFromSink::Nack(Some(NackOptions {
                reason: Some("rate limited".to_string()),
                max_deliveries: None,
                delay: Some(3000),
                ..Default::default()
            }))
        );

        // NACK without options -> Nack(None)
        let r_none = sink_response::Result {
            id: "id-2".to_string(),
            status: Status::Nack as i32,
            err_msg: String::new(),
            serve_response: None,
            on_success_msg: None,
            nack_options: None,
        };
        let resp_none: ResponseFromSink = r_none.into();
        assert_eq!(resp_none.status, ResponseStatusFromSink::Nack(None));

        // Regression guard: SUCCESS still maps to Success.
        let r_ok = sink_response::Result {
            id: "id-3".to_string(),
            status: Status::Success as i32,
            err_msg: String::new(),
            serve_response: None,
            on_success_msg: None,
            nack_options: None,
        };
        let resp_ok: ResponseFromSink = r_ok.into();
        assert_eq!(resp_ok.status, ResponseStatusFromSink::Success);
    }

    #[tokio::test]
    async fn test_streaming_write_bypass_preserves_nack_options() {
        // Regression: with a primary-sink bypass configured, process_batch returns
        // early (before any write). A nacked input must still NAK with its options
        // while the rest are acked (no handle leak).
        use crate::config::monovertex::BypassConditions;
        use crate::message::NackOptions;
        use numaflow_models::models::{ForwardConditions, TagConditions};

        // sink.is_some() is all that matters; content is ignored (`_sink`).
        let bypass = BypassConditions {
            sink: Some(Box::new(ForwardConditions::new(TagConditions::new(vec![])))),
            fallback: None,
            on_success: None,
        };
        let sink_writer =
            SinkWriterBuilder::new(10, Duration::from_millis(100), SinkClientType::Blackhole)
                .bypass_conditions(bypass)
                .build()
                .await
                .unwrap();

        let opts = NackOptions {
            delay: Some(750),
            max_deliveries: None,
            reason: Some("bypass nack".to_string()),
            ..Default::default()
        };
        let (h_ok, rx_ok) = sink_handle(0, None, None);
        let (h_nack, rx_nack) = sink_handle(1, Some(vec!["U+005C__NACK__"]), Some(opts.clone()));

        let (tx, rx) = mpsc::channel(10);
        for h in [h_ok, h_nack] {
            tx.send(h).await.unwrap();
        }
        drop(tx);

        let handle = sink_writer
            .streaming_write(ReceiverStream::new(rx), CancellationToken::new())
            .await
            .unwrap();
        handle.await.unwrap().unwrap();

        assert_eq!(rx_ok.await.unwrap(), ReadAck::Ack);
        assert_eq!(rx_nack.await.unwrap(), ReadAck::Nak(Some(opts)));
    }

    #[tokio::test]
    async fn test_sink_own_nack_status_end_to_end() {
        use crate::message::NackOptions;
        use crate::shared::grpc::create_rpc_channel;
        use numaflow::sink;
        use numaflow_pb::clients::sink::sink_client::SinkClient;

        // Mock sink that nacks every message with options (the sink's own Status::NACK).
        struct NackSink;
        #[tonic::async_trait]
        impl sink::Sinker for NackSink {
            async fn sink(
                &self,
                mut input: tokio::sync::mpsc::Receiver<sink::SinkRequest>,
            ) -> Vec<sink::Response> {
                let mut responses = vec![];
                while let Some(datum) = input.recv().await {
                    responses.push(sink::Response::nack(
                        datum.id,
                        Some(numaflow::shared::NackOptions {
                            delay: Some(5000),
                            max_deliveries: Some(3),
                            reason: Some("sink rate limited".to_string()),
                        }),
                    ));
                }
                responses
            }
        }

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sink.sock");
        let server_info_file = tmp_dir.path().join("sink-server-info");
        let (ss, si) = (sock_file.clone(), server_info_file.clone());
        let server_handle = tokio::spawn(async move {
            sink::Server::new(NackSink)
                .with_socket_file(ss)
                .with_server_info_file(si)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("sink server failed");
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let cln = CancellationToken::new();
        let sink_writer = SinkWriterBuilder::new(
            10,
            Duration::from_millis(100),
            SinkClientType::UserDefined(
                Box::new(SinkClient::new(
                    create_rpc_channel(sock_file.clone()).await.unwrap(),
                )),
                None,
            ),
        )
        .build()
        .await
        .unwrap();

        let (ack_tx, ack_rx) = oneshot::channel();
        let message = Message {
            keys: Arc::from(vec!["k".to_string()]),
            value: "v".into(),
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: Utc::now(),
            id: MessageID {
                vertex_name: "v".to_string().into(),
                offset: "0".into(),
                index: 0,
            },
            ..Default::default()
        };
        let (tx, rx) = mpsc::channel(10);
        tx.send(MessageHandle::new(message, ack_tx)).await.unwrap();
        drop(tx);

        let jh = sink_writer
            .streaming_write(ReceiverStream::new(rx), cln)
            .await
            .unwrap();
        jh.await.unwrap().unwrap();

        assert_eq!(
            ack_rx.await.unwrap(),
            ReadAck::Nak(Some(NackOptions {
                delay: Some(5000),
                max_deliveries: Some(3),
                reason: Some("sink rate limited".to_string()),
                ..Default::default()
            }))
        );

        shutdown_tx.send(()).expect("send shutdown");
        server_handle.await.expect("join server");
    }

    #[tokio::test]
    async fn test_split_batch_handles_acked_and_nacked() {
        use crate::message::{IntOffset, MessageID, NackOptions, Offset};

        let mk = |id: i64| {
            let (ack_tx, ack_rx) = oneshot::channel();
            let message = Message {
                offset: Offset::Int(IntOffset::new(id, 0)),
                id: MessageID {
                    vertex_name: "v".to_string().into(),
                    offset: format!("o-{id}").into(),
                    index: id as i32,
                },
                ..Default::default()
            };
            (MessageHandle::new(message, ack_tx), ack_rx)
        };

        let (h0, rx0) = mk(0);
        let (h1, rx1) = mk(1);
        let (h2, rx2) = mk(2);
        let id1 = h1.message().id.clone();

        let opts = NackOptions {
            delay: Some(500),
            max_deliveries: None,
            reason: Some("n".to_string()),
            ..Default::default()
        };
        // processed batch reports h1 as nacked, carrying options on the message.
        let nacked_msg = Message {
            id: id1.clone(),
            nack_options: Some(opts.clone()),
            ..Default::default()
        };
        let processed = ProcessedSinkBatch::new().with_nacked(vec![nacked_msg]);

        let (acked, nacked) = SinkWriter::split_batch_handles(vec![h0, h1, h2], processed);

        assert_eq!(nacked.len(), 1, "exactly one nacked handle");
        assert_eq!(nacked.first().map(|h| h.message().id.clone()), Some(id1));
        assert_eq!(
            nacked
                .first()
                .and_then(|h| h.message().nack_options.clone()),
            Some(opts.clone())
        );
        assert_eq!(acked.len(), 2, "the other two are acked (complement)");

        // Settle and confirm the ReadAcks (totality: every handle settled exactly once).
        for h in acked {
            h.mark_success();
        }
        for h in nacked {
            let o = h.message().nack_options.clone();
            h.mark_failed("message nacked", o);
        }
        assert_eq!(rx0.await.unwrap(), ReadAck::Ack);
        assert_eq!(rx2.await.unwrap(), ReadAck::Ack);
        assert_eq!(rx1.await.unwrap(), ReadAck::Nak(Some(opts)));
    }
}
