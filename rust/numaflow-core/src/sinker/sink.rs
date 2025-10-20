use numaflow_kafka::sink::KafkaSink;
use numaflow_pb::clients::serving::serving_store_client::ServingStoreClient;
use numaflow_pb::clients::sink::Status::{Failure, Fallback, Serve, Success};
use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::sink::sink_response;
use numaflow_pulsar::sink::Sink as PulsarSink;
use numaflow_sqs::sink::SqsSink;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::{pin, time};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{error, info};

use crate::Result;
use crate::config::components::sink::RetryConfig;
use crate::config::pipeline::VERTEX_TYPE_SINK;
use crate::config::{get_vertex_name, is_mono_vertex};
use crate::error::Error;
use crate::message::Message;
use crate::metrics::{
    PIPELINE_PARTITION_NAME_LABEL, monovertex_metrics, mvtx_forward_metric_labels,
    pipeline_metric_labels, pipeline_metrics,
};
use crate::sinker::actor::{SinkActor, SinkActorMessage, SinkActorResponse};
use serving::{DEFAULT_ID_HEADER, DEFAULT_POD_HASH_KEY};

use serve::{ServingStore, StoreEntry};
use user_defined::UserDefinedSink;

/// A [Blackhole] sink which reads but never writes to anywhere, semantic equivalent of `/dev/null`.
///
/// [Blackhole]: https://numaflow.numaproj.io/user-guide/sinks/blackhole/
#[path = "sink/blackhole.rs"]
mod blackhole;

/// [log] sink prints out the read messages on to stdout.
///
/// [Log]: https://numaflow.numaproj.io/user-guide/sinks/log/
#[path = "sink/log.rs"]
mod log;

/// Serving [ServingStore] to store the result of the serving pipeline. It also contains the builtin [serve::ServeSink]
/// to write to the serving store.
#[path = "sink/serve.rs"]
pub mod serve;

#[path = "sink/sqs.rs"]
mod sqs;

#[path = "sink/pulsar.rs"]
mod pulsar;

#[path = "sink/kafka.rs"]
mod kafka;

/// [User-Defined Sink] extends Numaflow to add custom sources supported outside the builtins.
///
/// [User-Defined Sink]: https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/
#[path = "sink/user_defined.rs"]
mod user_defined;

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
    UserDefined(SinkClient<Channel>),
    Sqs(SqsSink),
    Kafka(KafkaSink),
    Pulsar(Box<PulsarSink>),
}

/// User defined clients which will be used for doing sidecar health checks.
#[derive(Clone)]
struct HealthCheckClients {
    sink_client: Option<SinkClient<Channel>>,
    fb_sink_client: Option<SinkClient<Channel>>,
    store_client: Option<ServingStoreClient<Channel>>,
}

impl HealthCheckClients {
    pub(crate) async fn ready(&mut self) -> bool {
        let sink = if let Some(sink_client) = &mut self.sink_client {
            match sink_client.is_ready(tonic::Request::new(())).await {
                Ok(ready) => ready.into_inner().ready,
                Err(e) => {
                    error!(?e, "Sink client is not ready");
                    false
                }
            }
        } else {
            true
        };

        let fb_sink = if let Some(fb_sink_client) = &mut self.fb_sink_client {
            match fb_sink_client.is_ready(tonic::Request::new(())).await {
                Ok(ready) => ready.into_inner().ready,
                Err(e) => {
                    error!(?e, "Fallback Sink client is not ready");
                    false
                }
            }
        } else {
            true
        };

        let serve_store = if let Some(store_client) = &mut self.store_client {
            match store_client.is_ready(tonic::Request::new(())).await {
                Ok(ready) => ready.into_inner().ready,
                Err(e) => {
                    error!(?e, "Store client is not ready");
                    false
                }
            }
        } else {
            true
        };

        sink && fb_sink && serve_store
    }
}

/// HealthCheckClientsBuilder is a builder for HealthCheckClients.
struct HealthCheckClientsBuilder {
    sink_client: Option<SinkClient<Channel>>,
    fb_sink_client: Option<SinkClient<Channel>>,
    store_client: Option<ServingStoreClient<Channel>>,
}

impl HealthCheckClientsBuilder {
    fn new() -> Self {
        Self {
            sink_client: None,
            fb_sink_client: None,
            store_client: None,
        }
    }

    fn sink_client(mut self, sink_client: SinkClient<Channel>) -> Self {
        self.sink_client = Some(sink_client);
        self
    }

    fn fb_sink_client(mut self, fb_sink_client: SinkClient<Channel>) -> Self {
        self.fb_sink_client = Some(fb_sink_client);
        self
    }

    fn store_client(mut self, store_client: ServingStoreClient<Channel>) -> Self {
        self.store_client = Some(store_client);
        self
    }

    fn build(self) -> HealthCheckClients {
        HealthCheckClients {
            sink_client: self.sink_client,
            fb_sink_client: self.fb_sink_client,
            store_client: self.store_client,
        }
    }
}

/// SinkWriter is a writer that writes messages to the Sink.
///
/// Error handling and shutdown: There can non-retryable errors(udsink panics etc.), in that case we will
/// cancel the token to indicate the upstream not send any more messages to the sink, we drain any inflight
/// messages that are in input stream and nack them using the tracker, when the upstream stops sending
/// messages the input stream will be closed, and we will stop the component.
#[derive(Clone)]
pub(crate) struct SinkWriter {
    batch_size: usize,
    chunk_timeout: Duration,
    sink_handle: mpsc::Sender<SinkActorMessage>,
    fb_sink_handle: Option<mpsc::Sender<SinkActorMessage>>,
    shutting_down: bool,
    final_result: Result<()>,
    serving_store: Option<ServingStore>,
    health_check_clients: HealthCheckClients,
}

/// SinkWriterBuilder is a builder to build a SinkWriter.
pub(crate) struct SinkWriterBuilder {
    batch_size: usize,
    chunk_timeout: Duration,
    retry_config: RetryConfig,
    sink_client: SinkClientType,
    fb_sink_client: Option<SinkClientType>,
    serving_store: Option<ServingStore>,
}

impl SinkWriterBuilder {
    pub(crate) fn new(
        batch_size: usize,
        chunk_timeout: Duration,
        sink_type: SinkClientType,
    ) -> Self {
        Self {
            batch_size,
            chunk_timeout,
            retry_config: RetryConfig::default(),
            sink_client: sink_type,
            fb_sink_client: None,
            serving_store: None,
        }
    }

    pub(crate) fn retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    pub(crate) fn fb_sink_client(mut self, fb_sink_client: SinkClientType) -> Self {
        self.fb_sink_client = Some(fb_sink_client);
        self
    }

    pub(crate) fn serving_store(mut self, serving_store: ServingStore) -> Self {
        self.serving_store = Some(serving_store);
        self
    }

    /// Build the SinkWriter, it also starts the SinkActor to handle messages.
    pub(crate) async fn build(self) -> Result<SinkWriter> {
        let (sink_handle, receiver) = mpsc::channel(self.batch_size);
        let mut health_check_builder = HealthCheckClientsBuilder::new();
        let retry_config = self.retry_config.clone();

        // starting sinks
        match self.sink_client {
            SinkClientType::Log => {
                let log_sink = log::LogSink;
                tokio::spawn(async move {
                    let actor = SinkActor::new(receiver, log_sink, retry_config);
                    actor.run().await;
                });
            }
            SinkClientType::Serve => {
                let serve_sink = serve::ServeSink;
                tokio::spawn(async move {
                    let actor = SinkActor::new(receiver, serve_sink, retry_config);
                    actor.run().await;
                });
            }
            SinkClientType::Blackhole => {
                let blackhole_sink = blackhole::BlackholeSink;
                tokio::spawn(async move {
                    let actor = SinkActor::new(receiver, blackhole_sink, retry_config);
                    actor.run().await;
                });
            }
            SinkClientType::UserDefined(sink_client) => {
                health_check_builder = health_check_builder.sink_client(sink_client.clone());
                let sink = UserDefinedSink::new(sink_client).await?;
                tokio::spawn(async move {
                    let actor = SinkActor::new(receiver, sink, retry_config);
                    actor.run().await;
                });
            }
            SinkClientType::Sqs(sqs_sink) => {
                tokio::spawn(async move {
                    let actor = SinkActor::new(receiver, sqs_sink, retry_config);
                    actor.run().await;
                });
            }
            SinkClientType::Kafka(kafka_sink) => {
                tokio::spawn(async move {
                    let actor = SinkActor::new(receiver, kafka_sink, retry_config);
                    actor.run().await;
                });
            }
            SinkClientType::Pulsar(pulsar_sink) => {
                tokio::spawn(async move {
                    let actor = SinkActor::new(receiver, *pulsar_sink, retry_config);
                    actor.run().await;
                });
            }
        };

        // start fallback sinks
        let fb_sink_handle = if let Some(fb_sink_client) = self.fb_sink_client {
            let (fb_sender, fb_receiver) = mpsc::channel(self.batch_size);
            let fb_retry_config = self.retry_config.clone();
            match fb_sink_client {
                SinkClientType::Log => {
                    let log_sink = log::LogSink;
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, log_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::Serve => {
                    let serve_sink = serve::ServeSink;
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, serve_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::Blackhole => {
                    let blackhole_sink = blackhole::BlackholeSink;
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, blackhole_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::UserDefined(sink_client) => {
                    health_check_builder = health_check_builder.fb_sink_client(sink_client.clone());
                    let sink = UserDefinedSink::new(sink_client).await?;
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::Sqs(sqs_sink) => {
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, sqs_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::Kafka(kafka_sink) => {
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, kafka_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
                SinkClientType::Pulsar(pulsar_sink) => {
                    tokio::spawn(async move {
                        let actor = SinkActor::new(fb_receiver, *pulsar_sink, fb_retry_config);
                        actor.run().await;
                    });
                }
            };
            Some(fb_sender)
        } else {
            None
        };

        // NOTE: we do not start the serving store sink because it is over unary while the rest are
        // streaming.
        if let Some(ServingStore::UserDefined(store)) = &self.serving_store {
            health_check_builder = health_check_builder.store_client(store.get_store_client());
        }

        let health_check_clients = health_check_builder.build();

        Ok(SinkWriter {
            batch_size: self.batch_size,
            chunk_timeout: self.chunk_timeout,
            sink_handle,
            fb_sink_handle,
            shutting_down: false,
            final_result: Ok(()),
            serving_store: self.serving_store,
            health_check_clients,
        })
    }
}

impl SinkWriter {
    /// Sink the messages to the Sink.
    async fn sink(
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
        rx.await.expect("Error receiving response from sink actor")
    }

    /// Sink the messages to the Fallback Sink.
    async fn fb_sink(
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
        rx.await.unwrap()
    }

    /// Streaming write the messages to the Sink, it will keep writing messages until the stream is
    /// closed or the cancellation token is triggered.
    pub(crate) async fn streaming_write(
        mut self,
        messages_stream: ReceiverStream<Message>,
        cln_token: CancellationToken,
    ) -> Result<JoinHandle<Result<()>>> {
        let handle: JoinHandle<Result<()>> = tokio::spawn({
            async move {
                info!(?self.batch_size, ?self.chunk_timeout, "Starting sink writer");

                // Combine chunking and timeout into a stream
                let chunk_stream =
                    messages_stream.chunks_timeout(self.batch_size, self.chunk_timeout);
                pin!(chunk_stream);

                // Main processing loop
                while let Some(batch) = chunk_stream.next().await {
                    // empty batch means end of stream
                    if batch.is_empty() {
                        continue;
                    }

                    // we are in shutting down mode, we will not be writing to the sink
                    // tell tracker to nack the messages.
                    if self.shutting_down {
                        for msg in &batch {
                            msg.ack_handle
                                .as_ref()
                                .expect("ack handle should be present")
                                .is_failed
                                .store(true, Ordering::Relaxed);
                        }
                        continue;
                    }

                    // collect ack handles for later failure tracking
                    let ack_handles = batch
                        .iter()
                        .map(|msg| msg.ack_handle.clone())
                        .collect::<Vec<_>>();

                    // filter out messages that are marked for drop
                    let batch: Vec<_> = batch.into_iter().filter(|msg| !msg.dropped()).collect();

                    // skip if all were dropped
                    if batch.is_empty() {
                        continue;
                    }

                    // perform the write operation
                    if let Err(e) = self.write(batch, cln_token.clone()).await {
                        // critical error, cancel upstream and mark all acks as failed
                        error!(?e, "Error writing to sink, initiating shutdown.");
                        cln_token.cancel();

                        for ack_handle in ack_handles {
                            ack_handle
                                .as_ref()
                                .expect("ack handle should be present")
                                .is_failed
                                .store(true, Ordering::Relaxed);
                        }

                        self.final_result = Err(e);
                        self.shutting_down = true;
                    }

                    // if cancellation triggered externally, break early
                    if cln_token.is_cancelled() {
                        break;
                    }
                }

                // finalize
                self.final_result.clone()
            }
        });

        Ok(handle)
    }

    /// Write the messages to the Sink.
    /// Invokes the primary sink actor, handles fallback messages, serving messages, and errors.
    pub(crate) async fn write(
        &mut self,
        messages: Vec<Message>,
        cln_token: CancellationToken,
    ) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let write_start_time = time::Instant::now();
        let total_msgs = messages.len();
        let total_msgs_bytes: usize = messages.iter().map(|msg| msg.value.len()).sum();

        // Invoke primary sink actor (with retry logic inside)
        let response = self.sink(messages, cln_token.clone()).await?;

        let failed = !response.failed.is_empty();
        let fb_msgs_total = response.fallback.len();
        let fb_msgs_bytes_total: usize = response.fallback.iter().map(|msg| msg.value.len()).sum();
        let serving_msgs_total = response.serving.len();
        let serving_msgs_bytes_total: usize =
            response.serving.iter().map(|msg| msg.value.len()).sum();

        // If there are fallback messages, write them to the fallback sink
        if !response.fallback.is_empty() {
            let fallback_sink_start = time::Instant::now();
            // Invoke fallback sink actor (with retry logic inside)
            let fb_response = self.fb_sink(response.fallback, cln_token.clone()).await?;

            // Check if fallback returned fallback or serving status (not allowed)
            if !fb_response.fallback.is_empty() {
                return Err(Error::FbSink(
                    "Fallback response contains fallback messages. \
                    Specifying fallback status in fallback response is not allowed."
                        .to_string(),
                ));
            }
            if !fb_response.serving.is_empty() {
                return Err(Error::FbSink(
                    "Fallback response contains serving messages. \
                    Specifying serving status in fallback response is not allowed."
                        .to_string(),
                ));
            }
            if !fb_response.failed.is_empty() {
                return Err(Error::FbSink(
                    "Failed to write messages to fallback sink after retries".to_string(),
                ));
            }

            Self::send_fb_sink_metrics(fb_msgs_total, fb_msgs_bytes_total, fallback_sink_start);
        }

        // If there are serving messages, write them to the serving store
        if !response.serving.is_empty() {
            self.handle_serving_messages(response.serving).await?;
        }

        // record metrics
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
                .inc_by((total_msgs - fb_msgs_total) as u64);
            if failed {
                monovertex_metrics()
                    .sink
                    .write_errors_total
                    .get_or_create(mvtx_forward_metric_labels())
                    .inc_by(1);
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
                .inc_by((total_msgs - fb_msgs_total - serving_msgs_total) as u64);
            pipeline_metrics()
                .forwarder
                .write_bytes_total
                .get_or_create(&labels)
                .inc_by((total_msgs_bytes - fb_msgs_bytes_total - serving_msgs_bytes_total) as u64);
            pipeline_metrics()
                .forwarder
                .write_processing_time
                .get_or_create(&labels)
                .observe(write_start_time.elapsed().as_micros() as f64);
            if failed {
                pipeline_metrics()
                    .forwarder
                    .write_error_total
                    .get_or_create(&labels)
                    .inc_by(1);
            }
        }

        if failed {
            return Err(Error::Sink(
                "Failed to write messages after retries".to_string(),
            ));
        }

        Ok(())
    }

    // writes the serving messages to the serving store
    async fn handle_serving_messages(&mut self, serving_msgs: Vec<Message>) -> Result<()> {
        let Some(serving_store) = &mut self.serving_store else {
            return Err(Error::Sink(
                "Response contains serving messages but no serving store is configured. \
                Please consider updating spec to configure serving store."
                    .to_string(),
            ));
        };

        // convert Message to StoreEntry
        let mut payloads = Vec::with_capacity(serving_msgs.len());
        for msg in serving_msgs {
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

        Ok(())
    }

    // Check if the Sink is ready to accept messages.
    pub(crate) async fn ready(&mut self) -> bool {
        self.health_check_clients.ready().await
    }

    fn send_fb_sink_metrics(
        fb_msgs_total: usize,
        fb_msgs_bytes_total: usize,
        fallback_sink_start: time::Instant,
    ) {
        if is_mono_vertex() {
            monovertex_metrics()
                .fb_sink
                .write_total
                .get_or_create(mvtx_forward_metric_labels())
                .inc_by(fb_msgs_total as u64);
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
                .inc_by(fb_msgs_total as u64);
            pipeline_metrics()
                .sink_forwarder
                .fbsink_write_bytes_total
                .get_or_create(&labels)
                .inc_by(fb_msgs_bytes_total as u64);

            pipeline_metrics()
                .sink_forwarder
                .fbsink_write_processing_time
                .get_or_create(&labels)
                .observe(fallback_sink_start.elapsed().as_micros() as f64);
        }
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
    Serve,
}

/// Sink will give a response per [Message].
#[derive(Debug, PartialEq)]
pub(crate) struct ResponseFromSink {
    /// Unique id per [Message]. We need to track per [Message] status.
    pub(crate) id: String,
    /// Status of the "sink" operation per [Message].
    pub(crate) status: ResponseStatusFromSink,
    pub(crate) serve_response: Option<Vec<u8>>,
}

impl From<sink_response::Result> for ResponseFromSink {
    fn from(value: sink_response::Result) -> Self {
        let status = match value.status() {
            Success => ResponseStatusFromSink::Success,
            Failure => ResponseStatusFromSink::Failed(value.err_msg),
            Fallback => ResponseStatusFromSink::Fallback,
            Serve => ResponseStatusFromSink::Serve,
        };
        Self {
            id: value.id,
            status,
            serve_response: value.serve_response,
        }
    }
}

impl Drop for SinkWriter {
    fn drop(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::pipeline::NatsStoreConfig;
    use crate::message::{AckHandle, IntOffset, Message, MessageID, Offset, ReadAck};
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
                } else {
                    responses.push(sink::Response::ok(datum.id));
                }
            }
            responses
        }
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

        let result = sink_writer.write(messages.clone(), cln_token).await;
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
        let messages: Vec<Message> = (0..10)
            .map(|i| {
                let (ack_tx, ack_rx) = oneshot::channel();
                ack_rxs.push(ack_rx);
                Message {
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
                }
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
            SinkClientType::UserDefined(SinkClient::new(
                create_rpc_channel(sock_file).await.unwrap(),
            )),
        )
        .build()
        .await
        .unwrap();

        let mut ack_rxs = vec![];
        let messages: Vec<Message> = (0..10)
            .map(|i| {
                let (ack_tx, ack_rx) = oneshot::channel();
                ack_rxs.push(ack_rx);
                Message {
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
                    ack_handle: Some(Arc::new(AckHandle::new(ack_tx))),
                    ..Default::default()
                }
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
            assert_eq!(ack_rx.await.unwrap(), ReadAck::Nak);
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
            SinkClientType::UserDefined(SinkClient::new(
                create_rpc_channel(sock_file).await.unwrap(),
            )),
        )
        .fb_sink_client(SinkClientType::Log)
        .build()
        .await
        .unwrap();

        let mut ack_rxs = vec![];
        let messages: Vec<Message> = (0..20)
            .map(|i| {
                let (ack_tx, ack_rx) = oneshot::channel();
                ack_rxs.push(ack_rx);
                Message {
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
                    ack_handle: Some(Arc::new(AckHandle::new(ack_tx))),
                    ..Default::default()
                }
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
            SinkClientType::UserDefined(SinkClient::new(
                create_rpc_channel(sock_file).await.unwrap(),
            )),
        )
        .serving_store(serving_store.clone())
        .build()
        .await
        .unwrap();

        let mut ack_rxs = vec![];
        let messages: Vec<Message> = (0..10)
            .map(|i| {
                let mut headers = HashMap::new();
                headers.insert(DEFAULT_ID_HEADER.to_string(), format!("id_{}", i));
                headers.insert(DEFAULT_POD_HASH_KEY.to_string(), "abcd".to_string());
                let (ack_tx, ack_rx) = oneshot::channel();
                ack_rxs.push(ack_rx);
                Message {
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
                    ack_handle: Some(Arc::new(AckHandle::new(ack_tx))),
                    ..Default::default()
                }
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
}
