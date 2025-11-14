use crate::Result;
use crate::config::pipeline::VERTEX_TYPE_SINK;
use crate::config::{get_vertex_name, is_mono_vertex};
use crate::error::Error;
use crate::message::Message;
use crate::metrics::{
    PIPELINE_PARTITION_NAME_LABEL, monovertex_metrics, mvtx_forward_metric_labels,
    pipeline_drop_metric_labels, pipeline_metric_labels, pipeline_metrics,
};
use crate::sinker::actor::{SinkActorMessage, SinkActorResponse};
use numaflow_kafka::sink::KafkaSink;
use numaflow_pb::clients::sink::Status::{Failure, Fallback, OnSuccess, Serve, Success};
use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::sink::sink_response;
use numaflow_pulsar::sink::Sink as PulsarSink;
use numaflow_sqs::sink::SqsSink;
use serving::{DEFAULT_ID_HEADER, DEFAULT_POD_HASH_KEY};
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

use crate::sinker::builder::HealthCheckClients;
use serve::{ServingStore, StoreEntry};

// Re-export SinkWriterBuilder for external use
pub(crate) use crate::sinker::builder::SinkWriterBuilder;

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
    UserDefined(SinkClient<Channel>),
    Sqs(SqsSink),
    Kafka(KafkaSink),
    Pulsar(Box<PulsarSink>),
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
}

impl SinkWriter {
    /// Create a new SinkWriter instance.
    pub(super) fn new(
        batch_size: usize,
        chunk_timeout: Duration,
        sink_handle: mpsc::Sender<SinkActorMessage>,
        fb_sink_handle: Option<mpsc::Sender<SinkActorMessage>>,
        on_success_sink_handle: Option<mpsc::Sender<SinkActorMessage>>,
        serving_store: Option<ServingStore>,
        health_check_clients: HealthCheckClients,
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
        rx.await.expect("Error receiving response from sink actor")
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
        rx.await.unwrap()
    }

    /// Sink the messages to the OnSuccess Sink.
    async fn write_to_on_success_sink(
        &self,
        messages: Vec<Message>,
        cancel: CancellationToken,
    ) -> Result<SinkActorResponse> {
        if self.on_success_sink_handle.is_none() {
            // TODO update link
            return Err(Error::OsSink(
                "Response contains OnSuccess messages but no OnSuccess sink is configured. \
                Please update the spec to configure on-success sink https://numaflow.numaproj.io/user-guide/sinks/onsuccess/ ".to_string(),
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
        rx.await.unwrap()
    }

    /// Streaming write the messages to the Sink, it will keep writing messages until the stream is
    /// closed or the cancellation token is triggered.
    pub(crate) async fn streaming_write(
        mut self,
        messages_stream: ReceiverStream<Message>,
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
                while let Some(batch) = chunk_stream.next().await {
                    // we are in shutting down mode, we will not be writing to the sink,
                    // mark the messages as failed, and on Drop they will be nack'ed.
                    if self.shutting_down_on_err {
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
                        self.shutting_down_on_err = true;
                    }
                }

                // finalize
                self.final_result
            }
        }))
    }

    /// Write the messages to the Sink.
    /// Invokes the primary sink actor, handles fallback messages, serving messages, and errors.
    async fn write(&mut self, messages: Vec<Message>, cln_token: CancellationToken) -> Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let write_start_time = time::Instant::now();
        let messages_count = messages.len();
        let messages_size: usize = messages.iter().map(|msg| msg.value.len()).sum();

        // Invoke primary sink to write messages
        let response = self
            .write_to_primary_sink(messages, cln_token.clone())
            .await?;

        if !response.failed.is_empty() {
            error!(
                "Failed to write messages after retries: {:?}",
                response.failed
            );
            Self::send_error_metrics();
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
            self.write_to_fallback(response.fallback, cln_token.clone())
                .await?;
        }

        // If there are on_success messages, write them to the on_success sink
        if !response.on_success.is_empty() {
            self.write_to_on_success(response.on_success, cln_token.clone())
                .await?;
        }

        // If there are serving messages, write them to the serving store
        if !response.serving.is_empty() {
            self.write_to_serving_store(response.serving).await?;
        }

        Self::send_metrics(
            messages_count,
            messages_size,
            fallback_messages_count,
            fallback_messages_size,
            dropped_messages_count,
            dropped_messages_size,
            write_start_time,
        );

        Ok(())
    }

    /// Write messages to the OnSuccess Sink.
    async fn write_to_on_success(
        &mut self,
        messages: Vec<Message>,
        cln_token: CancellationToken,
    ) -> Result<()> {
        // Invoke on_success sink actor (with retry logic inside)
        let on_success_response = self
            .write_to_on_success_sink(messages, cln_token.clone())
            .await?;

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

        Ok(())
    }

    /// Write the messages to the Fallback Sink.
    async fn write_to_fallback(
        &mut self,
        messages: Vec<Message>,
        cln_token: CancellationToken,
    ) -> Result<()> {
        let fallback_sink_start = time::Instant::now();
        let messages_count = messages.len();
        let messages_size: usize = messages.iter().map(|msg| msg.value.len()).sum();

        // Invoke fallback sink actor (with retry logic inside)
        let fb_response = self.write_to_fb_sink(messages, cln_token.clone()).await?;

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

        Ok(())
    }

    /// Writes the serving messages to the serving store
    async fn write_to_serving_store(&mut self, messages: Vec<Message>) -> Result<()> {
        let Some(serving_store) = &mut self.serving_store else {
            return Err(Error::Sink(
                "Response contains serving messages but no serving store is configured. \
                Please consider updating spec to configure serving store."
                    .to_string(),
            ));
        };

        // convert Message to StoreEntry
        let mut payloads = Vec::with_capacity(messages.len());
        for msg in messages {
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

    /// Send metrics for errors
    fn send_error_metrics() {
        if is_mono_vertex() {
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
            SinkClientType::UserDefined(SinkClient::new(
                create_rpc_channel(sock_file).await.unwrap(),
            )),
        )
        .on_success_sink_client(SinkClientType::Log)
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
                on_success_msg: None,
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
