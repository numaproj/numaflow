use crate::config::components::sink::RetryConfig;
use crate::config::pipeline::VERTEX_TYPE_SOURCE;
use crate::config::{get_vertex_name, is_mono_vertex};
use crate::error::Error;
use crate::message::{Message, MessageHandle, Offset};
use crate::metrics::{
    monovertex_metrics, mvtx_forward_metric_labels, pipeline_metrics,
    pipeline_partition_metric_labels,
};
use crate::shared::otel;
use crate::shared::retry::{RetryController, RetryStep};
use crate::tracker::Tracker;
use crate::transformer::user_defined::{ReconnectConfig, UserDefinedTransformer};
use crate::{Result, mark_success};
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic::{Code, Status};
use tracing::{error, warn};

/// User-Defined Transformer is a custom transformer that can be built by the user.
///
/// [User-Defined Transformer]: https://numaflow.numaproj.io/user-guide/sources/transformer/overview/#build-your-own-transformer
pub(crate) mod user_defined;

/// Test utilities for transformer.
#[cfg(test)]
pub(crate) mod test_utils;

/// TransformerActorMessage is the message that is sent to the transformer actor.
struct TransformerActorMessage {
    message: Message,
    respond_to: oneshot::Sender<Result<Vec<Message>>>,
}

/// TransformerActor, handles the transformation of messages.
struct TransformerActor {
    receiver: mpsc::Receiver<TransformerActorMessage>,
    transformer: UserDefinedTransformer,
}

impl TransformerActor {
    fn new(
        receiver: mpsc::Receiver<TransformerActorMessage>,
        transformer: UserDefinedTransformer,
    ) -> Self {
        Self {
            receiver,
            transformer,
        }
    }

    /// Handles the incoming message, unlike standard actor pattern the downstream call is not blocking
    /// and the response is sent back to the caller using oneshot in this actor, this is because the
    /// downstream can handle multiple messages at once.
    async fn handle_message(&mut self, msg: TransformerActorMessage) {
        self.transformer
            .transform(msg.message, msg.respond_to)
            .await;
    }

    async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }
}

/// Transformer, transforms messages in a streaming fashion.
#[derive(Clone)]
pub(crate) struct Transformer {
    sender: mpsc::Sender<TransformerActorMessage>,
    concurrency: usize,
    graceful_shutdown_time: Duration,
    tracker: Tracker,
    health_checker: Option<SourceTransformClient<Channel>>,
    retry_config: Option<RetryConfig>,
}

impl Transformer {
    pub(crate) async fn new(
        batch_size: usize,
        concurrency: usize,
        graceful_timeout: Duration,
        client: SourceTransformClient<Channel>,
        tracker: Tracker,
        reconnect_config: ReconnectConfig,
        retry_config: Option<RetryConfig>,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(batch_size);
        let transformer_actor = TransformerActor::new(
            receiver,
            UserDefinedTransformer::new(batch_size, client.clone(), reconnect_config).await?,
        );

        tokio::spawn(async move {
            transformer_actor.run().await;
        });

        Ok(Self {
            concurrency,
            graceful_shutdown_time: graceful_timeout,
            sender,
            tracker,
            health_checker: Some(client),
            retry_config,
        })
    }

    /// Applies the transformation on the message and sends it to the next stage, it blocks if the
    /// concurrency limit is reached.
    async fn transform(
        transform_handle: mpsc::Sender<TransformerActorMessage>,
        read_msg: Message,
        hard_shutdown_token: CancellationToken,
    ) -> Result<Vec<Message>> {
        let (sender, receiver) = oneshot::channel();
        let msg = TransformerActorMessage {
            message: read_msg.clone(),
            respond_to: sender,
        };

        // invoke transformer
        transform_handle
            .send(msg)
            .await
            .map_err(|e| Error::Transformer(format!("failed to send message to server: {e}")))?;

        // wait for the response
        let response = tokio::select! {
            _ = hard_shutdown_token.cancelled() => {
                return Err(Error::Transformer("Operation cancelled".to_string()));
            }
            response = receiver => {
                response.map_err(|e| Error::Transformer(format!("failed to receive response from server: {e}")))??
            }
        };

        if response.is_empty() {
            error!("received empty response from server (transformer), gracefully exiting");
            critical_error!(VERTEX_TYPE_SOURCE, "eot_received_from_transformer");
            return Err(Error::Grpc(Box::new(Status::with_details(
                Code::Internal,
                "UDF_PARTIAL_RESPONSE(transformer)",
                Bytes::from_static(
                    b"received End-Of-Transmission (EOT) before all responses are received from the transformer. \
                    This indicates that there is a bug in the user-code. Please check whether you are accidentally \
                    skipping the messages.",
                ),
            ))));
        }

        Ok(response)
    }

    /// Transforms a batch of messages concurrently.
    /// Accepts MessageHandles so that ack tracking flows through to the transformed outputs —
    /// each output message shares the ack handle of its parent input (flatmap is handled correctly).
    pub(crate) async fn transform_batch(
        &self,
        msg_handles: Vec<MessageHandle>,
        cln_token: CancellationToken,
        dispatch_parent_contexts: Option<&HashMap<Offset, opentelemetry::Context>>,
    ) -> Result<Vec<MessageHandle>> {
        let batch_start_time = tokio::time::Instant::now();
        let transform_handle = self.sender.clone();
        let tracker = self.tracker.clone();
        let labels = pipeline_partition_metric_labels(VERTEX_TYPE_SOURCE, get_vertex_name());

        // create a new cancellation token for the transformer component, this token is used for hard
        // shutdown, the parent token is used for graceful shutdown.
        let hard_shutdown_token = CancellationToken::new();
        // the one that calls shutdown
        let hard_shutdown_token_owner = hard_shutdown_token.clone();
        let graceful_timeout = self.graceful_shutdown_time;

        // clone the token before moving it into the async closure
        let cln_token_for_shutdown = cln_token.clone();

        // spawn a task to cancel the token after graceful timeout when the main token is cancelled
        let shutdown_handle = tokio::spawn(async move {
            // initiate graceful shutdown
            cln_token_for_shutdown.cancelled().await;
            // wait for graceful timeout
            tokio::time::sleep(graceful_timeout).await;
            // cancel the token to hard shutdown
            hard_shutdown_token_owner.cancel();
        });

        // increment read message count for pipeline
        if !is_mono_vertex() {
            pipeline_metrics()
                .source_forwarder
                .transformer_read_total
                .get_or_create(&labels)
                .inc_by(msg_handles.len() as u64);
        }

        let message_count = msg_handles.len();
        let dropped_message_count = Arc::new(AtomicUsize::new(0));
        // Template controller; cloned per message (and per redrive) so each gets a fresh backoff.
        let retry_controller = RetryController::new(&self.retry_config);

        let transform_futs = msg_handles.into_iter().map(|msg_handle| {
            let transform_handle = transform_handle.clone();
            let tracker = tracker.clone();
            let hard_shutdown_token = hard_shutdown_token.clone();
            let read_msg = msg_handle.message().clone();
            let source_transform_parent = dispatch_parent_contexts
                .and_then(|parent_contexts| parent_contexts.get(&read_msg.offset).cloned());
            let mut retry_controller = retry_controller.clone();
            let drop_count_cln = Arc::clone(&dropped_message_count);

            async move {
                let offset = read_msg.offset.clone();
                let source_transform_span = otel::SourceTransformSpan::new(
                    source_transform_parent,
                    offset.to_string(),
                    otel::TraceTopology::current(),
                );
                let transformed_messages = loop {
                    match Transformer::transform(
                        transform_handle.clone(),
                        read_msg.clone(),
                        hard_shutdown_token.clone(),
                    )
                    .await
                    {
                        Ok(messages) => {
                            if messages.iter().any(|msg| msg.failed()) {
                                match retry_controller.next_step(&hard_shutdown_token).await {
                                    // Retry the transformer: after the backoff wait, or immediately when no retry config
                                    // is set (retry forever, no backoff), similar to the sink.
                                    RetryStep::Again => {}
                                    // On cancellation, give up so the message is nacked.
                                    RetryStep::Cancelled => {
                                        return Err(Error::Transformer(
                                            "Operation cancelled".to_string(),
                                        ));
                                    }
                                    // Retries exhausted under `onFailure: drop` — drop the message.
                                    RetryStep::Drop => {
                                        warn!("Retries exhausted, dropping message.");
                                        drop_count_cln.fetch_add(1, Ordering::Relaxed);
                                        break vec![];
                                    }
                                    // Retries exhausted under `onFailure: retry` — give up so the message is nacked.
                                    RetryStep::Nack => {
                                        return Err(Error::Transformer(
                                            "Retries exhausted".to_string(),
                                        ));
                                    }
                                }
                            } else {
                                break messages;
                            }
                        }
                        Err(Error::UdfRedrive(e)) => {
                            error!(?e, ?offset, "transformer stream redrive requested");
                        }
                        Err(e) => return Err(e),
                    }
                };
                source_transform_span.record_output_count(transformed_messages.len());

                // update the tracker with the number of responses for each message
                tracker
                    .serving_update(
                        &offset,
                        transformed_messages
                            .iter()
                            .map(|m| m.tags.clone())
                            .collect(),
                    )
                    .await?;

                // Fan out: each transformed message shares the parent's ack handle.
                // mark_success on the parent decrements its ref_count contribution.
                let output: Vec<MessageHandle> = transformed_messages
                    .into_iter()
                    .map(|m| msg_handle.with_message(m))
                    .collect();

                mark_success!(msg_handle);
                Ok::<Vec<MessageHandle>, Error>(output)
            }
        });

        // Use buffered to limit concurrency without spawning tasks.
        // This polls up to `concurrency` futures at a time, reducing scheduling overhead.
        let mut stream = stream::iter(transform_futs).buffered(self.concurrency);

        let mut transformed_handles = Vec::with_capacity(message_count * 2);

        while let Some(result) = stream.next().await {
            match result {
                Ok(mut handles) => transformed_handles.append(&mut handles),
                Err(e) => {
                    // increment transform error metric for pipeline
                    // error here indicates that there was some problem in transformation
                    if !is_mono_vertex() {
                        pipeline_metrics()
                            .source_forwarder
                            .transformer_error_total
                            .get_or_create(&labels)
                            .inc();
                    }
                    // Early exit - remaining futures are dropped when stream goes out of scope
                    return Err(e);
                }
            }
        }

        // batch transformation was successful
        // send transformer metrics
        //
        // `retry_dropped_count` are messages dropped after exhausting retries under
        // `OnFailureStrategy::Drop`. They never produced an output handle, so they are NOT part of
        // `transformed_handles` and must be tracked separately from the tag-based drops that ARE
        // present in `transformed_handles`.
        let retry_dropped_count = dropped_message_count.load(Ordering::Relaxed);
        let tag_dropped_count = transformed_handles
            .iter()
            .filter(|h| h.message().dropped())
            .count();
        let nacked_message_count = transformed_handles
            .iter()
            .filter(|h| h.message().nacked())
            .count();
        let dropped_messages_count = tag_dropped_count + retry_dropped_count;
        let elapsed_time = batch_start_time.elapsed().as_micros() as f64;
        // Only the tag-dropped and nacked handles are present in `transformed_handles`; subtract
        // just those (retry-drops are not in the vec) and saturate to avoid any underflow.
        let write_messages_count = transformed_handles
            .len()
            .saturating_sub(tag_dropped_count + nacked_message_count);
        // TODO: emit nacked message metrics
        Self::send_transformer_metrics(
            dropped_messages_count,
            elapsed_time,
            write_messages_count,
            &labels,
        );

        // cleanup the shutdown handle
        shutdown_handle.abort();
        Ok(transformed_handles)
    }

    fn send_transformer_metrics(
        dropped_messages_count: usize,
        elapsed_time: f64,
        write_messages_count: usize,
        labels: &Vec<(String, String)>,
    ) {
        if is_mono_vertex() {
            monovertex_metrics()
                .transformer
                .time
                .get_or_create(mvtx_forward_metric_labels())
                .observe(elapsed_time);
            monovertex_metrics()
                .transformer
                .dropped_total
                .get_or_create(mvtx_forward_metric_labels())
                .inc_by(dropped_messages_count as u64);
        } else {
            pipeline_metrics()
                .source_forwarder
                .transformer_processing_time
                .get_or_create(labels)
                .observe(elapsed_time);
            pipeline_metrics()
                .source_forwarder
                .transformer_drop_total
                .get_or_create(labels)
                .inc_by(dropped_messages_count as u64);
            pipeline_metrics()
                .source_forwarder
                .transformer_write_total
                .get_or_create(labels)
                .inc_by(write_messages_count as u64);
        }
    }

    pub(crate) async fn ready(&mut self) -> bool {
        if let Some(client) = &mut self.health_checker {
            let request = tonic::Request::new(());
            match client.is_ready(request).await {
                Ok(response) => response.into_inner().ready,
                Err(e) => {
                    error!("Transformer is not ready: {:?}", e);
                    false
                }
            }
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::components::sink::OnFailureStrategy;
    use crate::message::StringOffset;
    use crate::message::{Message, MessageHandle, MessageID, Offset, ReadAck};
    use crate::shared::grpc::create_rpc_channel;
    use chrono::Utc;
    use numaflow::shared::ServerExtras;
    use numaflow::sourcetransform;
    use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::oneshot;

    const TEST_GRPC_MAX_MESSAGE_SIZE: usize =
        crate::config::components::transformer::DEFAULT_GRPC_MAX_MESSAGE_SIZE;

    struct SimpleTransformer;

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for SimpleTransformer {
        async fn transform(
            &self,
            input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            let message =
                sourcetransform::Message::new(input.value, Utc::now()).with_keys(input.keys);
            vec![message]
        }
    }

    #[test]
    fn source_transform_span_without_parent_is_inert() {
        let span = otel::SourceTransformSpan::new(
            None,
            "msg-1".to_string(),
            otel::TraceTopology::MonoVertex,
        );
        assert!(!span.is_active());
        span.record_output_count(1);
    }

    #[tokio::test]
    async fn transformer_operations() -> Result<()> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sourcetransform.sock");
        let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            sourcetransform::Server::new(SimpleTransformer)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        let tracker = Tracker::new(None, CancellationToken::new());

        let client = SourceTransformClient::new(create_rpc_channel(sock_file.clone()).await?);
        let transformer = Transformer::new(
            500,
            10,
            Duration::from_secs(10),
            client,
            tracker.clone(),
            ReconnectConfig::new(
                crate::shared::grpc::GrpcClientConfig::new(
                    sock_file.clone(),
                    server_info_file.clone(),
                    TEST_GRPC_MAX_MESSAGE_SIZE,
                ),
                CancellationToken::new(),
                crate::shared::grpc::DEFAULT_RECONNECT_INTERVAL,
            ),
            None,
        )
        .await?;

        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["first".into()]),
            tags: None,
            value: "hello".into(),
            offset: Offset::String(StringOffset::new("0".to_string(), 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        let transformed_messages = Transformer::transform(
            transformer.sender.clone(),
            message,
            CancellationToken::new(),
        )
        .await;

        assert!(transformed_messages.is_ok());
        let transformed_messages = transformed_messages?;
        assert_eq!(transformed_messages.len(), 1);
        assert_eq!(
            transformed_messages
                .first()
                .expect("Expected first message")
                .value,
            "hello"
        );

        // we need to drop the transformer, because if there are any in-flight requests
        // server fails to shut down. https://github.com/numaproj/numaflow-rs/issues/85
        drop(transformer);

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            handle.is_finished(),
            "Expected gRPC server to have shut down"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_transform_stream() -> Result<()> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sourcetransform.sock");
        let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            sourcetransform::Server::new(SimpleTransformer)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let tracker = Tracker::new(None, CancellationToken::new());
        let client = SourceTransformClient::new(create_rpc_channel(sock_file.clone()).await?);
        let transformer = Transformer::new(
            500,
            10,
            Duration::from_secs(10),
            client,
            tracker.clone(),
            ReconnectConfig::new(
                crate::shared::grpc::GrpcClientConfig::new(
                    sock_file.clone(),
                    server_info_file.clone(),
                    TEST_GRPC_MAX_MESSAGE_SIZE,
                ),
                CancellationToken::new(),
                crate::shared::grpc::DEFAULT_RECONNECT_INTERVAL,
            ),
            None,
        )
        .await?;

        let mut messages = vec![];
        for i in 0..5 {
            let message = Message {
                typ: Default::default(),
                keys: Arc::from(vec![format!("key_{}", i)]),
                tags: None,
                value: format!("value_{}", i).into(),
                offset: Offset::String(StringOffset::new(i.to_string(), 0)),
                event_time: chrono::Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex_name".to_string().into(),
                    offset: i.to_string().into(),
                    index: i,
                },
                ..Default::default()
            };
            let (ack_tx, _ack_rx) = tokio::sync::oneshot::channel();
            messages.push(MessageHandle::new(message, ack_tx));
        }

        let transformed_messages = transformer
            .transform_batch(messages, CancellationToken::new(), None)
            .await?;

        for (i, transformed_message) in transformed_messages.iter().enumerate() {
            assert_eq!(transformed_message.message().value, format!("value_{}", i));
        }

        // we need to drop the transformer, because if there are any in-flight requests
        // server fails to shut down. https://github.com/numaproj/numaflow-rs/issues/85
        drop(transformer);

        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            handle.is_finished(),
            "Expected gRPC server to have shut down"
        );
        Ok(())
    }

    struct SimpleTransformerPanic;

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for SimpleTransformerPanic {
        async fn transform(
            &self,
            _input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            panic!("SimpleTransformerPanic panicked!");
        }
    }

    #[cfg(feature = "global-state-tests")]
    #[tokio::test]
    async fn test_transform_stream_with_panic() -> Result<()> {
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sourcetransform.sock");
        let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();
        let handle = tokio::spawn(async move {
            sourcetransform::Server::new(SimpleTransformerPanic)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start()
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let cln_token = CancellationToken::new();
        let tracker = Tracker::new(None, cln_token.clone());
        let client = SourceTransformClient::new(create_rpc_channel(sock_file.clone()).await?);
        let transformer = Transformer::new(
            500,
            10,
            Duration::from_millis(10),
            client,
            tracker.clone(),
            ReconnectConfig::new(
                crate::shared::grpc::GrpcClientConfig::new(
                    sock_file.clone(),
                    server_info_file.clone(),
                    TEST_GRPC_MAX_MESSAGE_SIZE,
                ),
                cln_token.clone(),
                crate::shared::grpc::DEFAULT_RECONNECT_INTERVAL,
            ),
            None,
        )
        .await?;

        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["first".into()]),
            tags: None,
            value: "hello".into(),
            offset: Offset::String(StringOffset::new("0".to_string(), 0)),
            event_time: chrono::Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            ..Default::default()
        };

        let (ack_tx, _ack_rx) = tokio::sync::oneshot::channel();
        cln_token.cancel();
        let result = transformer
            .transform_batch(vec![MessageHandle::new(message, ack_tx)], cln_token, None)
            .await;
        assert!(
            matches!(&result, Err(Error::Transformer(e)) if e == "Operation cancelled"),
            "Expected cancellation to stop redriving the panicking transformer, got {result:?}"
        );

        // we need to drop the transformer, because if there are any in-flight requests
        // server fails to shut down. https://github.com/numaproj/numaflow-rs/issues/85
        drop(transformer);

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            handle.is_finished(),
            "Expected gRPC server to have shut down"
        );
        Ok(())
    }

    // ---- retryStrategy tests ----

    /// Source transformer that fails (via the reserved FAIL tag) the first `fail_first_n`
    /// invocations, then succeeds. Exercises the "retry then succeed" path.
    struct RetryThenSucceedTransformer {
        attempts: AtomicUsize,
        fail_first_n: usize,
    }

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for RetryThenSucceedTransformer {
        async fn transform(
            &self,
            input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            let tags = if attempt < self.fail_first_n {
                vec!["U+005C__FAIL__".to_string()] // must match message.rs FAIL const
            } else {
                vec![]
            };
            vec![
                sourcetransform::Message::new(input.value, Utc::now())
                    .with_keys(input.keys)
                    .with_tags(tags),
            ]
        }
    }

    /// Source transformer that always fails (via the reserved FAIL tag).
    struct AlwaysFailTransformer;

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for AlwaysFailTransformer {
        async fn transform(
            &self,
            input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            vec![
                sourcetransform::Message::new(input.value, Utc::now())
                    .with_keys(input.keys)
                    .with_tags(vec!["U+005C__FAIL__".to_string()]),
            ]
        }
    }

    fn fast_retry_config(strategy: OnFailureStrategy, max_attempts: u16) -> RetryConfig {
        RetryConfig {
            sink_max_retry_attempts: max_attempts,
            sink_initial_retry_interval_in_ms: 1,
            sink_retry_factor: 1.0,
            sink_retry_jitter: 0.0,
            sink_max_retry_interval_in_ms: 5,
            sink_retry_on_fail_strategy: strategy,
        }
    }

    fn retry_test_message() -> Message {
        Message {
            typ: Default::default(),
            keys: Arc::from(vec!["k".to_string()]),
            tags: None,
            value: "hello".into(),
            offset: Offset::String(StringOffset::new("0".to_string(), 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "vertex_name".to_string().into(),
                offset: "0".to_string().into(),
                index: 0,
            },
            ..Default::default()
        }
    }

    /// Spins up a source-transform server backed by `svc` and builds a [Transformer] wired to it
    /// with the given retry config. The returned `TempDir` must be kept alive for the socket to
    /// remain valid for the duration of the test.
    async fn setup_transformer_with_retry<T>(
        svc: T,
        retry_config: Option<RetryConfig>,
    ) -> (
        Transformer,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
        TempDir,
    )
    where
        T: sourcetransform::SourceTransformer + Send + Sync + 'static,
    {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sourcetransform.sock");
        let server_info_file = tmp_dir.path().join("sourcetransformer-server-info");

        let (ss, si) = (sock_file.clone(), server_info_file.clone());
        let handle = tokio::spawn(async move {
            sourcetransform::Server::new(svc)
                .with_socket_file(ss)
                .with_server_info_file(si)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let tracker = Tracker::new(None, CancellationToken::new());
        let client = SourceTransformClient::new(
            create_rpc_channel(sock_file.clone())
                .await
                .expect("failed to create rpc channel"),
        );
        let transformer = Transformer::new(
            500,
            10,
            Duration::from_secs(10),
            client,
            tracker,
            ReconnectConfig::new(
                crate::shared::grpc::GrpcClientConfig::new(
                    sock_file.clone(),
                    server_info_file.clone(),
                    TEST_GRPC_MAX_MESSAGE_SIZE,
                ),
                CancellationToken::new(),
                crate::shared::grpc::DEFAULT_RECONNECT_INTERVAL,
            ),
            retry_config,
        )
        .await
        .expect("failed to create transformer");

        (transformer, shutdown_tx, handle, tmp_dir)
    }

    #[tokio::test]
    async fn transformer_retries_then_succeeds() -> Result<()> {
        let (transformer, shutdown_tx, handle, _tmp_dir) = setup_transformer_with_retry(
            RetryThenSucceedTransformer {
                attempts: AtomicUsize::new(0),
                fail_first_n: 2,
            },
            Some(fast_retry_config(OnFailureStrategy::Retry, 10)),
        )
        .await;

        let (ack_tx, ack_rx) = oneshot::channel();
        let msg_handle = MessageHandle::new(retry_test_message(), ack_tx);
        let out = transformer
            .transform_batch(vec![msg_handle], CancellationToken::new(), None)
            .await?;

        assert_eq!(out.len(), 1);
        let mapped = out.into_iter().next().expect("expected one output");
        assert!(!mapped.message().failed());
        assert_eq!(mapped.message().value, "hello");
        // Simulate the downstream ISB writer marking the forwarded copy success so the shared
        // ack ref-count reaches 0 and the input can be ACK'd.
        mapped.mark_success();
        assert_eq!(ack_rx.await.expect("ack channel closed"), ReadAck::Ack);

        drop(transformer);
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(handle.is_finished(), "expected gRPC server to shut down");
        Ok(())
    }

    #[tokio::test]
    async fn transformer_retries_without_config_until_success() -> Result<()> {
        // With no retry config, a FAIL-tagged response must still be retried (indefinitely,
        // with no backoff) rather than passed through as-is.
        let (transformer, shutdown_tx, handle, _tmp_dir) = setup_transformer_with_retry(
            RetryThenSucceedTransformer {
                attempts: AtomicUsize::new(0),
                fail_first_n: 3,
            },
            None,
        )
        .await;

        let (ack_tx, ack_rx) = oneshot::channel();
        let msg_handle = MessageHandle::new(retry_test_message(), ack_tx);
        let out = transformer
            .transform_batch(vec![msg_handle], CancellationToken::new(), None)
            .await?;

        assert_eq!(out.len(), 1);
        let mapped = out.into_iter().next().expect("expected one output");
        assert!(!mapped.message().failed());
        mapped.mark_success();
        assert_eq!(ack_rx.await.expect("ack channel closed"), ReadAck::Ack);

        drop(transformer);
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(handle.is_finished(), "expected gRPC server to shut down");
        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn transformer_drops_message_on_retries_exhausted() -> Result<()> {
        let (transformer, shutdown_tx, handle, _tmp_dir) = setup_transformer_with_retry(
            AlwaysFailTransformer,
            Some(fast_retry_config(OnFailureStrategy::Drop, 1)),
        )
        .await;

        // Same label set send_transformer_metrics uses for the drop counter.
        let labels = pipeline_partition_metric_labels(VERTEX_TYPE_SOURCE, get_vertex_name());
        let drop_before = pipeline_metrics()
            .source_forwarder
            .transformer_drop_total
            .get_or_create(&labels)
            .get();

        let (ack_tx, ack_rx) = oneshot::channel();
        let msg_handle = MessageHandle::new(retry_test_message(), ack_tx);
        let out = transformer
            .transform_batch(vec![msg_handle], CancellationToken::new(), None)
            .await?;

        assert!(out.is_empty(), "dropped message must not be forwarded");
        assert_eq!(
            ack_rx.await.expect("ack channel closed"),
            ReadAck::Ack,
            "dropped message must be ACK'd"
        );

        let drop_after = pipeline_metrics()
            .source_forwarder
            .transformer_drop_total
            .get_or_create(&labels)
            .get();
        assert_eq!(drop_after, drop_before + 1);

        drop(transformer);
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(handle.is_finished(), "expected gRPC server to shut down");
        Ok(())
    }

    #[tokio::test]
    async fn transformer_retries_exhausted_propagates_error() -> Result<()> {
        // A finite `Retry` budget is only reachable by constructing RetryConfig directly (the
        // spec conversion forces u16::MAX for onFailure=Retry); this exercises the exhaustion arm.
        let (transformer, shutdown_tx, handle, _tmp_dir) = setup_transformer_with_retry(
            AlwaysFailTransformer,
            Some(fast_retry_config(OnFailureStrategy::Retry, 1)),
        )
        .await;

        let (ack_tx, ack_rx) = oneshot::channel();
        let msg_handle = MessageHandle::new(retry_test_message(), ack_tx);
        let result = transformer
            .transform_batch(vec![msg_handle], CancellationToken::new(), None)
            .await;

        assert!(
            matches!(&result, Err(Error::Transformer(e)) if e.contains("Retries exhausted")),
            "expected retries-exhausted error, got {result:?}"
        );
        assert_eq!(
            ack_rx.await.expect("ack channel closed"),
            ReadAck::Nak(None)
        );

        drop(transformer);
        shutdown_tx
            .send(())
            .expect("failed to send shutdown signal");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(handle.is_finished(), "expected gRPC server to shut down");
        Ok(())
    }
}
