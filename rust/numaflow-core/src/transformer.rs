use bytes::Bytes;
use futures::stream::{self, StreamExt};
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic::{Code, Status};
use tracing::error;

use crate::config::pipeline::VERTEX_TYPE_SOURCE;
use crate::config::{get_vertex_name, is_mono_vertex};
use crate::error::Error;
use crate::message::{Message, MessageHandle, Offset};
use crate::metrics::{
    PIPELINE_PARTITION_NAME_LABEL, monovertex_metrics, mvtx_forward_metric_labels,
    pipeline_metric_labels, pipeline_metrics,
};
use crate::shared::otel;
use crate::tracker::Tracker;
use crate::transformer::user_defined::{ReconnectConfig, UserDefinedTransformer};
use crate::{Result, mark_success};

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
}

impl Transformer {
    pub(crate) async fn new(
        batch_size: usize,
        concurrency: usize,
        graceful_timeout: Duration,
        client: SourceTransformClient<Channel>,
        tracker: Tracker,
        reconnect_config: ReconnectConfig,
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
            message: read_msg,
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
        let mut labels = pipeline_metric_labels(VERTEX_TYPE_SOURCE).clone();
        labels.push((
            PIPELINE_PARTITION_NAME_LABEL.to_string(),
            get_vertex_name().to_string(),
        ));

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

        let transform_futs = msg_handles.into_iter().map(|msg_handle| {
            let transform_handle = transform_handle.clone();
            let tracker = tracker.clone();
            let hard_shutdown_token = hard_shutdown_token.clone();
            let read_msg = msg_handle.message().clone();
            let source_transform_parent = dispatch_parent_contexts
                .and_then(|parent_contexts| parent_contexts.get(&read_msg.offset).cloned());

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
                        Ok(messages) => break messages,
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
        let dropped_messages_count = transformed_handles
            .iter()
            .filter(|h| h.message().dropped())
            .count();
        let elapsed_time = batch_start_time.elapsed().as_micros() as f64;
        let write_messages_count = transformed_handles.len() - dropped_messages_count;
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
    use chrono::Utc;
    use numaflow::shared::ServerExtras;
    use numaflow::sourcetransform;
    use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::oneshot;

    use super::*;
    use crate::message::StringOffset;
    use crate::message::{Message, MessageHandle, MessageID, Offset};
    use crate::shared::grpc::create_rpc_channel;

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
}
