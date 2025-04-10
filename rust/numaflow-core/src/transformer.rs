use bytes::Bytes;
use numaflow_monitor::runtime;
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic::{Code, Status};
use tracing::error;

use crate::error::Error;
use crate::message::Message;
use crate::metrics::{monovertex_metrics, mvtx_forward_metric_labels};
use crate::tracker::TrackerHandle;
use crate::transformer::user_defined::UserDefinedTransformer;
use crate::Result;

/// User-Defined Transformer is a custom transformer that can be built by the user.
///
/// [User-Defined Transformer]: https://numaflow.numaproj.io/user-guide/sources/transformer/overview/#build-your-own-transformer
pub(crate) mod user_defined;

pub enum ActorMessage {
    Transform {
        message: Message,
        respond_to: oneshot::Sender<Result<Vec<Message>>>,
    },
    IsReady {
        respond_to: oneshot::Sender<bool>,
    },
}

/// TransformerActor, handles the transformation of messages.
struct TransformerActor {
    receiver: mpsc::Receiver<ActorMessage>,
    transformer: UserDefinedTransformer,
}

impl TransformerActor {
    fn new(receiver: mpsc::Receiver<ActorMessage>, transformer: UserDefinedTransformer) -> Self {
        Self {
            receiver,
            transformer,
        }
    }

    /// Handles the incoming message, unlike standard actor pattern the downstream call is not blocking
    /// and the response is sent back to the caller using oneshot in this actor, this is because the
    /// downstream can handle multiple messages at once.
    async fn handle_message(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::Transform {
                message,
                respond_to,
            } => self.transformer.transform(message, respond_to).await,
            ActorMessage::IsReady { respond_to } => {
                let _ = respond_to.send(self.transformer.ready().await);
            }
        }
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
    sender: mpsc::Sender<ActorMessage>,
    concurrency: usize,
    tracker_handle: TrackerHandle,
}

impl Transformer {
    pub(crate) async fn new(
        batch_size: usize,
        concurrency: usize,
        client: SourceTransformClient<Channel>,
        tracker_handle: TrackerHandle,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(batch_size);
        let transformer_actor = TransformerActor::new(
            receiver,
            UserDefinedTransformer::new(batch_size, client).await?,
        );

        tokio::spawn(async move {
            transformer_actor.run().await;
        });

        Ok(Self {
            concurrency,
            sender,
            tracker_handle,
        })
    }

    /// Applies the transformation on the message and sends it to the next stage, it blocks if the
    /// concurrency limit is reached.
    async fn transform(
        transform_handle: mpsc::Sender<ActorMessage>,
        read_msg: Message,
        cln_token: CancellationToken,
    ) -> Result<Vec<Message>> {
        let start_time = tokio::time::Instant::now();

        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Transform {
            message: read_msg,
            respond_to: sender,
        };

        // invoke transformer
        transform_handle
            .send(msg)
            .await
            .map_err(|e| Error::Transformer(format!("failed to send message: {}", e)))?;

        // wait for the response
        let response = tokio::select! {
            _ = cln_token.cancelled() => {
                return Err(Error::Transformer("cancelled".to_string()));
            }
            response = receiver => {
                response.map_err(|e| Error::Transformer(format!("failed to receive response: {}", e)))??
            }
        };

        if response.is_empty() {
            error!("received empty response from server (transformer), we will wait indefinitely");
            // persist the error for debugging
            runtime::persist_application_error(Status::with_details(
                Code::Internal,
                "UDF_PARTIAL_RESPONSE(transformer)",
                Bytes::from_static(
                    b"received empty response from server (transformer), we will wait indefinitely",
                ),
            ));
            futures::future::pending::<()>().await;
        }

        monovertex_metrics()
            .transformer
            .time
            .get_or_create(mvtx_forward_metric_labels())
            .observe(start_time.elapsed().as_micros() as f64);

        Ok(response)
    }

    /// Transforms a batch of messages concurrently.
    pub(crate) async fn transform_batch(
        &self,
        messages: Vec<Message>,
        cln_token: CancellationToken,
    ) -> Result<Vec<Message>> {
        let transform_handle = self.sender.clone();
        let tracker_handle = self.tracker_handle.clone();
        let semaphore = Arc::new(Semaphore::new(self.concurrency));

        let tasks: Vec<_> = messages
            .into_iter()
            .map(|read_msg| {
                let permit_fut = Arc::clone(&semaphore).acquire_owned();
                let transform_handle = transform_handle.clone();
                let tracker_handle = tracker_handle.clone();
                let cln_token = cln_token.clone();

                tokio::spawn(async move {
                    let permit = permit_fut.await.map_err(|e| {
                        Error::Transformer(format!("failed to acquire semaphore: {}", e))
                    })?;
                    let _permit = permit;

                    let transformed_messages = Transformer::transform(
                        transform_handle,
                        read_msg.clone(),
                        cln_token.clone(),
                    )
                    .await?;

                    // update the tracker with the number of responses for each message
                    tracker_handle
                        .update(
                            read_msg.offset.clone(),
                            transformed_messages
                                .iter()
                                .map(|m| m.tags.clone())
                                .collect(),
                        )
                        .await?;
                    tracker_handle.eof(read_msg.offset.clone()).await?;

                    Ok::<Vec<Message>, Error>(transformed_messages)
                })
            })
            .collect();

        let mut transformed_messages = Vec::new();
        for task in tasks {
            match task.await {
                Ok(Ok(mut msgs)) => transformed_messages.append(&mut msgs),
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(Error::Transformer(format!("task join failed: {}", e))),
            }
        }
        Ok(transformed_messages)
    }

    pub(crate) async fn is_ready(&mut self) -> bool {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::IsReady { respond_to: sender };

        self.sender.send(msg).await.expect("failed to send message");
        receiver.await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use numaflow::sourcetransform;
    use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::oneshot;

    use super::*;
    use crate::message::StringOffset;
    use crate::message::{Message, MessageID, Offset};
    use crate::shared::grpc::create_rpc_channel;

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
        let tracker_handle = TrackerHandle::new(None, None);

        let client = SourceTransformClient::new(create_rpc_channel(sock_file).await?);
        let transformer = Transformer::new(500, 10, client, tracker_handle.clone()).await?;

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
            headers: Default::default(),
            metadata: None,
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
        assert_eq!(transformed_messages[0].value, "hello");

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

        let tracker_handle = TrackerHandle::new(None, None);
        let client = SourceTransformClient::new(create_rpc_channel(sock_file).await?);
        let transformer = Transformer::new(500, 10, client, tracker_handle.clone()).await?;

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
                headers: Default::default(),
                metadata: None,
            };
            messages.push(message);
        }

        let transformed_messages = transformer
            .transform_batch(messages, CancellationToken::new())
            .await?;

        for (i, transformed_message) in transformed_messages.iter().enumerate() {
            assert_eq!(transformed_message.value, format!("value_{}", i));
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

        let tracker_handle = TrackerHandle::new(None, None);
        let client = SourceTransformClient::new(create_rpc_channel(sock_file).await?);
        let transformer = Transformer::new(500, 10, client, tracker_handle.clone()).await?;

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
            headers: Default::default(),
            metadata: None,
        };

        let result = transformer
            .transform_batch(vec![message], CancellationToken::new())
            .await;
        assert!(result.is_err(), "Expected an error due to panic");
        assert!(result.unwrap_err().to_string().contains("panic"));

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
