use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::error;

use crate::message::Message;
use crate::tracker::TrackerHandle;
use crate::transformer::user_defined::UserDefinedTransformer;
use crate::Result;

/// User-Defined Transformer extends Numaflow to add custom sources supported outside the builtins.
///
/// [User-Defined Transformer]: https://numaflow.numaproj.io/user-guide/sources/transformer/overview/#build-your-own-transformer
pub(crate) mod user_defined;

pub enum ActorMessage {
    Transform {
        message: Message,
        respond_to: oneshot::Sender<Result<Vec<Message>>>,
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
        }
    }

    async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }
}

/// StreamingTransformer, transforms messages in a streaming fashion.
pub(crate) struct Transformer {
    batch_size: usize,
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
            batch_size,
            concurrency,
            sender,
            tracker_handle,
        })
    }

    /// Applies the transformation on the message and sends it to the next stage, it blocks if the
    /// concurrency limit is reached.
    pub(crate) async fn transform(
        transform_handle: mpsc::Sender<ActorMessage>,
        permit: OwnedSemaphorePermit,
        read_msg: Message,
        output_tx: mpsc::Sender<Message>,
        tracker_handle: TrackerHandle,
    ) -> Result<()> {
        // only if we have tasks < max_concurrency

        let output_tx = output_tx.clone();

        // invoke transformer and then wait for the one-shot
        tokio::spawn(async move {
            let _permit = permit;

            let (sender, receiver) = oneshot::channel();
            let msg = ActorMessage::Transform {
                message: read_msg.clone(),
                respond_to: sender,
            };

            // invoke trf
            transform_handle
                .send(msg)
                .await
                .expect("failed to send message");

            // wait for one-shot
            match receiver.await {
                Ok(Ok(mut transformed_messages)) => {
                    tracker_handle
                        .update(
                            read_msg.id.offset.clone(),
                            transformed_messages.len() as u32,
                            false,
                        )
                        .await
                        .expect("failed to update tracker");
                    for transformed_message in transformed_messages.drain(..) {
                        let _ = output_tx.send(transformed_message).await;
                    }
                }
                Err(_) | Ok(Err(_)) => {
                    error!("Failed to transform message");
                    tracker_handle
                        .discard(read_msg.id.offset.clone())
                        .await
                        .expect("failed to discard tracker");
                }
            }
        });

        Ok(())
    }

    /// Starts reading messages in the form of chunks and transforms them and
    /// sends them to the next stage.
    pub(crate) fn transform_stream(
        &self,
        input_stream: ReceiverStream<Message>,
    ) -> Result<(ReceiverStream<Message>, JoinHandle<Result<()>>)> {
        let (output_tx, output_rx) = mpsc::channel(self.batch_size);

        let transform_handle = self.sender.clone();
        let tracker_handle = self.tracker_handle.clone();
        // FIXME: batch_size should not be used, introduce a new config called udf concurrenc
        let semaphore = Arc::new(Semaphore::new(self.concurrency));

        let handle = tokio::spawn(async move {
            let mut input_stream = input_stream;

            while let Some(read_msg) = input_stream.next().await {
                let permit = semaphore.clone().acquire_owned().await.unwrap();

                Self::transform(
                    transform_handle.clone(),
                    permit,
                    read_msg,
                    output_tx.clone(),
                    tracker_handle.clone(),
                )
                .await?;
            }
            Ok(())
        });

        Ok((ReceiverStream::new(output_rx), handle))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use numaflow::sourcetransform;
    use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
    use tempfile::TempDir;
    use tokio::sync::oneshot;

    use super::*;
    use crate::message::{Message, MessageID, Offset};
    use crate::shared::grpc::create_rpc_channel;

    struct SimpleTransformer;

    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for SimpleTransformer {
        async fn transform(
            &self,
            input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            let message = sourcetransform::Message::new(input.value, chrono::offset::Utc::now())
                .keys(input.keys);
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
        let tracker_handle = TrackerHandle::new();

        let client = SourceTransformClient::new(create_rpc_channel(sock_file).await?);
        let transformer = Transformer::new(500, 10, client, tracker_handle.clone()).await?;

        let message = Message {
            keys: vec!["first".into()],
            tags: None,
            value: "hello".into(),
            offset: Some(Offset::String(crate::message::StringOffset::new(
                "0".to_string(),
                0,
            ))),
            event_time: chrono::Utc::now(),
            id: MessageID {
                vertex_name: "vertex_name".to_string(),
                offset: "0".to_string(),
                index: 0,
            },
            headers: Default::default(),
        };

        let (output_tx, mut output_rx) = mpsc::channel(10);

        let semaphore = Arc::new(Semaphore::new(10));
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        Transformer::transform(
            transformer.sender.clone(),
            permit,
            message,
            output_tx,
            tracker_handle,
        )
        .await?;

        let transformed_message = output_rx.recv().await.unwrap();
        assert_eq!(transformed_message.value, "hello");

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

        let tracker_handle = TrackerHandle::new();
        let client = SourceTransformClient::new(create_rpc_channel(sock_file).await?);
        let transformer = Transformer::new(500, 10, client, tracker_handle.clone()).await?;

        let (input_tx, input_rx) = mpsc::channel(10);
        let input_stream = ReceiverStream::new(input_rx);

        for i in 0..5 {
            let message = Message {
                keys: vec![format!("key_{}", i)],
                tags: None,
                value: format!("value_{}", i).into(),
                offset: Some(Offset::String(crate::message::StringOffset::new(
                    i.to_string(),
                    0,
                ))),
                event_time: chrono::Utc::now(),
                id: MessageID {
                    vertex_name: "vertex_name".to_string(),
                    offset: i.to_string(),
                    index: i,
                },
                headers: Default::default(),
            };
            input_tx.send(message).await.unwrap();
        }
        drop(input_tx);

        let (output_stream, transform_handle) = transformer.transform_stream(input_stream)?;

        let mut output_rx = output_stream.into_inner();

        for i in 0..5 {
            let transformed_message = output_rx.recv().await.unwrap();
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
        assert!(
            transform_handle.is_finished(),
            "Expected transformer to have shut down"
        );
        Ok(())
    }
}
