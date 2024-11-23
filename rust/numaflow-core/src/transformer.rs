use crate::error;
use crate::message::{Message, ReadAck, ReadMessage};
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;

use crate::transformer::user_defined::UserDefinedTransformer;
use crate::Result;

/// User-Defined Transformer extends Numaflow to add custom sources supported outside the builtins.
///
/// [User-Defined Transformer]: https://numaflow.numaproj.io/user-guide/sources/transformer/overview/#build-your-own-transformer
pub(crate) mod user_defined;

pub(self) enum ActorMessage {
    Transform {
        messages: Vec<Message>,
        respond_to: oneshot::Sender<Result<Vec<Message>>>,
    },
}

/// StreamingTransformer, transforms messages in a streaming fashion.
#[derive(Clone)]
pub(crate) struct Transformer {
    batch_size: usize,
    timeout: Duration,
    sender: mpsc::Sender<ActorMessage>,
}

impl Transformer {
    pub(crate) async fn new(
        batch_size: usize,
        timeout: Duration,
        client: SourceTransformClient<Channel>,
    ) -> Result<Self> {
        let (sender, mut receiver) = mpsc::channel(batch_size);
        let mut client = UserDefinedTransformer::new(batch_size, client).await?;
        tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                client.handle_message(msg).await;
            }
        });
        Ok(Self {
            batch_size,
            timeout,
            sender,
        })
    }

    pub(crate) async fn transform(&self, messages: Vec<Message>) -> Result<Vec<Message>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Transform {
            messages,
            respond_to: sender,
        };
        let _ = self.sender.send(msg).await;
        receiver.await.unwrap()
    }

    /// Starts reading messages in the form of chunks and transforms them and
    /// sends them to the next stage.
    pub(crate) fn transform_stream(
        &self,
        input_stream: ReceiverStream<ReadMessage>,
    ) -> Result<(ReceiverStream<ReadMessage>, JoinHandle<Result<()>>)> {
        let (output_tx, output_rx) = mpsc::channel(self.batch_size);
        let transform_handle = self.clone();
        let batch_size = self.batch_size;
        let timeout_duration = self.timeout;

        let handle = tokio::spawn(async move {
            let chunk_stream = input_stream.chunks_timeout(batch_size, timeout_duration);
            tokio::pin!(chunk_stream);

            while let Some(batch) = chunk_stream.next().await {
                if batch.is_empty() {
                    continue;
                }

                // Create a vector of tuples (message, oneshot::Sender<ReadAck>)
                let messages_with_acks: Vec<(Message, oneshot::Sender<ReadAck>)> = batch
                    .into_iter()
                    .map(|read_msg| (read_msg.message, read_msg.ack))
                    .collect();

                // Extract the messages for transformation
                let messages: Vec<Message> = messages_with_acks
                    .iter()
                    .map(|(msg, _)| msg.clone())
                    .collect();

                // FIXME: it should be streaming
                let transformed_messages = match transform_handle.transform(messages).await {
                    Ok(messages) => messages,
                    Err(e) => {
                        error!("Error while transforming messages: {:?}", e);
                        return Err(e);
                    }
                };

                // FIXME: We can have more than one transformed message for each input message
                // Iterate over transformed messages and their corresponding oneshot::Sender<ReadAck>
                for (transformed_msg, (_, ack)) in transformed_messages
                    .into_iter()
                    .zip(messages_with_acks.into_iter())
                {
                    if output_tx
                        .send(ReadMessage {
                            message: transformed_msg,
                            ack,
                        })
                        .await
                        .is_err()
                    {
                        error!("Error while sending transformed message");
                        return Err(error::Error::Source("Send error".to_string()));
                    }
                }
            }
            Ok(())
        });

        Ok((ReceiverStream::new(output_rx), handle))
    }
}
