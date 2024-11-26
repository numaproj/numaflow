use std::sync::Arc;

use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use user_defined::ActorMessage;

use crate::message::{ReadAck, ReadMessage};
use crate::transformer::user_defined::UserDefinedTransformer;
use crate::Result;

/// User-Defined Transformer extends Numaflow to add custom sources supported outside the builtins.
///
/// [User-Defined Transformer]: https://numaflow.numaproj.io/user-guide/sources/transformer/overview/#build-your-own-transformer
pub(crate) mod user_defined;

/// StreamingTransformer, transforms messages in a streaming fashion.
#[derive(Clone)]
pub(crate) struct Transformer {
    batch_size: usize,
    sender: mpsc::Sender<ActorMessage>,
    semaphore: Arc<Semaphore>,
}
impl Transformer {
    pub(crate) async fn new(
        batch_size: usize,
        client: SourceTransformClient<Channel>,
    ) -> Result<Self> {
        let (sender, mut receiver) = mpsc::channel(batch_size);
        let mut client = UserDefinedTransformer::new(batch_size, client).await?;

        // FIXME: batch_size should not be used, introduce a new config called udf concurrency
        let semaphore = Arc::new(Semaphore::new(batch_size));

        tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                client.handle_message(msg).await;
            }
        });

        Ok(Self {
            batch_size,
            sender,
            semaphore,
        })
    }

    pub(crate) async fn transform(
        &self,
        read_msg: ReadMessage,
        output_tx: mpsc::Sender<ReadMessage>,
    ) -> Result<()> {
        let permit = self.semaphore.clone().acquire_owned().await.unwrap();
        let transform_handle = self.clone();
        let output_tx = output_tx.clone();

        tokio::spawn(async move {
            let _permit = permit;
            let message = read_msg.message.clone();

            let (sender, receiver) = oneshot::channel();
            let msg = ActorMessage::Transform {
                message,
                respond_to: sender,
            };

            transform_handle.sender.send(msg).await.unwrap();

            match receiver.await {
                Ok(Ok(mut transformed_messages)) => {
                    // FIXME: handle the case where the transformer does flat map operation
                    if let Some(transformed_msg) = transformed_messages.pop() {
                        output_tx
                            .send(ReadMessage {
                                message: transformed_msg,
                                ack: read_msg.ack,
                            })
                            .await
                            .unwrap();
                    }
                }
                Err(_) | Ok(Err(_)) => {
                    let _ = read_msg.ack.send(ReadAck::Nak);
                }
            }
        });

        Ok(())
    }
    /// Starts reading messages in the form of chunks and transforms them and
    /// sends them to the next stage.
    pub(crate) fn transform_stream(
        &self,
        input_stream: ReceiverStream<ReadMessage>,
    ) -> Result<(ReceiverStream<ReadMessage>, JoinHandle<Result<()>>)> {
        let (output_tx, output_rx) = mpsc::channel(self.batch_size);
        let transform_handle = self.clone();

        let handle = tokio::spawn(async move {
            let mut input_stream = input_stream;

            while let Some(read_msg) = input_stream.next().await {
                transform_handle
                    .transform(read_msg, output_tx.clone())
                    .await?;
            }
            Ok(())
        });

        Ok((ReceiverStream::new(output_rx), handle))
    }
}
