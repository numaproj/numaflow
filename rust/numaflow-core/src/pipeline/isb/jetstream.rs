use crate::error::Error;
use crate::message::Message;
use crate::pipeline::isb::jetstream::writer::JetstreamWriter;
use crate::Result;
use async_nats::jetstream::context::PublishAckFuture;
use async_nats::jetstream::publish::PublishAck;
use async_nats::jetstream::Context;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tracing::error;

/// Jetstream Writer is responsible for writing messages to Jetstream ISB.
/// it exposes both sync and async methods to write messages.
pub(super) mod writer;

/// ISB Writer accepts an Actor pattern based messages.
enum ActorMessage {
    /// Write the messages to ISB
    Write {
        stream: &'static str,
        message: Message,
        /// once the message has been successfully written, we can let the sender know.
        /// This can be used to trigger Acknowledgement of the message from the Reader.
        success: oneshot::Sender<Result<PublishResult>>,
    },
    /// Stop the writer. Once Stop is send, we can drop the rx.
    Stop,
}

/// PublishResult is the result of the write operation.
/// It contains the PublishAckFuture which can be awaited to get the PublishAck.
/// It also exposes a method to handle any future failures.
#[derive(Debug)]
pub struct PublishResult {
    paf: PublishAckFuture,
    js_writer: JetstreamWriter,
    stream: &'static str,
    message: Message,
}

impl PublishResult {
    async fn handle_failure(
        js_writer: JetstreamWriter,
        stream_name: &'static str,
        message: Message,
    ) -> Result<PublishAck> {
        js_writer.sync_write(stream_name, message).await
    }

    pub(crate) async fn get_ack(self) -> Result<PublishAck> {
        // await on the future first, then return the result
        // if it fails invoke the handle_failure method
        match self.paf.await {
            Ok(ack) => Ok(ack),
            Err(e) => {
                error!("Failed to write message: {}", e);
                Self::handle_failure(self.js_writer, self.stream, self.message).await
            }
        }
    }
}

/// WriterActor will handle the messages and write them to the Jetstream ISB.
struct WriterActor {
    js_writer: JetstreamWriter,
    receiver: Receiver<ActorMessage>,
}

impl WriterActor {
    fn new(js_writer: JetstreamWriter, receiver: Receiver<ActorMessage>) -> Self {
        Self {
            js_writer,
            receiver,
        }
    }

    async fn handle_message(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::Write {
                stream,
                message,
                success,
            } => match self.js_writer.async_write(stream, message.clone()).await {
                Ok(paf) => {
                    let result = PublishResult {
                        paf,
                        js_writer: self.js_writer.clone(),
                        stream,
                        message,
                    };
                    success.send(Ok(result)).expect("send should not fail");
                }
                Err(e) => {
                    log::error!("Failed to write message: {}", e);
                    success.send(Err(e)).expect("send should not fail");
                }
            },
            // TODO: do we really need stop?
            ActorMessage::Stop => {
                // Handle stop logic if necessary
            }
        }
    }
}

/// WriterHandle is the handle to the WriterActor. It exposes a method to send messages to the Actor.
pub(crate) struct WriterHandle {
    sender: mpsc::Sender<ActorMessage>,
}

impl WriterHandle {
    pub(super) fn new(js_ctx: Context, batch_size: usize) -> Self {
        let (sender, receiver) = mpsc::channel::<ActorMessage>(batch_size);

        let js_writer = JetstreamWriter::new(js_ctx);

        let mut actor = WriterActor::new(js_writer, receiver);

        tokio::spawn(async move {
            while let Some(msg) = actor.receiver.recv().await {
                actor.handle_message(msg).await;
            }
        });

        Self { sender }
    }

    pub(crate) async fn write(
        &self,
        stream: &'static str,
        message: Message,
    ) -> Result<PublishResult> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::Write {
            stream,
            message,
            success: sender,
        };
        self.sender
            .send(msg)
            .await
            .map_err(|e| Error::ISB(format!("Failed to write message to actor channel: {}", e)))?;

        // wait for PAF
        receiver
            .await
            .map_err(|e| Error::ISB(format!("Failed to write message to ISB: {}", e)))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Message, Offset};
    use async_nats::jetstream;
    use async_nats::jetstream::stream;
    use chrono::Utc;
    use std::collections::HashMap;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn test_publish_messages() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream_name = "default";
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await
            .unwrap();

        // Create ISBMessageHandler
        let batch_size = 500;
        let handler = WriterHandle::new(context.clone(), batch_size);

        // Publish 500 messages
        for i in 0..500 {
            let message = Message {
                keys: vec![format!("key_{}", i)],
                value: format!("message {}", i).as_bytes().to_vec(),
                offset: Offset {
                    offset: format!("offset_{}", i),
                    partition_id: i,
                },
                event_time: Utc::now(),
                id: format!("id_{}", i),
                headers: HashMap::new(),
            };
            let (sender, receiver) = oneshot::channel();
            let msg = ActorMessage::Write {
                stream: stream_name,
                message,
                success: sender,
            };
            handler.sender.send(msg).await.unwrap();

            // Await the result
            let result = receiver.await.unwrap();
            assert!(result.is_ok());

            let result = result.unwrap();
            assert!(result.get_ack().await.is_ok());
        }
        context.delete_stream(stream_name).await.unwrap();
    }
}
