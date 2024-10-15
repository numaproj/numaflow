use crate::error::Error;
use crate::message::Message;
use crate::pipeline::isb::jetstream::writer::JetstreamWriter;
use crate::Result;
use async_nats::jetstream::context::PublishAckFuture;
use async_nats::jetstream::Context;
use log::error;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};

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
        success: oneshot::Sender<Result<()>>,
    },
    /// Stop the writer. Once Stop is send, we can drop the rx.
    Stop,
}

/// PublishResult is the result of the write operation.
/// It contains the PublishAckFuture which can be awaited to get the PublishAck.
/// It also exposes a method to handle any future failures.
#[derive(Debug)]
pub(super) struct PublishResult {
    paf: PublishAckFuture,
    stream: &'static str,
    message: Message,
    callee_tx: oneshot::Sender<Result<()>>,
}

/// WriterActor will handle the messages and write them to the Jetstream ISB.
struct WriterActor {
    js_writer: JetstreamWriter,
    receiver: Receiver<ActorMessage>,
    paf_resolver_tx: mpsc::Sender<PublishResult>,
}

impl WriterActor {
    fn new(
        js_writer: JetstreamWriter,
        receiver: Receiver<ActorMessage>,
        batch_size: usize,
    ) -> Self {
        let (paf_resolver_tx, paf_resolver_rx) = mpsc::channel::<PublishResult>(batch_size);

        let mut resolver_actor = PafResolverActor::new(js_writer.clone(), paf_resolver_rx);

        tokio::spawn(async move {
            resolver_actor.run().await;
        });

        Self {
            js_writer,
            receiver,
            paf_resolver_tx,
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
                        stream,
                        message,
                        callee_tx: success,
                    };
                    self.paf_resolver_tx
                        .send(result)
                        .await
                        .expect("send should not fail");
                }
                Err(e) => {
                    log::error!("Failed to write message: {}", e);
                    success
                        .send(Err(Error::ISB(format!("Failed to write message: {}", e))))
                        .expect("send should not fail");
                }
            },
            ActorMessage::Stop => {
                // Handle stop logic if necessary
            }
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }
}

pub(super) struct PafResolverActor {
    js_writer: JetstreamWriter,
    receiver: Receiver<PublishResult>,
}

impl PafResolverActor {
    fn new(js_writer: JetstreamWriter, receiver: Receiver<PublishResult>) -> Self {
        PafResolverActor {
            js_writer,
            receiver,
        }
    }
    pub(super) async fn handle_result(&mut self, result: PublishResult) {
        match result.paf.await {
            Ok(ack) => result.callee_tx.send(Ok(())).unwrap(),
            Err(e) => {
                error!("Failed to resolve the future, trying sync write");
                match self
                    .js_writer
                    .sync_write(result.stream, result.message.clone())
                    .await
                {
                    Ok(_) => result.callee_tx.send(Ok(())).unwrap(),
                    Err(e) => result.callee_tx.send(Err(e)).unwrap(),
                }
            }
        }
    }

    pub(super) async fn run(&mut self) {
        while let Some(result) = self.receiver.recv().await {
            self.handle_result(result).await;
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
        let mut actor = WriterActor::new(js_writer.clone(), receiver, batch_size);

        tokio::spawn(async move {
            actor.run().await;
        });

        Self { sender }
    }

    pub(crate) async fn write(
        &self,
        stream: &'static str,
        message: Message,
    ) -> Result<oneshot::Receiver<Result<()>>> {
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

        Ok(receiver)
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
        }
        context.delete_stream(stream_name).await.unwrap();
    }
}
