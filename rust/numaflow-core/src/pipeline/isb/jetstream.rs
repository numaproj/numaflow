use async_nats::jetstream::Context;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::config::pipeline::isb::BufferWriterConfig;
use crate::error::Error;
use crate::message::{Message, Offset};
use crate::pipeline::isb::jetstream::writer::JetstreamWriter;
use crate::Result;

/// JetStream Writer is responsible for writing messages to JetStream ISB.
/// it exposes both sync and async methods to write messages. It has gates
/// to prevent writing into the buffer if the buffer is full. After successful
/// writes, it will let the callee know the status (or return a non-retryable
/// exception).
pub(super) mod writer;

/// ISB Writer accepts an Actor pattern based messages.
#[derive(Debug)]
struct ActorMessage {
    /// Write the messages to ISB
    message: Message,
    /// once the message has been successfully written, we can let the sender know.
    /// This can be used to trigger Acknowledgement of the message from the Reader.
    // FIXME: concrete type and better name
    callee_tx: oneshot::Sender<Result<Offset>>,
}

impl ActorMessage {
    fn new(message: Message, callee_tx: oneshot::Sender<Result<Offset>>) -> Self {
        Self { message, callee_tx }
    }
}

/// WriterActor will handle the messages and write them to the Jetstream ISB.
struct WriterActor {
    js_writer: JetstreamWriter,
    receiver: Receiver<ActorMessage>,
    cancel_token: CancellationToken,
}

impl WriterActor {
    fn new(
        js_writer: JetstreamWriter,
        receiver: Receiver<ActorMessage>,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            js_writer,
            receiver,
            cancel_token,
        }
    }

    async fn handle_message(&mut self, msg: ActorMessage) {
        let payload: Vec<u8> = msg
            .message
            .try_into()
            .expect("message serialization should not fail");
        self.js_writer.write(payload, msg.callee_tx).await
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }
}

/// WriterHandle is the handle to the WriterActor. It exposes a method to send messages to the Actor.
pub(crate) struct WriterHandle {
    sender: mpsc::Sender<ActorMessage>,
}

impl WriterHandle {
    pub(crate) fn new(
        stream_name: String,
        partition_idx: u16,
        config: BufferWriterConfig,
        js_ctx: Context,
        batch_size: usize,
        cancel_token: CancellationToken,
    ) -> Self {
        let (sender, receiver) = mpsc::channel::<ActorMessage>(batch_size);

        let js_writer = JetstreamWriter::new(
            stream_name,
            partition_idx,
            config,
            js_ctx,
            batch_size,
            cancel_token.clone(),
        );
        let mut actor = WriterActor::new(js_writer.clone(), receiver, cancel_token);

        tokio::spawn(async move {
            actor.run().await;
        });

        Self { sender }
    }

    pub(crate) async fn write(
        &self,
        message: Message,
    ) -> Result<oneshot::Receiver<Result<Offset>>> {
        let (sender, receiver) = oneshot::channel();
        let msg = ActorMessage::new(message, sender);
        self.sender
            .send(msg)
            .await
            .map_err(|e| Error::ISB(format!("Failed to write message to actor channel: {}", e)))?;

        Ok(receiver)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::*;
    use crate::message::{Message, MessageID};
    use async_nats::jetstream;
    use async_nats::jetstream::stream;
    use chrono::Utc;
    use tokio::sync::oneshot;
    use tokio::time::Instant;
    use tracing::info;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_publish_messages() {
        let cln_token = CancellationToken::new();
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
        let handler = WriterHandle::new(
            stream_name.to_string(),
            0,
            Default::default(),
            context.clone(),
            batch_size,
            cln_token.clone(),
        );

        let mut result_receivers = Vec::new();
        // Publish 500 messages
        for i in 0..500 {
            let message = Message {
                keys: vec![format!("key_{}", i)],
                value: format!("message {}", i).as_bytes().to_vec(),
                offset: None,
                event_time: Utc::now(),
                id: MessageID {
                    vertex_name: "vertex".to_string(),
                    offset: format!("offset_{}", i),
                    index: i,
                },
                headers: HashMap::new(),
            };
            let (sender, receiver) = oneshot::channel();
            let msg = ActorMessage {
                message,
                callee_tx: sender,
            };
            handler.sender.send(msg).await.unwrap();
            result_receivers.push(receiver);
        }

        for receiver in result_receivers {
            let result = receiver.await.unwrap();
            assert!(result.is_ok());
        }

        context.delete_stream(stream_name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_publish_messages_with_cancellation() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream_name = "test_publish_cancellation";
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                max_message_size: 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        let cancel_token = CancellationToken::new();
        let handler = WriterHandle::new(
            stream_name.to_string(),
            0,
            Default::default(),
            context.clone(),
            500,
            cancel_token.clone(),
        );

        let mut receivers = Vec::new();
        // Publish 100 messages successfully
        for i in 0..100 {
            let message = Message {
                keys: vec![format!("key_{}", i)],
                value: format!("message {}", i).as_bytes().to_vec(),
                offset: None,
                event_time: Utc::now(),
                id: MessageID {
                    vertex_name: "vertex".to_string(),
                    offset: format!("offset_{}", i),
                    index: i,
                },
                headers: HashMap::new(),
            };
            receivers.push(handler.write(message).await.unwrap());
        }

        // Attempt to publish the 101th message, which should get stuck in the retry loop
        // because the max message size is set to 1024
        let message = Message {
            keys: vec!["key_101".to_string()],
            value: vec![0; 1024],
            offset: None,
            event_time: Utc::now(),
            id: MessageID {
                vertex_name: "vertex".to_string(),
                offset: "offset_101".to_string(),
                index: 101,
            },
            headers: HashMap::new(),
        };
        let receiver = handler.write(message).await.unwrap();
        receivers.push(receiver);

        // Cancel the token to exit the retry loop
        cancel_token.cancel();

        // Check the results
        for (i, receiver) in receivers.into_iter().enumerate() {
            let result = receiver.await.unwrap();
            if i < 100 {
                assert!(result.is_ok());
            } else {
                assert!(result.is_err());
            }
        }

        context.delete_stream(stream_name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[ignore]
    #[tokio::test]
    async fn benchmark_publish_messages() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream_name = "benchmark_publish";
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let cancel_token = CancellationToken::new();
        let handler = WriterHandle::new(
            stream_name.to_string(),
            0,
            Default::default(),
            context.clone(),
            500,
            cancel_token.clone(),
        );

        let (tx, mut rx) = mpsc::channel(100);
        let test_start_time = Instant::now();
        let duration = Duration::from_secs(10);

        // Task to publish messages
        let publish_task = tokio::spawn(async move {
            let mut i = 0;
            let mut sent_count = 0;
            let mut start_time = Instant::now();
            while Instant::now().duration_since(test_start_time) < duration {
                let message = Message {
                    keys: vec![format!("key_{}", i)],
                    value: format!("message {}", i).as_bytes().to_vec(),
                    offset: None,
                    event_time: Utc::now(),
                    id: MessageID {
                        vertex_name: "".to_string(),
                        offset: format!("offset_{}", i),
                        index: i,
                    },
                    headers: HashMap::new(),
                };
                tx.send(handler.write(message).await.unwrap())
                    .await
                    .unwrap();
                sent_count += 1;
                i += 1;

                if start_time.elapsed().as_secs() >= 1 {
                    println!("Messages sent: {}", sent_count);
                    sent_count = 0;
                    start_time = Instant::now();
                }
            }
        });

        // Task to await responses
        let await_task = tokio::spawn(async move {
            let mut start_time = Instant::now();
            let mut count = 0;
            while let Some(receiver) = rx.recv().await {
                if receiver.await.unwrap().is_ok() {
                    count += 1;
                }

                if start_time.elapsed().as_secs() >= 1 {
                    println!("Messages received: {}", count);
                    count = 0;
                    start_time = Instant::now();
                }
            }
        });

        let _ = tokio::join!(publish_task, await_task);

        context.delete_stream(stream_name).await.unwrap();
    }
}
