use crate::message::Message;
use crate::Result;
use async_nats::jetstream::context::PublishAckFuture;
use async_nats::jetstream::publish::PublishAck;
use async_nats::jetstream::Context;
use bytes::Bytes;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tracing::error;

#[derive(Clone, Debug)]
/// Writes to JetStream ISB. Exposes both sync and async methods to write messages.
pub(super) struct JetstreamWriter {
    js_ctx: Context,
    paf_resolver_tx: mpsc::Sender<PublishResult>,
}

impl JetstreamWriter {
    pub(super) fn new(js_ctx: Context, batch_size: usize) -> Self {
        let (paf_resolver_tx, paf_resolver_rx) = mpsc::channel::<PublishResult>(batch_size);

        let this = Self {
            js_ctx,
            paf_resolver_tx,
        };

        let mut resolver_actor = PafResolverActor::new(this.clone(), paf_resolver_rx);

        tokio::spawn(async move {
            resolver_actor.run().await;
        });

        this
    }

    /// Writes the message to the JetStream ISB and returns a future which can be
    /// awaited to get the PublishAck. It will do infinite retries until the message
    /// gets published successfully. If it returns an error it means it is fatal
    /// error
    pub(super) async fn write(
        &self,
        stream: &'static str,
        msg: Message,
        success: oneshot::Sender<Result<()>>,
    ) {
        let js_ctx = self.js_ctx.clone();
        // this is for passing over for paf to do sync_write
        let message = msg.clone();

        let payload = Bytes::from(
            msg.to_bytes()
                .expect("message serialization should not fail"),
        );
        // FIXME: expose a way to exit the retry loop during shutdown
        let paf = loop {
            match js_ctx.publish(stream, payload.clone()).await {
                Ok(paf) => {
                    break paf;
                }
                Err(e) => {
                    error!(?e, "publishing failed, retrying");
                    sleep(Duration::from_millis(10)).await;
                }
            }
        };

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

    /// Writes the message to the JetStream ISB and returns the PublishAck. It will do
    /// infinite retries until the message gets published successfully. If it returns
    /// an error it means it is fatal non-retryable error.
    pub(super) async fn blocking_write(
        &self,
        stream: &'static str,
        msg: Message,
    ) -> Result<PublishAck> {
        let js_ctx = self.js_ctx.clone();
        let payload = Bytes::from(
            msg.to_bytes()
                .expect("message serialization should not fail"),
        );

        // TODO: expose a way to exit the retry loop during shutdown
        loop {
            match js_ctx.publish(stream, payload.clone()).await {
                Ok(paf) => match paf.await {
                    Ok(ack) => return Ok(ack),
                    Err(e) => {
                        error!("awaiting publish ack failed, retrying: {}", e);
                        sleep(Duration::from_millis(10)).await;
                    }
                },
                Err(e) => {
                    error!("publishing failed, retrying: {}", e);
                    sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }
}

/// PublishResult is the result of the write operation.
/// It contains the PublishAckFuture which can be awaited to get the PublishAck.
#[derive(Debug)]
pub(super) struct PublishResult {
    paf: PublishAckFuture,
    stream: &'static str,
    message: Message,
    callee_tx: oneshot::Sender<Result<()>>,
}

struct PafResolverActor {
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
    async fn handle_result(&mut self, result: PublishResult) {
        match result.paf.await {
            Ok(ack) => result.callee_tx.send(Ok(())).unwrap(),
            Err(e) => {
                error!("Failed to resolve the future, trying blocking write");
                match self
                    .js_writer
                    .blocking_write(result.stream, result.message.clone())
                    .await
                {
                    Ok(_) => result.callee_tx.send(Ok(())).unwrap(),
                    Err(e) => result.callee_tx.send(Err(e)).unwrap(),
                }
            }
        }
    }

    async fn run(&mut self) {
        while let Some(result) = self.receiver.recv().await {
            self.handle_result(result).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Message, MessageID, Offset};
    use async_nats::jetstream;
    use async_nats::jetstream::stream;
    use chrono::Utc;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_async_write() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream_name = "test_async";
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let writer = JetstreamWriter::new(context.clone(), 500);

        let message = Message {
            keys: vec!["key_0".to_string()],
            value: "message 0".as_bytes().to_vec(),
            offset: Offset {
                offset: "offset_0".to_string(),
                partition_id: 0,
            },
            event_time: Utc::now(),
            id: MessageID {
                vertex_name: "vertex".to_string(),
                offset: "offset_0".to_string(),
                index: 0,
            },
            headers: HashMap::new(),
        };

        let (success_tx, success_rx) = oneshot::channel::<Result<()>>();
        writer.write(stream_name, message.clone(), success_tx).await;
        assert!(success_rx.await.is_ok());

        // let publish_ack_future = result.unwrap();
        // let publish_ack = publish_ack_future.await;
        // assert!(publish_ack.is_ok());

        context.delete_stream(stream_name).await.unwrap();
    }

    #[tokio::test]
    async fn test_sync_write() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream_name = "test_sync";
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await
            .unwrap();

        let writer = JetstreamWriter::new(context.clone(), 500);

        let message = Message {
            keys: vec!["key_0".to_string()],
            value: "message 0".as_bytes().to_vec(),
            offset: Offset {
                offset: "offset_0".to_string(),
                partition_id: 1,
            },
            event_time: Utc::now(),
            id: MessageID {
                vertex_name: "vertex".to_string(),
                offset: "offset_0".to_string(),
                index: 0,
            },
            headers: HashMap::new(),
        };

        let result = writer.blocking_write(stream_name, message.clone()).await;
        assert!(result.is_ok());

        let publish_ack = result.unwrap();
        assert_eq!(publish_ack.stream, stream_name);

        context.delete_stream(stream_name).await.unwrap();
    }
}
