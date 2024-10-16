use crate::error::Error;
use crate::Result;
use async_nats::jetstream::context::PublishAckFuture;
use async_nats::jetstream::publish::PublishAck;
use async_nats::jetstream::Context;
use bytes::Bytes;
use log::warn;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::error;

#[derive(Clone, Debug)]
/// Writes to JetStream ISB. Exposes both sync and async methods to write messages.
pub(super) struct JetstreamWriter {
    js_ctx: Context,
    paf_resolver_tx: mpsc::Sender<PublishResult>,
    cancel_token: CancellationToken,
}

impl JetstreamWriter {
    pub(super) fn new(js_ctx: Context, batch_size: usize, cancel_token: CancellationToken) -> Self {
        let (paf_resolver_tx, paf_resolver_rx) = mpsc::channel::<PublishResult>(batch_size);

        let this = Self {
            js_ctx,
            paf_resolver_tx,
            cancel_token,
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
        payload: Vec<u8>,
        success: oneshot::Sender<Result<u64>>,
    ) {
        let js_ctx = self.js_ctx.clone();

        let paf = loop {
            match js_ctx.publish(stream, Bytes::from(payload.clone())).await {
                Ok(paf) => {
                    break paf;
                }
                Err(e) => {
                    error!(?e, "publishing failed, retrying");
                    sleep(Duration::from_millis(10)).await;
                }
            }
            if self.cancel_token.is_cancelled() {
                error!("Shutdown signal received, exiting write loop");
                success
                    .send(Err(Error::ISB("Shutdown signal received".to_string())))
                    .unwrap();
                return;
            }
        };

        let result = PublishResult {
            paf,
            stream,
            payload,
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
        payload: Vec<u8>,
    ) -> Result<PublishAck> {
        let js_ctx = self.js_ctx.clone();

        loop {
            match js_ctx.publish(stream, Bytes::from(payload.clone())).await {
                Ok(paf) => match paf.await {
                    Ok(ack) => {
                        if ack.duplicate {
                            // should we return an error here? Because duplicate messages are not fatal
                            // But it can mess up the watermark progression because the offset will be
                            // same as the previous message offset
                            warn!("Duplicate message detected, ignoring {:?}", ack);
                        }
                        return Ok(ack);
                    }
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
            if self.cancel_token.is_cancelled() {
                return Err(Error::ISB("Shutdown signal received".to_string()));
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
    payload: Vec<u8>,
    callee_tx: oneshot::Sender<Result<u64>>,
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
            Ok(ack) => result.callee_tx.send(Ok(ack.sequence)).unwrap(),
            Err(e) => {
                error!("Failed to resolve the future, trying blocking write");
                match self
                    .js_writer
                    .blocking_write(result.stream, result.payload.clone())
                    .await
                {
                    Ok(ack) => result.callee_tx.send(Ok(ack.sequence)).unwrap(),
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

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_async_write() {
        let cln_token = CancellationToken::new();
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

        let writer = JetstreamWriter::new(context.clone(), 500, cln_token.clone());

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

        let (success_tx, success_rx) = oneshot::channel::<Result<u64>>();
        writer
            .write(stream_name, message.try_into().unwrap(), success_tx)
            .await;
        assert!(success_rx.await.is_ok());

        context.delete_stream(stream_name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_sync_write() {
        let cln_token = CancellationToken::new();
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

        let writer = JetstreamWriter::new(context.clone(), 500, cln_token.clone());

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

        let result = writer
            .blocking_write(stream_name, message.try_into().unwrap())
            .await;
        assert!(result.is_ok());

        let publish_ack = result.unwrap();
        assert_eq!(publish_ack.stream, stream_name);

        context.delete_stream(stream_name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_write_with_cancellation() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream_name = "test_cancellation";
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
        let writer = JetstreamWriter::new(context.clone(), 500, cancel_token.clone());

        let mut result_receivers = Vec::new();
        // Publish 10 messages successfully
        for i in 0..10 {
            let message = Message {
                keys: vec![format!("key_{}", i)],
                value: format!("message {}", i).as_bytes().to_vec(),
                offset: Offset {
                    offset: format!("offset_{}", i),
                    partition_id: i,
                },
                event_time: Utc::now(),
                id: MessageID {
                    vertex_name: "vertex".to_string(),
                    offset: format!("offset_{}", i),
                    index: i,
                },
                headers: HashMap::new(),
            };
            let (success_tx, success_rx) = oneshot::channel::<Result<u64>>();
            writer
                .write(stream_name, message.try_into().unwrap(), success_tx)
                .await;
            result_receivers.push(success_rx);
        }

        // Attempt to publish a message which has a payload size greater than the max_message_size
        // so that it fails and sync write will be attempted and it will be blocked
        let message = Message {
            keys: vec!["key_11".to_string()],
            value: vec![0; 1025],
            offset: Offset {
                offset: "offset_11".to_string(),
                partition_id: 11,
            },
            event_time: Utc::now(),
            id: MessageID {
                vertex_name: "vertex".to_string(),
                offset: "offset_11".to_string(),
                index: 11,
            },
            headers: HashMap::new(),
        };
        let (success_tx, success_rx) = oneshot::channel::<Result<u64>>();
        writer
            .write(stream_name, message.try_into().unwrap(), success_tx)
            .await;
        result_receivers.push(success_rx);

        // Cancel the token to exit the retry loop
        cancel_token.cancel();

        // Check the results
        for (i, receiver) in result_receivers.into_iter().enumerate() {
            let result = receiver.await.unwrap();
            if i < 10 {
                assert!(
                    result.is_ok(),
                    "Message {} should be published successfully",
                    i
                );
            } else {
                assert!(
                    result.is_err(),
                    "Message 11 should fail with cancellation error"
                );
                assert_eq!(
                    result.err().unwrap().to_string(),
                    "ISB Error - Shutdown signal received",
                );
            }
        }

        context.delete_stream(stream_name).await.unwrap();
    }
}
