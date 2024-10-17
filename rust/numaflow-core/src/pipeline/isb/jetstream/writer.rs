use crate::error::Error;
use crate::Result;
use async_nats::jetstream::context::PublishAckFuture;
use async_nats::jetstream::publish::PublishAck;
use async_nats::jetstream::Context;
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::{debug, warn};

#[derive(Clone, Debug)]
/// Writes to JetStream ISB. Exposes both write and blocking methods to write messages.
/// It accepts a cancellation token to stop infinite retries during shutdown.
pub(super) struct JetstreamWriter {
    js_ctx: Context,
    is_full: Arc<AtomicBool>,
    paf_resolver_tx: mpsc::Sender<ResolveAndPublishResult>,
    cancel_token: CancellationToken,
}

impl JetstreamWriter {
    /// Creates a JetStream Writer and a background task to make sure the Write futures (PAFs) are
    /// successful. Batch Size determines the maximum pending futures.
    pub(super) fn new(js_ctx: Context, batch_size: usize, cancel_token: CancellationToken) -> Self {
        let (paf_resolver_tx, paf_resolver_rx) =
            mpsc::channel::<ResolveAndPublishResult>(batch_size);

        let this = Self {
            js_ctx,
            is_full: Arc::new(AtomicBool::new(false)),
            paf_resolver_tx,
            cancel_token,
        };

        // spawn a task for checking whether buffer is_full

        tokio::task::spawn({
            let mut this = this.clone();
            async move {
                this.get_stream_stats().await;
            }
        });

        // spawn a task for resolving PAFs
        let mut resolver_actor = PafResolverActor::new(this.clone(), paf_resolver_rx);
        tokio::spawn(async move {
            resolver_actor.run().await;
        });

        this
    }

    async fn get_stream_stats(&mut self) {
        self.is_full.store(true, Ordering::Relaxed);
    }

    /// Writes the message to the JetStream ISB and returns a future which can be
    /// awaited to get the PublishAck. It will do infinite retries until the message
    /// gets published successfully. If it returns an error it means it is fatal error
    pub(super) async fn write(
        &self,
        stream: &'static str,
        payload: Vec<u8>,
        callee_tx: oneshot::Sender<Result<u64>>,
    ) {
        let js_ctx = self.js_ctx.clone();

        // loop till we get a PAF, there could be other reasons why PAFs cannot be created.
        let paf = loop {
            // let's write only if the buffer is not full
            match self.is_full.load(Ordering::Relaxed) {
                true => {
                    // FIXME: add metrics
                    debug!(%stream, "buffer is full");
                }
                false => match js_ctx.publish(stream, Bytes::from(payload.clone())).await {
                    Ok(paf) => {
                        break paf;
                    }
                    Err(e) => {
                        error!(?e, "publishing failed, retrying");
                    }
                },
            }
            // short-circuit out in failure mode if shutdown has been initiated
            if self.cancel_token.is_cancelled() {
                error!("Shutdown signal received, exiting write loop");
                callee_tx
                    .send(Err(Error::ISB("Shutdown signal received".to_string())))
                    .unwrap();
                return;
            }

            // FIXME: make it configurable
            // sleep to avoid busy looping
            sleep(Duration::from_millis(10)).await;
        };

        // send the paf and callee_tx over
        self.paf_resolver_tx
            .send(ResolveAndPublishResult {
                paf,
                stream,
                payload,
                callee_tx,
            })
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

/// ResolveAndPublishResult resolves the result of the write PAF operation.
/// It contains the PublishAckFuture which can be awaited to get the PublishAck. Once PAF has
/// resolved, the information is published to callee_tx.
#[derive(Debug)]
pub(super) struct ResolveAndPublishResult {
    paf: PublishAckFuture,
    stream: &'static str,
    payload: Vec<u8>,
    callee_tx: oneshot::Sender<Result<u64>>,
}

/// Resolves the PAF from the write call, if not successful it will do a blocking write so that
/// it is eventually successful. Once the PAF has been resolved (by either means) it will notify
/// the top-level callee via the oneshot rx.
struct PafResolverActor {
    js_writer: JetstreamWriter,
    receiver: Receiver<ResolveAndPublishResult>,
}

impl PafResolverActor {
    fn new(js_writer: JetstreamWriter, receiver: Receiver<ResolveAndPublishResult>) -> Self {
        PafResolverActor {
            js_writer,
            receiver,
        }
    }

    /// Tries to the resolve the original PAF from the write call. If it is successful, will send
    /// the successful result to the top-level callee's oneshot channel. If the original PAF does
    /// not successfully resolve, it will do blocking write till write to JetStream succeeds.
    async fn successfully_resolve_paf(&mut self, result: ResolveAndPublishResult) {
        match result.paf.await {
            Ok(ack) => result.callee_tx.send(Ok(ack.sequence)).unwrap(),
            Err(e) => {
                error!(?e, "Failed to resolve the future, trying blocking write");
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
            self.successfully_resolve_paf(result).await;
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
