use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::context::PublishAckFuture;
use async_nats::jetstream::publish::PublishAck;
use async_nats::jetstream::stream::RetentionPolicy::Limits;
use async_nats::jetstream::Context;
use bytes::Bytes;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::config::pipeline::isb::BufferWriterConfig;
use crate::error::Error;
use crate::message::{ Offset};
use crate::Result;

#[derive(Clone, Debug)]
/// Writes to JetStream ISB. Exposes both write and blocking methods to write messages.
/// It accepts a cancellation token to stop infinite retries during shutdown.
pub(super) struct JetstreamWriter {
    stream_name: String,
    partition_idx: u16,
    config: BufferWriterConfig,
    js_ctx: Context,
    is_full: Arc<AtomicBool>,
    paf_resolver_tx: mpsc::Sender<ResolveAndPublishResult>,
    cancel_token: CancellationToken,
}

impl JetstreamWriter {
    /// Creates a JetStream Writer and a background task to make sure the Write futures (PAFs) are
    /// successful. Batch Size determines the maximum pending futures.
    pub(super) fn new(
        stream_name: String,
        partition_idx: u16,
        config: BufferWriterConfig,
        js_ctx: Context,
        paf_batch_size: usize,
        cancel_token: CancellationToken,
    ) -> Self {
        let (paf_resolver_tx, paf_resolver_rx) =
            mpsc::channel::<ResolveAndPublishResult>(paf_batch_size);

        let this = Self {
            stream_name,
            partition_idx,
            config,
            js_ctx,
            is_full: Arc::new(AtomicBool::new(false)),
            paf_resolver_tx,
            cancel_token,
        };

        // spawn a task for checking whether buffer is_full
        tokio::task::spawn({
            let mut this = this.clone();
            async move {
                this.check_stream_status().await;
            }
        });

        // spawn a task for resolving PAFs
        let mut resolver_actor = PafResolverActor::new(this.clone(), paf_resolver_rx);
        tokio::spawn(async move {
            resolver_actor.run().await;
        });

        this
    }

    /// Checks the buffer usage metrics (soft and solid usage) for a given stream.
    /// If the usage is greater than the bufferUsageLimit, it sets the is_full flag to true.
    async fn check_stream_status(&mut self) {
        let mut interval = tokio::time::interval(self.config.refresh_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match Self::fetch_buffer_usage(self.js_ctx.clone(), self.stream_name.as_str(), self.config.max_length).await {
                        Ok((soft_usage, solid_usage)) => {
                            if solid_usage >= self.config.usage_limit && soft_usage >= self.config.usage_limit {
                                self.is_full.store(true, Ordering::Relaxed);
                            } else {
                                self.is_full.store(false, Ordering::Relaxed);
                            }
                        }
                        Err(e) => {
                            error!(?e, "Failed to fetch buffer usage, updating isFull to true");
                            self.is_full.store(true, Ordering::Relaxed);
                        }
                    }
                }
                _ = self.cancel_token.cancelled() => {
                    return;
                }
            }
        }
    }

    /// Fetches the buffer usage metrics (soft and solid usage) for a given stream.
    ///
    /// Soft Usage:
    /// Formula: (NumPending + NumAckPending) / maxLength
    /// - NumPending: The number of pending messages.
    /// - NumAckPending: The number of messages that are in processing state(yet to be acked).
    /// - maxLength: The maximum length of the buffer.
    ///
    /// Solid Usage:
    /// Formula:
    /// - If the stream's retention policy is LimitsPolicy: solidUsage = softUsage
    /// - Otherwise: solidUsage = State.Msgs / maxLength
    /// - State.Msgs: The total number of messages in the stream.
    /// - maxLength: The maximum length of the buffer.
    async fn fetch_buffer_usage(
        js_ctx: Context,
        stream_name: &str,
        max_length: usize,
    ) -> Result<(f64, f64)> {
        let mut stream = js_ctx
            .get_stream(stream_name)
            .await
            .map_err(|_| Error::ISB("Failed to get stream".to_string()))?;

        let stream_info = stream
            .info()
            .await
            .map_err(|e| Error::ISB(format!("Failed to get the stream info {:?}", e)))?;

        let mut consumer: PullConsumer = js_ctx
            .get_consumer_from_stream(stream_name, stream_name)
            .await
            .map_err(|e| Error::ISB(format!("Failed to get the consumer {:?}", e)))?;

        let consumer_info = consumer
            .info()
            .await
            .map_err(|e| Error::ISB(format!("Failed to get the consumer info {:?}", e)))?;

        let soft_usage = (consumer_info.num_pending as f64 + consumer_info.num_ack_pending as f64)
            / max_length as f64;
        let solid_usage = if stream_info.config.retention == Limits {
            soft_usage
        } else {
            stream_info.state.messages as f64 / max_length as f64
        };

        Ok((soft_usage, solid_usage))
    }

    /// Writes the message to the JetStream ISB and returns a future which can be
    /// awaited to get the PublishAck. It will do infinite retries until the message
    /// gets published successfully. If it returns an error it means it is fatal error
    pub(super) async fn write(&self, payload: Vec<u8>, callee_tx: oneshot::Sender<Result<Offset>>) {
        let js_ctx = self.js_ctx.clone();

        // loop till we get a PAF, there could be other reasons why PAFs cannot be created.
        let paf = loop {
            // let's write only if the buffer is not full
            match self.is_full.load(Ordering::Relaxed) {
                true => {
                    // FIXME: add metrics
                    info!(%self.stream_name, "stream is full");
                    // FIXME: consider buffer-full strategy
                }
                false => match js_ctx
                    .publish(self.stream_name.clone(), Bytes::from(payload.clone()))
                    .await
                {
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

            // sleep to avoid busy looping
            sleep(self.config.retry_interval).await;
        };

        // send the paf and callee_tx over
        self.paf_resolver_tx
            .send(ResolveAndPublishResult {
                paf,
                payload,
                callee_tx,
            })
            .await
            .expect("send should not fail");
    }

    /// Writes the message to the JetStream ISB and returns the PublishAck. It will do
    /// infinite retries until the message gets published successfully. If it returns
    /// an error it means it is fatal non-retryable error.
    pub(super) async fn blocking_write(&self, payload: Vec<u8>) -> Result<PublishAck> {
        let js_ctx = self.js_ctx.clone();

        loop {
            match js_ctx
                .publish(self.stream_name.clone(), Bytes::from(payload.clone()))
                .await
            {
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
                        error!(?e, "awaiting publish ack failed, retrying");
                        sleep(Duration::from_millis(10)).await;
                    }
                },
                Err(e) => {
                    error!(?e, "publishing failed, retrying");
                    sleep(self.config.retry_interval).await;
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
    payload: Vec<u8>,
    callee_tx: oneshot::Sender<Result<Offset>>,
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
    async fn successfully_resolve_paf(&mut self, _result: ResolveAndPublishResult) {
        // result
        //     .callee_tx
        //     .send(Ok(Offset::Int(IntOffset::new(
        //         0,
        //         self.js_writer.partition_idx,
        //     ))))
        //     .unwrap();

        // match result.paf.await {
        //     Ok(ack) => {
        //         if ack.duplicate {
        //             warn!("Duplicate message detected, ignoring {:?}", ack);
        //         }
        //         // result
        //         //     .callee_tx
        //         //     .send(Ok(Offset::Int(IntOffset::new(
        //         //         ack.sequence,
        //         //         self.js_writer.partition_idx,
        //         //     ))))
        //         //     .unwrap_or_else(|e| {
        //         //         error!("Failed to send offset: {:?}", e);
        //         //     })
        //     }
        //     Err(e) => {
        //         error!(?e, "Failed to resolve the future, trying blocking write");
        //         match self.js_writer.blocking_write(result.payload.clone()).await {
        //             Ok(ack) => {
        //                 if ack.duplicate {
        //                     warn!("Duplicate message detected, ignoring {:?}", ack);
        //                 }
        //                 // result
        //                 //     .callee_tx
        //                 //     .send(Ok(Offset::Int(IntOffset::new(
        //                 //         ack.sequence,
        //                 //         self.js_writer.partition_idx,
        //                 //     ))))
        //                 //     .unwrap()
        //             }
        //             Err(e) => {}
        //         }
        //     }
        // }
    }

    async fn run(&mut self) {
        while let Some(result) = self.receiver.recv().await {
            self.successfully_resolve_paf(result).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Instant;

    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};
    use bytes::BytesMut;
    use chrono::Utc;

    use super::*;
    use crate::message::{Message, MessageID, Offset};

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

        let _consumer = context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream_name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream_name,
            )
            .await
            .unwrap();

        let writer = JetstreamWriter::new(
            stream_name.to_string(),
            0,
            Default::default(),
            context.clone(),
            500,
            cln_token.clone(),
        );

        let message = Message {
            keys: vec!["key_0".to_string()],
            value: "message 0".as_bytes().to_vec().into(),
            offset: None,
            event_time: Utc::now(),
            id: MessageID {
                vertex_name: "vertex".to_string(),
                offset: "offset_0".to_string(),
                index: 0,
            },
            headers: HashMap::new(),
        };

        let (success_tx, success_rx) = oneshot::channel::<Result<Offset>>();
        let message_bytes: BytesMut = message.try_into().unwrap();
        writer.write(message_bytes.into(), success_tx).await;
        // FIXME: Uncomment after we start awaiting for PAF completion
        // assert!(success_rx.await.is_ok());

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

        let _consumer = context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream_name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream_name,
            )
            .await
            .unwrap();

        let writer = JetstreamWriter::new(
            stream_name.to_string(),
            0,
            Default::default(),
            context.clone(),
            500,
            cln_token.clone(),
        );

        let message = Message {
            keys: vec!["key_0".to_string()],
            value: "message 0".as_bytes().to_vec().into(),
            offset: None,
            event_time: Utc::now(),
            id: MessageID {
                vertex_name: "vertex".to_string(),
                offset: "offset_0".to_string(),
                index: 0,
            },
            headers: HashMap::new(),
        };

        let message_bytes: BytesMut = message.try_into().unwrap();
        let result = writer.blocking_write(message_bytes.into()).await;
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

        let _consumer = context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream_name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream_name,
            )
            .await
            .unwrap();

        let cancel_token = CancellationToken::new();
        let writer = JetstreamWriter::new(
            stream_name.to_string(),
            0,
            Default::default(),
            context.clone(),
            500,
            cancel_token.clone(),
        );

        let mut result_receivers = Vec::new();
        // Publish 10 messages successfully
        for i in 0..10 {
            let message = Message {
                keys: vec![format!("key_{}", i)],
                value: format!("message {}", i).as_bytes().to_vec().into(),
                offset: None,
                event_time: Utc::now(),
                id: MessageID {
                    vertex_name: "vertex".to_string(),
                    offset: format!("offset_{}", i),
                    index: i,
                },
                headers: HashMap::new(),
            };
            let (success_tx, success_rx) = oneshot::channel::<Result<Offset>>();
            let message_bytes: BytesMut = message.try_into().unwrap();
            writer.write(message_bytes.into(), success_tx).await;
            result_receivers.push(success_rx);
        }

        // Attempt to publish a message which has a payload size greater than the max_message_size
        // so that it fails and sync write will be attempted and it will be blocked
        let message = Message {
            keys: vec!["key_11".to_string()],
            value: vec![0; 1025].into(),
            offset: None,
            event_time: Utc::now(),
            id: MessageID {
                vertex_name: "vertex".to_string(),
                offset: "offset_11".to_string(),
                index: 11,
            },
            headers: HashMap::new(),
        };
        let (success_tx, success_rx) = oneshot::channel::<Result<Offset>>();
        let message_bytes: BytesMut = message.try_into().unwrap();
        writer.write(message_bytes.into(), success_tx).await;
        result_receivers.push(success_rx);

        // Cancel the token to exit the retry loop
        cancel_token.cancel();

        // Check the results
        // FIXME: Uncomment after we start awaiting for PAFs.
        //for (i, receiver) in result_receivers.into_iter().enumerate() {
        //    let result = receiver.await.unwrap();
        //    if i < 10 {
        //        assert!(
        //            result.is_ok(),
        //            "Message {} should be published successfully",
        //            i
        //        );
        //    } else {
        //        assert!(
        //            result.is_err(),
        //            "Message 11 should fail with cancellation error"
        //        );
        //        assert_eq!(
        //            result.err().unwrap().to_string(),
        //            "ISB Error - Shutdown signal received",
        //        );
        //    }
        //}

        context.delete_stream(stream_name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_fetch_buffer_usage() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream_name = "test_fetch_buffer_usage";
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                max_messages: 1000,
                max_message_size: 1024,
                max_messages_per_subject: 1000,
                retention: Limits, // Set retention policy to Limits for solid usage
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream_name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream_name,
            )
            .await;

        let _consumer = context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream_name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream_name,
            )
            .await
            .unwrap();

        let max_length = 100;

        // Publish messages to fill the buffer
        for _ in 0..80 {
            context
                .publish(stream_name, Bytes::from("test message"))
                .await
                .unwrap();
        }

        // Fetch buffer usage
        let (soft_usage, _) =
            JetstreamWriter::fetch_buffer_usage(context.clone(), stream_name, max_length)
                .await
                .unwrap();

        // Verify the buffer usage metrics
        assert_eq!(soft_usage, 0.8);
        assert_eq!(soft_usage, 0.8);

        // Clean up
        context
            .delete_consumer_from_stream(stream_name, stream_name)
            .await
            .unwrap();
        context.delete_stream(stream_name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_check_stream_status() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream_name = "test_check_stream_status";
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                max_messages: 1000,
                max_message_size: 1024,
                max_messages_per_subject: 1000,
                retention: Limits, // Set retention policy to Limits for solid usage
                ..Default::default()
            })
            .await
            .unwrap();

        let _consumer = context
            .create_consumer_on_stream(
                consumer::Config {
                    name: Some(stream_name.to_string()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                stream_name,
            )
            .await
            .unwrap();

        let cancel_token = CancellationToken::new();
        let writer = JetstreamWriter::new(
            stream_name.to_string(),
            0,
            BufferWriterConfig {
                max_length: 100,
                ..Default::default()
            },
            context.clone(),
            500,
            cancel_token.clone(),
        );

        let mut js_writer = writer.clone();
        // Simulate the stream status check
        tokio::spawn(async move {
            js_writer.check_stream_status().await;
        });

        // Publish messages to fill the buffer, since max_length is 100, we need to publish 80 messages
        for _ in 0..80 {
            context
                .publish(stream_name, Bytes::from("test message"))
                .await
                .unwrap();
        }

        let start_time = Instant::now();
        while !writer.is_full.load(Ordering::Relaxed) && start_time.elapsed().as_millis() < 1000 {
            sleep(Duration::from_millis(5)).await;
        }

        // Verify the is_full flag
        assert!(
            writer.is_full.load(Ordering::Relaxed),
            "Buffer should be full after publishing messages"
        );

        // Clean up
        context.delete_stream(stream_name).await.unwrap();
    }
}
