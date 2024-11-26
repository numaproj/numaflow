use async_nats::jetstream::Context;
use bytes::BytesMut;
use log::info;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::config::pipeline::isb::BufferWriterConfig;
use crate::message::ReadMessage;
use crate::metrics::{pipeline_isb_metric_labels, pipeline_metrics};
use crate::pipeline::isb::jetstream::writer::{
    JetstreamWriter, PafResolver, ResolveAndPublishResult,
};
use crate::Result;

/// JetStream Writer is responsible for writing messages to JetStream ISB.
/// it exposes both sync and async methods to write messages. It has gates
/// to prevent writing into the buffer if the buffer is full. After successful
/// writes, it will let the callee know the status (or return a non-retryable
/// exception).
pub(super) mod writer;

pub(crate) mod reader;

type Stream = (String, u16);

/// StreamingJetstreamWriter is a streaming version of JetstreamWriter. It accepts a stream of messages
/// and writes them to Jetstream ISB. It also has a PAF resolver actor to resolve the PAFs.
#[derive(Clone)]
pub(crate) struct ISBWriter {
    concurrency: usize,
    config: Vec<BufferWriterConfig>,
    writer: JetstreamWriter,
}

impl ISBWriter {
    pub(crate) async fn new(
        concurrency: usize,
        config: Vec<BufferWriterConfig>,
        js_ctx: Context,
        cancel_token: CancellationToken,
    ) -> Self {
        info!(
            "New StreamingJetstreamWriter created with {:?} config and paf batch size {}",
            config, concurrency
        );

        let js_writer = JetstreamWriter::new(
            // flatten the streams across the config
            config.iter().flat_map(|c| c.streams.clone()).collect(),
            config.get(0).unwrap().clone(),
            js_ctx,
            cancel_token.clone(),
        );

        Self {
            config,
            writer: js_writer,
            concurrency,
        }
    }

    /// Starts reading messages from the stream and writes them to Jetstream ISB.
    pub(crate) async fn streaming_write(
        &self,
        messages_stream: ReceiverStream<ReadMessage>,
        cancellation_token: CancellationToken,
    ) -> Result<JoinHandle<Result<()>>> {
        let handle: JoinHandle<Result<()>> = tokio::spawn({
            let this = self.clone();
            let mut messages_stream = messages_stream;
            let mut index = 0;

            info!(
                "Starting streaming JetstreamWriter with config: {:?}",
                this.config
            );

            async move {
                let paf_resolver = PafResolver::new(this.concurrency, this.writer.clone());

                while let Some(read_message) = messages_stream.next().await {
                    let mut pafs = vec![];

                    // FIXME(CF): This is a temporary solution to round-robin the streams
                    for buffer in &this.config {
                        let payload: BytesMut = read_message
                            .message
                            .clone()
                            .try_into()
                            .expect("message serialization should not fail");
                        let stream = buffer.streams.get(index).unwrap();
                        index = (index + 1) % buffer.streams.len();

                        let paf = this.writer.write(stream.clone(), payload.into()).await;
                        pafs.push((stream.clone(), paf));
                    }

                    pipeline_metrics()
                        .isb
                        .write_total
                        .get_or_create(pipeline_isb_metric_labels())
                        .inc();

                    paf_resolver
                        .resolve_pafs(ResolveAndPublishResult {
                            pafs,
                            payload: read_message.message.value.clone().into(),
                            ack_tx: read_message.ack,
                        })
                        .await?;

                    if cancellation_token.is_cancelled() {
                        warn!("Cancellation token is cancelled. Exiting JetstreamWriter");
                        break;
                    }
                }
                Ok(())
            }
        });
        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use async_nats::jetstream;
    use async_nats::jetstream::stream;
    use chrono::Utc;
    use tokio::sync::oneshot;
    use tokio::time::Instant;
    use tracing::info;

    use super::*;
    use crate::message::{Message, MessageID};

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
            1000,
            cln_token.clone(),
        );

        let mut result_receivers = Vec::new();
        // Publish 500 messages
        for i in 0..500 {
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
            let (sender, receiver) = oneshot::channel();
            let msg = ActorMessage {
                message,
                callee_tx: sender,
            };
            handler.sender.send(msg).await.unwrap();
            result_receivers.push(receiver);
        }

        // FIXME: Uncomment after we start awaiting for PAFs
        //for receiver in result_receivers {
        //    let result = receiver.await.unwrap();
        //    assert!(result.is_ok());
        //}

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
            1000,
            cancel_token.clone(),
        );

        let mut receivers = Vec::new();
        // Publish 100 messages successfully
        for i in 0..100 {
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
            receivers.push(handler.write(message).await.unwrap());
        }

        // Attempt to publish the 101th message, which should get stuck in the retry loop
        // because the max message size is set to 1024
        let message = Message {
            keys: vec!["key_101".to_string()],
            value: vec![0; 1024].into(),
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
        // FIXME: Uncomment after we start awaiting for PAFs
        //for (i, receiver) in receivers.into_iter().enumerate() {
        //    let result = receiver.await.unwrap();
        //    if i < 100 {
        //        assert!(result.is_ok());
        //    } else {
        //        assert!(result.is_err());
        //    }
        //}

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
            1000,
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
                    value: format!("message {}", i).as_bytes().to_vec().into(),
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
                    info!("Messages sent: {}", sent_count);
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
                    info!("Messages received: {}", count);
                    count = 0;
                    start_time = Instant::now();
                }
            }
        });

        let _ = tokio::join!(publish_task, await_task);

        context.delete_stream(stream_name).await.unwrap();
    }
}
