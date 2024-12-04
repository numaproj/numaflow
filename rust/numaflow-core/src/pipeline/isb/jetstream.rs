use async_nats::jetstream::Context;
use bytes::BytesMut;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::pipeline::isb::BufferWriterConfig;
use crate::error::Error;
use crate::message::{ReadAck, ReadMessage};
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

/// Stream is a combination of stream name and partition id.
type Stream = (String, u16);

/// StreamingJetstreamWriter is a streaming version of JetstreamWriter. It accepts a stream of messages
/// and writes them to Jetstream ISB. It also has a PAF resolver actor to resolve the PAFs.
#[derive(Clone)]
pub(crate) struct ISBWriter {
    paf_concurrency: usize,
    config: Vec<BufferWriterConfig>,
    writer: JetstreamWriter,
}

impl ISBWriter {
    pub(crate) async fn new(
        paf_concurrency: usize,
        config: Vec<BufferWriterConfig>,
        js_ctx: Context,
        cancel_token: CancellationToken,
    ) -> Self {
        info!(?config, paf_concurrency, "Streaming JetstreamWriter",);

        let js_writer = JetstreamWriter::new(
            // flatten the streams across the config
            config.iter().flat_map(|c| c.streams.clone()).collect(),
            config.first().unwrap().clone(),
            js_ctx,
            cancel_token.clone(),
        );

        Self {
            config,
            writer: js_writer,
            paf_concurrency,
        }
    }

    /// Starts reading messages from the stream and writes them to Jetstream ISB.
    pub(crate) async fn streaming_write(
        &self,
        messages_stream: ReceiverStream<ReadMessage>,
    ) -> Result<JoinHandle<Result<()>>> {
        let handle: JoinHandle<Result<()>> = tokio::spawn({
            let this = self.clone();
            let mut messages_stream = messages_stream;
            let mut index = 0;

            async move {
                let paf_resolver = PafResolver::new(this.paf_concurrency, this.writer.clone());
                while let Some(read_message) = messages_stream.next().await {
                    // if message needs to be dropped, ack and continue
                    // TODO: add metric for dropped count
                    if read_message.message.dropped() {
                        read_message
                            .ack
                            .send(ReadAck::Ack)
                            .map_err(|e| Error::ISB(format!("Failed to send ack: {:?}", e)))?;
                        continue;
                    }
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
                        .forwarder
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

    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};
    use chrono::Utc;
    use tokio::sync::oneshot;

    use super::*;
    use crate::message::{Message, MessageID, ReadAck};

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_publish_messages() {
        let cln_token = CancellationToken::new();
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream_name = "test_publish_messages";
        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                max_messages: 1000,
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

        let writer = ISBWriter::new(
            10,
            vec![BufferWriterConfig {
                streams: vec![(stream_name.to_string(), 0)],
                max_length: 1000,
                ..Default::default()
            }],
            context.clone(),
            cln_token.clone(),
        )
        .await;

        let mut ack_receivers = Vec::new();
        let (messages_tx, messages_rx) = tokio::sync::mpsc::channel(500);
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
            let read_message = ReadMessage {
                message,
                ack: sender,
            };
            messages_tx.send(read_message).await.unwrap();
            ack_receivers.push(receiver);
        }
        drop(messages_tx);

        let receiver_stream = ReceiverStream::new(messages_rx);
        let _handle = writer.streaming_write(receiver_stream).await.unwrap();

        for receiver in ack_receivers {
            let result = receiver.await.unwrap();
            assert_eq!(result, ReadAck::Ack);
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
        let writer = ISBWriter::new(
            10,
            vec![BufferWriterConfig {
                streams: vec![(stream_name.to_string(), 0)],
                ..Default::default()
            }],
            context.clone(),
            cancel_token.clone(),
        )
        .await;

        let mut ack_receivers = Vec::new();
        let (tx, rx) = tokio::sync::mpsc::channel(500);
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
            let (sender, receiver) = oneshot::channel();
            let read_message = ReadMessage {
                message,
                ack: sender,
            };
            tx.send(read_message).await.unwrap();
            ack_receivers.push(receiver);
        }

        let receiver_stream = ReceiverStream::new(rx);
        let _handle = writer.streaming_write(receiver_stream).await.unwrap();

        // Attempt to publish the 101th message, which should get stuck in the retry loop
        // because the max message size is set to 1024
        let message = Message {
            keys: vec!["key_101".to_string()],
            value: vec![0; 1025].into(),
            offset: None,
            event_time: Utc::now(),
            id: MessageID {
                vertex_name: "vertex".to_string(),
                offset: "offset_101".to_string(),
                index: 101,
            },
            headers: HashMap::new(),
        };
        let (sender, receiver) = oneshot::channel();
        let read_message = ReadMessage {
            message,
            ack: sender,
        };
        tx.send(read_message).await.unwrap();
        ack_receivers.push(receiver);
        drop(tx);

        // Cancel the token to exit the retry loop
        cancel_token.cancel();

        // Check the results
        for (i, receiver) in ack_receivers.into_iter().enumerate() {
            let result = receiver.await.unwrap();
            if i < 100 {
                assert_eq!(result, ReadAck::Ack);
            } else {
                assert_eq!(result, ReadAck::Nak);
            }
        }
        context.delete_stream(stream_name).await.unwrap();
    }
}
