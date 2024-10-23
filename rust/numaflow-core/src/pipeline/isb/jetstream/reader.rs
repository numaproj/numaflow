use std::time::Duration;

use async_nats::jetstream::{
    consumer::PullConsumer, AckKind, Context, Message as JetstreamMessage,
};
use futures::StreamExt;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use crate::config::pipeline::isb::BufferReaderConfig;
use crate::error::Error;
use crate::message::{IntOffset, Message, Offset, ReadAck, ReadMessage};
use crate::Result;

// The JetstreamReader is a handle to the background actor that continuously fetches messages from Jetstream.
// It can be used to cancel the background task and stop reading from Jetstream.
// The sender end of the channel is not stored in this struct, since the struct is clone-able and the mpsc channel is only closed when all the senders are dropped.
// Storing the Sender end of channel in this struct would make it difficult to close the channel with `cancel` method.
#[derive(Clone)]
pub(crate) struct JetstreamReader {
    name: String,
    partition_idx: u16,
    config: BufferReaderConfig,
    consumer: PullConsumer,
}

impl JetstreamReader {
    pub(crate) async fn new(
        name: String,
        partition_idx: u16,
        js_ctx: Context,
        config: BufferReaderConfig,
    ) -> Result<Self> {
        let mut config = config;

        let mut consumer: PullConsumer = js_ctx
            .get_consumer_from_stream(&name, &name)
            .await
            .map_err(|e| Error::ISB(format!("Failed to get consumer for stream {}", e)))?;

        let consumer_info = consumer
            .info()
            .await
            .map_err(|e| Error::ISB(format!("Failed to get consumer info {}", e)))?;

        // Calculate inProgressTickSeconds based on the ack_wait_seconds.
        let ack_wait_seconds = consumer_info.config.ack_wait.as_secs();
        let wip_ack_interval = Duration::from_secs(std::cmp::max(
            config.wip_ack_interval.as_secs(),
            ack_wait_seconds * 2 / 3,
        ));
        config.wip_ack_interval = wip_ack_interval;

        let this = Self {
            name,
            partition_idx,
            config: config.clone(),
            consumer,
        };

        let (messages_tx, messages_rx) = mpsc::channel(2 * config.batch_size);
        tokio::spawn({
            let this = this.clone();
            async move {
                this.start(messages_tx).await;
            }
        });
        Ok((this, messages_rx))
    }

    // Stop reading from Jetstream and close the sender end of the channel.
    pub(crate) fn cancel(&self) {
        self.ctx.cancel();
    }

    // When we encounter an error, we log the error and return from the function. This drops the sender end of the channel.
    // The closing of the channel should propagate to the receiver end and the receiver should exit gracefully.
    // Within the loop, we only consider cancellationToken cancellation during the permit reservation and fetching messages,
    // since rest of the operations should finish immediately.
    pub(crate) async fn start(
        &self,
        cancel_token: CancellationToken,
    ) -> (Receiver<ReadMessage>, JoinHandle<()>) {
        let (messages_tx, messages_rx) = mpsc::channel(2 * self.config.batch_size);

        let handle = tokio::spawn({
            let this = self.clone();
            async move {
                loop {
                    let permit = tokio::select! {
                        _ = cancel_token.cancelled() => {
                            warn!("Cancellation token is cancelled. Exiting JetstreamReader");
                            return;
                        }
                        permit = messages_tx.reserve_many(this.config.batch_size) => permit,
                    };

                    let mut permit = match permit {
                        Ok(permit) => permit,
                        Err(e) => {
                            // Channel is closed
                            error!("Error while reserving permits: {:?}", e);
                            return;
                        }
                    };

                    let messages_fut = this
                        .consumer
                        .fetch()
                        .max_messages(this.config.batch_size)
                        .expires(Duration::from_millis(10))
                        .messages();

                    let messages = tokio::select! {
                        _ = cancel_token.cancelled() => {
                            warn!("Cancellation token is cancelled. Exiting JetstreamReader");
                            return;
                        }
                        messages = messages_fut => messages,
                    };

                    let mut messages = match messages {
                        Ok(messages) => messages,
                        Err(e) => {
                            error!(?e, "Failed to fetch next batch of messages from Jetstream");
                            return;
                        }
                    };

                    while let Some(message) = messages.next().await {
                        let jetstream_message = match message {
                            Ok(message) => {
                                // info!("Message read from js consumer - {:?}", message);
                                message
                            }
                            Err(e) => {
                                error!(?e, "Failed to fetch messages from the Jetstream");
                                continue;
                            }
                        };

                        let mut message: Message =
                            match jetstream_message.payload.clone().try_into() {
                                Ok(message) => message,
                                Err(e) => {
                                    error!(
                                        ?e,
                                        "Failed to parse message payload received from Jetstream"
                                    );
                                    return;
                                }
                            };

                        let msg_info = jetstream_message
                            .info()
                            .map_err(|e| {
                                Error::ISB(format!(
                                    "Failed to get message info from Jetstream: {}",
                                    e
                                ))
                            })
                            .unwrap();

                        // Set the offset of the message to the sequence number of the message and the partition index.
                        message.offset = Some(Offset::Int(IntOffset::new(
                            msg_info.stream_sequence,
                            this.partition_idx,
                        )));

                        let (ack_tx, ack_rx) = oneshot::channel();

                        tokio::spawn(start_work_in_progress(
                            jetstream_message,
                            ack_rx,
                            this.config.wip_ack_interval,
                        ));

                        let read_message = ReadMessage {
                            message,
                            ack: ack_tx,
                        };
                        match permit.next() {
                            Some(permit) => {
                                permit.send(read_message);
                            }
                            None => {
                                // The number of permits >= number of messages in the batch.
                                unreachable!(
                                    "Permits should be reserved for all messages in the batch"
                                );
                            }
                        }
                    }
                }
            }
        });
        (messages_rx, handle)
    }
}

// Intended to be run as background task which will continuously send InProgress acks to Jetstream.
// We will continuously retry if there is an error in acknowledging the message as work-in-progress.
// If the sender end of the ack_rx channel was dropped before sending a final Ack or Nak (due to some unhandled/unknown failure), we will send a Nak to Jetstream.
async fn start_work_in_progress(
    msg: JetstreamMessage,
    mut ack_rx: oneshot::Receiver<ReadAck>,
    tick: Duration,
) {
    let mut interval = time::interval_at(Instant::now() + tick, tick);

    loop {
        let wip = async {
            interval.tick().await;
            let ack_result = msg.ack_with(AckKind::Progress).await;
            if let Err(e) = ack_result {
                // We expect that the ack in the next iteration will be successful.
                // If its some unrecoverable Jetstream error, the fetching messages in the JestreamReader implementation should also fail and cause the system to shut down.
                error!(?e, "Failed to send InProgress Ack to Jetstream for message");
            }
        };

        let ack = tokio::select! {
            ack = &mut ack_rx => ack,
            _ = wip => continue,
        };

        let ack = ack.unwrap_or_else(|e| {
            error!(?e, "Received error while waiting for Ack oneshot channel");
            ReadAck::Nak
        });

        match ack {
            ReadAck::Ack => {
                let ack_result = msg.ack().await;
                if let Err(e) = ack_result {
                    error!(?e, "Failed to send Ack to Jetstream for message");
                }
                return;
            }
            ReadAck::Nak => {
                let ack_result = msg.ack_with(AckKind::Nak(None)).await;
                if let Err(e) = ack_result {
                    error!(?e, "Failed to send Nak to Jetstream for message");
                }
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};
    use bytes::BytesMut;
    use chrono::Utc;
    use std::collections::HashMap;

    use super::*;
    use crate::message::{Message, MessageID, Offset};
    use crate::pipeline::isb::jetstream::writer::JetstreamWriter;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_read() {
        let js_url = "localhost:4222";
        // Create JetStream context
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let stream_name = "test_cancellation-2";
        context
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

        let buf_reader_config = BufferReaderConfig {
            name: stream_name.to_string(),
            partitions: 0,
            streams: vec![],
            batch_size: 2,
            wip_ack_interval: Duration::from_millis(5),
        };
        let (js_reader_handle, mut js_reader_rx) = JetstreamReader::new(
            stream_name.to_string(),
            0,
            context.clone(),
            buf_reader_config,
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

        for i in 0..10 {
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
            let (success_tx, success_rx) = oneshot::channel::<Result<Offset>>();
            let message_bytes: BytesMut = message.try_into().unwrap();
            writer.write(message_bytes.into(), success_tx).await;
            success_rx.await.unwrap().unwrap();
        }
        // Cancel the token to exit the retry loop
        cancel_token.cancel();

        let mut buffer = vec![];
        for i in 0..10 {
            let Some(val) = js_reader_rx.recv().await else {
                break;
            };
            buffer.push(val);
        }
        assert_eq!(
            buffer.len(),
            10,
            "Expected 10 messages from the Jestream reader"
        );
        js_reader_handle.cancel();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(js_reader_rx.is_closed());

        context.delete_stream(stream_name).await.unwrap();
    }
}
