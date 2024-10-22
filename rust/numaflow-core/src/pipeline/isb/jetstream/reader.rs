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

        Ok(this)
    }
    // 1 buffer - multiple streams

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
