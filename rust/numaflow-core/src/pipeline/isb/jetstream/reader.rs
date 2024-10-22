use std::time::Duration;

use async_nats::jetstream::{
    consumer::PullConsumer, AckKind, Context, Message as JetstreamMessage,
};
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

use crate::config::pipeline::isb::BufferReaderConfig;
use crate::message::{Message, ReadAck, ReadMessage};

// The JestreamReader is a handle to the background actor that continuously fetches messages from Jetstream.
// It can be used to cancel the background task and stop reading from Jetstream.
// The sender end of the channel is not stored in this struct, since the struct is clone-able and the mpsc channel is only closed when all the senders are dropped.
// Storing the Sender end of channel in this struct would make it difficult to close the channel with `cancel` method.
#[derive(Clone)]
pub(super) struct JetstreamReader {
    config: BufferReaderConfig,
    consumer: PullConsumer,
    ctx: CancellationToken,
}

impl JetstreamReader {
    pub(super) async fn new(
        js_ctx: Context,
        cfg: BufferReaderConfig,
    ) -> (Self, mpsc::Receiver<ReadMessage>) {
        let consumer: PullConsumer = js_ctx
            .get_consumer_from_stream(&cfg.name, &cfg.name)
            .await
            .unwrap(); // FIXME:

        let ctx = CancellationToken::new();
        let this = Self {
            config: cfg.clone(),
            consumer,
            ctx,
        };

        let (messages_tx, messages_rx) = mpsc::channel(2 * cfg.batch_size);
        tokio::spawn({
            let this = this.clone();
            async move {
                this.start(messages_tx).await;
            }
        });
        (this, messages_rx)
    }

    // Stop reading from Jetstream and close the sender end of the channel.
    pub(crate) async fn cancel(&self) {
        self.ctx.cancel();
    }

    // When we encounter an error, we log the error and return from the function. This drops the sender end of the channel.
    // The closing of the channel should propagate to the receiver end and the receiver should exit gracefully.
    // Within the loop, we only consider cancellationToken cancellation during the permit reservation and fetching messages,
    // since rest of the operations should finish immediately.
    pub(super) async fn start(&self, messages: mpsc::Sender<ReadMessage>) {
        loop {
            let permit = tokio::select! {
                _ = self.ctx.cancelled() => {
                    warn!("Cancellation token is cancelled. Exiting JetstreamReader");
                    return;
                }
                permit = messages.reserve_many(self.config.batch_size) => permit,
            };

            let mut permit = match permit {
                Ok(permit) => permit,
                Err(e) => {
                    // Channel is closed
                    error!("Error while reserving permits: {:?}", e);
                    return;
                }
            };

            let messages_fut = self
                .consumer
                .fetch()
                .max_messages(self.config.batch_size)
                .expires(Duration::from_millis(10))
                .messages();

            let messages = tokio::select! {
                _ = self.ctx.cancelled() => {
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
                    Ok(message) => message,
                    Err(e) => {
                        error!(?e, "Failed to fetch messages from the Jetstream");
                        continue;
                    }
                };
                let message: Message = match jetstream_message.payload.clone().try_into() {
                    Ok(message) => message,
                    Err(e) => {
                        error!(
                            ?e,
                            "Failed to parse message payload received from Jetstream"
                        );
                        return;
                    }
                };

                let (ack_tx, ack_rx) = oneshot::channel();

                tokio::spawn(start_work_in_progress(
                    jetstream_message,
                    ack_rx,
                    self.config.wip_ack_interval,
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
                        unreachable!("Permits should be reserved for all messages in the batch");
                    }
                }
            }
        }
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
