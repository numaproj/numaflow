use std::time::Duration;

use async_nats::jetstream::{consumer::PullConsumer, Context, Message as JetstreamMessage};
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{self, Instant};

use crate::config::pipeline::isb::jetstream::StreamReaderConfig;
use crate::message::{Message, ReadAck, ReadMessage};

pub(super) struct JetstreamReader {
    config: StreamReaderConfig,
    consumer: PullConsumer,
}

impl JetstreamReader {
    pub(super) async fn init(
        js_ctx: Context,
        cfg: StreamReaderConfig,
    ) -> mpsc::Receiver<ReadMessage> {
        let consumer: PullConsumer = js_ctx
            .get_consumer_from_stream(&cfg.name, &cfg.name)
            .await
            .unwrap(); // FIXME:
        let (messages_tx, messages_rx) = mpsc::channel(2 * cfg.batch_size);
        tokio::spawn(Self::start(cfg, messages_tx, consumer));
        messages_rx
    }

    //TODO: Error handling
    pub(super) async fn start(
        cfg: StreamReaderConfig,
        messages: mpsc::Sender<ReadMessage>,
        consumer: PullConsumer,
    ) {
        loop {
            let mut permit = messages
                .reserve_many(cfg.batch_size)
                .await
                .expect("Waiting for permits on the buffer channel failed");

            let messages = consumer
                .fetch()
                .max_messages(cfg.batch_size)
                .expires(Duration::from_millis(10))
                .messages()
                .await;

            let mut messages = match messages {
                Ok(messages) => messages,
                Err(e) => {
                    continue;
                }
            };

            while let Some(message) = messages.next().await {
                let jetstream_message = match message {
                    Ok(message) => message,
                    Err(e) => {
                        continue;
                    }
                };
                let message: Message = match jetstream_message.payload.clone().try_into() {
                    Ok(message) => message,
                    Err(e) => {
                        continue;
                    }
                };
                let (ack_tx, ack_rx) = oneshot::channel();

                tokio::spawn(start_work_in_progress(
                    jetstream_message,
                    ack_rx,
                    cfg.wip_acks,
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
                        continue;
                    }
                }
            }
        }
    }
}

async fn start_work_in_progress(
    msg: JetstreamMessage,
    mut ack_rx: oneshot::Receiver<ReadAck>,
    tick: Duration,
) {
    let mut interval = time::interval_at(Instant::now() + tick, tick);

    loop {
        let wip = async {
            interval.tick().await;
            msg.ack_with(async_nats::jetstream::AckKind::Progress)
                .await
                .unwrap(); // FIXME: error handling
        };

        tokio::select! {
            _ = &mut ack_rx => {
                return;
            }
            _ = wip => {},
        }
    }
}
