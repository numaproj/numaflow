use std::time::Duration;

use async_nats::jetstream::{consumer::PullConsumer, Context};
use futures::StreamExt;
use tokio::sync::mpsc;

use crate::config::pipeline::isb::jetstream::StreamReaderConfig;
use crate::message::Message;
use crate::Error;

pub(super) struct JetstreamReader {
    config: StreamReaderConfig,
    consumer: PullConsumer,
    messages: mpsc::Sender<Message>,
}

impl JetstreamReader {
    pub(super) async fn new(
        js_ctx: Context,
        cfg: StreamReaderConfig,
        messages: mpsc::Sender<Message>,
    ) -> Self {
        let consumer: PullConsumer = js_ctx
            .get_consumer_from_stream(&cfg.name, &cfg.name)
            .await
            .unwrap(); // FIXME:

        Self {
            consumer,
            config: cfg,
            messages,
        }
    }

    //TODO: Error handling
    pub(super) async fn start(&mut self) {
        loop {
            let mut permit = self
                .messages
                .reserve_many(self.config.batch_size)
                .await
                .expect("Waiting for permits on the buffer channel failed");

            let messages = self
                .consumer
                .fetch()
                .max_messages(self.config.batch_size)
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
                let message = match message {
                    Ok(message) => message,
                    Err(e) => {
                        continue;
                    }
                };
                let message: Message = match message.payload.clone().try_into() {
                    Ok(message) => message,
                    Err(e) => {
                        continue;
                    }
                };
                match permit.next() {
                    Some(permit) => {
                        permit.send(message);
                    }
                    None => {
                        continue;
                    }
                }
            }
        }
    }
}
