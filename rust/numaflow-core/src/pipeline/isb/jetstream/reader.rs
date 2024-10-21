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
    error: Option<Error>, // handler_messages: mpsc::Receiver<ReaderActorMessage>,
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
            // handler_messages,
            error: None,
            messages,
        }
    }

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
                    self.error = Some(Error::ISB(e.to_string()));
                    return;
                }
            };

            while let Some(message) = messages.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(e) => {
                        self.error = Some(Error::ISB(e.to_string()));
                        return;
                    }
                };
                let message: Message = match message.payload.clone().try_into() {
                    Ok(message) => message,
                    Err(e) => {
                        self.error = Some(Error::ISB(e.to_string()));
                        return;
                    }
                };
                match permit.next() {
                    Some(permit) => {
                        permit.send(message);
                    }
                    None => {
                        self.error = Some(Error::ISB("No permit available".to_string()));
                        return;
                    }
                }
            }
        }
    }
}
