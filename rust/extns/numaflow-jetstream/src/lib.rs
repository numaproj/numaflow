use std::{collections::HashMap, time::Duration};

use async_nats::jetstream::Message as JetstreamMessage;
use async_nats::{
    jetstream::consumer::{
        pull::{Config, Stream},
        Consumer, PullConsumer,
    },
    ConnectOptions,
};
use bytes::Bytes;
use tokio_stream::StreamExt;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Connecting to NATS {server} - {error}")]
    Connection { server: String, error: String },

    #[error("Jestream - {0}")]
    Jetstream(String),

    #[error("{0}")]
    Other(String),
}

pub struct NatsAuth {
    pub username: String,
    pub password: String,
}
pub struct JetstreamSourceConfig {
    pub addr: String,
    pub stream: String,
    pub consumer: String,
    pub auth: Option<NatsAuth>,
    read_timeout: Duration,
}

pub struct Jetstream {
    consumer: Consumer<Config>,
    messages: Stream,
    read_timeout: Duration,
}

#[derive(Debug)]
pub struct Message {
    pub value: Bytes,
    pub stream_sequence: u64,
    pub headers: HashMap<String, String>,
}

impl TryFrom<JetstreamMessage> for Message {
    type Error = Error;
    fn try_from(msg: JetstreamMessage) -> Result<Self> {
        let headers = match msg.message.headers.as_ref() {
            Some(headers) => headers
                .iter()
                .map(|(k, v)| (k.to_string(), v[0].as_str().to_string())) //NOTE: we are only using the first value of the header
                .collect(),
            None => HashMap::new(),
        };

        let stream_sequence = msg
            .info()
            .map_err(|e| {
                Error::Jetstream(format!("fetching message metadata from Jetstream: {e:?}"))
            })?
            .stream_sequence;

        Ok(Message {
            value: msg.message.payload,
            stream_sequence,
            headers,
        })
    }
}

impl Jetstream {
    async fn connect(config: JetstreamSourceConfig) -> Result<Self> {
        let mut conn_opts = ConnectOptions::new();
        if let Some(auth) = config.auth {
            conn_opts = conn_opts.user_and_password(auth.username, auth.password);
        }
        let client = async_nats::connect_with_options(&config.addr, conn_opts)
            .await
            .map_err(|err| Error::Connection {
                server: config.addr.to_string(),
                error: err.to_string(),
            })?;

        let js_ctx = async_nats::jetstream::new(client);
        let consumer: PullConsumer = js_ctx
            .get_consumer_from_stream(&config.stream, &config.consumer)
            .await
            .map_err(|err| {
                Error::Jetstream(format!(
                    "Getting consumer {} from stream {}: {err:?}",
                    config.consumer, config.stream
                ))
            })?;
        let mut message_stream = consumer.messages().await.unwrap();
        Ok(Self {
            consumer,
            messages: message_stream,
            read_timeout: config.read_timeout,
        })
    }

    async fn read_messages(&mut self) -> Result<Vec<Message>> {
        let mut messages: Vec<Message> = vec![];
        let timeout = tokio::time::timeout(self.read_timeout, std::future::pending::<()>());
        tokio::pin!(timeout);
        loop {
            tokio::select! {
                biased;

                _ = &mut timeout => {
                    break;
                }

                message = self.messages.next() => {
                    let Some(message) = message else {
                        break;
                    };
                    let message = message
                        .map_err(|e| Error::Jetstream(format!("Getting next message from the stream: {e:?}")))?
                        .try_into()
                        .map_err(|e| Error::Jetstream(format!("converting raw Jestreams message as Jetstream source message: {e:?}")))?;
                    messages.push(message);
                }
            }
        }
        Ok(messages)
    }
}
