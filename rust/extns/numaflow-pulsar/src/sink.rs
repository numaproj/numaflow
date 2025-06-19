use std::collections::HashMap;

use bytes::Bytes;
use pulsar::{Producer, Pulsar, SerializeMessage, TokioExecutor, producer};

use crate::{Error, PulsarAuth, Result};

pub struct Sink {
    producer: Producer<TokioExecutor>,
}

/// Configuration for creating a Pulsar producer
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    /// Pulsar broker address
    pub addr: String,
    /// The topic to send messages to
    pub topic: String,
    /// The name of the producer
    pub producer_name: String,
    /// The authentication mechanism to use for the Pulsar producer
    pub auth: Option<PulsarAuth>,
}

/// The message to send to a Pulsar topic
pub struct Message {
    /// ID of the message
    pub id: String,
    /// User-defined propreties of the message
    pub properties: HashMap<String, String>,
    /// Event time of the message. Epoch time in milliseconds
    pub event_time_epoch_ms: u64,
    /// The message payload
    pub payload: Bytes,
}

impl SerializeMessage for Message {
    fn serialize_message(input: Self) -> std::result::Result<producer::Message, pulsar::Error> {
        Ok(producer::Message {
            payload: input.payload.to_vec(),
            properties: input.properties,
            event_time: Some(input.event_time_epoch_ms),
            ..Default::default()
        })
    }
}

/// The result of sending a message to Pulsar broker.
pub struct Response {
    /// id of the corresponding original message
    pub id: String,
    /// Status of the send operation
    pub status: Result<()>,
}

pub async fn new_sink(config: Config) -> Result<Sink> {
    let pulsar = Pulsar::builder(&config.addr, TokioExecutor)
        .build()
        .await
        .map_err(Error::Pulsar)?;
    let producer = pulsar
        .producer()
        .with_topic(&config.topic)
        .with_name(&config.producer_name)
        .build()
        .await
        .map_err(Error::Pulsar)?;

    producer.check_connection().await?;
    Ok(Sink { producer })
}

impl Sink {
    pub async fn sink_messages(&mut self, messages: Vec<Message>) -> Result<Vec<Response>> {
        let mut responses = Vec::with_capacity(messages.len());
        let mut server_confirmation = Vec::with_capacity(messages.len());
        for message in messages {
            let id = message.id.clone();
            let confirm_status = self.producer.send_non_blocking(message).await.unwrap();
            server_confirmation.push((id, confirm_status));
        }
        for (id, confirm_status) in server_confirmation {
            let status = match confirm_status.await {
                Ok(_) => Response { id, status: Ok(()) },
                Err(e) => Response {
                    id,
                    status: Err(Error::Pulsar(e)),
                },
            };
            responses.push(status);
        }
        Ok(responses)
    }
}
