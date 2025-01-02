use crate::config;
use crate::error::Error;
use crate::message::{Message, MessageID};
use async_nats::HeaderValue;
use chrono::Utc;
use std::collections::HashMap;

/// JetStream Writer is responsible for writing messages to JetStream ISB.
/// it exposes both sync and async methods to write messages. It has gates
/// to prevent writing into the buffer if the buffer is full. After successful
/// writes, it will let the callee know the status (or return a non-retryable
/// exception).
pub(crate) mod writer;

pub(crate) mod reader;

/// Stream is a combination of stream name and partition id.
type Stream = (String, u16);

impl TryFrom<async_nats::Message> for Message {
    type Error = Error;

    fn try_from(message: async_nats::Message) -> Result<Self, Self::Error> {
        let payload = message.payload;
        let headers: HashMap<String, String> = message
            .headers
            .unwrap_or_default()
            .iter()
            .map(|(key, value)| {
                (
                    key.to_string(),
                    value.first().unwrap_or(&HeaderValue::from("")).to_string(),
                )
            })
            .collect();
        // FIXME(cr): we should not be using subject. keys are in the payload
        let keys = message.subject.split('.').map(|s| s.to_string()).collect();
        let event_time = Utc::now();
        let offset = None;
        let id = MessageID {
            vertex_name: config::get_vertex_name().to_string().into(),
            offset: "0".to_string().into(),
            index: 0,
        };

        Ok(Self {
            keys,
            tags: None,
            value: payload,
            offset,
            event_time,
            id,
            headers,
        })
    }
}
