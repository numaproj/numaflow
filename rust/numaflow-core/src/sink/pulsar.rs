use numaflow_pulsar::sink::{
    Message as PulsarMessage, Response as PulsarResponse, Sink as PulsarSink,
};

use crate::error::{Error, Result};
use crate::message::Message;
use crate::sink::{ResponseFromSink, ResponseStatusFromSink, Sink};

impl TryFrom<Message> for PulsarMessage {
    type Error = Error;

    fn try_from(mut msg: Message) -> Result<Self> {
        let id = msg.id.to_string();
        // Add all message keys to the Pulsar properties
        // We do the same for Kafka sink with Kafka headers.
        msg.headers
            .insert("__key_len".to_string(), msg.keys.len().to_string());
        for (i, key) in msg.keys.iter().enumerate() {
            msg.headers.insert(format!("__key_{i}"), key.to_string());
        }

        let event_time = msg.event_time.timestamp_millis();
        // The pulsar client library uses u64 for event time.
        // So the value can not be earlier than 1970-01-01 00:00:00 UTC.
        if event_time < 0 {
            return Err(Error::Sink(format!("Event time is negative: {event_time}")));
        }

        Ok(Self {
            id,
            properties: msg.headers,
            payload: msg.value,
            event_time_epoch_ms: event_time as u64,
        })
    }
}

impl From<PulsarResponse> for ResponseFromSink {
    fn from(resp: PulsarResponse) -> Self {
        match &resp.status {
            Ok(_) => ResponseFromSink {
                id: resp.id,
                status: ResponseStatusFromSink::Success,
                serve_response: None,
            },
            Err(e) => ResponseFromSink {
                id: resp.id,
                status: ResponseStatusFromSink::Failed(e.to_string()),
                serve_response: None,
            },
        }
    }
}

impl Sink for PulsarSink {
    async fn sink(&mut self, messages: Vec<Message>) -> Result<Vec<ResponseFromSink>> {
        let kafka_messages: Vec<PulsarMessage> = messages
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_>>()?;
        Ok(self
            .sink_messages(kafka_messages)
            .await
            .map_err(|e| Error::Sink(e.to_string()))?
            .into_iter()
            .map(|msg| msg.into())
            .collect::<Vec<ResponseFromSink>>())
    }
}
