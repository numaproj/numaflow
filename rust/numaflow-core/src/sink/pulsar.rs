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
        // Add all message keys to the Kafka headers
        msg.headers
            .insert("__key_len".to_string(), msg.keys.len().to_string());
        for (i, key) in msg.keys.iter().enumerate() {
            msg.headers.insert(format!("__key_{i}"), key.to_string());
        }

        Ok(Self {
            id,
            properties: msg.headers,
            payload: msg.value,
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
