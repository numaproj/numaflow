use numaflow_kafka::sink::{KafkaSink, KafkaSinkMessage, KafkaSinkResponse};

use crate::error::{Error, Result};
use crate::message::Message;
use crate::sink::{ResponseFromSink, ResponseStatusFromSink, Sink};

impl TryFrom<Message> for KafkaSinkMessage {
    type Error = Error;

    fn try_from(mut msg: Message) -> Result<Self> {
        let id = msg.id.to_string();
        msg.headers
            .insert(format!("__key_len"), msg.keys.len().to_string());
        for (i, key) in msg.keys.iter().enumerate() {
            msg.headers.insert(format!("__key_{i}"), key.to_string());
        }
        let partition_key = if msg.keys.is_empty() {
            None
        } else {
            Some(msg.keys.join(":"))
        };
        Ok(Self {
            id,
            partition_key,
            headers: msg.headers,
            payload: msg.value,
        })
    }
}

impl From<KafkaSinkResponse> for ResponseFromSink {
    fn from(resp: KafkaSinkResponse) -> Self {
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

impl Sink for KafkaSink {
    async fn sink(&mut self, messages: Vec<Message>) -> Result<Vec<ResponseFromSink>> {
        let kafka_messages: Vec<KafkaSinkMessage> = messages
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
