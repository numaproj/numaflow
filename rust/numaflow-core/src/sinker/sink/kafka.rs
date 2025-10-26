use numaflow_kafka::sink::{KafkaSink, KafkaSinkMessage, KafkaSinkResponse};
use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::message::Message;
use crate::sinker::sink::{ResponseFromSink, ResponseStatusFromSink, Sink};

impl TryFrom<Message> for KafkaSinkMessage {
    type Error = Error;

    fn try_from(msg: Message) -> Result<Self> {
        let mut headers: HashMap<String, String> = msg
            .headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        let id = msg.id.to_string();
        // Add all message keys to the Kafka headers
        headers.insert("__key_len".to_string(), msg.keys.len().to_string());
        for (i, key) in msg.keys.iter().enumerate() {
            headers.insert(format!("__key_{i}"), key.to_string());
        }

        let partition_key = if msg.keys.is_empty() {
            None
        } else {
            Some(msg.keys.join(":"))
        };

        Ok(Self {
            id,
            partition_key,
            headers,
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
            },
            Err(e) => ResponseFromSink {
                id: resp.id,
                status: ResponseStatusFromSink::Failed(e.to_string()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{IntOffset, Message, MessageID, Offset};
    use bytes::Bytes;
    use chrono::Utc;
    use numaflow_kafka::sink::test_utils;
    use numaflow_kafka::sink::{KafkaSinkConfig, new_sink};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[cfg(feature = "kafka-tests")]
    #[tokio::test]
    async fn test_kafka_sink_multiple_messages() {
        let (_producer, topic_name) = test_utils::setup_test_topic().await;
        let config = KafkaSinkConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: topic_name.clone(),
            auth: None,
            tls: None,
            set_partition_key: false,
            kafka_raw_config: HashMap::new(),
        };
        let mut sink = new_sink(config).expect("Failed to create KafkaSink");
        let mut messages = Vec::new();
        for i in 0..5 {
            let mut headers = HashMap::new();
            headers.insert("header".to_string(), format!("value{}", i));
            messages.push(Message {
                typ: Default::default(),
                keys: Arc::from(vec![]),
                tags: None,
                value: Bytes::from(format!("payload-{}", i)),
                offset: Offset::Int(IntOffset::new(i, 0)),
                event_time: Utc::now(),
                watermark: None,
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: i.to_string().into(),
                    index: i as i32,
                },
                headers: Arc::new(headers),
                ..Default::default()
            });
        }
        let responses = sink.sink(messages).await.expect("Failed to send messages");
        assert_eq!(responses.len(), 5);
        for resp in responses {
            assert!(matches!(resp.status, ResponseStatusFromSink::Success));
        }
        // Consume all messages
        let messages = test_utils::consume_messages_from_topic(&topic_name, 5).await;
        let mut seen_payloads = std::collections::HashSet::new();
        for msg in messages {
            seen_payloads.insert(String::from_utf8_lossy(&msg.payload).to_string());
        }
        for i in 0..5 {
            assert!(seen_payloads.contains(&format!("payload-{}", i)));
        }
    }
}
