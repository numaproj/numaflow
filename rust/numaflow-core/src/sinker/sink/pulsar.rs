use numaflow_pulsar::sink::{
    Message as PulsarMessage, Response as PulsarResponse, Sink as PulsarSink,
};
use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::message::Message;
use crate::sinker::sink::{ResponseFromSink, ResponseStatusFromSink, Sink};

impl TryFrom<Message> for PulsarMessage {
    type Error = Error;

    fn try_from(msg: Message) -> Result<Self> {
        let mut headers: HashMap<String, String> = msg
            .headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        let id = msg.id.to_string();
        // Add all message keys to the Pulsar properties
        // We do the same for Kafka sink with Kafka headers.
        headers.insert("__key_len".to_string(), msg.keys.len().to_string());
        for (i, key) in msg.keys.iter().enumerate() {
            headers.insert(format!("__key_{i}"), key.to_string());
        }

        let event_time = msg.event_time.timestamp_millis();
        // The pulsar client library uses u64 for event time.
        // So the value can not be earlier than 1970-01-01 00:00:00 UTC.
        if event_time < 0 {
            return Err(Error::Sink(format!("Event time is negative: {event_time}")));
        }

        Ok(Self {
            id,
            properties: headers,
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
            },
            Err(e) => ResponseFromSink {
                id: resp.id,
                status: ResponseStatusFromSink::Failed(e.to_string()),
            },
        }
    }
}

impl Sink for PulsarSink {
    async fn sink(&mut self, messages: Vec<Message>) -> Result<Vec<ResponseFromSink>> {
        let pulsar_messages: Vec<PulsarMessage> = messages
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_>>()?;
        Ok(self
            .sink_messages(pulsar_messages)
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
    use chrono::{TimeZone, Utc};
    use numaflow_pulsar::sink::{Config, new_sink};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_message(
        id: &str,
        keys: Vec<String>,
        value: &str,
        event_time: i64,
        headers: HashMap<String, String>,
    ) -> Message {
        Message {
            typ: Default::default(),
            keys: Arc::from(keys),
            tags: None,
            value: Bytes::from(value.to_string()),
            offset: Offset::Int(IntOffset::new(event_time, 0)),
            event_time: Utc.timestamp_millis_opt(event_time).unwrap(),
            watermark: None,
            id: MessageID {
                vertex_name: "test-vertex".to_string().into(),
                offset: id.to_string().into(),
                index: 0,
            },
            headers: Arc::new(headers),
            ..Default::default()
        }
    }

    #[test]
    fn test_message_to_pulsar_message_conversion() {
        let headers = HashMap::from([("custom_header".to_string(), "custom_value".to_string())]);

        let message = create_test_message(
            "msg1",
            vec!["key1".to_string(), "key2".to_string()],
            "test-payload",
            1234567890,
            headers,
        );

        let pulsar_message: PulsarMessage = message.try_into().expect("Conversion should succeed");

        assert_eq!(pulsar_message.id, "test-vertex-msg1-0");
        assert_eq!(pulsar_message.payload, Bytes::from("test-payload"));
        assert_eq!(pulsar_message.event_time_epoch_ms, 1234567890);
        assert_eq!(
            pulsar_message.properties.get("custom_header"),
            Some(&"custom_value".to_string())
        );
        assert_eq!(
            pulsar_message.properties.get("__key_len"),
            Some(&"2".to_string())
        );
        assert_eq!(
            pulsar_message.properties.get("__key_0"),
            Some(&"key1".to_string())
        );
        assert_eq!(
            pulsar_message.properties.get("__key_1"),
            Some(&"key2".to_string())
        );
    }

    #[test]
    fn test_message_to_pulsar_message_with_empty_keys() {
        let message = create_test_message(
            "msg2",
            vec![],
            "empty-keys-payload",
            1234567890,
            HashMap::new(),
        );

        let pulsar_message: PulsarMessage = message.try_into().expect("Conversion should succeed");

        assert_eq!(pulsar_message.id, "test-vertex-msg2-0");
        assert_eq!(pulsar_message.payload, Bytes::from("empty-keys-payload"));
        assert_eq!(pulsar_message.event_time_epoch_ms, 1234567890);
        assert_eq!(
            pulsar_message.properties.get("__key_len"),
            Some(&"0".to_string())
        );
    }

    #[test]
    fn test_message_to_pulsar_message_negative_event_time() {
        let message = create_test_message(
            "msg3",
            vec![],
            "test-payload",
            -1000, // Negative event time
            HashMap::new(),
        );

        let result: Result<PulsarMessage> = message.try_into();
        assert!(result.is_err());
        match result {
            Err(Error::Sink(msg)) => {
                assert!(msg.contains("Event time is negative"));
            }
            _ => panic!("Expected Sink error"),
        }
    }

    #[test]
    fn test_pulsar_response_to_response_from_sink_success() {
        let pulsar_response = PulsarResponse {
            id: "test-id".to_string(),
            status: Ok(()),
        };

        let response: ResponseFromSink = pulsar_response.into();

        assert_eq!(response.id, "test-id");
        assert!(matches!(response.status, ResponseStatusFromSink::Success));
    }

    #[test]
    fn test_pulsar_response_to_response_from_sink_failure() {
        let pulsar_response = PulsarResponse {
            id: "test-id".to_string(),
            status: Err(numaflow_pulsar::Error::Pulsar(pulsar::Error::Custom(
                "test error".to_string(),
            ))),
        };

        let response: ResponseFromSink = pulsar_response.into();

        assert_eq!(response.id, "test-id");
        match response.status {
            ResponseStatusFromSink::Failed(error_msg) => {
                assert!(error_msg.contains("test error"));
            }
            _ => panic!("Expected Failed status"),
        }
    }

    #[cfg(feature = "pulsar-tests")]
    #[tokio::test]
    async fn test_pulsar_sink_multiple_messages() {
        let (topic_name, subscription_name) =
            numaflow_pulsar::sink::test_utils::setup_test_topic().await;
        let config = Config {
            addr: "pulsar://localhost:6650".to_string(),
            topic: topic_name.clone(),
            producer_name: "test-producer-multi".to_string(),
            auth: None,
        };
        let mut sink = new_sink(config).await.expect("Failed to create PulsarSink");

        let mut messages = Vec::new();
        for i in 0..5 {
            let mut headers = HashMap::new();
            headers.insert("index".to_string(), i.to_string());
            headers.insert("test".to_string(), "multiple".to_string());

            messages.push(create_test_message(
                &format!("msg{}", i),
                vec![format!("key{}", i)],
                &format!("payload-{}", i),
                1234567890 + i,
                headers,
            ));
        }

        let responses = sink.sink(messages).await.expect("Failed to send messages");
        assert_eq!(responses.len(), 5);
        for resp in responses {
            assert!(matches!(resp.status, ResponseStatusFromSink::Success));
        }

        // Consume all messages to verify they were sent correctly
        let consumed_messages = numaflow_pulsar::sink::test_utils::consume_messages_from_topic(
            &topic_name,
            &subscription_name,
            5,
        )
        .await;

        let mut seen_payloads = std::collections::HashSet::new();
        for msg in consumed_messages {
            seen_payloads.insert(msg.payload);
        }
        for i in 0..5 {
            assert!(seen_payloads.contains(&format!("payload-{}", i)));
        }
    }

    #[cfg(feature = "pulsar-tests")]
    #[tokio::test]
    async fn test_pulsar_sink_empty_messages() {
        let (topic_name, _) = numaflow_pulsar::sink::test_utils::setup_test_topic().await;
        let config = Config {
            addr: "pulsar://localhost:6650".to_string(),
            topic: topic_name,
            producer_name: "test-producer-empty".to_string(),
            auth: None,
        };
        let mut sink = new_sink(config).await.expect("Failed to create PulsarSink");

        let responses = sink
            .sink(vec![])
            .await
            .expect("Failed to send empty messages");
        assert_eq!(responses.len(), 0);
    }

    #[cfg(feature = "pulsar-tests")]
    #[tokio::test]
    async fn test_pulsar_sink_with_keys_and_headers() {
        let (topic_name, subscription_name) =
            numaflow_pulsar::sink::test_utils::setup_test_topic().await;
        let config = Config {
            addr: "pulsar://localhost:6650".to_string(),
            topic: topic_name.clone(),
            producer_name: "test-producer-keys".to_string(),
            auth: None,
        };
        let mut sink = new_sink(config).await.expect("Failed to create PulsarSink");

        let mut headers = HashMap::new();
        headers.insert("source".to_string(), "test".to_string());
        headers.insert("version".to_string(), "1.0".to_string());

        let message = create_test_message(
            "keyed-msg",
            vec!["user_id".to_string(), "session_id".to_string()],
            "keyed-payload",
            1234567890,
            headers,
        );

        let responses = sink
            .sink(vec![message])
            .await
            .expect("Failed to send message");
        assert_eq!(responses.len(), 1);
        assert!(matches!(
            responses[0].status,
            ResponseStatusFromSink::Success
        ));

        // Verify the message was sent with correct properties
        let consumed_messages = numaflow_pulsar::sink::test_utils::consume_messages_from_topic(
            &topic_name,
            &subscription_name,
            1,
        )
        .await;

        let msg = &consumed_messages[0];
        assert_eq!(msg.payload, "keyed-payload");
        assert_eq!(msg.properties.get("source"), Some(&"test".to_string()));
        assert_eq!(msg.properties.get("version"), Some(&"1.0".to_string()));
        assert_eq!(msg.properties.get("__key_len"), Some(&"2".to_string()));
        assert_eq!(msg.properties.get("__key_0"), Some(&"user_id".to_string()));
        assert_eq!(
            msg.properties.get("__key_1"),
            Some(&"session_id".to_string())
        );
        assert_eq!(msg.event_time, Some(1234567890));
    }

    #[cfg(feature = "pulsar-tests")]
    #[tokio::test]
    async fn test_pulsar_sink_large_payload() {
        let (topic_name, subscription_name) =
            numaflow_pulsar::sink::test_utils::setup_test_topic().await;
        let config = Config {
            addr: "pulsar://localhost:6650".to_string(),
            topic: topic_name.clone(),
            producer_name: "test-producer-large".to_string(),
            auth: None,
        };
        let mut sink = new_sink(config).await.expect("Failed to create PulsarSink");

        // Create a large payload (1KB)
        let large_payload = vec![b'x'; 1024];
        let message = Message {
            typ: Default::default(),
            keys: Arc::from(vec![]),
            tags: None,
            value: Bytes::from(large_payload.clone()),
            offset: Offset::Int(IntOffset::new(1234567890, 0)),
            event_time: Utc.timestamp_millis_opt(1234567890).unwrap(),
            watermark: None,
            id: MessageID {
                vertex_name: "test-vertex".to_string().into(),
                offset: "large-msg".to_string().into(),
                index: 0,
            },
            headers: Arc::new(HashMap::new()),
            ..Default::default()
        };

        let responses = sink
            .sink(vec![message])
            .await
            .expect("Failed to send large message");
        assert_eq!(responses.len(), 1);
        assert!(matches!(
            responses[0].status,
            ResponseStatusFromSink::Success
        ));

        // Verify the large message was received correctly
        let consumed_messages = numaflow_pulsar::sink::test_utils::consume_messages_from_topic(
            &topic_name,
            &subscription_name,
            1,
        )
        .await;

        let msg = &consumed_messages[0];
        assert_eq!(msg.payload.as_bytes(), large_payload);
    }

    #[cfg(feature = "pulsar-tests")]
    #[tokio::test]
    async fn test_pulsar_sink_event_time_handling() {
        let (topic_name, subscription_name) =
            numaflow_pulsar::sink::test_utils::setup_test_topic().await;
        let config = Config {
            addr: "pulsar://localhost:6650".to_string(),
            topic: topic_name.clone(),
            producer_name: "test-producer-time".to_string(),
            auth: None,
        };
        let mut sink = new_sink(config).await.expect("Failed to create PulsarSink");

        // Test with a specific event time
        let event_time = 1640995200000; // 2022-01-01 00:00:00 UTC
        let message = create_test_message(
            "time-msg",
            vec![],
            "time-test-payload",
            event_time,
            HashMap::new(),
        );

        let responses = sink
            .sink(vec![message])
            .await
            .expect("Failed to send message");
        assert_eq!(responses.len(), 1);
        assert!(matches!(
            responses[0].status,
            ResponseStatusFromSink::Success
        ));

        // Verify the event time was preserved
        let consumed_messages = numaflow_pulsar::sink::test_utils::consume_messages_from_topic(
            &topic_name,
            &subscription_name,
            1,
        )
        .await;

        let msg = &consumed_messages[0];
        assert_eq!(msg.event_time, Some(event_time as u64));
    }
}
