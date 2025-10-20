use std::collections::HashMap;

use bytes::Bytes;
use pulsar::{Authentication, Producer, Pulsar, SerializeMessage, TokioExecutor, producer};
use tracing::info;

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
    let mut pulsar = Pulsar::builder(&config.addr, TokioExecutor);
    match config.auth {
        Some(PulsarAuth::JWT(token)) => {
            let auth_token = Authentication {
                name: "token".into(),
                data: token.into(),
            };
            pulsar = pulsar.with_auth(auth_token);
        }
        Some(PulsarAuth::HTTPBasic { username, password }) => {
            let auth_token = Authentication {
                name: "basic".into(),
                data: format!("{username}:{password}").into(),
            };
            pulsar = pulsar.with_auth(auth_token);
        }
        None => info!("No authentication mechanism specified for Pulsar"),
    }

    let pulsar = pulsar.build().await.map_err(Error::Pulsar)?;
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
            // this function returns a SendFuture because the receipt may come long after this function was called
            let confirm_status = self.producer.send_non_blocking(message).await?;
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

/// Expose methods so that numaflow-core crate doesn't have to depend on pulsar.
#[cfg(feature = "pulsar-tests-utils")]
pub mod test_utils {
    use pulsar::consumer::InitialPosition;
    use pulsar::{ConsumerOptions, Pulsar, SubType, TokioExecutor};
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::time::timeout;
    use tokio_stream::StreamExt;
    use uuid::Uuid;

    pub async fn setup_test_topic() -> (String, String) {
        let topic_name = format!(
            "persistent://public/default/pulsar_sink_{}",
            Uuid::new_v4().to_string().replace("-", "")
        );
        let subscription_name = format!(
            "sink_subscription_{}",
            Uuid::new_v4().to_string().replace("-", "")
        );

        (topic_name, subscription_name)
    }

    /// The message received from Pulsar.
    pub struct TestPulsarMessage {
        pub properties: HashMap<String, String>,
        pub payload: String,
        pub event_time: Option<u64>,
    }

    pub async fn consume_messages_from_topic(
        topic: &str,
        subscription: &str,
        count: usize,
    ) -> Vec<TestPulsarMessage> {
        let pulsar = Pulsar::builder("pulsar://localhost:6650", TokioExecutor)
            .build()
            .await
            .expect("Failed to connect to Pulsar");

        let mut consumer = pulsar
            .consumer()
            .with_topic(topic)
            .with_subscription_type(SubType::Exclusive)
            .with_options(
                ConsumerOptions::default().with_initial_position(InitialPosition::Earliest),
            )
            .with_subscription(subscription)
            .build::<String>()
            .await
            .expect("Failed to create consumer");

        let mut messages = Vec::with_capacity(count);
        let mut received = 0;
        while received < count {
            let result = timeout(Duration::from_secs(5), consumer.next()).await;
            assert!(result.is_ok(), "Did not receive message from Pulsar");
            let msg = result
                .unwrap()
                .expect("No message received from Pulsar")
                .expect("Pulsar error");
            let payload = String::from_utf8_lossy(&msg.payload.data).to_string();
            let properties = msg
                .metadata()
                .properties
                .iter()
                .map(|kv| (kv.key.clone(), kv.value.clone()))
                .collect();
            let event_time = msg.metadata().event_time;

            messages.push(TestPulsarMessage {
                properties,
                payload,
                event_time,
            });
            received += 1;
        }
        messages
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    #[cfg(all(feature = "pulsar-tests", feature = "pulsar-tests-utils"))]
    #[tokio::test]
    async fn test_pulsar_sink_send_and_consume() {
        let (topic_name, subscription_name) = test_utils::setup_test_topic().await;
        let config = Config {
            addr: "pulsar://localhost:6650".to_string(),
            topic: topic_name.clone(),
            producer_name: "test-producer".to_string(),
            auth: None,
        };
        let mut sink = new_sink(config).await.expect("Failed to create PulsarSink");

        let properties = HashMap::from([
            ("property1".to_string(), "value1".to_string()),
            ("property2".to_string(), "value2".to_string()),
        ]);

        let msg = Message {
            id: "msg1".to_string(),
            properties: properties.clone(),
            event_time_epoch_ms: 1234567890,
            payload: Bytes::from("test-payload"),
        };

        let responses = sink
            .sink_messages(vec![msg])
            .await
            .expect("Failed to send messages");
        assert_eq!(responses.len(), 1);
        assert!(responses[0].status.is_ok());

        // Now consume the message from Pulsar to verify
        let messages =
            test_utils::consume_messages_from_topic(&topic_name, &subscription_name, 1).await;
        let msg = &messages[0];
        assert_eq!(msg.payload, "test-payload");
        assert_eq!(msg.properties.get("property1"), Some(&"value1".to_string()));
        assert_eq!(msg.properties.get("property2"), Some(&"value2".to_string()));
        assert_eq!(msg.event_time, Some(1234567890));
    }

    #[cfg(feature = "pulsar-tests")]
    #[tokio::test]
    async fn test_pulsar_sink_multiple_messages() {
        let (topic_name, subscription_name) = test_utils::setup_test_topic().await;
        let config = Config {
            addr: "pulsar://localhost:6650".to_string(),
            topic: topic_name.clone(),
            producer_name: "test-producer-multi".to_string(),
            auth: None,
        };
        let mut sink = new_sink(config).await.expect("Failed to create PulsarSink");

        let mut messages = Vec::new();
        for i in 0..5 {
            let mut properties = HashMap::new();
            properties.insert("index".to_string(), i.to_string());
            properties.insert("test".to_string(), "multiple".to_string());

            messages.push(Message {
                id: format!("msg{}", i),
                properties,
                event_time_epoch_ms: 1234567890 + i,
                payload: Bytes::from(format!("payload-{}", i)),
            });
        }

        let responses = sink
            .sink_messages(messages)
            .await
            .expect("Failed to send messages");
        assert_eq!(responses.len(), 5);
        for resp in responses {
            assert!(resp.status.is_ok());
        }

        // Consume all messages
        let messages =
            test_utils::consume_messages_from_topic(&topic_name, &subscription_name, 5).await;
        let mut seen_payloads = std::collections::HashSet::new();
        for msg in messages {
            seen_payloads.insert(msg.payload);
        }
        for i in 0..5 {
            assert!(seen_payloads.contains(&format!("payload-{}", i)));
        }
    }

    #[cfg(feature = "pulsar-tests")]
    #[tokio::test]
    async fn test_pulsar_sink_empty_messages() {
        let (topic_name, _) = test_utils::setup_test_topic().await;
        let config = Config {
            addr: "pulsar://localhost:6650".to_string(),
            topic: topic_name,
            producer_name: "test-producer-empty".to_string(),
            auth: None,
        };
        let mut sink = new_sink(config).await.expect("Failed to create PulsarSink");

        let responses = sink
            .sink_messages(vec![])
            .await
            .expect("Failed to send empty messages");
        assert_eq!(responses.len(), 0);
    }

    #[cfg(feature = "pulsar-tests")]
    #[tokio::test]
    async fn test_pulsar_sink_large_payload() {
        let (topic_name, subscription_name) = test_utils::setup_test_topic().await;
        let config = Config {
            addr: "pulsar://localhost:6650".to_string(),
            topic: topic_name.clone(),
            producer_name: "test-producer-large".to_string(),
            auth: None,
        };
        let mut sink = new_sink(config).await.expect("Failed to create PulsarSink");

        // Create a large payload (1KB)
        let large_payload = vec![b'x'; 1024];
        let msg = Message {
            id: "large-msg".to_string(),
            properties: HashMap::new(),
            event_time_epoch_ms: 1234567890,
            payload: Bytes::from(large_payload.clone()),
        };

        let responses = sink
            .sink_messages(vec![msg])
            .await
            .expect("Failed to send large message");
        assert_eq!(responses.len(), 1);
        assert!(responses[0].status.is_ok());

        // Verify the large message was received correctly
        let messages =
            test_utils::consume_messages_from_topic(&topic_name, &subscription_name, 1).await;
        let msg = &messages[0];
        assert_eq!(msg.payload.as_bytes(), large_payload);
    }
}
