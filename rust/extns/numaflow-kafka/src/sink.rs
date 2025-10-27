use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use futures::{StreamExt, stream::FuturesUnordered};
use rdkafka::{
    ClientConfig,
    config::RDKafkaLogLevel,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
};
use tracing::info;

use crate::{KafkaSaslAuth, TlsConfig};

#[derive(Debug, Clone, PartialEq)]
pub struct KafkaSinkConfig {
    /// The Kafka brokers to connect to.
    pub brokers: Vec<String>,
    /// The Kafka topic to send messages to.
    pub topic: String,
    /// The authentication mechanism to use for the Kafka consumer.
    pub auth: Option<KafkaSaslAuth>,
    /// The TLS configuration for the Kafka consumer.
    pub tls: Option<TlsConfig>,
    /// Whether to set the partition key when sending the messages to Kafka.
    /// Whether to set the partition key when sending messages to Kafka.
    /// When set to true, messages will be routed to specific partitions based on their partition_key.
    /// The partition key is constructed using the list of keys associated with a message.
    /// This ensures that messages with the same keys always go to the same partition,
    /// which is useful for maintaining message ordering for related messages.
    /// When set to false, messages will be distributed across partitions randomly.
    pub set_partition_key: bool,
    /// Any supported kafka client configuration options from
    /// https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html
    pub kafka_raw_config: HashMap<String, String>,
}

/// The Kafka sink client.
pub struct KafkaSink {
    topic: String,
    producer: FutureProducer,
    set_partition_key: bool,
}

/// The result of a sink operation corresponding to a [KafkaSinkMessage] with same id.
pub struct KafkaSinkResponse {
    /// ID of the message that was sent
    pub id: String,
    /// Status of the send operation
    pub status: crate::Result<()>,
}

/// The message to send to Kafka. Input to [KafkaSink::sink_messages].
pub struct KafkaSinkMessage {
    /// ID of the message
    pub id: String,
    /// The partition key for the message. If not provided, the message will be sent to a random partition.
    /// This is only used if [KafkaSinkConfig::set_partition_key] is true.
    pub partition_key: Option<String>,
    /// The headers to send with the message.
    pub headers: HashMap<String, String>,
    /// The message payload
    pub payload: Bytes,
}

/// Create a new Kafka sink client.
pub fn new_sink(config: KafkaSinkConfig) -> crate::Result<KafkaSink> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("message.timeout.ms", "5000")
        .set("client.id", "numaflow-kafka-sink");
    if !config.kafka_raw_config.is_empty() {
        info!(
            "Applying user-specified kafka config: {}",
            config
                .kafka_raw_config
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<String>>()
                .join(", ")
        );
        for (key, value) in config.kafka_raw_config {
            client_config.set(key, value);
        }
    }
    client_config
        .set("bootstrap.servers", config.brokers.join(","))
        .set_log_level(RDKafkaLogLevel::Warning);

    crate::update_auth_config(&mut client_config, config.tls, config.auth);

    let producer: FutureProducer = client_config
        .create()
        .map_err(|e| crate::Error::Kafka(format!("Failed to create producer: {e}")))?;

    Ok(KafkaSink {
        producer,
        topic: config.topic,
        set_partition_key: config.set_partition_key,
    })
}

impl KafkaSink {
    /// Send the messages to Kafka.
    pub async fn sink_messages(
        &mut self,
        messages: Vec<KafkaSinkMessage>,
    ) -> crate::Result<Vec<KafkaSinkResponse>> {
        // Create futures for all send operations and concurrently await on them
        let mut send_futures = FuturesUnordered::new();
        let message_count = messages.len();
        for msg in messages {
            let fut = async {
                let KafkaSinkMessage {
                    id,
                    partition_key,
                    headers: inp_headers,
                    payload,
                } = msg;
                let mut headers = OwnedHeaders::new();
                for (key, value) in inp_headers {
                    headers = headers.insert(Header {
                        key: &key,
                        value: Some(&value),
                    });
                }
                let mut record: FutureRecord<'_, String, _> = FutureRecord::to(&self.topic)
                    .headers(headers)
                    .payload(payload.as_ref());

                // The set_partition_key option comes from the user configuration.
                // The partition key for a message is constructed by message.keys.join(":").
                // Messages with same partition key will always go to the same partition on Kafka.
                // If the partition key is not provided, the message will be sent to a random partition.
                if self.set_partition_key
                    && let Some(ref partition_key) = partition_key
                {
                    record = record.key(partition_key);
                }
                match self.producer.send(record, Duration::from_secs(1)).await {
                    Ok(_) => KafkaSinkResponse { id, status: Ok(()) },
                    Err(e) => {
                        tracing::error!(?e, "Sending payload to Kafka topic");
                        KafkaSinkResponse {
                            id,
                            status: Err(crate::Error::Kafka(format!(
                                "Sending payload to kafka: {e:?}"
                            ))),
                        }
                    }
                }
            };
            send_futures.push(fut);
        }
        let mut results = Vec::with_capacity(message_count);
        while let Some(status) = send_futures.next().await {
            results.push(status);
        }
        Ok(results)
    }
}

/// Expose methods so that numaflow-core crate doesn't have to depend on rdkafka.
#[cfg(feature = "kafka-tests-utils")]
pub mod test_utils {
    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::message::Headers;
    use rdkafka::message::Message;
    use rdkafka::producer::FutureProducer;
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::time::timeout;
    use uuid::Uuid;

    pub async fn setup_test_topic() -> (FutureProducer, String) {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .create()
            .expect("Failed to create producer");

        let topic_name = format!(
            "kafka_sink_test_topic_{}",
            Uuid::new_v4().to_string().replace("-", "")
        );

        // Create topic if it doesn't exist
        let admin_client = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .create::<AdminClient<_>>()
            .expect("Failed to create admin client");

        let topic_config = NewTopic::new(topic_name.as_str(), 1, TopicReplication::Fixed(1));
        let _ = admin_client
            .create_topics(&[topic_config], &AdminOptions::new())
            .await
            .expect("Failed to create topic");

        (producer, topic_name)
    }

    /// The message received from Kafka.
    pub struct TestKafkaMessage {
        pub headers: HashMap<String, String>,
        pub payload: Vec<u8>,
    }

    pub async fn consume_messages_from_topic(topic: &str, count: usize) -> Vec<TestKafkaMessage> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("group.id", format!("test-consumer-{}", Uuid::new_v4()))
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Failed to create consumer");
        consumer.subscribe(&[topic]).expect("Failed to subscribe");
        let mut messages = Vec::with_capacity(count);
        let mut received = 0;
        while received < count {
            let result = timeout(Duration::from_secs(10), consumer.recv()).await;
            assert!(result.is_ok(), "Did not receive message from Kafka");
            let msg = result.unwrap().expect("Kafka error");
            let payload = msg.payload().unwrap_or(&[]).to_vec();
            let mut headers = HashMap::new();
            if let Some(kafka_headers) = msg.headers() {
                for header in kafka_headers.iter() {
                    if let (Some(value), key) = (header.value, header.key) {
                        headers.insert(key.to_string(), String::from_utf8_lossy(value).to_string());
                    }
                }
            }
            messages.push(TestKafkaMessage { headers, payload });
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

    #[cfg(all(feature = "kafka-tests", feature = "kafka-tests-utils"))]
    #[tokio::test]
    async fn test_kafka_sink_send_and_consume() {
        let (_producer, topic_name) = test_utils::setup_test_topic().await;
        let config = KafkaSinkConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: topic_name.clone(),
            auth: None,
            tls: None,
            set_partition_key: true,
            kafka_raw_config: HashMap::from([
                ("connections.max.idle.ms".to_string(), "540000".to_string()), // 9 minutes, default value
            ]),
        };
        let mut sink = new_sink(config).expect("Failed to create KafkaSink");
        let mut headers = HashMap::new();
        headers.insert("header1".to_string(), "value1".to_string());
        headers.insert("header2".to_string(), "value2".to_string());
        let msg = KafkaSinkMessage {
            id: "msg1".to_string(),
            partition_key: Some("key1".to_string()),
            headers: headers.clone(),
            payload: Bytes::from("test-payload"),
        };
        let responses = sink
            .sink_messages(vec![msg])
            .await
            .expect("Failed to send messages");
        assert_eq!(responses.len(), 1);
        assert!(responses[0].status.is_ok());

        // Now consume the message from Kafka to verify
        let messages = test_utils::consume_messages_from_topic(&topic_name, 1).await;
        let msg = &messages[0];
        assert_eq!(msg.payload, b"test-payload");
        assert_eq!(msg.headers.get("header1"), Some(&"value1".to_string()));
        assert_eq!(msg.headers.get("header2"), Some(&"value2".to_string()));
    }

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
            messages.push(KafkaSinkMessage {
                id: format!("msg{}", i),
                partition_key: None,
                headers,
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
