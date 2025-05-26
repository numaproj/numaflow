use bytes::Bytes;
use futures::{StreamExt, stream::FuturesUnordered};
use rdkafka::{
    ClientConfig,
    config::RDKafkaLogLevel,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
};
use std::{collections::HashMap, time::Duration};

use crate::{KafkaSaslAuth, TlsConfig};

#[derive(Debug, Clone, PartialEq)]
pub struct KafkaSinkConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    /// The authentication mechanism to use for the Kafka consumer.
    pub auth: Option<KafkaSaslAuth>,
    /// The TLS configuration for the Kafka consumer.
    pub tls: Option<TlsConfig>,
    /// Whether to set the partition key for the Kafka sink.
    pub set_partition_key: bool,
}

pub struct KafkaSink {
    topic: String,
    producer: FutureProducer,
    set_partition_key: bool,
}

pub struct KafkaSinkResponse {
    /// ID of the message that was sent
    pub id: String,
    /// Status of the send operation
    pub status: crate::Result<()>,
}

pub struct KafkaSinkMessage {
    pub id: String,
    pub partition_key: Option<String>,
    pub headers: HashMap<String, String>,
    pub payload: Bytes,
}

pub fn new_sink(config: KafkaSinkConfig) -> crate::Result<KafkaSink> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", config.brokers.join(","))
        .set("message.timeout.ms", "5000")
        .set("client.id", "numaflow-kafka-sink")
        .set_log_level(RDKafkaLogLevel::Warning);

    crate::update_auth_config(&mut client_config, config.tls, config.auth);

    let producer: FutureProducer = client_config
        .create()
        .map_err(|e| crate::Error::Kafka(format!("Failed to create producer: {}", e)))?;

    Ok(KafkaSink {
        producer,
        topic: config.topic,
        set_partition_key: config.set_partition_key,
    })
}

impl KafkaSink {
    pub async fn sink_messages(
        &mut self,
        messages: Vec<KafkaSinkMessage>,
    ) -> crate::Result<Vec<KafkaSinkResponse>> {
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
                if self.set_partition_key {
                    if let Some(ref partition_key) = partition_key {
                        record = record.key(partition_key);
                    }
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

#[cfg(test)]
mod test_utils {
    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::producer::FutureProducer;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rdkafka::ClientConfig;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::message::Headers;
    use rdkafka::message::Message;
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::time::timeout;

    #[cfg(feature = "kafka-tests")]
    #[tokio::test]
    async fn test_kafka_sink_send_and_consume() {
        let (_producer, topic_name) = test_utils::setup_test_topic().await;
        let config = KafkaSinkConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: topic_name.clone(),
            auth: None,
            tls: None,
            set_partition_key: true,
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
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set(
                "group.id",
                format!("test-consumer-{}", uuid::Uuid::new_v4()),
            )
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Failed to create consumer");
        consumer
            .subscribe(&[&topic_name])
            .expect("Failed to subscribe");
        let result = timeout(Duration::from_secs(10), consumer.recv()).await;
        assert!(result.is_ok(), "Did not receive message from Kafka");
        let msg = result.unwrap().expect("Kafka error");
        assert_eq!(msg.payload().unwrap(), b"test-payload");
        assert_eq!(msg.key().unwrap(), b"key1");
        let kafka_headers = msg.headers().unwrap();
        let mut found_headers = HashMap::new();
        for header in kafka_headers.iter() {
            found_headers.insert(
                header.key,
                String::from_utf8_lossy(header.value.unwrap()).to_string(),
            );
        }
        assert_eq!(found_headers.get("header1"), Some(&"value1".to_string()));
        assert_eq!(found_headers.get("header2"), Some(&"value2".to_string()));
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
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set(
                "group.id",
                format!("test-consumer-{}", uuid::Uuid::new_v4()),
            )
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Failed to create consumer");
        consumer
            .subscribe(&[&topic_name])
            .expect("Failed to subscribe");
        let mut received = 0;
        let mut seen_payloads = std::collections::HashSet::new();
        while received < 5 {
            let result = timeout(Duration::from_secs(10), consumer.recv()).await;
            assert!(result.is_ok(), "Did not receive message from Kafka");
            let msg = result.unwrap().expect("Kafka error");
            seen_payloads.insert(String::from_utf8_lossy(msg.payload().unwrap()).to_string());
            received += 1;
        }
        for i in 0..5 {
            assert!(seen_payloads.contains(&format!("payload-{}", i)));
        }
    }
}
