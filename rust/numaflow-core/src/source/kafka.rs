use std::sync::Arc;
use std::time::Duration;

use numaflow_kafka::{KafkaMessage, KafkaSource, KafkaSourceConfig};

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::error::Error;
use crate::message::{Message, MessageID, Offset, StringOffset};
use crate::source;

impl TryFrom<KafkaMessage> for Message {
    type Error = Error;

    fn try_from(message: KafkaMessage) -> crate::Result<Self> {
        let offset = Offset::String(StringOffset::new(
            format!("{}:{}:{}", message.topic, message.partition, message.offset),
            *get_vertex_replica(),
        ));

        Ok(Message {
            typ: Default::default(),
            keys: Arc::from(vec![]),
            tags: None,
            value: message.value,
            offset: offset.clone(),
            event_time: Default::default(),
            watermark: None,
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: offset.to_string().into(),
                index: 0,
            },
            headers: message.headers,
            metadata: None,
            is_late: false,
        })
    }
}

impl From<numaflow_kafka::Error> for Error {
    fn from(value: numaflow_kafka::Error) -> Self {
        match value {
            numaflow_kafka::Error::Kafka(e) => Error::Source(e.to_string()),
            numaflow_kafka::Error::Connection { server, error } => Error::Source(format!(
                "Failed to connect to Kafka server: {server} - {error}"
            )),
            numaflow_kafka::Error::Other(e) => Error::Source(e),
        }
    }
}

pub(crate) async fn new_kafka_source(
    cfg: KafkaSourceConfig,
    batch_size: usize,
    timeout: Duration,
) -> crate::Result<KafkaSource> {
    Ok(KafkaSource::connect(cfg, batch_size, timeout).await?)
}

impl source::SourceReader for KafkaSource {
    fn name(&self) -> &'static str {
        "Kafka"
    }

    async fn read(&mut self) -> crate::Result<Vec<Message>> {
        self.read_messages()
            .await?
            .into_iter()
            .map(|msg| msg.try_into())
            .collect()
    }

    async fn partitions(&mut self) -> crate::error::Result<Vec<u16>> {
        let partitions = self.partitions_info().await?;
        Ok(partitions.into_iter().map(|p| p as u16).collect())
    }
}

impl source::SourceAcker for KafkaSource {
    async fn ack(&mut self, offsets: Vec<Offset>) -> crate::error::Result<()> {
        let mut kafka_offsets = Vec::with_capacity(offsets.len());
        for offset in offsets {
            let Offset::String(string_offset) = offset else {
                return Err(Error::Source(format!(
                    "Expected Offset::String type for Kafka. offset={offset:?}"
                )));
            };

            let offset = String::from_utf8_lossy(&string_offset.offset);
            let parts: Vec<&str> = offset.split(':').collect();
            if parts.len() != 3 {
                return Err(Error::Source(format!(
                    "Invalid Kafka offset format. Expected format: <topic>:<partition>:<offset>. offset={offset:?}"
                )));
            }
            let topic = parts[0].to_string();
            let partition = parts[1].parse::<i32>().map_err(|e| {
                Error::Source(format!(
                    "invalid partition id. kafka_offset={offset}, error={e:?}"
                ))
            })?;

            let partition_offset = parts[2].parse::<i64>().map_err(|e| {
                Error::Source(format!(
                    "invalid offset id. kafka_offset={offset}, error={e:?}"
                ))
            })?;
            kafka_offsets.push(numaflow_kafka::KafkaOffset {
                topic,
                partition,
                offset: partition_offset,
            });
        }
        self.ack_messages(kafka_offsets).await.map_err(Into::into)
    }
}

impl source::LagReader for KafkaSource {
    async fn pending(&mut self) -> crate::error::Result<Option<usize>> {
        Ok(self.pending_messages().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use numaflow_kafka::{KafkaMessage, test_utils};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_try_from_kafka_message_success() {
        let kafka_message = KafkaMessage {
            topic: "test_topic".to_string(),
            value: Bytes::from("test_value"),
            partition: 1,
            offset: 42,
            headers: {
                let mut headers = HashMap::new();
                headers.insert("key".to_string(), "value".to_string());
                headers
            },
        };

        let message: Message = kafka_message.try_into().unwrap();

        assert_eq!(message.value, Bytes::from("test_value"));
        assert_eq!(message.offset.to_string(), "test_topic:1:42-0");
        assert_eq!(message.headers.get("key"), Some(&"value".to_string()));
    }

    #[cfg(feature = "kafka-tests")]
    #[tokio::test]
    async fn test_kafka_source_reader_acker_lagreader() {
        use crate::{
            reader::LagReader,
            source::{SourceAcker, SourceReader},
        };

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // Setup Kafka producer and topic
        let (producer, topic_name) = test_utils::setup_test_topic().await;

        // Publish messages to the topic
        test_utils::produce_test_messages(&producer, &topic_name, 50).await;

        // Configure KafkaSource
        let config = numaflow_kafka::KafkaSourceConfig {
            brokers: vec!["localhost:9092".to_string()],
            topics: vec![topic_name.clone()],
            consumer_group: "test_consumer_group".to_string(),
            auth: None,
            tls: None,
        };

        let read_timeout = Duration::from_secs(5);
        let mut source = super::new_kafka_source(config, 20, read_timeout)
            .await
            .unwrap();

        assert_eq!(source.partitions().await.unwrap(), vec![0]);

        // Test SourceReader::read
        let messages = source.read().await.unwrap();
        assert_eq!(messages.len(), 20, "Should read 20 messages in a batch");
        assert_eq!(messages[0].value, Bytes::from("message 0"));
        assert_eq!(messages[19].value, Bytes::from("message 19"));

        // Test SourceAcker::ack
        let offsets: Vec<Offset> = messages.iter().map(|msg| msg.offset.clone()).collect();
        source.ack(offsets).await.unwrap();

        // Test LagReader::pending
        let pending = source.pending().await.unwrap();
        assert_eq!(
            pending,
            Some(30),
            "Pending messages should be 30 after acking 20 messages"
        );

        // Read and ack remaining messages
        let messages = source.read().await.unwrap();
        assert_eq!(messages.len(), 20, "Should read another 20 messages");
        let offsets: Vec<Offset> = messages.iter().map(|msg| msg.offset.clone()).collect();
        source.ack(offsets).await.unwrap();

        let pending = source.pending().await.unwrap();
        assert_eq!(
            pending,
            Some(10),
            "Pending messages should be 10 after acking another 20 messages"
        );

        let messages = source.read().await.unwrap();
        assert_eq!(messages.len(), 10, "Should read the last 10 messages");
        let offsets: Vec<Offset> = messages.iter().map(|msg| msg.offset.clone()).collect();
        source.ack(offsets).await.unwrap();

        let pending = source.pending().await.unwrap();
        assert_eq!(
            pending,
            Some(0),
            "Pending messages should be 0 after acking all messages"
        );
    }
}
