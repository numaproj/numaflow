use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use numaflow_kafka::ConsumerErrorKind;
use numaflow_kafka::source::{KafkaMessage, KafkaSource, KafkaSourceConfig};
use tracing::info;

use crate::config::get_vertex_name;
use crate::error::{Error, SourceFailureAction, SourceFailureImpact};
use crate::message::{Message, MessageID, NackOffset, Offset, StringOffset};
use crate::metadata::Metadata;
use crate::source;
use crate::source::builtin::{BuiltinSourceBackend, ConnectFactory, SourceBackend};
use tokio_util::sync::CancellationToken;

impl TryFrom<KafkaMessage> for Message {
    type Error = Error;

    fn try_from(message: KafkaMessage) -> crate::Result<Self> {
        let offset = Offset::String(StringOffset::new(
            format!("{}:{}:{}", message.topic, message.partition, message.offset),
            message.global_partition_id,
        ));

        // Use Kafka timestamp if available, otherwise fall back to current time
        let event_time = match message.timestamp {
            Some(timestamp_millis) => DateTime::from_timestamp_millis(timestamp_millis)
                .unwrap_or_else(|| {
                    tracing::warn!(
                        timestamp_millis = timestamp_millis,
                        "Invalid Kafka timestamp, falling back to current time"
                    );
                    Utc::now()
                }),
            None => {
                tracing::debug!("Kafka message has no timestamp, using current time");
                Utc::now()
            }
        };

        Ok(Message {
            typ: Default::default(),
            keys: Arc::from(message.key.map(|k| vec![k]).unwrap_or_default()),
            tags: None,
            value: message.value,
            offset: offset.clone(),
            event_time,
            watermark: None,
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: offset.to_string().into(),
                index: 0,
            },
            headers: Arc::new(message.headers),
            // Set default metadata so that metadata is always present.
            metadata: Some(Arc::new(Metadata::default())),
            is_late: false,
            nack_options: None,
        })
    }
}

impl From<numaflow_kafka::Error> for Error {
    fn from(value: numaflow_kafka::Error) -> Self {
        match value {
            numaflow_kafka::Error::Kafka(message) => source_redrive(
                SourceFailureAction::RetrySame,
                SourceFailureImpact::Outage,
                "kafka_transport",
                message,
            ),
            numaflow_kafka::Error::Connection { server, error } => source_redrive(
                SourceFailureAction::StayDegraded,
                SourceFailureImpact::Outage,
                "kafka_config",
                format!("Failed to create Kafka client for {server}: {error}"),
            ),
            numaflow_kafka::Error::Consumer { kind, message } => {
                let (action, impact, code) = match kind {
                    ConsumerErrorKind::RebalanceInProgress => (
                        SourceFailureAction::RetrySame,
                        SourceFailureImpact::Benign,
                        "kafka_rebalance_in_progress",
                    ),
                    ConsumerErrorKind::CoordinatorUnavailable => (
                        SourceFailureAction::RetrySame,
                        SourceFailureImpact::Benign,
                        "kafka_coordinator_unavailable",
                    ),
                    ConsumerErrorKind::UnknownMemberId => (
                        SourceFailureAction::Recreate,
                        SourceFailureImpact::Outage,
                        "kafka_unknown_member_id",
                    ),
                    ConsumerErrorKind::IllegalGeneration => (
                        SourceFailureAction::Recreate,
                        SourceFailureImpact::Outage,
                        "kafka_illegal_generation",
                    ),
                    ConsumerErrorKind::FencedInstanceId => (
                        SourceFailureAction::Recreate,
                        SourceFailureImpact::Outage,
                        "kafka_fenced_instance_id",
                    ),
                    ConsumerErrorKind::FencedMemberEpoch => (
                        SourceFailureAction::Recreate,
                        SourceFailureImpact::Outage,
                        "kafka_fenced_member_epoch",
                    ),
                    ConsumerErrorKind::StaleMemberEpoch => (
                        SourceFailureAction::Recreate,
                        SourceFailureImpact::Outage,
                        "kafka_stale_member_epoch",
                    ),
                    ConsumerErrorKind::GroupAuthorizationFailed => (
                        SourceFailureAction::StayDegraded,
                        SourceFailureImpact::Outage,
                        "kafka_group_authorization_failed",
                    ),
                    ConsumerErrorKind::TopicAuthorizationFailed => (
                        SourceFailureAction::StayDegraded,
                        SourceFailureImpact::Outage,
                        "kafka_topic_authorization_failed",
                    ),
                    ConsumerErrorKind::ClusterAuthorizationFailed => (
                        SourceFailureAction::StayDegraded,
                        SourceFailureImpact::Outage,
                        "kafka_cluster_authorization_failed",
                    ),
                    ConsumerErrorKind::SaslAuthenticationFailed => (
                        SourceFailureAction::StayDegraded,
                        SourceFailureImpact::Outage,
                        "kafka_sasl_authentication_failed",
                    ),
                };
                source_redrive(action, impact, code, message)
            }
            numaflow_kafka::Error::Other(message) => source_redrive(
                SourceFailureAction::Recreate,
                SourceFailureImpact::Outage,
                "kafka_internal",
                message,
            ),
        }
    }
}

fn source_redrive(
    action: SourceFailureAction,
    impact: SourceFailureImpact,
    code: &'static str,
    message: impl Into<String>,
) -> Error {
    Error::SourceRedrive {
        source_name: "Kafka",
        operation: "backend",
        action,
        impact,
        code,
        message: message.into(),
    }
}

pub(crate) async fn new_kafka_source(
    cfg: KafkaSourceConfig,
    batch_size: usize,
    timeout: Duration,
    cancel_token: tokio_util::sync::CancellationToken,
) -> crate::Result<KafkaSource> {
    Ok(KafkaSource::connect(cfg, batch_size, timeout, cancel_token).await?)
}

pub(crate) fn new_kafka_source_factory(
    config: KafkaSourceConfig,
    batch_size: usize,
    timeout: Duration,
    cancel_token: CancellationToken,
) -> ConnectFactory {
    ConnectFactory::new("Kafka", move || {
        let config = config.clone();
        let cancel_token = cancel_token.clone();
        async move {
            let source = new_kafka_source(config, batch_size, timeout, cancel_token).await?;
            let source_to_retire = source.clone();
            Ok(Box::new(SourceBackend::with_retire(source, move || {
                let source = source_to_retire.clone();
                async move {
                    source.shutdown().await;
                }
            })) as Box<dyn BuiltinSourceBackend>)
        }
    })
}

impl source::SourceReader for KafkaSource {
    fn name(&self) -> &'static str {
        "Kafka"
    }

    async fn read(&mut self) -> Option<crate::Result<Vec<Message>>> {
        match self.read_messages().await {
            Some(Ok(messages)) => {
                let result: crate::Result<Vec<Message>> =
                    messages.into_iter().map(|msg| msg.try_into()).collect();
                Some(result)
            }
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }

    async fn partitions(&mut self) -> crate::error::Result<source::SourcePartitions> {
        let partitions_info = self.partitions_info().await?;
        Ok(source::SourcePartitions::new(
            partitions_info.active_partitions,
            Some(partitions_info.total_partitions),
        ))
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
            let topic = (*parts.first().expect("should have topic part")).to_string();
            let partition = parts
                .get(1)
                .expect("should have partition part")
                .parse::<i32>()
                .map_err(|e| {
                    Error::Source(format!(
                        "invalid partition id. kafka_offset={offset}, error={e:?}"
                    ))
                })?;

            let partition_offset = parts
                .get(2)
                .expect("should have offset part")
                .parse::<i64>()
                .map_err(|e| {
                    Error::Source(format!(
                        "invalid offset id. kafka_offset={offset}, error={e:?}"
                    ))
                })?;
            kafka_offsets.push(numaflow_kafka::source::KafkaOffset {
                topic,
                partition,
                offset: partition_offset,
            });
        }
        self.ack_messages(kafka_offsets).await.map_err(Into::into)
    }

    async fn nack(&mut self, offsets: Vec<NackOffset>) -> crate::error::Result<()> {
        info!(?offsets, "Nack invoked for offsets (no-op for Kafka)");
        // Kafka doesn't support nack - no-op
        Ok(())
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
    use numaflow_kafka::source::{KafkaMessage, test_utils};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_try_from_kafka_message_success() {
        let kafka_message = KafkaMessage {
            topic: "test_topic".to_string(),
            value: Bytes::from("test_value"),
            partition: 1,
            global_partition_id: 1,
            offset: 42,
            key: Some("test_key".to_string()),
            headers: {
                let mut headers = HashMap::new();
                headers.insert("key".to_string(), "value".to_string());
                headers
            },
            timestamp: Some(1640995200000), // 2022-01-01 00:00:00 UTC in milliseconds
        };

        let message: Message = kafka_message.try_into().unwrap();

        assert_eq!(message.value, Bytes::from("test_value"));
        // The offset format is "<topic>:<partition>:<offset>-<partition_idx>"
        // where partition_idx is the Kafka partition number
        assert_eq!(message.offset.to_string(), "test_topic:1:42-1");
        assert_eq!(message.headers.get("key"), Some(&"value".to_string()));
        // Verify that the event time is set from the Kafka timestamp
        assert_eq!(message.event_time.timestamp_millis(), 1640995200000);
    }

    #[tokio::test]
    async fn test_try_from_kafka_message_no_timestamp() {
        let kafka_message = KafkaMessage {
            topic: "test_topic".to_string(),
            value: Bytes::from("test_value"),
            partition: 1,
            global_partition_id: 1,
            offset: 42,
            key: None,
            headers: HashMap::new(),
            timestamp: None, // No timestamp available
        };

        let before_conversion = Utc::now();
        let message: Message = kafka_message.try_into().unwrap();
        let after_conversion = Utc::now();

        assert_eq!(message.value, Bytes::from("test_value"));
        // The offset format is "<topic>:<partition>:<offset>-<partition_idx>"
        // where partition_idx is the Kafka partition number
        assert_eq!(message.offset.to_string(), "test_topic:1:42-1");
        // Verify that the event time falls back to current time when no timestamp is available
        assert!(message.event_time >= before_conversion);
        assert!(message.event_time <= after_conversion);
    }

    #[tokio::test]
    async fn test_try_from_kafka_message_invalid_timestamp() {
        let kafka_message = KafkaMessage {
            topic: "test_topic".to_string(),
            value: Bytes::from("test_value"),
            partition: 1,
            global_partition_id: 1,
            offset: 42,
            key: None,
            headers: HashMap::new(),
            timestamp: Some(i64::MAX), // Invalid timestamp that will cause overflow
        };

        let before_conversion = Utc::now();
        let message: Message = kafka_message.try_into().unwrap();
        let after_conversion = Utc::now();

        assert_eq!(message.value, Bytes::from("test_value"));
        // The offset format is "<topic>:<partition>:<offset>-<partition_idx>"
        // where partition_idx is the Kafka partition number
        assert_eq!(message.offset.to_string(), "test_topic:1:42-1");
        // Verify that the event time falls back to current time when timestamp is invalid
        assert!(message.event_time >= before_conversion);
        assert!(message.event_time <= after_conversion);
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
        let config = numaflow_kafka::source::KafkaSourceConfig {
            brokers: vec!["localhost:9092".to_string()],
            topics: vec![topic_name.clone()],
            consumer_group: "test_consumer_group".to_string(),
            auth: None,
            tls: None,
            kafka_raw_config: HashMap::new(),
        };

        let read_timeout = Duration::from_secs(5);
        let mut source = super::new_kafka_source(
            config,
            20,
            read_timeout,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();

        // Test SourceReader::read
        let messages = source.read().await.unwrap().unwrap();
        assert_eq!(messages.len(), 20, "Should read 20 messages in a batch");
        assert_eq!(
            messages
                .first()
                .expect("Expected at least one message")
                .value,
            Bytes::from("message 0")
        );
        assert_eq!(
            messages
                .last()
                .expect("Expected at least one message")
                .value,
            Bytes::from("message 19")
        );

        // Query partition info after reading messages
        let source_partitions = source.partitions().await.unwrap();
        assert_eq!(source_partitions.active_partitions, vec![0]);
        assert_eq!(source_partitions.total_partitions, Some(1));

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
        let messages = source.read().await.unwrap().unwrap();
        assert_eq!(messages.len(), 20, "Should read another 20 messages");
        let offsets: Vec<Offset> = messages.iter().map(|msg| msg.offset.clone()).collect();
        source.ack(offsets).await.unwrap();

        let pending = source.pending().await.unwrap();
        assert_eq!(
            pending,
            Some(10),
            "Pending messages should be 10 after acking another 20 messages"
        );

        let messages = source.read().await.unwrap().unwrap();
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

    #[tokio::test]
    async fn kafka_source_factory_build_does_not_gate_on_broker_liveness() {
        use crate::source::builtin::BuiltinSourceFactory;

        let factory = new_kafka_source_factory(
            KafkaSourceConfig {
                brokers: vec!["127.0.0.1:1".into()],
                topics: vec!["test-topic".into()],
                consumer_group: "test-group".into(),
                auth: None,
                tls: None,
                kafka_raw_config: HashMap::from([(
                    "socket.timeout.ms".to_string(),
                    "100".to_string(),
                )]),
            },
            1,
            Duration::from_millis(100),
            CancellationToken::new(),
        );
        let result = tokio::time::timeout(Duration::from_secs(5), factory.build()).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
    }

    #[test]
    fn kafka_consumer_failures_map_to_distinct_source_actions() {
        let cases = [
            (
                ConsumerErrorKind::RebalanceInProgress,
                SourceFailureAction::RetrySame,
                SourceFailureImpact::Benign,
            ),
            (
                ConsumerErrorKind::CoordinatorUnavailable,
                SourceFailureAction::RetrySame,
                SourceFailureImpact::Benign,
            ),
            (
                ConsumerErrorKind::IllegalGeneration,
                SourceFailureAction::Recreate,
                SourceFailureImpact::Outage,
            ),
            (
                ConsumerErrorKind::GroupAuthorizationFailed,
                SourceFailureAction::StayDegraded,
                SourceFailureImpact::Outage,
            ),
            (
                ConsumerErrorKind::SaslAuthenticationFailed,
                SourceFailureAction::StayDegraded,
                SourceFailureImpact::Outage,
            ),
        ];

        for (kind, expected_action, expected_impact) in cases {
            let error: Error = numaflow_kafka::Error::Consumer {
                kind,
                message: "consumer operation failed".into(),
            }
            .into();
            assert!(matches!(
                error,
                Error::SourceRedrive {
                    action,
                    impact,
                    ..
                } if action == expected_action && impact == expected_impact
            ));
        }
    }
}
