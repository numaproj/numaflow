use std::sync::Arc;
use std::time::Duration;

use numaflow_pulsar::source::{PulsarMessage, PulsarSource, PulsarSourceConfig};

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::error::Error;
use crate::message::{Message, MessageID, NackOffset, Offset, StringOffset};
use crate::metadata::Metadata;
use crate::source;

impl TryFrom<PulsarMessage> for Message {
    type Error = Error;

    fn try_from(message: PulsarMessage) -> crate::Result<Self> {
        let offset = Offset::String(StringOffset::new(message.offset, *get_vertex_replica()));

        Ok(Message {
            typ: Default::default(),
            keys: Arc::from(vec![message.key]),
            tags: None,
            value: message.payload,
            offset: offset.clone(),
            event_time: message.event_time,
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

impl From<numaflow_pulsar::Error> for Error {
    fn from(value: numaflow_pulsar::Error) -> Self {
        match value {
            numaflow_pulsar::Error::Pulsar(e) => Error::Source(e.to_string()),
            numaflow_pulsar::Error::UnknownOffset(_) => Error::Source(value.to_string()),
            numaflow_pulsar::Error::AckPendingExceeded(pending) => {
                Error::AckPendingExceeded(pending)
            }
            numaflow_pulsar::Error::ActorTaskTerminated(_) => {
                Error::ActorPatternRecv(value.to_string())
            }
            numaflow_pulsar::Error::Other(e) => Error::Source(e),
        }
    }
}

pub(crate) async fn new_pulsar_source(
    cfg: PulsarSourceConfig,
    batch_size: usize,
    timeout: Duration,
    vertex_replica: u16,
    cancel_token: tokio_util::sync::CancellationToken,
) -> crate::Result<PulsarSource> {
    Ok(PulsarSource::new(cfg, batch_size, timeout, vertex_replica, cancel_token).await?)
}

impl source::SourceReader for PulsarSource {
    fn name(&self) -> &'static str {
        "Pulsar"
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
        let partitions = self.partitions_vec();
        // For Pulsar with shared subscriptions, there's no partition assignment like Kafka.
        // Each vertex replica is treated as a separate "partition" for watermark purposes.
        // total_partitions is None because we don't know the total replica count at runtime.
        Ok(source::SourcePartitions::new(partitions, None))
    }
}

impl source::SourceAcker for PulsarSource {
    async fn ack(&mut self, offsets: Vec<Offset>) -> crate::error::Result<()> {
        let mut pulsar_offsets = Vec::with_capacity(offsets.len());
        for offset in offsets {
            let Offset::String(string_offset) = offset else {
                return Err(Error::Source(format!(
                    "Expected Offset::String type for Pulsar. offset={offset:?}"
                )));
            };
            pulsar_offsets.push(
                String::from_utf8(string_offset.offset.to_vec()).map_err(|e| {
                    Error::Source(format!("Pulsar offset must be valid UTF-8. error={e}"))
                })?,
            );
        }
        self.ack_offsets(pulsar_offsets).await.map_err(Into::into)
    }

    async fn nack(&mut self, offsets: Vec<NackOffset>) -> crate::error::Result<()> {
        let mut pulsar_offsets = Vec::with_capacity(offsets.len());
        let mut logged = false;

        for offset in offsets {
            if !logged && offset.option.is_some() {
                tracing::error!(
                    "Pulsar does not support per-message nack options; ignoring supplied options."
                );
                logged = true;
            }

            let Offset::String(string_offset) = offset.offset else {
                return Err(Error::Source(format!(
                    "Expected Offset::String type for Pulsar. offset={:?}",
                    offset.offset
                )));
            };

            pulsar_offsets.push(
                String::from_utf8(string_offset.offset.to_vec()).map_err(|e| {
                    Error::Source(format!("Pulsar offset must be valid UTF-8. error={e}"))
                })?,
            );
        }

        self.nack_offsets(pulsar_offsets).await.map_err(Into::into)
    }
}

impl source::LagReader for PulsarSource {
    async fn pending(&mut self) -> crate::error::Result<Option<usize>> {
        Ok(self.pending_count().await)
    }
}

#[cfg(feature = "pulsar-tests")]
#[cfg(test)]
mod tests {
    use pulsar::{Pulsar, TokioExecutor, producer, proto};
    use source::{LagReader, SourceAcker, SourceReader};
    use std::collections::HashSet;

    use super::*;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    const PULSAR_ADDR: &str = "pulsar://localhost:6650";
    const PULSAR_ADMIN_ADDR: &str = "http://localhost:8080";

    fn unique_topic(prefix: &str) -> (String, String) {
        let suffix = rand::random::<u64>();
        (
            format!("persistent://public/default/{prefix}-{suffix}"),
            format!("{prefix}-subscription-{suffix}"),
        )
    }

    fn source_config(topic: String, subscription: String) -> PulsarSourceConfig {
        PulsarSourceConfig {
            pulsar_server_addr: PULSAR_ADDR.into(),
            topic,
            consumer_name: "numaflow-pulsar-test".into(),
            subscription,
            max_unack: 100,
            dead_letter_policy: None,
            auth: None,
            tls: None,
        }
    }

    async fn create_partitioned_topic(topic: &str, partitions: usize) -> Result<()> {
        let topic_name = topic
            .rsplit('/')
            .next()
            .ok_or_else(|| format!("Invalid Pulsar topic: {topic}"))?;
        reqwest::Client::new()
            .put(format!(
                "{PULSAR_ADMIN_ADDR}/admin/v2/persistent/public/default/{topic_name}/partitions"
            ))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(partitions.to_string())
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    async fn produce_messages(topic: &str, messages: Vec<String>) -> Result<()> {
        let pulsar: Pulsar<_> = Pulsar::builder(PULSAR_ADDR, TokioExecutor).build().await?;
        let mut producer = pulsar
            .producer()
            .with_topic(topic)
            .with_name("numaflow-pulsar-test-producer")
            .with_options(producer::ProducerOptions {
                schema: Some(proto::Schema {
                    r#type: proto::schema::Type::String as i32,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .build()
            .await?;

        let send_futures = producer.send_all(messages).await?;
        for future in send_futures {
            future.await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_non_partitioned_pulsar_source() -> Result<()> {
        let (topic, subscription) = unique_topic("numaflow-source");
        let mut pulsar = new_pulsar_source(
            source_config(topic.clone(), subscription),
            10,
            Duration::from_millis(200),
            0,
            tokio_util::sync::CancellationToken::new(),
        )
        .await?;
        assert_eq!(pulsar.name(), "Pulsar");

        // Read should return before the timeout
        let msgs = tokio::time::timeout(Duration::from_millis(400), pulsar.read_messages()).await;
        assert!(msgs.is_ok());

        assert!(pulsar.pending().await.unwrap().is_none());

        let data: Vec<String> = (0..10).map(|i| format!("test_data_{i}")).collect();
        produce_messages(&topic, data).await?;

        let messages = pulsar.read().await.unwrap()?;
        assert_eq!(messages.len(), 10);
        assert!(
            messages
                .iter()
                .all(|message| matches!(message.offset, Offset::String(_)))
        );

        let offsets: Vec<Offset> = messages.into_iter().map(|m| m.offset).collect();

        pulsar.ack(offsets).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_partitioned_pulsar_source_ack_and_nack() -> Result<()> {
        let (topic, subscription) = unique_topic("numaflow-partitioned-source");
        let partition_count = 3;
        create_partitioned_topic(&topic, partition_count).await?;

        let mut source = new_pulsar_source(
            source_config(topic.clone(), subscription),
            partition_count,
            Duration::from_secs(1),
            0,
            tokio_util::sync::CancellationToken::new(),
        )
        .await?;

        for partition in 0..partition_count {
            produce_messages(
                &format!("{topic}-partition-{partition}"),
                vec![format!("partition-{partition}")],
            )
            .await?;
        }

        let messages = source.read().await.unwrap()?;
        assert_eq!(messages.len(), partition_count);

        let offsets: HashSet<Offset> = messages
            .iter()
            .map(|message| message.offset.clone())
            .collect();
        assert_eq!(offsets.len(), partition_count);
        assert!(
            offsets
                .iter()
                .all(|offset| matches!(offset, Offset::String(_)))
        );

        source.ack(offsets.into_iter().collect()).await?;

        let messages = source.read().await.unwrap()?;
        assert!(
            messages.is_empty(),
            "acked messages must not be redelivered"
        );

        produce_messages(
            &format!("{topic}-partition-0"),
            vec!["message-to-nack".to_string()],
        )
        .await?;
        let messages = source.read().await.unwrap()?;
        assert_eq!(messages.len(), 1);
        source
            .nack(vec![NackOffset {
                offset: messages
                    .into_iter()
                    .next()
                    .expect("one message was asserted above")
                    .offset,
                option: None,
            }])
            .await?;

        Ok(())
    }
}
