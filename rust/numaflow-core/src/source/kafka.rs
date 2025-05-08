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
            format!("{}:{}", message.partition, message.offset),
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
        Ok(vec![*get_vertex_replica()])
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
            if parts.len() != 2 {
                return Err(Error::Source(format!(
                    "Invalid Kafka offset format. Expected format: <partition>:<offset>. offset={offset:?}"
                )));
            }
            let partition = parts[0].parse::<i32>().map_err(|e| {
                Error::Source(format!(
                    "invalid partition id. kafka_offset={offset}, error={e:?}"
                ))
            })?;
            let partition_offset = parts[1].parse::<i64>().map_err(|e| {
                Error::Source(format!(
                    "invalid offset id. kafka_offset={offset}, error={e:?}"
                ))
            })?;
            kafka_offsets.push((partition, partition_offset));
        }
        self.ack_messages(kafka_offsets).await.map_err(Into::into)
    }
}

impl source::LagReader for KafkaSource {
    async fn pending(&mut self) -> crate::error::Result<Option<usize>> {
        Ok(self.pending_messages().await?)
    }
}
