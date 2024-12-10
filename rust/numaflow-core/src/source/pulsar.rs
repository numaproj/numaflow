use std::sync::Arc;
use std::time::Duration;

use numaflow_pulsar::source::{PulsarMessage, PulsarSource, PulsarSourceConfig};

use crate::config::get_vertex_name;
use crate::error::Error;
use crate::message::{IntOffset, Message, MessageID, Offset};
use crate::source;

impl TryFrom<PulsarMessage> for Message {
    type Error = Error;

    fn try_from(message: PulsarMessage) -> crate::Result<Self> {
        let offset = Offset::Int(IntOffset::new(message.offset, 1)); // FIXME: partition id

        Ok(Message {
            keys: Arc::from(vec![message.key]),
            tags: None,
            value: message.payload,
            offset: Some(offset.clone()),
            event_time: message.event_time,
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: offset.to_string().into(),
                index: 0,
            },
            headers: message.headers,
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
) -> crate::Result<PulsarSource> {
    Ok(PulsarSource::new(cfg, batch_size, timeout).await?)
}

impl source::SourceReader for PulsarSource {
    fn name(&self) -> &'static str {
        "Pulsar"
    }

    async fn read(&mut self) -> crate::Result<Vec<Message>> {
        self.read_messages()
            .await?
            .into_iter()
            .map(|msg| msg.try_into())
            .collect()
    }

    fn partitions(&self) -> Vec<u16> {
        Self::partitions(self)
    }
}

impl source::SourceAcker for PulsarSource {
    async fn ack(&mut self, offsets: Vec<Offset>) -> crate::error::Result<()> {
        let mut pulsar_offsets = Vec::with_capacity(offsets.len());
        for offset in offsets {
            let Offset::Int(int_offset) = offset else {
                return Err(Error::Source(format!(
                    "Expected Offset::Int type for Pulsar. offset={offset:?}"
                )));
            };
            pulsar_offsets.push(int_offset.offset);
        }
        self.ack_offsets(pulsar_offsets).await.map_err(Into::into)
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
    use pulsar::{producer, proto, Pulsar, TokioExecutor};
    use source::{LagReader, SourceAcker, SourceReader};

    use super::*;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn test_pulsar_source() -> Result<()> {
        let cfg = PulsarSourceConfig {
            pulsar_server_addr: "pulsar://localhost:6650".into(),
            topic: "persistent://public/default/test_persistent".into(),
            consumer_name: "test".into(),
            subscription: "test".into(),
            max_unack: 100,
            auth: None,
        };
        let mut pulsar = new_pulsar_source(cfg, 10, Duration::from_millis(200)).await?;
        assert_eq!(pulsar.name(), "Pulsar");

        // Read should return before the timeout
        let msgs = tokio::time::timeout(Duration::from_millis(400), pulsar.read_messages()).await;
        assert!(msgs.is_ok());

        assert!(pulsar.pending().await.unwrap().is_none());

        let pulsar_producer: Pulsar<_> = Pulsar::builder("pulsar://localhost:6650", TokioExecutor)
            .build()
            .await
            .unwrap();
        let mut pulsar_producer = pulsar_producer
            .producer()
            .with_topic("persistent://public/default/test_persistent")
            .with_name("my producer")
            .with_options(producer::ProducerOptions {
                schema: Some(proto::Schema {
                    r#type: proto::schema::Type::String as i32,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();

        let data: Vec<String> = (0..10).map(|i| format!("test_data_{i}")).collect();
        let send_futures = pulsar_producer
            .send_all(data)
            .await
            .map_err(|e| format!("Sending messages to Pulsar: {e:?}"))?;
        for fut in send_futures {
            fut.await?;
        }

        let messages = pulsar.read().await?;
        assert_eq!(messages.len(), 10);

        let offsets: Vec<Offset> = messages.into_iter().map(|m| m.offset.unwrap()).collect();
        println!("offsets: {offsets:?}");
        pulsar.ack(offsets).await?;

        Ok(())
    }
}
