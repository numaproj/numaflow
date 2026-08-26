use std::sync::Arc;
use std::time::Duration;

use numaflow_pulsar::source::{PulsarMessage, PulsarSource, PulsarSourceConfig, PulsarSourceState};

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::error::{Error, SourceFailureAction, SourceFailureImpact};
use crate::message::{IntOffset, Message, MessageID, NackOffset, Offset};
use crate::metadata::Metadata;
use crate::source;
use crate::source::builtin::{BuiltinSourceBackend, ConnectFactory, SourceBackend};
use tokio_util::sync::CancellationToken;

impl TryFrom<PulsarMessage> for Message {
    type Error = Error;

    fn try_from(message: PulsarMessage) -> crate::Result<Self> {
        let offset = Offset::Int(IntOffset::new(message.offset as i64, *get_vertex_replica()));

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
            numaflow_pulsar::Error::AckPendingExceeded(pending) => Error::SourceRedrive {
                source_name: "Pulsar",
                operation: "read",
                action: SourceFailureAction::RetrySame,
                impact: SourceFailureImpact::Benign,
                code: "pulsar_ack_pending_limit",
                message: format!("Pulsar ack-pending limit reached: {pending}"),
            },
            numaflow_pulsar::Error::ActorTaskTerminated(_) => {
                Error::ActorPatternRecv(value.to_string())
            }
            numaflow_pulsar::Error::Other(e) => Error::Source(e),
        }
    }
}

#[cfg(test)]
pub(crate) async fn new_pulsar_source(
    cfg: PulsarSourceConfig,
    batch_size: usize,
    timeout: Duration,
    vertex_replica: u16,
    cancel_token: tokio_util::sync::CancellationToken,
) -> crate::Result<PulsarSource> {
    Ok(PulsarSource::new(cfg, batch_size, timeout, vertex_replica, cancel_token).await?)
}

pub(crate) fn new_pulsar_source_factory(
    config: PulsarSourceConfig,
    batch_size: usize,
    timeout: Duration,
    vertex_replica: u16,
    cancel_token: CancellationToken,
) -> ConnectFactory {
    let state = PulsarSourceState::default();
    ConnectFactory::new("Pulsar", move || {
        let config = config.clone();
        let cancel_token = cancel_token.clone();
        let state = state.clone();
        async move {
            let source = PulsarSource::new_with_state(
                config,
                batch_size,
                timeout,
                vertex_replica,
                cancel_token,
                state,
            )
            .await?;
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
            let Offset::Int(int_offset) = offset else {
                return Err(Error::Source(format!(
                    "Expected Offset::Int type for Pulsar. offset={offset:?}"
                )));
            };
            pulsar_offsets.push(int_offset.offset as u64);
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

            let Offset::Int(int_offset) = offset.offset else {
                return Err(Error::Source(format!(
                    "Expected Offset::Int type for Pulsar. offset={:?}",
                    offset.offset
                )));
            };

            pulsar_offsets.push(int_offset.offset as u64);
        }

        self.nack_offsets(pulsar_offsets).await.map_err(Into::into)
    }
}

impl source::LagReader for PulsarSource {
    async fn pending(&mut self) -> crate::error::Result<Option<usize>> {
        Ok(self.pending_count().await)
    }
}

#[cfg(test)]
mod policy_tests {
    use super::*;

    #[test]
    fn ack_pending_limit_retries_same_pulsar_generation() {
        let error: Error = numaflow_pulsar::Error::AckPendingExceeded(10).into();
        assert!(matches!(
            error,
            Error::SourceRedrive {
                action: SourceFailureAction::RetrySame,
                impact: SourceFailureImpact::Benign,
                code: "pulsar_ack_pending_limit",
                ..
            }
        ));
    }
}

#[cfg(feature = "pulsar-tests")]
#[cfg(test)]
mod tests {
    use pulsar::{Pulsar, TokioExecutor, producer, proto};
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
            dead_letter_policy: None,
            auth: None,
        };
        let mut pulsar = new_pulsar_source(
            cfg,
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

        let messages = pulsar.read().await.unwrap()?;
        assert_eq!(messages.len(), 10);

        let offsets: Vec<Offset> = messages.into_iter().map(|m| m.offset).collect();

        pulsar.ack(offsets).await?;

        Ok(())
    }
    #[tokio::test]
    async fn pulsar_source_factory_build_fails_with_invalid_server() {
        use crate::source::builtin::BuiltinSourceFactory;

        let factory = new_pulsar_source_factory(
            PulsarSourceConfig {
                pulsar_server_addr: "not-a-valid-pulsar-url".into(),
                topic: "persistent://public/default/test".into(),
                consumer_name: "test-consumer".into(),
                subscription: "test-sub".into(),
                max_unack: 10,
                dead_letter_policy: None,
                auth: None,
            },
            1,
            Duration::from_millis(100),
            0,
            CancellationToken::new(),
        );
        assert!(factory.build().await.is_err());
    }
}
