use crate::config::components::source::PulsarSourceConfig;
use crate::message::{Message, MessageID, Offset};
use crate::source;
use numaflow_pulsar::source::PulsarSource;
use std::time::Duration;

impl From<PulsarSourceConfig> for numaflow_pulsar::source::PulsarSourceConfig {
    fn from(value: PulsarSourceConfig) -> Self {
        numaflow_pulsar::source::PulsarSourceConfig {
            pulsar_server_addr: value.pulsar_server_addr,
            topic: value.topic,
            consumer_name: value.consumer_name,
            subscription: value.subscription,
        }
    }
}

pub(crate) async fn new_pulsar_source(
    cfg: PulsarSourceConfig,
    batch_size: usize,
    timeout: Duration,
) -> crate::Result<PulsarSource> {
    let pulsar_source = PulsarSource::new(cfg.into(), batch_size, timeout).await;
    Ok(pulsar_source)
}

impl source::SourceReader for PulsarSource {
    fn name(&self) -> &'static str {
        "Pulsar"
    }

    async fn read(&mut self) -> crate::error::Result<Vec<Message>> {
        Ok(Self::read(self)
            .await
            .into_iter()
            .map(|msg| msg.into())
            .collect())
    }

    fn partitions(&self) -> Vec<u16> {
        Self::partitions(self)
    }
}

impl source::SourceAcker for PulsarSource {
    async fn ack(&mut self, offsets: Vec<Offset>) -> crate::error::Result<()> {
        let mut pulsar_offsets = Vec::with_capacity(offsets.len());
        for offset in offsets {
            let Offset::Bytes(offset) = offset else {
                return Err(crate::error::Error::Source(
                    "expected offset type Offset::Bytes for Pulsar source".into(),
                )); // FIXME:
            };
            pulsar_offsets.push(offset);
        }
        Self::ack(self, pulsar_offsets).await;
        Ok(())
    }
}

impl source::LagReader for PulsarSource {
    async fn pending(&mut self) -> crate::error::Result<Option<usize>> {
        Ok(Self::pending(self).await)
    }
}

impl From<numaflow_pulsar::source::Message> for Message {
    fn from(value: numaflow_pulsar::source::Message) -> Self {
        Message {
            keys: value.keys,
            value: value.value,
            offset: Some(Offset::Bytes(value.offset)),
            event_time: value.event_time,
            id: MessageID {
                vertex_name: value.id.vertex_name,
                offset: value.id.offset,
                index: value.id.index,
            },
            headers: value.headers,
        }
    }
}
