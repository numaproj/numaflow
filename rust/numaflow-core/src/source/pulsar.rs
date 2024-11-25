use std::time::Duration;

use crate::config::components::source::PulsarSourceConfig;
use crate::error::Error;
use crate::message::{Message, Offset};
use crate::source;
use numaflow_pulsar::source::PulsarSource;

impl From<PulsarSourceConfig> for numaflow_pulsar::source::PulsarSourceConfig {
    fn from(value: PulsarSourceConfig) -> Self {
        numaflow_pulsar::source::PulsarSourceConfig {
            pulsar_server_addr: value.pulsar_server_addr,
            topic: value.topic,
            consumer_name: value.consumer_name,
            subscription: value.subscription,
            max_unack: value.max_unack,
        }
    }
}

pub(crate) async fn new_pulsar_source(
    cfg: PulsarSourceConfig,
    batch_size: usize,
    timeout: Duration,
) -> crate::Result<PulsarSource> {
    Ok(PulsarSource::new(cfg.into(), batch_size, timeout).await?)
}

impl source::SourceReader for PulsarSource {
    fn name(&self) -> &'static str {
        "Pulsar"
    }

    async fn read(&mut self) -> crate::Result<Vec<Message>> {
        Self::read(self)
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
        Self::ack(self, pulsar_offsets).await.map_err(Into::into)
    }
}

impl source::LagReader for PulsarSource {
    async fn pending(&mut self) -> crate::error::Result<Option<usize>> {
        Ok(Self::pending(self).await)
    }
}
