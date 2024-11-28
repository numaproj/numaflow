use std::time::Duration;

use crate::config::components::source::PulsarSourceConfig;
use crate::error::Error;
use crate::message::{get_vertex_name, IntOffset, Message, MessageID, Offset};
use crate::source;
use numaflow_pulsar::source::{PulsarMessage, PulsarSource};

impl From<PulsarSourceConfig> for numaflow_pulsar::source::PulsarSourceConfig {
    fn from(value: PulsarSourceConfig) -> Self {
        numaflow_pulsar::source::PulsarSourceConfig {
            pulsar_server_addr: value.server_addr,
            topic: value.topic,
            consumer_name: value.consumer_name,
            subscription: value.subscription,
            max_unack: value.max_unack,
        }
    }
}

impl TryFrom<PulsarMessage> for Message {
    type Error = Error;

    fn try_from(message: PulsarMessage) -> crate::Result<Self> {
        let offset = Offset::Int(IntOffset::new(message.offset, 1)); // FIXME: partition id

        Ok(Message {
            keys: vec![message.key],
            value: message.payload,
            offset: Some(offset.clone()),
            event_time: message.event_time,
            id: MessageID {
                vertex_name: get_vertex_name().to_string(),
                offset: offset.to_string(),
                index: 0,
            },
            headers: message.headers,
        })
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
