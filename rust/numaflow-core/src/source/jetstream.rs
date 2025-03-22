use std::sync::Arc;
use std::time::Duration;

use numaflow_jetstream::{Jetstream, JetstreamSourceConfig};

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::message::{IntOffset, MessageID, Metadata, Offset};
use crate::source::SourceReader;
use crate::{message::Message, Error, Result};

use super::SourceAcker;

impl TryFrom<numaflow_jetstream::Message> for Message {
    type Error = Error;
    fn try_from(message: numaflow_jetstream::Message) -> Result<Self> {
        let offset = Offset::Int(IntOffset::new(
            message.stream_sequence as i64, // FIXME:
            *get_vertex_replica(),
        ));

        Ok(Message {
            typ: Default::default(),
            keys: Arc::from(vec![]), // FIXME:
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
            metadata: Some(Metadata {
                previous_vertex: get_vertex_name().to_string(),
            }),
        })
    }
}

impl From<numaflow_jetstream::Error> for crate::Error {
    fn from(value: numaflow_jetstream::Error) -> Self {
        Self::Source(format!("Jetstream source: {value:?}"))
    }
}

pub(crate) async fn new_jetstream_source(
    cfg: JetstreamSourceConfig,
    batch_size: usize,
    timeout: Duration,
    vertex_replica: u16,
) -> crate::Result<Jetstream> {
    Ok(Jetstream::connect(cfg).await?)
}

impl SourceReader for Jetstream {
    fn name(&self) -> &'static str {
        "Jetstream"
    }

    async fn read(&mut self) -> Result<Vec<Message>> {
        self.read_messages()
            .await?
            .into_iter()
            .map(|msg| msg.try_into())
            .collect()
    }

    async fn partitions(&mut self) -> Result<Vec<u16>> {
        Ok(vec![*get_vertex_replica()])
    }

    async fn is_ready(&mut self) -> bool {
        true
    }
}

impl SourceAcker for Jetstream {
    async fn ack(&mut self, offsets: Vec<Offset>) -> Result<()> {
        let mut jetstream_offsets = Vec::with_capacity(offsets.len());
        for offset in offsets {
            let Offset::Int(seq_num) = offset else {
                return Err(Error::Source(format!(
                    "Expected integer offset for Jetstream source. Got: {offset:?}"
                )));
            };
            jetstream_offsets.push(seq_num.offset as u64);
        }
        self.ack_messages(jetstream_offsets).await?;
        Ok(())
    }
}

impl super::LagReader for Jetstream {
    async fn pending(&mut self) -> crate::error::Result<Option<usize>> {
        Ok(self.pending_messages().await?)
    }
}
