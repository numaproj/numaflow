//! HTTP source for Numaflow.

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::error::Result;
use crate::message::{Message, MessageID, Offset, StringOffset};
use crate::source;
use crate::source::{SourceAcker, SourceReader};
use numaflow_http::HttpMessage;
use std::sync::Arc;

impl From<numaflow_http::Error> for crate::error::Error {
    fn from(value: numaflow_http::Error) -> Self {
        use numaflow_http::Error;
        match value {
            Error::ChannelFull() => Self::Source(format!("HTTP source: {value:?}")),
            Error::Server(_) | Error::ChannelSend(_) | Error::ChannelRecv(_) => {
                Self::Source(format!("HTTP source: {value:?}"))
            }
        }
    }
}

impl From<HttpMessage> for Message {
    fn from(value: HttpMessage) -> Self {
        Message {
            typ: Default::default(),
            keys: Arc::from(vec![]),
            tags: None,
            value: value.body,
            offset: Offset::String(StringOffset::new(value.id.clone(), *get_vertex_replica())),
            event_time: value.event_time,
            watermark: None,
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: value.id.into(),
                index: 0,
            },
            headers: value.headers,
            metadata: None,
            is_late: false,
        }
    }
}

#[derive(Clone)]
pub(crate) struct CoreHttpSource {
    batch_size: usize,
    http_source: numaflow_http::HttpSourceHandle,
}

impl CoreHttpSource {
    pub(crate) fn new(batch_size: usize, http_source: numaflow_http::HttpSourceHandle) -> Self {
        Self {
            batch_size,
            http_source,
        }
    }
}

impl SourceReader for CoreHttpSource {
    fn name(&self) -> &'static str {
        "HTTP"
    }

    async fn read(&mut self) -> Result<Vec<Message>> {
        self.http_source
            .read(self.batch_size)
            .await
            .map_err(|e| e.into())
            .map(|msgs| msgs.into_iter().map(|msg| msg.into()).collect())
    }

    async fn partitions(&mut self) -> Result<Vec<u16>> {
        Ok(vec![*get_vertex_replica()])
    }
}

impl SourceAcker for CoreHttpSource {
    async fn ack(&mut self, _: Vec<Offset>) -> Result<()> {
        self.http_source.ack(vec![]).await.map_err(|e| e.into())
    }
}

impl source::LagReader for CoreHttpSource {
    async fn pending(&mut self) -> Result<Option<usize>> {
        Ok(self.http_source.pending().await)
    }
}
