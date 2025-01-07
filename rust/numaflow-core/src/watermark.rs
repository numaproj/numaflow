use crate::error::Error;
use crate::message::{Offset, OffsetType};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use prost::Message as ProtoMessage;

use crate::error::Result;
use crate::tracker::TrackerHandle;
use crate::watermark::fetcher::Fetcher;
use crate::watermark::publisher::Publisher;

mod fetcher;
mod manager;
mod publisher;
mod timeline;

type Watermark = DateTime<Utc>;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WMB {
    pub idle: bool,
    pub offset: i64,
    pub watermark: i64,
    pub partition: i32,
}

impl TryFrom<Bytes> for WMB {
    type Error = Error;

    fn try_from(bytes: Bytes) -> std::result::Result<Self, Self::Error> {
        let proto_wmb = numaflow_pb::objects::wmb::Wmb::decode(bytes)
            .map_err(|e| Error::Proto(e.to_string()))?;

        Ok(WMB {
            idle: proto_wmb.idle,
            offset: proto_wmb.offset,
            watermark: proto_wmb.watermark,
            partition: proto_wmb.partition,
        })
    }
}

impl TryFrom<WMB> for BytesMut {
    type Error = Error;

    fn try_from(wmb: WMB) -> std::result::Result<Self, Self::Error> {
        let mut bytes = BytesMut::new();
        let proto_wmb = numaflow_pb::objects::wmb::Wmb {
            idle: wmb.idle,
            offset: wmb.offset,
            watermark: wmb.watermark,
            partition: wmb.partition,
        };

        proto_wmb
            .encode(&mut bytes)
            .map_err(|e| Error::Proto(e.to_string()))?;

        Ok(bytes)
    }
}

enum ActorMessage {
    FetchWatermark {
        offset: Offset,
        oneshot_tx: tokio::sync::oneshot::Sender<Result<Watermark>>,
    },
    PublishWatermark(Offset),
}

struct WatermarkActor {
    fetcher: Fetcher,
    tracker: TrackerHandle,
    publisher: Publisher,
}

impl WatermarkActor {
    pub fn new(fetcher: Fetcher, tracker: TrackerHandle, publisher: Publisher) -> Self {
        Self {
            fetcher,
            tracker,
            publisher,
        }
    }

    pub async fn handle_message(&self, message: ActorMessage) -> Result<()> {
        match message {
            ActorMessage::FetchWatermark { offset, oneshot_tx } => match offset {
                Offset::Source(_) => {
                    let watermark = self.fetcher.fetch_source_watermark().await?;
                    oneshot_tx
                        .send(Ok(watermark))
                        .map_err(|_| Error::Watermark("failed to send response".to_string()))?;
                }
                Offset::ISB(offset) => {
                    match offset {
                        OffsetType::Int(offset) => {
                            let watermark = self
                                .fetcher
                                .fetch_watermark(offset.offset, offset.partition_idx)
                                .await?;
                            oneshot_tx.send(Ok(watermark)).map_err(|_| {
                                Error::Watermark("failed to send response".to_string())
                            })?;
                        }
                        OffsetType::String(_) => {
                            // string offset is not supported
                            return Err(Error::Watermark(
                                "string offset is not supported".to_string(),
                            ));
                        }
                    }
                }
            },
            ActorMessage::PublishWatermark(offset) => match offset {
                Offset::Source(_) => {
                    // for publishing to source, we don't need to fetch (nothing to fetcher here :))
                }
                Offset::ISB(_) => {
                    // get the smallest offset using the tracker
                    // fetch the watermark for the smallest offset
                    // publish watermark for the offset
                }
            },
        }

        Ok(())
    }
}

pub(crate) struct WatermarkHandle {}

impl WatermarkHandle {
    pub(crate) fn new() -> Self {
        Self {}
    }

    pub(crate) async fn fetch_watermark(&self, offset: Offset) -> Result<Watermark> {
        Ok(Watermark::default())
    }

    pub(crate) async fn publish_watermark(&self, offset: Offset) {}

    pub(crate) async fn track_offset(&self, offset: Offset) {}

    pub(crate) async fn delete_offset(&self, offset: Offset) {}
}
