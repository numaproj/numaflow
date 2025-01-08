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

// map vertex
// (js reader) - fetch watermark for offset and assign to the message (each read partition will be invoking fetch simultaneously
// (js writer) - publish watermark for the write offsets
//                  * get the lowest inflight offset using the tracker
//                  * fetch the watermark for that offset
//                  * publish the fetched watermark to downstream
//                  * check if the watermark is greater than the previously fetched watermark
//                  * if its greater publish the watermark for all the yet to be published offsets
//                  * if it's not greater add it to the yet to be published offsets map
//
// source vertex
// (js writer) - publish watermark for the read source message -> Here processing entity is partition so publish the watermark for that partition
//             - publish watermark for the isb write offsets -> fetch the watermark we don't care about the offset here
//             - check if the watermark has changed compared to the previous watermark if so publish the watermark

// Now lets think about idle watermark
// map vertex
// branch idling - while publishing watermark check if we are not publishing to the other edges and partitions if we are not
//               - check if the ctrl message is already present for that partition, if so publish idle wm for the ctrl message
//               - else write a ctrl message and get the offset and write the watermark for that offset
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
