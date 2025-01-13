use crate::error::Error;
use crate::error::Result;
use crate::message::{Offset, OffsetType};
use crate::watermark::fetcher::Fetcher;
use crate::watermark::publisher::Publisher;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use prost::Message as ProtoMessage;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::collections::HashMap;

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
    pub partition: u16,
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
            partition: proto_wmb.partition as u16,
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
            partition: wmb.partition as i32,
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
    PublishWatermark {
        offset: Offset,
        vertex_name: String,
    },
    RemoveOffset(Offset),
}

#[derive(Eq, PartialEq)]
struct OffsetWatermark {
    offset: u64,
    watermark: Watermark,
}

impl Ord for OffsetWatermark {
    fn cmp(&self, other: &Self) -> Ordering {
        self.offset.cmp(&other.offset)
    }
}

impl PartialOrd for OffsetWatermark {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct WatermarkActor {
    fetcher: Fetcher,
    publisher: Publisher,
    offset_set: HashMap<u16, BTreeSet<OffsetWatermark>>, // partition_id -> BTreeSet of OffsetWatermark
}

impl WatermarkActor {
    pub fn new(fetcher: Fetcher, publisher: Publisher) -> Self {
        Self {
            fetcher,
            publisher,
            offset_set: HashMap::new(),
        }
    }

    pub async fn handle_message(&mut self, message: ActorMessage) -> Result<()> {
        match message {
            ActorMessage::FetchWatermark { offset, oneshot_tx } => {
                let watermark = match offset {
                    Offset::Source(_) => {
                        return Err(Error::Watermark(
                            "source offset is not supported".to_string(),
                        ));
                    }
                    Offset::ISB(offset) => match offset {
                        OffsetType::Int(offset) => {
                            let watermark = self
                                .fetcher
                                .fetch_watermark(offset.offset, offset.partition_idx)
                                .await?;
                            self.insert_offset(offset.partition_idx, offset.offset, watermark);
                            watermark
                        }
                        OffsetType::String(_) => {
                            return Err(Error::Watermark(
                                "string offset is not supported".to_string(),
                            ));
                        }
                    },
                };

                oneshot_tx
                    .send(Ok(watermark))
                    .map_err(|_| Error::Watermark("failed to send response".to_string()))?;
            }

            ActorMessage::PublishWatermark {
                offset,
                vertex_name,
            } => match offset {
                Offset::Source(_) => {
                    // for publishing to source, we don't need to fetch (nothing to fetcher here :))
                }
                Offset::ISB(offset_type) => match offset_type {
                    OffsetType::Int(int_offset) => {
                        let min_wm = self
                            .get_lowest_watermark()
                            .unwrap_or(Watermark::from_timestamp_millis(-1).unwrap());

                        self.publisher
                            .publish_watermark(
                                int_offset.partition_idx,
                                int_offset.offset as i64,
                                min_wm.timestamp_millis(),
                                vertex_name,
                            )
                            .await?;
                    }
                    OffsetType::String(_) => {}
                },
            },

            ActorMessage::RemoveOffset(offset) => {
                // remove the offset from the heap
                match offset {
                    Offset::Source(_) => {
                        return Err(Error::Watermark(
                            "source offset is not supported".to_string(),
                        ));
                    }
                    Offset::ISB(offset) => match offset {
                        OffsetType::Int(offset) => {
                            self.remove_offset(offset.partition_idx, offset.offset)?;
                        }
                        OffsetType::String(_) => {
                            return Err(Error::Watermark(
                                "string offset is not supported".to_string(),
                            ));
                        }
                    },
                }
            }
        }

        Ok(())
    }

    fn insert_offset(&mut self, partition_id: u16, offset: u64, watermark: Watermark) {
        let set = self.offset_set.entry(partition_id).or_default();
        set.insert(OffsetWatermark { offset, watermark });
    }

    fn remove_offset(&mut self, partition_id: u16, offset: u64) -> Result<()> {
        if let Some(set) = self.offset_set.get_mut(&partition_id) {
            if let Some(&OffsetWatermark { watermark, .. }) =
                set.iter().find(|ow| ow.offset == offset)
            {
                set.remove(&OffsetWatermark { offset, watermark });
            }
        }
        Ok(())
    }

    fn get_lowest_watermark(&self) -> Option<Watermark> {
        self.offset_set
            .values()
            .filter_map(|set| set.iter().next().map(|ow| ow.watermark))
            .min()
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

    pub(crate) async fn publish_watermark(&self, vertex_name: String, offset: Offset) {}

    pub(crate) async fn remove_offset(&self, offset: Offset) {}
}
