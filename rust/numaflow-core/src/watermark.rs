use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use prost::Message as ProtoMessage;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::sync::mpsc::Receiver;
use tracing::error;

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::{EdgeWatermarkConfig, SourceWatermarkConfig};
use crate::config::{get_vertex_name, get_vertex_replica};
use crate::error::Error;
use crate::error::Result;
use crate::message::{IntOffset, Message, Offset};
use crate::watermark::fetcher::{EdgeFetcher, SourceFetcher};
use crate::watermark::publisher::{EdgePublisher, SourcePublisher};

mod fetcher;
mod manager;
mod publisher;
mod timeline;

type Watermark = DateTime<Utc>;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct WMB {
    pub idle: bool,
    pub offset: i64,
    pub watermark: i64,
    pub partition: u16,
}

impl TryFrom<Bytes> for WMB {
    type Error = Error;

    fn try_from(bytes: Bytes) -> std::result::Result<Self, Self::Error> {
        let proto_wmb = numaflow_pb::objects::watermark::Wmb::decode(bytes)
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
        let proto_wmb = numaflow_pb::objects::watermark::Wmb {
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

enum EdgeActorMessage {
    FetchWatermark {
        offset: IntOffset,
        oneshot_tx: tokio::sync::oneshot::Sender<Result<Watermark>>,
    },
    PublishWatermark {
        offset: IntOffset,
        stream: Stream,
    },
    RemoveOffset(IntOffset),
}

enum SourceActorMessage {
    PublishSourceWatermark {
        map: HashMap<u16, i64>,
    },
    PublishEdgeWatermark {
        offset: IntOffset,
        stream: Stream,
        input_partition: u16,
    },
}

#[derive(Eq, PartialEq)]
struct OffsetWatermark {
    offset: i64,
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

struct EdgeWatermarkActor {
    fetcher: EdgeFetcher,
    publisher: EdgePublisher,
    offset_set: HashMap<u16, BTreeSet<OffsetWatermark>>, // partition_id -> BTreeSet of OffsetWatermark
}

impl EdgeWatermarkActor {
    fn new(fetcher: EdgeFetcher, publisher: EdgePublisher) -> Self {
        Self {
            fetcher,
            publisher,
            offset_set: HashMap::new(),
        }
    }

    async fn run(mut self, mut receiver: Receiver<EdgeActorMessage>) {
        while let Some(message) = receiver.recv().await {
            if let Err(e) = self.handle_message(message).await {
                error!("error handling message: {:?}", e);
            }
        }
    }

    async fn handle_message(&mut self, message: EdgeActorMessage) -> Result<()> {
        match message {
            EdgeActorMessage::FetchWatermark { offset, oneshot_tx } => {
                let watermark = self
                    .fetcher
                    .fetch_watermark(offset.offset, offset.partition_idx)
                    .await?;
                self.insert_offset(offset.partition_idx, offset.offset, watermark);

                oneshot_tx
                    .send(Ok(watermark))
                    .map_err(|_| Error::Watermark("failed to send response".to_string()))?;
            }

            EdgeActorMessage::PublishWatermark { offset, stream } => {
                let min_wm = self
                    .get_lowest_watermark()
                    .unwrap_or(Watermark::from_timestamp_millis(-1).unwrap());

                self.publisher
                    .publish_watermark(stream, offset.offset, min_wm.timestamp_millis())
                    .await?;
            }

            EdgeActorMessage::RemoveOffset(offset) => {
                self.remove_offset(offset.partition_idx, offset.offset)?;
            }
        }

        Ok(())
    }

    fn insert_offset(&mut self, partition_id: u16, offset: i64, watermark: Watermark) {
        let set = self.offset_set.entry(partition_id).or_default();
        set.insert(OffsetWatermark { offset, watermark });
    }

    fn remove_offset(&mut self, partition_id: u16, offset: i64) -> Result<()> {
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

struct SourceWatermarkActor {
    publisher: SourcePublisher,
    fetcher: SourceFetcher,
}

impl SourceWatermarkActor {
    fn new(publisher: SourcePublisher, fetcher: SourceFetcher) -> Self {
        Self { publisher, fetcher }
    }

    async fn run(mut self, mut receiver: Receiver<SourceActorMessage>) {
        while let Some(message) = receiver.recv().await {
            if let Err(e) = self.handle_message(message).await {
                error!("error handling message: {:?}", e);
            }
        }
    }

    async fn handle_message(&mut self, message: SourceActorMessage) -> Result<()> {
        match message {
            SourceActorMessage::PublishSourceWatermark { map } => {
                for (partition, event_time) in map {
                    self.publisher
                        .publish_source_watermark(partition, event_time)
                        .await;
                }
            }
            SourceActorMessage::PublishEdgeWatermark {
                offset,
                stream,
                input_partition,
            } => {
                let watermark = self.fetcher.fetch_source_watermark().await?;
                self.publisher
                    .publish_edge_watermark(
                        input_partition,
                        stream,
                        offset.offset,
                        watermark.timestamp_millis(),
                    )
                    .await;
            }
        }

        Ok(())
    }
}

#[derive(Clone)]
pub(crate) enum WatermarkHandle {
    Edge(EdgeWatermarkHandle),
    Source(SourceWatermarkHandle),
}

#[derive(Clone)]
pub(crate) struct EdgeWatermarkHandle {
    sender: tokio::sync::mpsc::Sender<EdgeActorMessage>,
}

impl EdgeWatermarkHandle {
    pub(crate) async fn new(
        js_context: async_nats::jetstream::Context,
        config: &EdgeWatermarkConfig,
    ) -> Result<Self> {
        let (sender, receiver) = tokio::sync::mpsc::channel(100);
        let fetcher = EdgeFetcher::new(js_context.clone(), &config.from_vertex_config).await?;

        let processor_name = format!("{}-{}", get_vertex_name(), get_vertex_replica());
        let publisher =
            EdgePublisher::new(processor_name, js_context.clone(), &config.to_vertex_config)
                .await?;

        let actor = EdgeWatermarkActor::new(fetcher, publisher);
        tokio::spawn(async move { actor.run(receiver).await });
        Ok(Self { sender })
    }

    pub(crate) async fn fetch_watermark(&self, offset: Offset) -> Result<Watermark> {
        if let Offset::Int(offset) = offset {
            let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
            self.sender
                .send(EdgeActorMessage::FetchWatermark { offset, oneshot_tx })
                .await
                .map_err(|_| Error::Watermark("failed to send message".to_string()))?;

            oneshot_rx
                .await
                .map_err(|_| Error::Watermark("failed to receive response".to_string()))?
        } else {
            Err(Error::Watermark("invalid offset type".to_string()))
        }
    }

    pub(crate) async fn publish_watermark(&self, stream: Stream, offset: Offset) -> Result<()> {
        if let Offset::Int(offset) = offset {
            self.sender
                .send(EdgeActorMessage::PublishWatermark { offset, stream })
                .await
                .map_err(|_| Error::Watermark("failed to send message".to_string()))?;
            Ok(())
        } else {
            Err(Error::Watermark("invalid offset type".to_string()))
        }
    }

    pub(crate) async fn remove_offset(&self, offset: Offset) -> Result<()> {
        if let Offset::Int(offset) = offset {
            self.sender
                .send(EdgeActorMessage::RemoveOffset(offset))
                .await
                .map_err(|_| Error::Watermark("failed to send message".to_string()))?;
            Ok(())
        } else {
            Err(Error::Watermark("invalid offset type".to_string()))
        }
    }
}

#[derive(Clone)]
pub(crate) struct SourceWatermarkHandle {
    sender: tokio::sync::mpsc::Sender<SourceActorMessage>,
}

impl SourceWatermarkHandle {
    pub(crate) async fn new(
        js_context: async_nats::jetstream::Context,
        config: &SourceWatermarkConfig,
    ) -> Result<Self> {
        let (sender, receiver) = tokio::sync::mpsc::channel(100);
        let fetcher = SourceFetcher::new(js_context.clone(), &config.source_bucket_config)
            .await
            .map_err(|e| Error::Watermark(e.to_string()))?;

        let publisher = SourcePublisher::new(
            js_context.clone(),
            config.source_bucket_config.clone(),
            config.to_vertex_bucket_config.clone(),
        )
        .await
        .map_err(|e| Error::Watermark(e.to_string()))?;

        let actor = SourceWatermarkActor::new(publisher, fetcher);
        tokio::spawn(async move { actor.run(receiver).await });
        Ok(Self { sender })
    }

    pub(crate) async fn publish_source_watermark(&self, messages: &[Message]) -> Result<()> {
        // from the offsets of the transformed messages store the lowest eventtime for each partition using HashMap<u16, i64>
        let partition_to_lowest_event_time =
            messages.iter().fold(HashMap::new(), |mut acc, message| {
                let partition_id = match &message.offset {
                    Offset::Int(offset) => offset.partition_idx,
                    Offset::String(offset) => offset.partition_idx,
                };

                let event_time = message.event_time.timestamp_millis();
                let lowest_event_time = acc.entry(partition_id).or_insert(event_time);
                if event_time < *lowest_event_time {
                    *lowest_event_time = event_time;
                }
                acc
            });

        self.sender
            .send(SourceActorMessage::PublishSourceWatermark {
                map: partition_to_lowest_event_time,
            })
            .await
            .map_err(|_| Error::Watermark("failed to send message".to_string()))?;
        Ok(())
    }

    pub(crate) async fn publish_source_edge_watermark(
        &self,
        stream: Stream,
        offset: Offset,
        input_partition: u16,
    ) -> Result<()> {
        if let Offset::Int(offset) = offset {
            self.sender
                .send(SourceActorMessage::PublishEdgeWatermark {
                    offset,
                    stream,
                    input_partition,
                })
                .await
                .map_err(|_| Error::Watermark("failed to send message".to_string()))?;
            Ok(())
        } else {
            Err(Error::Watermark("invalid offset type".to_string()))
        }
    }
}
