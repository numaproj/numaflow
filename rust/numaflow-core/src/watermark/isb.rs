use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

use tokio::sync::mpsc::Receiver;
use tracing::error;

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::EdgeWatermarkConfig;
use crate::config::{get_vertex_name, get_vertex_replica};
use crate::error::{Error, Result};
use crate::message::{IntOffset, Offset};
use crate::watermark::isb::wm_fetcher::ISBWatermarkFetcher;
use crate::watermark::isb::wm_publisher::ISBWatermarkPublisher;
use crate::watermark::processor::manager::ProcessorManager;
use crate::watermark::wmb::Watermark;

pub(crate) mod wm_fetcher;
pub(crate) mod wm_publisher;

/// Messages that can be sent to the [ISBWatermarkActor].
enum ISBWaterMarkActorMessage {
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

/// OffsetWatermark is a tuple of offset and watermark.
#[derive(Eq, PartialEq)]
struct OffsetWatermark {
    /// offset can be -1 if watermark cannot be derived.
    offset: i64,
    watermark: Watermark,
}

/// Ordering will be based on the offset in OffsetWatermark
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

/// EdgeWatermarkActor comprises EdgeFetcher and EdgePublisher.
/// Tracks the watermarks of all the inflight messages for each partition, and publishes
/// the lowest watermark.
struct ISBWatermarkActor {
    fetcher: ISBWatermarkFetcher,
    publisher: ISBWatermarkPublisher,
    offset_set: HashMap<u16, BTreeSet<OffsetWatermark>>, // partition_id -> BTreeSet of OffsetWatermark
}

impl ISBWatermarkActor {
    fn new(fetcher: ISBWatermarkFetcher, publisher: ISBWatermarkPublisher) -> Self {
        Self {
            fetcher,
            publisher,
            offset_set: HashMap::new(),
        }
    }

    /// run listens for messages and handles them
    async fn run(mut self, mut receiver: Receiver<ISBWaterMarkActorMessage>) {
        while let Some(message) = receiver.recv().await {
            if let Err(e) = self.handle_message(message).await {
                error!("error handling message: {:?}", e);
            }
        }
    }

    /// handle_message handles the incoming actor message
    async fn handle_message(&mut self, message: ISBWaterMarkActorMessage) -> Result<()> {
        match message {
            // fetches the watermark for the given offset
            ISBWaterMarkActorMessage::FetchWatermark { offset, oneshot_tx } => {
                let watermark = self
                    .fetcher
                    .fetch_watermark(offset.offset, offset.partition_idx)
                    .await?;
                self.insert_offset(offset.partition_idx, offset.offset, watermark);

                oneshot_tx
                    .send(Ok(watermark))
                    .map_err(|_| Error::Watermark("failed to send response".to_string()))?;
            }

            // gets the lowest watermark among the inflight requests and publishes the watermark
            // for the offset and stream
            ISBWaterMarkActorMessage::PublishWatermark { offset, stream } => {
                let min_wm = self
                    .get_lowest_watermark()
                    .unwrap_or(Watermark::from_timestamp_millis(-1).unwrap());

                self.publisher
                    .publish_watermark(stream, offset.offset, min_wm.timestamp_millis())
                    .await?;
            }

            // removes the offset from the tracked offsets
            ISBWaterMarkActorMessage::RemoveOffset(offset) => {
                self.remove_offset(offset.partition_idx, offset.offset)?;
            }
        }

        Ok(())
    }

    /// insert the offset and watermark for inflight requests set
    fn insert_offset(&mut self, partition_id: u16, offset: i64, watermark: Watermark) {
        let set = self.offset_set.entry(partition_id).or_default();
        set.insert(OffsetWatermark { offset, watermark });
    }

    /// removes the offset from the inflight offsets set of the partition
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

    /// gets the lowest watermark among all the inflight requests
    fn get_lowest_watermark(&self) -> Option<Watermark> {
        self.offset_set
            .values()
            .filter_map(|set| set.iter().next().map(|ow| ow.watermark))
            .min()
    }
}

/// Handle to interact with the EdgeWatermarkActor, exposes methods to fetch and publish watermarks
/// for the edges
#[derive(Clone)]
pub(crate) struct ISBWatermarkHandle {
    sender: tokio::sync::mpsc::Sender<ISBWaterMarkActorMessage>,
}

impl ISBWatermarkHandle {
    /// new creates a new [ISBWatermarkHandle].
    pub(crate) async fn new(
        js_context: async_nats::jetstream::Context,
        config: &EdgeWatermarkConfig,
    ) -> Result<Self> {
        let (sender, receiver) = tokio::sync::mpsc::channel(100);

        // create a processor manager map (from_vertex -> ProcessorManager)
        let mut processor_managers = HashMap::new();
        for from_bucket_config in &config.from_vertex_config {
            let processor_manager =
                ProcessorManager::new(js_context.clone(), from_bucket_config).await?;
            processor_managers.insert(from_bucket_config.vertex, processor_manager);
        }
        let fetcher =
            ISBWatermarkFetcher::new(processor_managers, &config.from_vertex_config).await?;

        let processor_name = format!("{}-{}", get_vertex_name(), get_vertex_replica());
        let publisher = ISBWatermarkPublisher::new(
            processor_name,
            js_context.clone(),
            &config.to_vertex_config,
        )
        .await?;

        let actor = ISBWatermarkActor::new(fetcher, publisher);
        tokio::spawn(async move { actor.run(receiver).await });
        Ok(Self { sender })
    }

    /// Fetches the watermark for the given offset.
    pub(crate) async fn fetch_watermark(&self, offset: Offset) -> Result<Watermark> {
        if let Offset::Int(offset) = offset {
            let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
            self.sender
                .send(ISBWaterMarkActorMessage::FetchWatermark { offset, oneshot_tx })
                .await
                .map_err(|_| Error::Watermark("failed to send message".to_string()))?;

            oneshot_rx
                .await
                .map_err(|_| Error::Watermark("failed to receive response".to_string()))?
        } else {
            Err(Error::Watermark("invalid offset type".to_string()))
        }
    }

    /// publish_watermark publishes the watermark for the given stream and offset.
    pub(crate) async fn publish_watermark(&self, stream: Stream, offset: Offset) -> Result<()> {
        if let Offset::Int(offset) = offset {
            self.sender
                .send(ISBWaterMarkActorMessage::PublishWatermark { offset, stream })
                .await
                .map_err(|_| Error::Watermark("failed to send message".to_string()))?;
            Ok(())
        } else {
            Err(Error::Watermark("invalid offset type".to_string()))
        }
    }

    /// remove_offset removes the offset from the tracked offsets.
    pub(crate) async fn remove_offset(&self, offset: Offset) -> Result<()> {
        if let Offset::Int(offset) = offset {
            self.sender
                .send(ISBWaterMarkActorMessage::RemoveOffset(offset))
                .await
                .map_err(|_| Error::Watermark("failed to send message".to_string()))?;
            Ok(())
        } else {
            Err(Error::Watermark("invalid offset type".to_string()))
        }
    }
}
