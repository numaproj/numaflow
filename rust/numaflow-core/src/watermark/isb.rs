//! Exposes methods to fetch the watermark for the messages read from [crate::pipeline::isb], and
//! publish the watermark for the messages written to [crate::pipeline::isb]. Manages the timelines
//! of the watermark published by the previous vertices for each partition and fetches the lowest
//! watermark among them. It tracks the watermarks of all the inflight messages for each partition,
//! and publishes the lowest watermark. The watermark published to the ISB will always be monotonically
//! increasing. Fetch and publish will be two different flows, but we will have natural ordering
//! because we use actor model. Since we do streaming within the vertex we have to track the
//! messages so that even if any messages get stuck we consider them while publishing watermarks.
//!
//!
//! **Fetch Flow**
//! ```text
//! (Read from ISB) -------> (Fetch Watermark) -------> (Track Offset and WM)
//! ```
//!
//! **Publish Flow**
//! ```text
//! (Write to ISB) -------> (Publish Watermark) ------> (Remove tracked Offset)
//! ```

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

use tokio::sync::mpsc::Receiver;
use tracing::error;

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::EdgeWatermarkConfig;
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
    InsertOffset {
        offset: IntOffset,
        watermark: Watermark,
    },
}

/// Tuple of offset and watermark. We will use this to track the inflight messages.
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
    /// BTreeSet is used to track the watermarks of the inflight messages because we frequently
    /// need to get the lowest watermark among the inflight messages and BTreeSet provides O(1)
    /// time complexity for getting the lowest watermark, even though insertion and deletion are
    /// O(log n). If we use map or hashset, our lowest watermark fetch call would be O(n) even
    /// though insertion and deletion are O(1). We do almost same amount insertion, deletion and
    /// getting the lowest watermark so BTreeSet is the best choice.
    offset_set: HashMap<u16, BTreeSet<OffsetWatermark>>,
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

    /// handle_message handles the incoming actor message.
    async fn handle_message(&mut self, message: ISBWaterMarkActorMessage) -> Result<()> {
        match message {
            // fetches the watermark for the given offset
            ISBWaterMarkActorMessage::FetchWatermark { offset, oneshot_tx } => {
                let watermark = self
                    .fetcher
                    .fetch_watermark(offset.offset, offset.partition_idx)
                    .await?;

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

            // inserts the offset to the tracked offsets
            ISBWaterMarkActorMessage::InsertOffset { offset, watermark } => {
                self.insert_offset(offset.partition_idx, offset.offset, watermark);
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
        vertex_name: &'static str,
        vertex_replica: u16,
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

        let processor_name = format!("{}-{}", vertex_name, vertex_replica);
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

    /// insert_offset inserts the offset to the tracked offsets.
    pub(crate) async fn insert_offset(
        &self,
        offset: Offset,
        watermark: Option<Watermark>,
    ) -> Result<()> {
        if let Offset::Int(offset) = offset {
            self.sender
                .send(ISBWaterMarkActorMessage::InsertOffset {
                    offset,
                    watermark: watermark.unwrap_or(
                        Watermark::from_timestamp_millis(-1).expect("failed to parse time"),
                    ),
                })
                .await
                .map_err(|_| Error::Watermark("failed to send message".to_string()))?;
            Ok(())
        } else {
            Err(Error::Watermark("invalid offset type".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;
    use tokio::time::sleep;

    use super::*;
    use crate::config::pipeline::isb::Stream;
    use crate::config::pipeline::watermark::BucketConfig;
    use crate::message::IntOffset;
    use crate::watermark::wmb::WMB;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_isb_watermark_handle_publish_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "test_isb_publish_watermark_OT";
        let hb_bucket_name = "test_isb_publish_watermark_PROCESSORS";
        let to_ot_bucket_name = "test_isb_publish_watermark_to_OT";
        let to_hb_bucket_name = "test_isb_publish_watermark_to_PROCESSORS";

        let vertex_name = "test-vertex";

        let from_bucket_config = BucketConfig {
            vertex: "from_vertex",
            partitions: 1,
            ot_bucket: ot_bucket_name,
            hb_bucket: hb_bucket_name,
        };

        let to_bucket_config = BucketConfig {
            vertex: "to_vertex",
            partitions: 1,
            ot_bucket: to_ot_bucket_name,
            hb_bucket: to_hb_bucket_name,
        };

        // create key value stores
        js_context
            .create_key_value(Config {
                bucket: ot_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        js_context
            .create_key_value(Config {
                bucket: hb_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        js_context
            .create_key_value(Config {
                bucket: to_ot_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        js_context
            .create_key_value(Config {
                bucket: to_hb_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let edge_config = EdgeWatermarkConfig {
            from_vertex_config: vec![from_bucket_config.clone()],
            to_vertex_config: vec![to_bucket_config.clone()],
        };

        let handle = ISBWatermarkHandle::new(vertex_name, 0, js_context.clone(), &edge_config)
            .await
            .expect("Failed to create ISBWatermarkHandle");

        handle
            .insert_offset(
                Offset::Int(IntOffset {
                    offset: 1,
                    partition_idx: 0,
                }),
                Some(Watermark::from_timestamp_millis(100).unwrap()),
            )
            .await
            .expect("Failed to insert offset");

        handle
            .insert_offset(
                Offset::Int(IntOffset {
                    offset: 2,
                    partition_idx: 0,
                }),
                Some(Watermark::from_timestamp_millis(200).unwrap()),
            )
            .await
            .expect("Failed to insert offset");

        handle
            .publish_watermark(
                Stream {
                    name: "test_stream",
                    vertex: "from_vertex",
                    partition: 0,
                },
                Offset::Int(IntOffset {
                    offset: 1,
                    partition_idx: 0,
                }),
            )
            .await
            .expect("Failed to publish watermark");

        let ot_bucket = js_context
            .get_key_value(ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        let mut wmb_found = true;
        for _ in 0..10 {
            let wmb = ot_bucket
                .get("test-vertex-0")
                .await
                .expect("Failed to get wmb");

            if let Some(wmb) = wmb {
                let wmb: WMB = wmb.try_into().unwrap();
                assert_eq!(wmb.watermark, 100);
                wmb_found = true;
            }
            sleep(std::time::Duration::from_millis(10)).await;
        }

        if !wmb_found {
            panic!("WMB not found");
        }

        // remove the smaller offset and then publish watermark and see
        handle
            .remove_offset(Offset::Int(IntOffset {
                offset: 1,
                partition_idx: 0,
            }))
            .await
            .expect("Failed to remove offset");

        handle
            .publish_watermark(
                Stream {
                    name: "test_stream",
                    vertex: "from_vertex",
                    partition: 0,
                },
                Offset::Int(IntOffset {
                    offset: 2,
                    partition_idx: 0,
                }),
            )
            .await
            .unwrap();

        let mut wmb_found = true;
        for _ in 0..10 {
            let wmb = ot_bucket
                .get("test-vertex-0")
                .await
                .expect("Failed to get wmb");

            if let Some(wmb) = wmb {
                let wmb: WMB = wmb.try_into().unwrap();
                assert_eq!(wmb.watermark, 200);
                wmb_found = true;
            }
            sleep(std::time::Duration::from_millis(10)).await;
        }

        if !wmb_found {
            panic!("WMB not found");
        }

        // delete the stores
        js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await
            .unwrap();
        js_context
            .delete_key_value(hb_bucket_name.to_string())
            .await
            .unwrap();
        js_context
            .delete_key_value(to_ot_bucket_name.to_string())
            .await
            .unwrap();
        js_context
            .delete_key_value(to_hb_bucket_name.to_string())
            .await
            .unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_fetch_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "test_fetch_watermark_OT";
        let hb_bucket_name = "test_fetch_watermark_PROCESSORS";

        let vertex_name = "test-vertex";

        let from_bucket_config = BucketConfig {
            vertex: "from_vertex",
            partitions: 1,
            ot_bucket: ot_bucket_name,
            hb_bucket: hb_bucket_name,
        };

        // create key value stores
        js_context
            .create_key_value(Config {
                bucket: ot_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        js_context
            .create_key_value(Config {
                bucket: hb_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let edge_config = EdgeWatermarkConfig {
            from_vertex_config: vec![from_bucket_config.clone()],
            to_vertex_config: vec![from_bucket_config.clone()],
        };

        let handle = ISBWatermarkHandle::new(vertex_name, 0, js_context.clone(), &edge_config)
            .await
            .expect("Failed to create ISBWatermarkHandle");

        let mut fetched_watermark = -1;
        // publish watermark and try fetching to see if something is getting published
        for i in 0..10 {
            let offset = Offset::Int(IntOffset {
                offset: i,
                partition_idx: 0,
            });

            handle
                .insert_offset(
                    offset.clone(),
                    Some(Watermark::from_timestamp_millis(i * 100).unwrap()),
                )
                .await
                .expect("Failed to insert offset");

            handle
                .publish_watermark(
                    Stream {
                        name: "test_stream",
                        vertex: "from_vertex",
                        partition: 0,
                    },
                    Offset::Int(IntOffset {
                        offset: i,
                        partition_idx: 0,
                    }),
                )
                .await
                .expect("Failed to publish watermark");

            let watermark = handle
                .fetch_watermark(Offset::Int(IntOffset {
                    offset: 3,
                    partition_idx: 0,
                }))
                .await
                .expect("Failed to fetch watermark");

            if watermark.timestamp_millis() != -1 {
                fetched_watermark = watermark.timestamp_millis();
                break;
            }
            sleep(std::time::Duration::from_millis(10)).await;
            handle
                .remove_offset(offset.clone())
                .await
                .expect("Failed to insert offset");
        }

        assert_ne!(fetched_watermark, -1);

        // delete the stores
        js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await
            .unwrap();
        js_context
            .delete_key_value(hb_bucket_name.to_string())
            .await
            .unwrap();
    }
}
