//! Exposes methods to fetch the watermark for the messages read from [crate::pipeline::isb], and
//! publish the watermark for the messages written to [crate::pipeline::isb]. Manages the timelines
//! of the watermark published by the previous vertices for each partition and fetches the lowest
//! watermark among them. It tracks the watermarks of all the inflight messages for each partition,
//! and publishes the lowest watermark. The watermark published to the ISB will always be monotonically
//! increasing. Fetch and publish will be two different flows, but we will have natural ordering
//! because we use actor model. Since we do streaming within the vertex we have to track the
//! messages so that even if any messages get stuck we consider them while publishing watermarks.
//! Starts a background task to publish idle watermarks for the downstream idle partitions, idle
//! partitions are those partitions where we have not published any watermark for a certain time.
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
use std::time::Duration;

use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::EdgeWatermarkConfig;
use crate::config::pipeline::ToVertexConfig;
use crate::error::{Error, Result};
use crate::message::{IntOffset, Offset};
use crate::watermark::idle::isb::ISBIdleDetector;
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
    CheckAndPublishIdleWatermark,
}

/// Tuple of offset and watermark. We will use this to track the inflight messages.
#[derive(Eq, PartialEq, Debug)]
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
    idle_manager: ISBIdleDetector,
}

impl ISBWatermarkActor {
    fn new(
        fetcher: ISBWatermarkFetcher,
        publisher: ISBWatermarkPublisher,
        idle_manager: ISBIdleDetector,
    ) -> Self {
        Self {
            fetcher,
            publisher,
            offset_set: HashMap::new(),
            idle_manager,
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
                    .fetch_watermark(offset.offset, offset.partition_idx);

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
                    .publish_watermark(&stream, offset.offset, min_wm.timestamp_millis(), false)
                    .await;

                self.idle_manager.reset_idle(&stream).await;
            }

            // removes the offset from the tracked offsets
            ISBWaterMarkActorMessage::RemoveOffset(offset) => {
                self.remove_offset(offset.partition_idx, offset.offset)?;
            }

            // inserts the offset to the tracked offsets
            ISBWaterMarkActorMessage::InsertOffset { offset, watermark } => {
                self.insert_offset(offset.partition_idx, offset.offset, watermark);
            }

            // check for idleness and publish idle watermark for those downstream idle partitions
            ISBWaterMarkActorMessage::CheckAndPublishIdleWatermark => {
                // if there are any inflight messages, consider the lowest watermark among them
                let mut min_wm = self
                    .get_lowest_watermark()
                    .unwrap_or(Watermark::from_timestamp_millis(-1).expect("failed to parse time"));

                // if there are no inflight messages, use the head idle watermark
                if min_wm.timestamp_millis() == -1 {
                    min_wm = self.fetcher.fetch_head_idle_watermark();
                }

                // we are not able to compute WM, pod restarts, etc.
                if min_wm.timestamp_millis() == -1 {
                    return Ok(());
                }

                // identify the streams that are idle.
                let idle_streams = self.idle_manager.fetch_idle_streams().await;

                // publish the idle watermark for the idle partitions
                for stream in idle_streams.iter() {
                    let offset = match self.idle_manager.fetch_idle_offset(stream).await {
                        Ok(offset) => offset,
                        Err(e) => {
                            error!("failed to fetch idle offset: {:?}", e);
                            continue;
                        }
                    };
                    self.publisher
                        .publish_watermark(stream, offset, min_wm.timestamp_millis(), true)
                        .await;
                    self.idle_manager.update_idle_metadata(stream, offset).await;
                }
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
    /// new creates a new [ISBWatermarkHandle]. We also start a background task to detect WM idleness.
    pub(crate) async fn new(
        vertex_name: &'static str,
        vertex_replica: u16,
        idle_timeout: Duration,
        js_context: async_nats::jetstream::Context,
        config: &EdgeWatermarkConfig,
        to_vertex_configs: &[ToVertexConfig],
        cln_token: CancellationToken,
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

        let idle_manager =
            ISBIdleDetector::new(idle_timeout, to_vertex_configs, js_context.clone()).await;

        let actor = ISBWatermarkActor::new(fetcher, publisher, idle_manager);
        tokio::spawn(async move { actor.run(receiver).await });

        let isb_watermark_handle = Self { sender };

        // start a task to keep publishing idle watermarks every idle_timeout
        tokio::spawn({
            let isb_watermark_handle = isb_watermark_handle.clone();
            let mut interval_ticker = tokio::time::interval(idle_timeout);
            let cln_token = cln_token.clone();
            async move {
                loop {
                    tokio::select! {
                        _ = interval_ticker.tick() => {
                            isb_watermark_handle.publish_idle_watermark().await;
                        }
                        _ = cln_token.cancelled() => {
                            break;
                        }
                    }
                }
            }
        });

        Ok(isb_watermark_handle)
    }

    /// Fetches the watermark for the given offset, if we are not able to compute the watermark we
    /// return -1.
    pub(crate) async fn fetch_watermark(&self, offset: Offset) -> Watermark {
        if let Offset::Int(offset) = offset {
            let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
            if let Err(e) = self
                .sender
                .send(ISBWaterMarkActorMessage::FetchWatermark { offset, oneshot_tx })
                .await
            {
                error!(?e, "Failed to send message");
                return Watermark::from_timestamp_millis(-1).expect("failed to parse time");
            }

            match oneshot_rx.await {
                Ok(watermark) => watermark.unwrap_or_else(|e| {
                    error!(?e, "Failed to fetch watermark");
                    Watermark::from_timestamp_millis(-1).expect("failed to parse time")
                }),
                Err(e) => {
                    error!(?e, "Failed to receive response");
                    Watermark::from_timestamp_millis(-1).expect("failed to parse time")
                }
            }
        } else {
            error!(?offset, "Invalid offset type, cannot compute watermark");
            Watermark::from_timestamp_millis(-1).expect("failed to parse time")
        }
    }

    /// publish_watermark publishes the watermark for the given stream and offset.
    pub(crate) async fn publish_watermark(&self, stream: Stream, offset: Offset) {
        if let Offset::Int(offset) = offset {
            self.sender
                .send(ISBWaterMarkActorMessage::PublishWatermark { offset, stream })
                .await
                .unwrap_or_else(|e| {
                    error!("Failed to send message: {:?}", e);
                });
        } else {
            error!(?offset, "Invalid offset type, cannot publish watermark");
        }
    }

    /// remove_offset removes the offset from the tracked offsets.
    pub(crate) async fn remove_offset(&self, offset: Offset) {
        if let Offset::Int(offset) = offset {
            self.sender
                .send(ISBWaterMarkActorMessage::RemoveOffset(offset))
                .await
                .unwrap_or_else(|e| {
                    error!("Failed to send message: {:?}", e);
                });
        } else {
            error!(?offset, "Invalid offset type, cannot remove offset");
        }
    }

    /// insert_offset inserts the offset to the tracked offsets.
    pub(crate) async fn insert_offset(&self, offset: Offset, watermark: Option<Watermark>) {
        if let Offset::Int(offset) = offset {
            self.sender
                .send(ISBWaterMarkActorMessage::InsertOffset {
                    offset,
                    watermark: watermark.unwrap_or(
                        Watermark::from_timestamp_millis(-1).expect("failed to parse time"),
                    ),
                })
                .await
                .unwrap_or_else(|e| {
                    error!("Failed to send message: {:?}", e);
                });
        } else {
            error!(?offset, "Invalid offset type, cannot insert offset");
        }
    }

    /// publishes the idle watermark for the downstream idle partitions.
    pub(crate) async fn publish_idle_watermark(&self) {
        self.sender
            .send(ISBWaterMarkActorMessage::CheckAndPublishIdleWatermark)
            .await
            .unwrap_or_else(|e| {
                error!("Failed to send message: {:?}", e);
            });
    }
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;
    use tokio::time::sleep;

    use super::*;
    use crate::config::pipeline::isb::{BufferWriterConfig, Stream};
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

        let handle = ISBWatermarkHandle::new(
            vertex_name,
            0,
            Duration::from_millis(100),
            js_context.clone(),
            &edge_config,
            &vec![ToVertexConfig {
                name: "to_vertex",
                partitions: 0,
                writer_config: BufferWriterConfig {
                    streams: vec![Stream::new("test_stream", "to_vertex", 0)],
                    ..Default::default()
                },
                conditions: None,
            }],
            CancellationToken::new(),
        )
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
            .await;

        handle
            .insert_offset(
                Offset::Int(IntOffset {
                    offset: 2,
                    partition_idx: 0,
                }),
                Some(Watermark::from_timestamp_millis(200).unwrap()),
            )
            .await;

        handle
            .publish_watermark(
                Stream {
                    name: "test_stream",
                    vertex: "to_vertex",
                    partition: 0,
                },
                Offset::Int(IntOffset {
                    offset: 1,
                    partition_idx: 0,
                }),
            )
            .await;

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
            sleep(Duration::from_millis(10)).await;
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
            .await;

        handle
            .publish_watermark(
                Stream {
                    name: "test_stream",
                    vertex: "to_vertex",
                    partition: 0,
                },
                Offset::Int(IntOffset {
                    offset: 2,
                    partition_idx: 0,
                }),
            )
            .await;

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
            sleep(Duration::from_millis(10)).await;
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

        let handle = ISBWatermarkHandle::new(
            vertex_name,
            0,
            Duration::from_millis(100),
            js_context.clone(),
            &edge_config,
            &vec![ToVertexConfig {
                name: "from_vertex",
                partitions: 0,
                writer_config: BufferWriterConfig {
                    streams: vec![Stream::new("test_stream", "from_vertex", 0)],
                    ..Default::default()
                },
                conditions: None,
            }],
            CancellationToken::new(),
        )
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
                .await;

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
                .await;

            let watermark = handle
                .fetch_watermark(Offset::Int(IntOffset {
                    offset: 3,
                    partition_idx: 0,
                }))
                .await;

            if watermark.timestamp_millis() != -1 {
                fetched_watermark = watermark.timestamp_millis();
                break;
            }
            sleep(Duration::from_millis(10)).await;
            handle.remove_offset(offset.clone()).await;
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

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_publish_idle_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "test_publish_idle_watermark_OT";
        let hb_bucket_name = "test_publish_idle_watermark_PROCESSORS";
        let to_ot_bucket_name = "test_publish_idle_watermark_to_OT";
        let to_hb_bucket_name = "test_publish_idle_watermark_to_PROCESSORS";

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

        let handle = ISBWatermarkHandle::new(
            vertex_name,
            0,
            Duration::from_millis(10), // Set idle timeout to a very short duration
            js_context.clone(),
            &edge_config,
            &vec![ToVertexConfig {
                name: "to_vertex",
                partitions: 0,
                writer_config: BufferWriterConfig {
                    streams: vec![Stream::new("test_stream", "to_vertex", 0)],
                    ..Default::default()
                },
                conditions: None,
            }],
            CancellationToken::new(),
        )
        .await
        .expect("Failed to create ISBWatermarkHandle");

        // Insert multiple offsets
        for i in 1..=3 {
            handle
                .insert_offset(
                    Offset::Int(IntOffset {
                        offset: i,
                        partition_idx: 0,
                    }),
                    Some(Watermark::from_timestamp_millis(i * 100).unwrap()),
                )
                .await;
        }

        // Wait for the idle timeout to trigger
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Check if the idle watermark is published
        let ot_bucket = js_context
            .get_key_value(to_ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        let mut wmb_found = false;
        for _ in 0..10 {
            if let Some(wmb) = ot_bucket
                .get("test-vertex-0")
                .await
                .expect("Failed to get wmb")
            {
                let wmb: WMB = wmb.try_into().unwrap();
                if wmb.idle {
                    wmb_found = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(wmb_found, "Idle watermark not found");

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
}
