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
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::EdgeWatermarkConfig;
use crate::config::pipeline::{ToVertexConfig, VertexType};
use crate::error::{Error, Result};
use crate::message::{IntOffset, Offset};
use crate::reduce::reducer::WindowManager;
use crate::watermark::idle::isb::ISBIdleDetector;
use crate::watermark::isb::wm_fetcher::ISBWatermarkFetcher;
use crate::watermark::isb::wm_publisher::ISBWatermarkPublisher;
use crate::watermark::processor::manager::ProcessorManager;
use crate::watermark::wmb::Watermark;

pub(crate) mod wm_fetcher;
pub(crate) mod wm_publisher;

/// Messages that can be sent to the [ISBWatermarkActor].
enum ISBWaterMarkActorMessage {
    Fetch {
        offset: IntOffset,
        oneshot_tx: tokio::sync::oneshot::Sender<Result<Watermark>>,
    },
    Publish {
        stream: Stream,
        offset: i64,
        watermark: i64,
        is_idle: bool,
    },
    FetchHeadIdle {
        oneshot_tx: tokio::sync::oneshot::Sender<Result<Watermark>>,
    },
}

/// Tuple of offset and watermark. We will use this to track the inflight messages.
#[derive(Eq, PartialEq, Debug, Clone)]
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
/// Only responsible for the actual fetch and publish operations.
struct ISBWatermarkActor {
    fetcher: ISBWatermarkFetcher,
    publisher: ISBWatermarkPublisher,
}

impl ISBWatermarkActor {
    fn new(fetcher: ISBWatermarkFetcher, publisher: ISBWatermarkPublisher) -> Self {
        Self { fetcher, publisher }
    }

    /// run listens for messages and handles them
    async fn run(mut self, mut receiver: Receiver<ISBWaterMarkActorMessage>) {
        while let Some(message) = receiver.recv().await {
            if let Err(e) = self.handle_message(message).await {
                error!("error handling watermark actor message: {:?}", e);
            }
        }
    }

    async fn handle_message(&mut self, message: ISBWaterMarkActorMessage) -> Result<()> {
        match message {
            // fetches the watermark for the given offset
            ISBWaterMarkActorMessage::Fetch { offset, oneshot_tx } => {
                self.handle_fetch_watermark(offset, oneshot_tx).await
            }

            // publishes the watermark for the given stream and offset
            ISBWaterMarkActorMessage::Publish {
                stream,
                offset,
                watermark,
                is_idle,
            } => {
                self.handle_publish_watermark(stream, offset, watermark, is_idle)
                    .await
            }

            // fetches the head idle watermark
            ISBWaterMarkActorMessage::FetchHeadIdle { oneshot_tx } => {
                self.handle_fetch_head_idle_watermark(oneshot_tx).await
            }
        }
    }

    // fetches the watermark for the given offset and sends the response back via oneshot channel
    async fn handle_fetch_watermark(
        &mut self,
        offset: IntOffset,
        oneshot_tx: tokio::sync::oneshot::Sender<Result<Watermark>>,
    ) -> Result<()> {
        let watermark = self
            .fetcher
            .fetch_watermark(offset.offset, offset.partition_idx);

        oneshot_tx
            .send(Ok(watermark))
            .map_err(|_| Error::Watermark("failed to send response".to_string()))
    }

    // publishes the watermark for the given stream and offset
    async fn handle_publish_watermark(
        &mut self,
        stream: Stream,
        offset: i64,
        watermark: i64,
        is_idle: bool,
    ) -> Result<()> {
        self.publisher
            .publish_watermark(&stream, offset, watermark, is_idle)
            .await;
        Ok(())
    }

    // fetches the head idle watermark and sends the response back via oneshot channel
    async fn handle_fetch_head_idle_watermark(
        &mut self,
        oneshot_tx: tokio::sync::oneshot::Sender<Result<Watermark>>,
    ) -> Result<()> {
        let watermark = self.fetcher.fetch_head_idle_watermark();
        oneshot_tx
            .send(Ok(watermark))
            .map_err(|_| Error::Watermark("failed to send response".to_string()))
    }
}

/// Handle to interact with the EdgeWatermarkActor, exposes methods to fetch and publish watermarks
/// for the edges. Contains all the computation logic.
#[derive(Clone)]
pub(crate) struct ISBWatermarkHandle {
    sender: mpsc::Sender<ISBWaterMarkActorMessage>,
    /// BTreeSet is used to track the watermarks of the inflight messages because we frequently
    /// need to get the lowest watermark among the inflight messages and BTreeSet provides O(1)
    /// time complexity for getting the lowest watermark, even though insertion and deletion are
    /// O(log n). If we use map or hashset, our lowest watermark fetch call would be O(n) even
    /// though insertion and deletion are O(1). We do almost same amount insertion, deletion and
    /// getting the lowest watermark so BTreeSet is the best choice.
    offset_set: Arc<Mutex<HashMap<u16, BTreeSet<OffsetWatermark>>>>,
    idle_manager: ISBIdleDetector,
    /// Window manager is used to compute the minimum watermark for the reduce vertex.
    window_manager: Option<WindowManager>,
    latest_fetched_wm: Arc<Mutex<Watermark>>,
}

impl ISBWatermarkHandle {
    /// new creates a new [ISBWatermarkHandle]. We also start a background task to detect WM idleness.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        vertex_name: &'static str,
        vertex_replica: u16,
        vertex_type: VertexType,
        idle_timeout: Duration,
        js_context: async_nats::jetstream::Context,
        config: &EdgeWatermarkConfig,
        to_vertex_configs: &[ToVertexConfig],
        cln_token: CancellationToken,
        window_manager: Option<WindowManager>,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(100);

        // create a processor manager map (from_vertex -> ProcessorManager)
        let mut processor_managers = HashMap::new();
        for from_bucket_config in &config.from_vertex_config {
            let processor_manager = ProcessorManager::new(
                js_context.clone(),
                from_bucket_config,
                vertex_type,
                vertex_replica,
            )
            .await?;
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

        let actor = ISBWatermarkActor::new(fetcher, publisher);
        tokio::spawn(async move { actor.run(receiver).await });

        let isb_watermark_handle = Self {
            sender,
            offset_set: Arc::new(Mutex::new(HashMap::new())),
            idle_manager,
            window_manager,
            latest_fetched_wm: Arc::new(Mutex::new(
                Watermark::from_timestamp_millis(-1).expect("failed to parse timestamp"),
            )),
        };

        // start a task to keep publishing idle watermarks every idle_timeout
        tokio::spawn({
            let mut isb_watermark_handle = isb_watermark_handle.clone();
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
    pub(crate) async fn fetch_watermark(&mut self, offset: Offset) -> Watermark {
        let Offset::Int(offset) = offset else {
            error!(?offset, "Invalid offset type, cannot compute watermark");
            return Watermark::from_timestamp_millis(-1).expect("failed to parse time");
        };

        let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
        if let Err(e) = self
            .sender
            .send(ISBWaterMarkActorMessage::Fetch { offset, oneshot_tx })
            .await
        {
            error!(?e, "Failed to send message");
            return Watermark::from_timestamp_millis(-1).expect("failed to parse time");
        }

        match oneshot_rx.await {
            Ok(watermark) => {
                let wm = watermark.unwrap_or_else(|e| {
                    error!(?e, "Failed to fetch watermark");
                    Watermark::from_timestamp_millis(-1).expect("failed to parse time")
                });

                // Update the latest fetched watermark
                let mut latest_fetched_wm = self
                    .latest_fetched_wm
                    .lock()
                    .expect("failed to acquire lock");
                *latest_fetched_wm = std::cmp::max(wm, *latest_fetched_wm);

                wm
            }
            Err(e) => {
                error!(?e, "Failed to receive response");
                Watermark::from_timestamp_millis(-1).expect("failed to parse time")
            }
        }
    }

    /// publish_watermark publishes the watermark for the given stream and offset.
    pub(crate) async fn publish_watermark(&mut self, stream: Stream, offset: Offset) {
        let Offset::Int(offset) = offset else {
            error!(?offset, "Invalid offset type, cannot publish watermark");
            return;
        };

        // Compute the minimum watermark
        let min_wm = self.compute_min_watermark().await;

        // Send the publish watermark message to the actor
        self.sender
            .send(ISBWaterMarkActorMessage::Publish {
                stream: stream.clone(),
                offset: offset.offset,
                watermark: min_wm.timestamp_millis(),
                is_idle: false,
            })
            .await
            .unwrap_or_else(|e| {
                error!("Failed to send message: {:?}", e);
            });

        // Reset idle state for this stream
        self.idle_manager.reset_idle(&stream).await;
    }

    /// remove_offset removes the offset from the tracked offsets.
    pub(crate) async fn remove_offset(&mut self, offset: Offset) {
        let Offset::Int(offset) = offset else {
            error!(?offset, "Invalid offset type, cannot remove offset");
            return;
        };

        // Remove the offset from the tracked offsets
        let mut offset_set = self.offset_set.lock().expect("failed to acquire lock");
        if let Some(set) = offset_set.get_mut(&offset.partition_idx) {
            if let Some(&OffsetWatermark { watermark, .. }) =
                set.iter().find(|ow| ow.offset == offset.offset)
            {
                set.remove(&OffsetWatermark {
                    offset: offset.offset,
                    watermark,
                });
            }
        }
    }

    /// insert_offset inserts the offset to the tracked offsets.
    pub(crate) async fn insert_offset(&mut self, offset: Offset, watermark: Option<Watermark>) {
        let Offset::Int(offset) = offset else {
            error!(?offset, "Invalid offset type, cannot insert offset");
            return;
        };

        let wm = watermark
            .unwrap_or(Watermark::from_timestamp_millis(-1).expect("failed to parse time"));

        // Insert the offset and watermark to the tracked offsets
        let mut offset_set = self.offset_set.lock().expect("failed to acquire lock");
        let set = offset_set.entry(offset.partition_idx).or_default();
        set.insert(OffsetWatermark {
            offset: offset.offset,
            watermark: wm,
        });
    }

    /// publishes the idle watermark for the downstream idle partitions.
    pub(crate) async fn publish_idle_watermark(&mut self) {
        // Compute the minimum watermark
        let mut min_wm = self.compute_min_watermark().await;

        // if the computed min watermark is -1, means there is no data (no windows and inflight messages)
        // we should fetch the head idle watermark and publish it to downstream.
        if min_wm.timestamp_millis() == -1 {
            let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
            if let Err(e) = self
                .sender
                .send(ISBWaterMarkActorMessage::FetchHeadIdle { oneshot_tx })
                .await
            {
                error!(?e, "Failed to send message");
                return;
            }

            min_wm = match oneshot_rx.await {
                Ok(watermark) => watermark.unwrap_or_else(|e| {
                    error!(?e, "Failed to fetch head idle watermark");
                    Watermark::from_timestamp_millis(-1).expect("failed to parse time")
                }),
                Err(e) => {
                    error!(?e, "Failed to receive response");
                    Watermark::from_timestamp_millis(-1).expect("failed to parse time")
                }
            };
        }

        // Identify the streams that are idle and publish the idle watermark
        let idle_streams = self.idle_manager.fetch_idle_streams().await;
        for stream in idle_streams.iter() {
            if let Ok(offset) = self.idle_manager.fetch_idle_offset(stream).await {
                // publish the watermark
                self.sender
                    .send(ISBWaterMarkActorMessage::Publish {
                        stream: stream.clone(),
                        offset,
                        watermark: min_wm.timestamp_millis(),
                        is_idle: true,
                    })
                    .await
                    .unwrap_or_else(|e| {
                        error!("Failed to send message: {:?}", e);
                    });

                self.idle_manager.update_idle_metadata(stream, offset).await;
            }
        }
    }

    /// Computes the minimum watermark based on window manager and inflight messages.
    async fn compute_min_watermark(&self) -> Watermark {
        // If window manager is configured, we can use the oldest window's end time - 1ms as the
        // watermark.
        if let Some(window_manager) = &self.window_manager {
            let oldest_window_end_time = match window_manager {
                WindowManager::Aligned(aligned_manager) => {
                    aligned_manager.oldest_window().map(|w| w.end_time)
                }
                WindowManager::Unaligned(unaligned_manager) => {
                    unaligned_manager.oldest_window_end_time()
                }
            };

            if let Some(oldest_window_et) = oldest_window_end_time {
                // we should also compare it with the latest fetched watermark because sometimes
                // the window end time can be greater than the watermark of the messages in the window.
                // in that case we should use the latest fetched watermark.
                let latest_fetched_wm = self
                    .latest_fetched_wm
                    .lock()
                    .expect("failed to acquire lock");
                return std::cmp::min(
                    *latest_fetched_wm,
                    Watermark::from_timestamp_millis(oldest_window_et.timestamp_millis() - 1)
                        .expect("failed to parse time"),
                );
            }
        }

        // if window manager is not configured, we should use the lowest watermark among all the
        // inflight messages.
        self.get_lowest_watermark()
            .await
            .unwrap_or(Watermark::from_timestamp_millis(-1).unwrap())
    }

    /// Gets the lowest watermark among all the inflight requests
    async fn get_lowest_watermark(&self) -> Option<Watermark> {
        let offset_set = self.offset_set.lock().expect("failed to acquire lock");
        offset_set
            .values()
            .filter_map(|set| set.iter().next().map(|ow| ow.watermark))
            .min()
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

        let mut handle = ISBWatermarkHandle::new(
            vertex_name,
            0,
            VertexType::MapUDF,
            Duration::from_millis(100),
            js_context.clone(),
            &edge_config,
            &[ToVertexConfig {
                name: "to_vertex",
                partitions: 0,
                writer_config: BufferWriterConfig {
                    streams: vec![Stream::new("test_stream", "to_vertex", 0)],
                    ..Default::default()
                },
                conditions: None,
                vertex_type: VertexType::Sink,
            }],
            CancellationToken::new(),
            None,
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

        let mut handle = ISBWatermarkHandle::new(
            vertex_name,
            0,
            VertexType::MapUDF,
            Duration::from_millis(100),
            js_context.clone(),
            &edge_config,
            &[ToVertexConfig {
                name: "from_vertex",
                partitions: 0,
                writer_config: BufferWriterConfig {
                    streams: vec![Stream::new("test_stream", "from_vertex", 0)],
                    ..Default::default()
                },
                conditions: None,
                vertex_type: VertexType::Sink,
            }],
            CancellationToken::new(),
            None,
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

        let mut handle = ISBWatermarkHandle::new(
            vertex_name,
            0,
            VertexType::MapUDF,
            Duration::from_millis(10), // Set idle timeout to a very short duration
            js_context.clone(),
            &edge_config,
            &[ToVertexConfig {
                name: "to_vertex",
                partitions: 0,
                writer_config: BufferWriterConfig {
                    streams: vec![Stream::new("test_stream", "to_vertex", 0)],
                    ..Default::default()
                },
                conditions: None,
                vertex_type: VertexType::Sink,
            }],
            CancellationToken::new(),
            None,
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
