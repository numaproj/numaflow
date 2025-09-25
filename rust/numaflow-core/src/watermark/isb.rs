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
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::EdgeWatermarkConfig;
use crate::config::pipeline::{ToVertexConfig, VertexType};
use crate::error::{Error, Result};
use crate::message::{IntOffset, Offset};
use crate::reduce::reducer::WindowManager;
use crate::tracker::TrackerHandle;
use crate::watermark::idle::isb::ISBIdleDetector;
use crate::watermark::isb::wm_fetcher::ISBWatermarkFetcher;
use crate::watermark::isb::wm_publisher::ISBWatermarkPublisher;
use crate::watermark::processor::manager::ProcessorManager;
use crate::watermark::wmb::{WMB, Watermark};

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
        offset: IntOffset,
        oneshot_tx: tokio::sync::oneshot::Sender<()>,
    },
    FetchHead {
        partition_idx: u16,
        oneshot_tx: tokio::sync::oneshot::Sender<Result<Watermark>>,
    },
    FetchHeadIdleWmb {
        partition_idx: u16,
        oneshot_tx: tokio::sync::oneshot::Sender<Result<Option<WMB>>>,
    },
    PublishIdleWatermark {
        oneshot_tx: tokio::sync::oneshot::Sender<()>,
    },
}

/// EdgeWatermarkActor comprises EdgeFetcher and EdgePublisher.
/// Contains all computation logic and data structures.
struct ISBWatermarkActor {
    fetcher: ISBWatermarkFetcher,
    publisher: ISBWatermarkPublisher,
    idle_manager: ISBIdleDetector,
    /// Window manager is used to compute the minimum watermark for the reduce vertex.
    window_manager: Option<WindowManager>,
    latest_fetched_wm: Watermark,
    tracker_handle: TrackerHandle,
    from_partitions: Vec<u16>,
}

impl ISBWatermarkActor {
    fn new(
        fetcher: ISBWatermarkFetcher,
        publisher: ISBWatermarkPublisher,
        idle_manager: ISBIdleDetector,
        window_manager: Option<WindowManager>,
        tracker_handle: TrackerHandle,
        from_partitions: Vec<u16>,
    ) -> Self {
        Self {
            fetcher,
            publisher,
            idle_manager,
            window_manager,
            latest_fetched_wm: Watermark::from_timestamp_millis(-1)
                .expect("failed to parse timestamp"),
            tracker_handle,
            from_partitions,
        }
    }

    /// run listens for messages and handles them
    async fn run(mut self, mut receiver: Receiver<ISBWaterMarkActorMessage>) {
        while let Some(message) = receiver.recv().await {
            if let Err(e) = self.handle_message(message).await {
                warn!("error handling watermark actor message: {:?}", e);
            }
        }
    }

    async fn handle_message(&mut self, message: ISBWaterMarkActorMessage) -> Result<()> {
        match message {
            // fetches the watermark for the given offset
            ISBWaterMarkActorMessage::Fetch { offset, oneshot_tx } => {
                let watermark = self
                    .fetcher
                    .fetch_watermark(offset.offset, offset.partition_idx);

                self.latest_fetched_wm = std::cmp::max(watermark, self.latest_fetched_wm);

                oneshot_tx
                    .send(Ok(watermark))
                    .map_err(|_| Error::Watermark("failed to send response".to_string()))
            }

            // publishes the watermark for the given stream and offset
            ISBWaterMarkActorMessage::Publish {
                stream,
                offset,
                oneshot_tx,
            } => {
                self.handle_publish_watermark(stream, offset).await;
                oneshot_tx
                    .send(())
                    .map_err(|_| Error::Watermark("failed to send response".to_string()))
            }

            // fetches the head watermark
            ISBWaterMarkActorMessage::FetchHead {
                partition_idx,
                oneshot_tx,
            } => {
                let watermark = self.fetcher.fetch_head_watermark(partition_idx);

                oneshot_tx
                    .send(Ok(watermark))
                    .map_err(|_| Error::Watermark("failed to send response".to_string()))
            }

            // fetches the head idle WMB for a specific partition
            ISBWaterMarkActorMessage::FetchHeadIdleWmb {
                partition_idx,
                oneshot_tx,
            } => {
                let wmb = self.fetcher.fetch_head_idle_wmb(partition_idx);

                if let Some(wmb) = wmb {
                    self.latest_fetched_wm = std::cmp::max(
                        self.latest_fetched_wm,
                        Watermark::from_timestamp_millis(wmb.watermark)
                            .expect("failed to parse time"),
                    );
                }

                oneshot_tx
                    .send(Ok(wmb))
                    .map_err(|_| Error::Watermark("failed to send response".to_string()))
            }

            // publishes idle watermark
            ISBWaterMarkActorMessage::PublishIdleWatermark { oneshot_tx } => {
                self.handle_publish_idle_watermark().await;
                oneshot_tx
                    .send(())
                    .map_err(|_| Error::Watermark("failed to send response".to_string()))
            }
        }
    }

    /// publishes the watermark for the given stream and offset
    async fn handle_publish_watermark(&mut self, stream: Stream, offset: IntOffset) {
        // Compute the minimum watermark
        let min_wm = self.compute_min_watermark().await;

        // Publish the watermark
        self.publisher
            .publish_watermark(&stream, offset.offset, min_wm.timestamp_millis(), false)
            .await;

        // Reset idle state for this stream
        self.idle_manager.reset_idle(&stream).await;
    }

    /// Publishes idle watermark. We can directly publish idle watermark with min watermark any time
    /// of because we are publishing based on the lowest watermark in the system. This cannot be true
    /// if min-watermark is -1. This means that we do not have data
    async fn handle_publish_idle_watermark(&mut self) {
        // Compute the minimum watermark
        let min_wm = self.compute_min_watermark().await;

        // if the computed min watermark is -1, means there is no data (no windows and inflight messages)
        // we should also check if the tracker indicates the prev-vertex is idling
        let min_wm = if min_wm.timestamp_millis() == -1 {
            // Check if the tracker indicates the reader is idling, only when we are completely idle
            // we can fetch and publish the head idle watermark.
            let idle_head_wmb = self.get_idle_watermark().await;

            // -1 does not strictly represent idling, so we cannot publish the idle watermark
            if idle_head_wmb.timestamp_millis() == -1 {
                return;
            }
            idle_head_wmb
        } else {
            min_wm
        };

        // we now know the lowest watermark to publish to the streams that are idling and we have a
        // barrier offset which can be used safely to publish the idle watermark.

        // Identify the streams that are idle and publish the idle watermark
        let idle_streams = self.idle_manager.fetch_idle_streams().await;
        for stream in idle_streams.iter() {
            if let Ok(offset) = self.idle_manager.fetch_idle_offset(stream).await {
                // publish the watermark
                self.publisher
                    .publish_watermark(stream, offset, min_wm.timestamp_millis(), true)
                    .await;

                self.idle_manager.update_idle_metadata(stream, offset).await;
            }
        }
    }

    /// Computes the minimum watermark based on window manager and inflight messages. This will return
    /// -1 if there are no windows and no inflight messages. That means we are not doing anything, this
    /// does not mean we are idling, it could be just that we cannot read from ISB due to errors, etc.
    /// If it returns -1, we should check if the tracker indicates the reader is idling.
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
                return std::cmp::min(
                    self.latest_fetched_wm,
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

    /// Gets the lowest idle watermark among all the partitions. If we cannot determine the lowest
    /// watermark, we return -1.
    /// We achieve this via "optimistic locking", when we set idle status to true in tracker, we will
    /// be setting the Head WMB offset. Here we will make sure that for every partition, the Head WMB
    /// saved in the idle status matches the Head WMB offset in the partition's timeline.
    async fn get_idle_watermark(&mut self) -> Watermark {
        let idle_offsets = self
            .tracker_handle
            .get_idle_offset()
            .await
            .unwrap_or_default();

        // iterate over all the partitions and check if they are idling by fetching the head idle wmb
        // and comparing it with the tracker's idle state.
        let mut min_wm = i64::MAX;
        for partition_idx in self.from_partitions.iter() {
            // if tracker's offset is none means, that partitions is not idling, we can skip publishing
            // the head idle wmb.
            let Some(Some(idle_offset)) = idle_offsets.get(partition_idx) else {
                return Watermark::from_timestamp_millis(-1).unwrap();
            };

            let wmb = self.fetcher.fetch_head_idle_wmb(*partition_idx);
            let Some(wmb) = wmb else {
                return Watermark::from_timestamp_millis(-1).unwrap();
            };

            // if the offset doesn't match, that means it's not idling anymore
            if wmb.offset != *idle_offset {
                return Watermark::from_timestamp_millis(-1).unwrap();
            }

            if wmb.watermark < min_wm {
                min_wm = wmb.watermark;
            }
        }
        if min_wm == i64::MAX {
            min_wm = -1;
        }
        Watermark::from_timestamp_millis(min_wm).expect("failed to parse time")
    }

    /// Gets the lowest watermark among all the inflight requests using the tracker
    async fn get_lowest_watermark(&self) -> Option<Watermark> {
        if let Ok(lowest_watermark) = self.tracker_handle.lowest_watermark().await {
            Some(Watermark::from_timestamp_millis(lowest_watermark.timestamp_millis()).unwrap())
        } else {
            None
        }
    }
}

/// Handle to interact with the EdgeWatermarkActor, exposes methods to fetch and publish watermarks
/// for the edges. Lightweight handle that only sends messages to the actor.
#[derive(Clone)]
pub(crate) struct ISBWatermarkHandle {
    sender: mpsc::Sender<ISBWaterMarkActorMessage>,
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
        tracker_handle: TrackerHandle,
        from_partitions: Vec<u16>,
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

        let processor_name = format!("{vertex_name}-{vertex_replica}");
        let publisher = ISBWatermarkPublisher::new(
            processor_name,
            js_context.clone(),
            &config.to_vertex_config,
        )
        .await?;

        let idle_manager =
            ISBIdleDetector::new(idle_timeout, to_vertex_configs, js_context.clone()).await;

        let actor = ISBWatermarkActor::new(
            fetcher,
            publisher,
            idle_manager.clone(),
            window_manager,
            tracker_handle.clone(),
            from_partitions,
        );
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
    pub(crate) async fn fetch_watermark(&mut self, offset: Offset) -> Watermark {
        let Offset::Int(offset) = offset else {
            warn!(?offset, "Invalid offset type, cannot compute watermark");
            return Watermark::from_timestamp_millis(-1).expect("failed to parse time");
        };

        let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
        if let Err(e) = self
            .sender
            .send(ISBWaterMarkActorMessage::Fetch { offset, oneshot_tx })
            .await
        {
            warn!(?e, "Failed to send message");
            return Watermark::from_timestamp_millis(-1).expect("failed to parse time");
        }

        match oneshot_rx.await {
            Ok(watermark) => watermark.unwrap_or_else(|e| {
                warn!(?e, "Failed to fetch watermark");
                Watermark::from_timestamp_millis(-1).expect("failed to parse time")
            }),
            Err(e) => {
                warn!(?e, "Failed to receive response");
                Watermark::from_timestamp_millis(-1).expect("failed to parse time")
            }
        }
    }

    /// Fetches the head watermark using the watermark fetcher. This returns the minimum
    /// of the head watermarks across all processors for the specified partition.
    pub(crate) async fn fetch_head_watermark(&mut self, partition_idx: u16) -> Watermark {
        let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
        if let Err(e) = self
            .sender
            .send(ISBWaterMarkActorMessage::FetchHead {
                partition_idx,
                oneshot_tx,
            })
            .await
        {
            warn!(?e, "Failed to send message");
            return Watermark::from_timestamp_millis(-1).expect("failed to parse time");
        }

        match oneshot_rx.await {
            Ok(watermark) => watermark.unwrap_or_else(|e| {
                warn!(?e, "Failed to fetch head watermark");
                Watermark::from_timestamp_millis(-1).expect("failed to parse time")
            }),
            Err(e) => {
                warn!(?e, "Failed to receive response");
                Watermark::from_timestamp_millis(-1).expect("failed to parse time")
            }
        }
    }

    /// Fetches the head idle WMB for the given partition. Returns the minimum idle WMB across all
    /// processors for the specified partition, but only if all active processors are idle for that
    /// partition.
    pub(crate) async fn fetch_head_idle_wmb(&mut self, partition_idx: u16) -> Option<WMB> {
        let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
        if let Err(e) = self
            .sender
            .send(ISBWaterMarkActorMessage::FetchHeadIdleWmb {
                partition_idx,
                oneshot_tx,
            })
            .await
        {
            warn!(?e, "Failed to send message");
            return None;
        }

        match oneshot_rx.await {
            Ok(wmb_result) => wmb_result.unwrap_or_else(|e| {
                warn!(?e, "Failed to fetch head idle WMB");
                None
            }),
            Err(e) => {
                warn!(?e, "Failed to receive response");
                None
            }
        }
    }

    /// publish_watermark publishes the watermark for the given stream and offset.
    pub(crate) async fn publish_watermark(&mut self, stream: Stream, offset: Offset) {
        let Offset::Int(offset) = offset else {
            warn!(?offset, "Invalid offset type, cannot publish watermark");
            return;
        };

        let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
        if let Err(e) = self
            .sender
            .send(ISBWaterMarkActorMessage::Publish {
                stream,
                offset,
                oneshot_tx,
            })
            .await
        {
            warn!(?e, "Failed to send message");
            return;
        }

        match oneshot_rx.await {
            Ok(_) => {}
            Err(e) => {
                warn!(?e, "Failed to receive response");
            }
        }
    }

    /// publishes the idle watermark for the downstream idle partitions.
    pub(crate) async fn publish_idle_watermark(&self) {
        let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
        if let Err(e) = self
            .sender
            .send(ISBWaterMarkActorMessage::PublishIdleWatermark { oneshot_tx })
            .await
        {
            warn!(?e, "Failed to send message");
            return;
        }

        match oneshot_rx.await {
            Ok(_) => {}
            Err(e) => {
                warn!(?e, "Failed to receive response");
            }
        }
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
    use crate::message::{IntOffset, Message};
    use crate::tracker::TrackerHandle;
    use crate::watermark::wmb::WMB;
    use tokio::sync::oneshot;

    // Helper function to create test messages
    fn create_test_message(
        offset: i64,
        partition_idx: u16,
        watermark_millis: Option<i64>,
    ) -> Message {
        let watermark =
            watermark_millis.map(|millis| chrono::DateTime::from_timestamp_millis(millis).unwrap());
        Message {
            offset: Offset::Int(IntOffset {
                offset,
                partition_idx,
            }),
            watermark,
            ..Default::default()
        }
    }

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
            partitions: vec![0],
            ot_bucket: ot_bucket_name,
            hb_bucket: hb_bucket_name,
            delay: None,
        };

        let to_bucket_config = BucketConfig {
            vertex: "to_vertex",
            partitions: vec![0],
            ot_bucket: to_ot_bucket_name,
            hb_bucket: to_hb_bucket_name,
            delay: None,
        };

        let _ = js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(hb_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(to_ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(to_hb_bucket_name.to_string())
            .await;

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
        let tracker_handle = TrackerHandle::new(None);

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
                to_vertex_type: VertexType::Sink,
            }],
            CancellationToken::new(),
            None,
            tracker_handle.clone(),
            vec![0],
        )
        .await
        .expect("Failed to create ISBWatermarkHandle");

        // Insert test messages into the tracker
        let message1 = create_test_message(1, 0, Some(100));
        let message2 = create_test_message(2, 0, Some(200));

        let (ack_send1, _ack_recv1) = oneshot::channel();
        let (ack_send2, _ack_recv2) = oneshot::channel();

        tracker_handle.insert(&message1, ack_send1).await.unwrap();
        tracker_handle.insert(&message2, ack_send2).await.unwrap();

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
            .get_key_value(to_ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        let timeout_duration = Duration::from_secs(1);
        let result = tokio::time::timeout(timeout_duration, async {
            loop {
                let wmb = ot_bucket
                    .get("test-vertex-0")
                    .await
                    .expect("Failed to get wmb");

                if let Some(wmb) = wmb {
                    let wmb: WMB = wmb.try_into().unwrap();
                    assert_eq!(wmb.watermark, 100);
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        if result.is_err() {
            panic!("WMB not found");
        }

        tracker_handle
            .delete(Offset::Int(IntOffset {
                offset: 1,
                partition_idx: 0,
            }))
            .await
            .unwrap();

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

        let timeout_duration = Duration::from_secs(1);
        let result = tokio::time::timeout(timeout_duration, async {
            loop {
                let wmb = ot_bucket
                    .get("test-vertex-0")
                    .await
                    .expect("Failed to get wmb");

                if let Some(wmb) = wmb {
                    let wmb: WMB = wmb.try_into().unwrap();
                    assert_eq!(wmb.watermark, 200);
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        if result.is_err() {
            panic!("WMB not found");
        }
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
            partitions: vec![0],
            ot_bucket: ot_bucket_name,
            hb_bucket: hb_bucket_name,
            delay: None,
        };

        let _ = js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(hb_bucket_name.to_string())
            .await;

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
        let tracker_handle = TrackerHandle::new(None);

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
                to_vertex_type: VertexType::Sink,
            }],
            CancellationToken::new(),
            None,
            tracker_handle.clone(),
            vec![0],
        )
        .await
        .expect("Failed to create ISBWatermarkHandle");

        let mut fetched_watermark = -1;
        // publish watermark and try fetching to see if something is getting published
        let timeout_duration = Duration::from_secs(1);
        let result = tokio::time::timeout(timeout_duration, async {
            for i in 0..10 {
                let offset = Offset::Int(IntOffset {
                    offset: i,
                    partition_idx: 0,
                });

                // Insert message into tracker
                let message = create_test_message(i, 0, Some(i * 100));
                let (ack_send, ack_recv) = oneshot::channel();
                tracker_handle.insert(&message, ack_send).await.unwrap();

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
                tracker_handle.delete(offset.clone()).await.unwrap();
                ack_recv.await.unwrap();
            }
        })
        .await;

        assert!(result.is_ok(), "Timeout occurred while fetching watermark");
        assert_ne!(fetched_watermark, -1);
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
            partitions: vec![0],
            ot_bucket: ot_bucket_name,
            hb_bucket: hb_bucket_name,
            delay: None,
        };

        let to_bucket_config = BucketConfig {
            vertex: "to_vertex",
            partitions: vec![0],
            ot_bucket: to_ot_bucket_name,
            hb_bucket: to_hb_bucket_name,
            delay: None,
        };

        // delete the stores
        let _ = js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(hb_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(to_ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(to_hb_bucket_name.to_string())
            .await;

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
        let tracker_handle = TrackerHandle::new(None);

        let _handle = ISBWatermarkHandle::new(
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
                to_vertex_type: VertexType::Sink,
            }],
            CancellationToken::new(),
            None,
            tracker_handle.clone(),
            vec![0],
        )
        .await
        .expect("Failed to create ISBWatermarkHandle");

        // Insert multiple offsets into tracker
        for i in 1..=3 {
            let message = create_test_message(i, 0, Some(i * 100));
            let (ack_send, _ack_recv) = oneshot::channel();
            tracker_handle.insert(&message, ack_send).await.unwrap();
        }

        // Wait for the idle timeout to trigger
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Check if the idle watermark is published
        let ot_bucket = js_context
            .get_key_value(to_ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        let timeout_duration = Duration::from_secs(1);
        let result = tokio::time::timeout(timeout_duration, async {
            loop {
                if let Some(wmb) = ot_bucket
                    .get("test-vertex-0")
                    .await
                    .expect("Failed to get wmb")
                {
                    let wmb: WMB = wmb.try_into().unwrap();
                    if wmb.idle {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        assert!(result.is_ok(), "Idle watermark not found");
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_fetch_head_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "test_fetch_head_watermark_OT";
        let hb_bucket_name = "test_fetch_head_watermark_PROCESSORS";

        let vertex_name = "test-vertex";

        let from_bucket_config = BucketConfig {
            vertex: "from_vertex",
            partitions: vec![0],
            ot_bucket: ot_bucket_name,
            hb_bucket: hb_bucket_name,
            delay: None,
        };

        let _ = js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(hb_bucket_name.to_string())
            .await;

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
        let tracker_handle = TrackerHandle::new(None);

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
                to_vertex_type: VertexType::Sink,
            }],
            CancellationToken::new(),
            None,
            tracker_handle.clone(),
            vec![0],
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

            // Insert message into tracker
            let message = create_test_message(i, 0, Some(i * 100));
            let (ack_send, ack_recv) = oneshot::channel();
            tracker_handle.insert(&message, ack_send).await.unwrap();

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

            let watermark = handle.fetch_head_watermark(0).await;

            if watermark.timestamp_millis() != -1 {
                fetched_watermark = watermark.timestamp_millis();
                break;
            }
            sleep(Duration::from_millis(10)).await;
            tracker_handle.delete(offset.clone()).await.unwrap();
            ack_recv.await.unwrap();
        }

        assert_ne!(fetched_watermark, -1);
    }
}
