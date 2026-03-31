//! Exposes methods to fetch and publish the watermark for the messages read from [Source], also
//! exposes methods to publish the watermark for the messages written to [ISB]. Since the watermark
//! starts at source we publish and fetch inside the source to get the minimum event time across the
//! partitions. Since we write the messages to ISB, we publish the watermark for the messages written
//! to ISB. Hence, source publisher internally uses ISB publisher to publish the watermarks. Since source
//! is not streaming in nature we don't have to track the inflight offsets and their watermarks.
//!
//!
//! ##### Watermark flow
//!
//! ```text
//! (Read) ---> (Publish WM For Source) ---> (Fetch WM For Source) ---> (Write to ISB) ---> (Publish WM to ISB)
//! ```
//!
//! [Source]: https://numaflow.numaproj.io/user-guide/sources/overview/
//! [ISB]: https://numaflow.numaproj.io/core-concepts/inter-step-buffer/

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use numaflow_shared::kv::KVStore;
use numaflow_shared::kv::jetstream::JetstreamKVStore;

use crate::config::pipeline::isb::Stream;
use crate::config::pipeline::watermark::SourceWatermarkConfig;
use crate::config::pipeline::{ToVertexConfig, VertexType};
use crate::error::Result;
use crate::message::{IntOffset, Message, Offset};
use crate::watermark::idle::isb::ISBIdleDetector;
use crate::watermark::idle::source::SourceIdleDetector;
use crate::watermark::processor::manager::ProcessorManager;
use crate::watermark::source::source_wm_fetcher::SourceWatermarkFetcher;
use crate::watermark::source::source_wm_publisher::SourceWatermarkPublisher;
use crate::watermark::wmb::Watermark;

/// fetcher for fetching the source watermark
pub(crate) mod source_wm_fetcher;

/// publisher for publishing the source watermark
pub(crate) mod source_wm_publisher;

/// Shared state for SourceWatermarkHandle.
/// Contains all computation logic and data structures.
struct SourceWatermarkState {
    publisher: SourceWatermarkPublisher,
    fetcher: SourceWatermarkFetcher,
    isb_idle_manager: ISBIdleDetector,
    /// Source idle detector is always present (with default 100ms heartbeat interval).
    /// It handles both heartbeat publishing and idle detection (if user configured).
    source_idle_manager: SourceIdleDetector,
    /// Cache of partitions that have been active during the current interval.
    /// Used for ISB idle watermark publishing.
    active_input_partitions: HashMap<u16, bool>,
}

impl SourceWatermarkState {
    /// Creates a new SourceWatermarkState.
    fn new(
        publisher: SourceWatermarkPublisher,
        fetcher: SourceWatermarkFetcher,
        isb_idle_manager: ISBIdleDetector,
        source_idle_manager: SourceIdleDetector,
    ) -> Self {
        Self {
            publisher,
            fetcher,
            isb_idle_manager,
            source_idle_manager,
            active_input_partitions: HashMap::new(),
        }
    }

    /// Handles generating and publishing source watermark with computation
    async fn generate_and_publish_source_watermark(
        &mut self,
        messages: Vec<Message>,
    ) -> Result<()> {
        // we need to build a hash-map of the lowest event time for each partition
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

        if partition_to_lowest_event_time.is_empty() {
            return Ok(());
        }

        // Publish the watermark for each partition
        for (partition, event_time) in partition_to_lowest_event_time {
            self.publisher
                .publish_source_watermark(partition, event_time, false)
                .await;

            // cache the active input partitions, we need it for publishing isb idle watermark
            self.active_input_partitions.insert(partition, true);

            // Reset the source idle manager for this partition
            self.source_idle_manager.reset(partition);
        }

        Ok(())
    }

    /// Handles publishing source ISB watermark with computation
    async fn publish_source_isb_watermark(
        &mut self,
        stream: Stream,
        offset: IntOffset,
        input_partition: u16,
    ) -> Result<()> {
        // Fetch the source watermark
        let watermark = self.fetcher.fetch_source_watermark(Some(offset.offset));

        // Publish the watermark
        self.publisher
            .publish_isb_watermark(
                input_partition,
                &stream,
                offset.offset,
                watermark.timestamp_millis(),
                false,
            )
            .await;

        // Mark the vertex and partition as active since we published the watermark
        self.isb_idle_manager.reset_idle(&stream).await;

        Ok(())
    }

    /// Publishes source watermark for partitions that need it (step interval has passed).
    /// Watermarks serve as heartbeats for downstream vertices via KV entry timestamps.
    /// Each partition is tracked independently:
    /// - If partition is idle: watermark value is incremented, idle=true
    /// - If partition is not idle: watermark value stays the same, idle=false
    async fn publish_source_idle_watermark(&mut self) -> Result<()> {
        // Get partitions that need watermark publishing (step interval passed)
        let partitions_needing_publish = self.source_idle_manager.partitions_needing_publish();
        if partitions_needing_publish.is_empty() {
            return Ok(());
        }

        // Fetch the current source watermark (may be -1 if no data yet, but we still
        // publish to update KV entry timestamp which signals liveness to downstream vertices)
        let compute_wm = self.fetcher.fetch_source_watermark(None);

        // Process each partition that needs publishing
        for partition in partitions_needing_publish {
            // Check if this partition is truly idle (threshold passed)
            let is_idle = self.source_idle_manager.is_partition_idle(partition);

            // Compute the watermark value for this partition
            let wm_value = self
                .source_idle_manager
                .compute_watermark(partition, compute_wm.timestamp_millis());

            // Publish source watermark for this partition
            self.publisher
                .publish_source_watermark(partition, wm_value, is_idle)
                .await;
        }

        Ok(())
    }

    /// Publishes ISB watermark for streams that need it (step interval passed).
    /// Watermarks serve as heartbeats for downstream vertices via KV entry timestamps.
    /// This is called for ISB streams that haven't received data recently.
    async fn publish_isb_idle_watermark(&mut self) -> Result<()> {
        // Fetch the current source watermark (may be -1 if no data yet, but we still
        // publish to update KV entry timestamp which signals liveness to downstream vertices)
        let compute_wm = self.fetcher.fetch_source_watermark(None);

        // Fetch streams where step interval has passed since last publish
        let streams_needing_publish = self.isb_idle_manager.fetch_streams_needing_publish().await;

        for stream in streams_needing_publish.iter() {
            let offset = self
                .isb_idle_manager
                .fetch_idle_offset(stream)
                .await
                .unwrap_or(-1);

            for partition in self.active_input_partitions.keys() {
                // Publish watermark - KV entry timestamp is updated automatically
                self.publisher
                    .publish_isb_watermark(
                        *partition,
                        stream,
                        offset,
                        compute_wm.timestamp_millis(),
                        true,
                    )
                    .await;
            }

            self.isb_idle_manager
                .update_idle_metadata(stream, offset)
                .await;
        }

        Ok(())
    }

    /// Publishes idle watermarks for both source and ISB.
    /// This is called by the background task to handle both source and ISB idle watermark publishing.
    /// Watermarks serve as heartbeats for downstream vertices via KV entry timestamps.
    async fn publish_idle_watermarks(&mut self) -> Result<()> {
        // First, publish source idle watermark (if source idle manager is configured)
        self.publish_source_idle_watermark().await?;

        // Then, publish ISB idle watermark for streams that need it
        self.publish_isb_idle_watermark().await?;

        Ok(())
    }
}

/// SourceWatermarkHandle exposes methods to publish and fetch the source watermark.
/// Contains shared state protected by Arc<Mutex<>> for thread-safe access.
#[derive(Clone)]
pub(crate) struct SourceWatermarkHandle {
    state: Arc<Mutex<SourceWatermarkState>>,
}

impl SourceWatermarkHandle {
    /// Creates a new SourceWatermarkHandle.
    /// Uses `idle_config.step_interval` (default 100ms) as the WMB delay for all
    /// watermark publishing - both source and ISB idle detection.
    pub(crate) async fn new(
        js_context: async_nats::jetstream::Context,
        to_vertex_configs: &[ToVertexConfig],
        config: &SourceWatermarkConfig,
        cln_token: CancellationToken,
    ) -> Result<Self> {
        // Use step_interval from idle_config as the unified WMB delay
        let wmb_delay = config.idle_config.step_interval;

        // Create KV store for ProcessorManager
        let ot_bucket = js_context
            .get_key_value(config.source_bucket_config.ot_bucket)
            .await
            .expect("Failed to get OT bucket");
        let ot_store: Arc<dyn KVStore> = Arc::new(JetstreamKVStore::new(
            ot_bucket,
            config.source_bucket_config.ot_bucket,
        ));

        let processor_manager = ProcessorManager::new(
            ot_store,
            &config.source_bucket_config,
            VertexType::Source,
            *crate::config::get_vertex_replica(),
        )
        .await?;

        let fetcher = SourceWatermarkFetcher::new(processor_manager);
        let publisher = SourceWatermarkPublisher::new(
            js_context.clone(),
            config.max_delay,
            config.source_bucket_config.clone(),
            config.to_vertex_bucket_config.clone(),
        )
        .await?;

        // Source idle manager is always created with the idle config (which has default 100ms WMB delay)
        let source_idle_manager = SourceIdleDetector::new(config.idle_config.clone());

        // Use the same WMB delay for ISB idle detection
        let isb_idle_manager =
            ISBIdleDetector::new(wmb_delay, to_vertex_configs, js_context.clone()).await;

        let state =
            SourceWatermarkState::new(publisher, fetcher, isb_idle_manager, source_idle_manager);

        let source_watermark_handle = Self {
            state: Arc::new(Mutex::new(state)),
        };

        // start a task to keep publishing idle watermarks every wmb_delay
        tokio::spawn({
            let source_watermark_handle = source_watermark_handle.clone();
            let mut interval_ticker = tokio::time::interval(wmb_delay);
            async move {
                loop {
                    tokio::select! {
                        _ = interval_ticker.tick() => {
                            // Publish both source and ISB idle watermarks
                            source_watermark_handle.publish_idle_watermarks().await;
                        }
                        _ = cln_token.cancelled() => {
                            break;
                        }
                    }
                }
            }
        });

        Ok(source_watermark_handle)
    }

    /// Generates and Publishes the source watermark for the given messages.
    pub(crate) async fn generate_and_publish_source_watermark(&self, messages: &[Message]) {
        // Acquire lock, perform operation, and release immediately
        let result = {
            let mut state = self.state.lock().await;
            state
                .generate_and_publish_source_watermark(messages.to_vec())
                .await
        };

        if let Err(e) = result {
            warn!(?e, "Failed to generate and publish source watermark");
        }
    }

    /// Publishes the watermark for the given input partition on to the ISB of the next vertex.
    pub(crate) async fn publish_source_isb_watermark(
        &self,
        stream: Stream,
        offset: Offset,
        input_partition: u16,
    ) {
        let Offset::Int(offset) = offset else {
            warn!(?offset, "Invalid offset type, cannot publish watermark");
            return;
        };

        // Acquire lock, perform operation, and release immediately
        let result = {
            let mut state = self.state.lock().await;
            state
                .publish_source_isb_watermark(stream, offset, input_partition)
                .await
        };

        if let Err(e) = result {
            warn!(?e, "Failed to publish source ISB watermark");
        }
    }

    /// Fetches the source watermark.
    pub(crate) async fn fetch_source_watermark(&self) -> Watermark {
        // Acquire lock, fetch watermark, and release immediately
        let mut state = self.state.lock().await;
        state.fetcher.fetch_source_watermark(None)
    }

    /// Fetches the head watermark using the source watermark fetcher. This returns the minimum
    /// of the head watermarks across all active processors for the specified partition.
    pub(crate) async fn fetch_head_watermark(&self, partition_idx: u16) -> Watermark {
        // Acquire lock, fetch watermark, and release immediately
        let mut state = self.state.lock().await;
        state.fetcher.fetch_head_watermark(partition_idx)
    }

    /// Publishes idle watermarks for both source and ISB.
    /// This is called by the background task to handle both source and ISB idle watermark publishing.
    /// Watermarks serve as heartbeats for downstream vertices via KV entry timestamps.
    async fn publish_idle_watermarks(&self) {
        // Acquire lock, perform operation, and release immediately
        let result = {
            let mut state = self.state.lock().await;
            state.publish_idle_watermarks().await
        };

        if let Err(e) = result {
            warn!(?e, "Failed to publish idle watermarks");
        }
    }

    /// Initializes the active partitions by creating a publisher for each partition.
    /// Also sets the processor count (total number of partitions) for watermark stability.
    ///
    /// # Arguments
    /// * `partitions` - The list of active partitions being processed by this source instance.
    /// * `total_partitions` - The total number of partitions in the source (if known).
    ///   If provided, this is used as the processor count for watermark stability.
    ///   If None, the processor count is derived from the number of active partitions.
    pub(crate) async fn initialize_active_partitions(
        &self,
        partitions: Vec<u16>,
        total_partitions: Option<u32>,
    ) {
        let mut state = self.state.lock().await;
        if let Some(p) = total_partitions {
            state.publisher.set_processor_count(p);
        }

        // Initialize the source idle manager with the partitions for per-partition tracking
        // This also removes partitions that are no longer active
        state.source_idle_manager.initialize_partitions(&partitions);

        // Replace active_input_partitions with the new set of partitions
        // This handles dynamic partition changes (e.g., Kafka rebalancing)
        let new_partitions: std::collections::HashSet<u16> = partitions.iter().copied().collect();
        state
            .active_input_partitions
            .retain(|partition, _| new_partitions.contains(partition));
        for partition in &partitions {
            state.active_input_partitions.insert(*partition, true);
        }

        state.publisher.initialize_active_partitions(partitions);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;
    use async_nats::jetstream::stream;
    use bytes::BytesMut;
    use chrono::DateTime;
    use tokio::time::sleep;

    use super::*;
    use crate::config::pipeline::VertexType;
    use crate::config::pipeline::isb::BufferWriterConfig;
    use crate::config::pipeline::watermark::{BucketConfig, IdleConfig};
    use crate::message::{IntOffset, Message};
    use crate::watermark::wmb::WMB;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_publish_source_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "test_publish_source_watermark_OT";

        let source_config = SourceWatermarkConfig {
            max_delay: Default::default(),
            source_bucket_config: BucketConfig {
                vertex: "source_vertex",
                partitions: vec![0], // partitions is always vec![0] for source
                ot_bucket: ot_bucket_name,
                delay: None,
            },
            to_vertex_bucket_config: vec![],
            idle_config: IdleConfig::default(),
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

        let handle = SourceWatermarkHandle::new(
            js_context.clone(),
            Default::default(),
            &source_config,
            CancellationToken::new(),
        )
        .await
        .expect("Failed to create source watermark handle");

        let messages = vec![
            Message {
                offset: Offset::Int(IntOffset {
                    offset: 1,
                    partition_idx: 0,
                }),
                event_time: DateTime::from_timestamp_millis(60000).unwrap(),
                ..Default::default()
            },
            Message {
                offset: Offset::Int(IntOffset {
                    offset: 2,
                    partition_idx: 0,
                }),
                event_time: DateTime::from_timestamp_millis(70000).unwrap(),
                ..Default::default()
            },
        ];

        handle
            .generate_and_publish_source_watermark(&messages)
            .await;

        // try getting the value for the processor from the ot bucket to make sure
        // the watermark is getting published(min event time in the batch), wait until
        // one second if it's not there, fail the test
        let ot_bucket = js_context
            .get_key_value(ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        let timeout_duration = Duration::from_secs(1);
        let result = tokio::time::timeout(timeout_duration, async {
            loop {
                let wmb = ot_bucket
                    .get("source-source_vertex-0")
                    .await
                    .expect("Failed to get wmb");
                if let Some(wmb) = wmb {
                    let wmb: WMB = wmb.try_into().unwrap();
                    assert_eq!(wmb.watermark, 60000);
                    break;
                } else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        })
        .await;

        if result.is_err() {
            panic!("Failed to get watermark");
        }

        // delete the stores
        js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await
            .unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_publish_source_edge_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let source_ot_bucket_name = "test_publish_source_edge_watermark_source_OT";
        let edge_ot_bucket_name = "test_publish_source_edge_watermark_edge_OT";

        // delete the stores
        let _ = js_context
            .delete_key_value(source_ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(edge_ot_bucket_name.to_string())
            .await;

        let source_config = SourceWatermarkConfig {
            max_delay: Default::default(),
            source_bucket_config: BucketConfig {
                vertex: "source_vertex",
                partitions: vec![0, 1],
                ot_bucket: source_ot_bucket_name,
                delay: None,
            },
            to_vertex_bucket_config: vec![BucketConfig {
                vertex: "edge_vertex",
                partitions: vec![0, 1],
                ot_bucket: edge_ot_bucket_name,
                delay: None,
            }],
            idle_config: IdleConfig::default(),
        };

        // create key value stores for source
        js_context
            .create_key_value(Config {
                bucket: source_ot_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        // create key value stores for edge
        js_context
            .create_key_value(Config {
                bucket: edge_ot_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let handle = SourceWatermarkHandle::new(
            js_context.clone(),
            &[ToVertexConfig {
                name: "edge_vertex",
                writer_config: BufferWriterConfig {
                    streams: vec![Stream {
                        name: "edge_stream",
                        vertex: "edge_vertex",
                        partition: 0,
                    }],
                    ..Default::default()
                },
                conditions: None,
                partitions: 1,
                to_vertex_type: VertexType::MapUDF,
                ordered_processing_enabled: false,
            }],
            &source_config,
            CancellationToken::new(),
        )
        .await
        .expect("Failed to create source watermark handle");

        let ot_bucket = js_context
            .get_key_value(edge_ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        let stream = Stream {
            name: "edge_stream",
            vertex: "edge_vertex",
            partition: 0,
        };

        for i in 1..11 {
            // publish source watermarks before publishing edge watermarks
            let messages = vec![
                Message {
                    offset: Offset::Int(IntOffset {
                        offset: 1,
                        partition_idx: 0,
                    }),
                    event_time: DateTime::from_timestamp_millis(10000 * i).unwrap(),
                    ..Default::default()
                },
                Message {
                    offset: Offset::Int(IntOffset {
                        offset: 2,
                        partition_idx: 0,
                    }),
                    event_time: DateTime::from_timestamp_millis(20000 * i).unwrap(),
                    ..Default::default()
                },
            ];

            handle
                .generate_and_publish_source_watermark(&messages)
                .await;

            let offset = Offset::Int(IntOffset {
                offset: i,
                partition_idx: 0,
            });
            handle
                .publish_source_isb_watermark(stream.clone(), offset, 0)
                .await;
        }

        // check if the watermark is published
        let timeout_duration = Duration::from_secs(1);
        let result = tokio::time::timeout(timeout_duration, async {
            loop {
                let wmb = ot_bucket
                    .get("source_vertex-0")
                    .await
                    .expect("Failed to get wmb");
                if let Some(wmb) = wmb {
                    let wmb: WMB = wmb.try_into().unwrap();
                    assert_ne!(wmb.watermark, -1);
                    break;
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        })
        .await;

        if result.is_err() {
            panic!("Failed to get watermark");
        }
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_invoke_publish_source_idle_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "test_invoke_publish_source_idle_watermark_OT";
        let to_vertex_ot_bucket_name = "test_invoke_publish_source_idle_watermark_TO_VERTEX_OT";

        let to_vertex_configs = vec![ToVertexConfig {
            name: "edge_vertex",
            writer_config: BufferWriterConfig {
                streams: vec![Stream {
                    name: "edge_stream",
                    vertex: "edge_vertex",
                    partition: 0,
                }],
                ..Default::default()
            },
            conditions: None,
            partitions: 1,
            to_vertex_type: VertexType::MapUDF,
            ordered_processing_enabled: false,
        }];

        // create to vertex stream since we will be writing ctrl message to it
        js_context
            .get_or_create_stream(stream::Config {
                name: "edge_stream".to_string(),
                subjects: vec!["edge_stream".to_string()],
                max_message_size: 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        let source_bucket_config = BucketConfig {
            vertex: "v1",
            partitions: vec![0],
            ot_bucket: ot_bucket_name,
            delay: None,
        };

        let to_vertex_bucket_config = BucketConfig {
            vertex: "edge_vertex",
            partitions: vec![0],
            ot_bucket: to_vertex_ot_bucket_name,
            delay: None,
        };

        // delete stores if the exist
        let _ = js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(to_vertex_ot_bucket_name.to_string())
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
                bucket: to_vertex_ot_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let source_idle_config = IdleConfig {
            threshold: Duration::from_millis(10),
            step_interval: Duration::from_millis(5),
            increment_by: Duration::from_millis(1),
            init_source_delay: None,
        };

        let handle = SourceWatermarkHandle::new(
            js_context.clone(),
            &to_vertex_configs,
            &SourceWatermarkConfig {
                max_delay: Default::default(),
                source_bucket_config,
                to_vertex_bucket_config: vec![to_vertex_bucket_config],
                idle_config: source_idle_config,
            },
            CancellationToken::new(),
        )
        .await
        .expect("Failed to create SourceWatermarkHandle");

        // get ot bucket for source and publish some wmb entries
        let ot_bucket = js_context
            .get_key_value(ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        for i in 1..11 {
            let wmb: BytesMut = WMB {
                watermark: 1000 * i,
                offset: i,
                idle: false,
                partition: 0,
                processor_count: None,
            }
            .try_into()
            .unwrap();
            ot_bucket
                .put("source-v1-0", wmb.freeze())
                .await
                .expect("Failed to put wmb");
            sleep(Duration::from_millis(3)).await;
        }

        // Initialize the partitions so the background task can use them
        handle.initialize_active_partitions(vec![0], None).await;

        // sleep so that the idle condition is met
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Invoke publish_idle_watermarks (which calls both source and ISB idle watermark publishing)
        handle.publish_idle_watermarks().await;

        // Check if the idle watermark is published
        let ot_bucket = js_context
            .get_key_value(to_vertex_ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        let timeout_duration = Duration::from_secs(1);
        let result = tokio::time::timeout(timeout_duration, async {
            loop {
                if let Some(wmb) = ot_bucket.get("v1-0").await.expect("Failed to get wmb") {
                    let wmb: WMB = wmb.try_into().expect("Failed to convert to WMB");
                    // idle watermark should be published
                    if wmb.idle {
                        return true; // Found idle watermark
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        let wmb_found = result.unwrap_or(false); // false if timeout occurred
        assert!(wmb_found, "Idle watermark not found");
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_publish_source_isb_idle_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "test_publish_source_isb_idle_watermark_OT";
        let to_vertex_ot_bucket_name = "test_publish_source_isb_idle_watermark_TO_VERTEX_OT";

        let to_vertex_configs = vec![ToVertexConfig {
            name: "edge_vertex",
            writer_config: BufferWriterConfig {
                streams: vec![Stream {
                    name: "edge_stream",
                    vertex: "edge_vertex",
                    partition: 0,
                }],
                ..Default::default()
            },
            conditions: None,
            partitions: 1,
            to_vertex_type: VertexType::MapUDF,
            ordered_processing_enabled: false,
        }];

        // create to vertex stream since we will be writing ctrl message to it
        js_context
            .get_or_create_stream(stream::Config {
                name: "edge_stream".to_string(),
                subjects: vec!["edge_stream".to_string()],
                max_message_size: 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        let source_bucket_config = BucketConfig {
            vertex: "v1",
            partitions: vec![0],
            ot_bucket: ot_bucket_name,
            delay: None,
        };

        let to_vertex_bucket_config = BucketConfig {
            vertex: "edge_vertex",
            partitions: vec![0],
            ot_bucket: to_vertex_ot_bucket_name,
            delay: None,
        };

        let _ = js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await;
        let _ = js_context
            .delete_key_value(to_vertex_ot_bucket_name.to_string())
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
                bucket: to_vertex_ot_bucket_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let source_idle_config = IdleConfig {
            threshold: Duration::from_millis(2000), // set higher value so that the source won't be idling
            step_interval: Duration::from_millis(3),
            increment_by: Duration::from_millis(1),
            init_source_delay: None,
        };

        let handle = SourceWatermarkHandle::new(
            js_context.clone(),
            &to_vertex_configs,
            &SourceWatermarkConfig {
                max_delay: Default::default(),
                source_bucket_config,
                to_vertex_bucket_config: vec![to_vertex_bucket_config],
                idle_config: source_idle_config,
            },
            CancellationToken::new(),
        )
        .await
        .expect("Failed to create SourceWatermarkHandle");

        let messages = vec![Message {
            offset: Offset::Int(IntOffset {
                offset: 1,
                partition_idx: 0,
            }),
            event_time: DateTime::from_timestamp_millis(100).unwrap(),
            ..Default::default()
        }];

        // generate some watermarks to make partition active
        handle
            .generate_and_publish_source_watermark(&messages)
            .await;

        // get ot bucket for source and publish some wmb entries
        let ot_bucket = js_context
            .get_key_value(ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        for i in 1..10 {
            let wmb: BytesMut = WMB {
                watermark: 1000 * i,
                offset: i,
                idle: false,
                partition: 0,
                processor_count: None,
            }
            .try_into()
            .unwrap();
            ot_bucket
                .put("source-v1-0", wmb.freeze())
                .await
                .expect("Failed to put wmb");
            sleep(Duration::from_millis(3)).await;
        }

        // Check if the idle watermark is published
        let ot_bucket = js_context
            .get_key_value(to_vertex_ot_bucket_name)
            .await
            .expect("Failed to get ot bucket");

        let timeout_duration = Duration::from_secs(2);
        let result = tokio::time::timeout(timeout_duration, async {
            loop {
                if let Some(wmb) = ot_bucket.get("v1-0").await.expect("Failed to get wmb") {
                    let wmb: WMB = wmb.try_into().expect("Failed to convert to WMB");
                    // idle watermark should be published
                    if wmb.idle {
                        return true; // Found idle watermark
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;

        let wmb_found = result.unwrap_or(false);
        assert!(wmb_found, "Idle watermark not found");
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_fetch_head_watermark() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let ot_bucket_name = "test_fetch_head_watermark_source_OT";

        let source_config = SourceWatermarkConfig {
            max_delay: Default::default(),
            source_bucket_config: BucketConfig {
                vertex: "source_vertex",
                partitions: vec![0],
                ot_bucket: ot_bucket_name,
                delay: None,
            },
            to_vertex_bucket_config: vec![],
            idle_config: IdleConfig::default(),
        };

        // delete the stores first
        let _ = js_context
            .delete_key_value(ot_bucket_name.to_string())
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

        // Publish some WMB entries to the source OT bucket to simulate source processors
        let ot_bucket = js_context.get_key_value(ot_bucket_name).await.unwrap();

        // Create WMB entries that will be read by the ProcessorManager
        let wmb1 = WMB {
            watermark: 60000,
            offset: 1,
            idle: false,
            partition: 0,
            processor_count: None,
        };
        let wmb2 = WMB {
            watermark: 70000,
            offset: 2,
            idle: false,
            partition: 0,
            processor_count: None,
        };

        // Publish WMB entries to the OT bucket with a processor name
        let processor_name = "source-processor-0";
        let wmb1_bytes: bytes::BytesMut = wmb1.try_into().unwrap();
        let wmb2_bytes: bytes::BytesMut = wmb2.try_into().unwrap();
        ot_bucket
            .put(processor_name, wmb1_bytes.freeze())
            .await
            .unwrap();
        ot_bucket
            .put(processor_name, wmb2_bytes.freeze())
            .await
            .unwrap();

        let handle = SourceWatermarkHandle::new(
            js_context.clone(),
            Default::default(),
            &source_config,
            CancellationToken::new(),
        )
        .await
        .expect("Failed to create source watermark handle");

        // Poll for head watermark with timeout using tokio::time::timeout
        let timeout_duration = Duration::from_millis(200);
        let poll_interval = Duration::from_millis(10);

        let head_watermark = tokio::time::timeout(timeout_duration, async {
            loop {
                let watermark = handle.fetch_head_watermark(0).await;

                // Break if we got a valid watermark (not -1)
                if watermark.timestamp_millis() != -1 {
                    return watermark;
                }

                // Wait before next poll
                tokio::time::sleep(poll_interval).await;
            }
        })
        .await
        .expect("Timeout: head watermark still -1 after 200ms");

        // The head watermark should be a valid timestamp (not -1)
        assert_ne!(head_watermark.timestamp_millis(), -1);

        // delete the stores
        js_context
            .delete_key_value(ot_bucket_name.to_string())
            .await
            .unwrap();
    }
}
