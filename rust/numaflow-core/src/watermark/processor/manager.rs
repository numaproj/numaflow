//! Manages the processors and their lifecycle. It will keep track of all the active processors by listening
//! to the OT bucket and update their offset timelines. Processor liveness is now determined by the hb_time
//! field embedded in the WMB, eliminating the need for a separate heartbeat bucket.
//!
//! The refresher task periodically checks the hb_time of each processor and marks them as inactive or deleted
//! if they haven't published a WMB within the expected interval.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::SystemTime;

use bytes::Bytes;
use futures::StreamExt;
use numaflow_shared::kv::{KVStore, KVWatchOp};
use tracing::{info, warn};

use crate::config::pipeline::VertexType;
use crate::config::pipeline::watermark::BucketConfig;
use crate::error::Result;
use crate::watermark::processor::timeline::OffsetTimeline;
use crate::watermark::wmb::WMB;

const DEFAULT_PROCESSOR_REFRESH_RATE: u16 = 5;

/// Status of a processor.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Status {
    InActive,
    Active,
    Deleted,
}

/// Processor is the smallest unit of entity (from which we fetch data) that does inorder processing
/// or contains inorder data. It tracks OT for all the partitions of the from-buffer.
pub(crate) struct Processor {
    /// Name of the processor.
    pub(crate) name: Bytes,
    /// [Status] of the processor.
    pub(crate) status: Status,
    /// OffsetTimeline for each partition.
    pub(crate) timelines: HashMap<u16, OffsetTimeline>,
    /// Last heartbeat time (epoch seconds) extracted from the most recent WMB.
    /// Used to determine processor liveness.
    pub(crate) last_hb_time: i64,
}

impl Debug for Processor {
    /// Formats the processor as: "name(status)[p0:[entries],p1:[entries],...]"
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name_str = String::from_utf8_lossy(&self.name);
        let status = match self.status {
            Status::Active => "active",
            Status::Deleted => "deleted",
            Status::InActive => "inactive",
        };

        // Get complete timeline for each partition
        let mut timeline_parts: Vec<String> = Vec::new();
        for (partition, timeline) in &self.timelines {
            let entries: Vec<String> = timeline
                .entries()
                .iter()
                .map(|wmb| format!("(wm={},off={})", wmb.watermark, wmb.offset))
                .collect();
            let entries_str = if entries.is_empty() {
                "empty".to_string()
            } else {
                entries.join("->")
            };
            timeline_parts.push(format!("p{}:[{}]", partition, entries_str));
        }
        timeline_parts.sort();

        write!(f, "{}({})[{}]", name_str, status, timeline_parts.join(","))
    }
}

impl Processor {
    pub(crate) fn new(name: Bytes, status: Status, partitions: &[u16], hb_time: i64) -> Self {
        let mut timelines = HashMap::new();
        for &partition in partitions {
            timelines.insert(partition, OffsetTimeline::new(10));
        }
        Processor {
            name,
            status,
            timelines,
            last_hb_time: hb_time,
        }
    }

    /// Set the status of the processor.
    pub(crate) fn set_status(&mut self, status: Status) {
        self.status = status;
    }

    /// Update the last heartbeat time.
    pub(crate) fn update_hb_time(&mut self, hb_time: i64) {
        self.last_hb_time = hb_time;
    }

    /// Check if the processor is active.
    pub(crate) fn is_active(&self) -> bool {
        self.status == Status::Active
    }

    /// Check if the processor is deleted.
    pub(crate) fn is_deleted(&self) -> bool {
        self.status == Status::Deleted
    }
}

/// processorManager manages the point of view of Vn-1 from the Vn vertex processor (or source processor).
/// The code is running on Vn vertex. It has the mapping of all the processors which in turn has all the
/// information about each processor timelines.
pub(crate) struct ProcessorManager {
    /// Mapping of processor name to processor
    pub(crate) processors: Arc<RwLock<HashMap<Bytes, Processor>>>,
    /// Handles of ot listener and processor refresher tasks
    pub(crate) handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Debug for ProcessorManager {
    /// Formats as: "{proc1, proc2, ...}" where each processor uses its Debug format
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let processors = self.processors.read().expect("failed to acquire lock");
        let mut proc_infos: Vec<String> = processors.values().map(|p| format!("{:?}", p)).collect();
        proc_infos.sort();
        write!(f, "{{{}}}", proc_infos.join(", "))
    }
}

impl Drop for ProcessorManager {
    fn drop(&mut self) {
        for handle in self.handles.drain(..) {
            handle.abort();
        }
    }
}

impl ProcessorManager {
    /// Creates a new ProcessorManager. It prepopulates the processor-map with previous data
    /// fetched from the OT store. Processor liveness is determined by the hb_time field
    /// embedded in the WMB, eliminating the need for a separate heartbeat store.
    ///
    /// # Arguments
    /// * `ot_store` - The offset-timeline KV store for tracking watermark entries and processor liveness
    /// * `bucket_config` - Configuration for the bucket (partitions, etc.)
    /// * `vertex_type` - Type of vertex (Source, Sink, MapUDF, ReduceUDF)
    /// * `vertex_replica` - Replica number of this vertex
    pub(crate) async fn new(
        ot_store: Arc<dyn KVStore>,
        bucket_config: &BucketConfig,
        vertex_type: VertexType,
        vertex_replica: u16,
    ) -> Result<Self> {
        // fetch old data from OT store (hb_time is now embedded in WMB)
        let processors_map =
            Self::prepopulate_processors(&ot_store, bucket_config, vertex_type, vertex_replica)
                .await;
        // point to populated data
        let processors = Arc::new(RwLock::new(processors_map));

        // start the ot watcher, to listen to the OT store and update the timelines
        // This also handles processor creation and hb_time updates
        let ot_handle = tokio::spawn(Self::start_ot_watcher(
            Arc::clone(&ot_store),
            Arc::clone(&processors),
            bucket_config.partitions.clone(),
            vertex_type,
            vertex_replica,
        ));

        // start the processor refresher, to update the status of the processors
        // based on the last hb_time from WMB
        let refresh_handle = tokio::spawn(Self::start_refreshing_processors(
            DEFAULT_PROCESSOR_REFRESH_RATE,
            Arc::clone(&processors),
        ));

        Ok(ProcessorManager {
            processors,
            handles: vec![ot_handle, refresh_handle],
        })
    }

    /// Prepopulate processors and timelines from the OT store.
    /// Processor liveness is determined by the hb_time field embedded in the WMB.
    async fn prepopulate_processors(
        ot_store: &Arc<dyn KVStore>,
        bucket_config: &BucketConfig,
        vertex_type: VertexType,
        vertex_replica: u16,
    ) -> HashMap<Bytes, Processor> {
        let mut processors: HashMap<Bytes, Processor> = HashMap::new();

        // Get all existing entries from the ot store
        let ot_keys = ot_store.keys().await.unwrap_or_else(|e| {
            warn!(error = ?e, "Failed to get keys from ot store");
            Vec::new()
        });

        for ot_key in ot_keys {
            let processor_name = Bytes::from(ot_key.clone());

            let Ok(Some(ot_value)) = ot_store.get(&ot_key).await else {
                continue;
            };
            let wmb: WMB = ot_value.try_into().expect("Failed to decode WMB");

            // Get or create the processor
            let processor = processors.entry(processor_name.clone()).or_insert_with(|| {
                Processor::new(
                    processor_name.clone(),
                    Status::Active,
                    &bucket_config.partitions,
                    wmb.hb_time,
                )
            });

            // Update hb_time if this WMB has a more recent one
            if wmb.hb_time > processor.last_hb_time {
                processor.update_hb_time(wmb.hb_time);
            }

            match vertex_type {
                VertexType::Source | VertexType::Sink | VertexType::MapUDF => {
                    if let Some(timeline) = processor.timelines.get_mut(&wmb.partition) {
                        timeline.put(wmb);
                    }
                }
                VertexType::ReduceUDF => {
                    // reduce vertex only reads from one partition so we should only consider wmbs
                    // which belong to this vertex replica
                    if wmb.partition != vertex_replica {
                        continue;
                    }
                    if let Some(timeline) = processor.timelines.get_mut(&vertex_replica) {
                        timeline.put(wmb);
                    }
                }
            }
        }

        processors
    }

    /// Starts refreshing the processors status based on the last hb_time from WMB.
    /// If the last hb_time is more than 10 times the refreshing rate, the processor is marked as deleted.
    async fn start_refreshing_processors(
        refreshing_processors_rate: u16,
        processors: Arc<RwLock<HashMap<Bytes, Processor>>>,
    ) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(refreshing_processors_rate as u64));
        loop {
            interval.tick().await;
            let current_time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            let mut processors = processors.write().expect("failed to acquire lock");

            for processor in processors.values_mut() {
                let status = match current_time - processor.last_hb_time {
                    diff if diff > 10 * refreshing_processors_rate as i64 => Status::Deleted,
                    diff if diff > refreshing_processors_rate as i64 => Status::InActive,
                    _ => Status::Active,
                };
                processor.set_status(status);
            }
        }
    }

    /// Starts the ot watcher, to listen to the OT store and update the timelines for the
    /// processors. Also handles processor creation and hb_time updates from the WMB.
    /// Uses KVStore::watch() with revision=None to only watch for new changes
    /// (since we prepopulate the data first).
    async fn start_ot_watcher(
        ot_store: Arc<dyn KVStore>,
        processors: Arc<RwLock<HashMap<Bytes, Processor>>>,
        partitions: Vec<u16>,
        vertex_type: VertexType,
        vertex_replica: u16,
    ) {
        // Use watch with revision=None to only watch for new changes
        // (we prepopulate the data first, so we don't need historical data)
        let mut ot_watcher = ot_store
            .watch(None)
            .await
            .expect("Failed to create OT watcher");

        while let Some(kv) = ot_watcher.next().await {
            match kv.operation {
                KVWatchOp::Put => {
                    let processor_name = Bytes::from(kv.key);
                    let wmb: WMB = kv.value.try_into().expect("Failed to decode WMB");

                    let mut processors = processors.write().expect("failed to acquire lock");

                    // Get or create the processor
                    let processor = if let Some(processor) = processors.get_mut(&processor_name) {
                        processor
                    } else {
                        // Processor doesn't exist, create it
                        info!(processor = ?processor_name, "Processor not found, adding it");
                        let new_processor = Processor::new(
                            processor_name.clone(),
                            Status::Active,
                            &partitions,
                            wmb.hb_time,
                        );
                        processors.insert(processor_name.clone(), new_processor);
                        processors.get_mut(&processor_name).unwrap()
                    };

                    // Update hb_time if this WMB has a more recent one
                    if wmb.hb_time > processor.last_hb_time {
                        processor.update_hb_time(wmb.hb_time);
                    }

                    // If processor was inactive, mark it as active since we received a WMB
                    if !processor.is_active() && !processor.is_deleted() {
                        processor.set_status(Status::Active);
                    }

                    match vertex_type {
                        VertexType::Source | VertexType::Sink | VertexType::MapUDF => {
                            if let Some(timeline) = processor.timelines.get_mut(&wmb.partition) {
                                timeline.put(wmb);
                            }
                        }
                        // reduce vertex only reads from one partition so we should only consider wmbs
                        // which belong to this vertex replica
                        VertexType::ReduceUDF => {
                            if wmb.partition != vertex_replica {
                                continue;
                            }
                            if let Some(timeline) = processor.timelines.get_mut(&vertex_replica) {
                                timeline.put(wmb);
                            }
                        }
                    }
                }
                KVWatchOp::Delete => {
                    // When a processor's OT entry is deleted, mark it as deleted
                    let processor_name = Bytes::from(kv.key);
                    let mut processors = processors.write().expect("failed to acquire lock");
                    if let Some(processor) = processors.get_mut(&processor_name) {
                        processor.set_status(Status::Deleted);
                    }
                }
                KVWatchOp::Purge => {
                    // When the bucket is purged, mark all processors as deleted
                    let mut processors = processors.write().expect("failed to acquire lock");
                    for processor in processors.values_mut() {
                        processor.set_status(Status::Deleted);
                    }
                }
            }
        }
    }

    /// Delete a processor from the processors map
    pub(crate) fn delete_processor(&self, processor_name: &Bytes) {
        let mut processors = self.processors.write().expect("failed to acquire lock");
        processors.remove(processor_name);
    }
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use async_nats::jetstream;
    use async_nats::jetstream::context::Context;
    use async_nats::jetstream::kv::Config;
    use async_nats::jetstream::kv::Store;
    use bytes::{Bytes, BytesMut};

    use numaflow_shared::kv::jetstream::JetstreamKVStore;

    use super::*;

    async fn setup_nats() -> Context {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        jetstream::new(client)
    }

    async fn create_kv_bucket(js: &Context, bucket_name: &str) -> Store {
        js.create_key_value(Config {
            bucket: bucket_name.to_string(),
            history: 1,
            ..Default::default()
        })
        .await
        .unwrap()
    }

    /// Create OT KV store wrapped in JetstreamKVStore for ProcessorManager
    #[cfg(feature = "nats-tests")]
    fn create_ot_store(ot_bucket: Store, ot_bucket_name: &'static str) -> Arc<dyn KVStore> {
        Arc::new(JetstreamKVStore::new(ot_bucket, ot_bucket_name))
    }

    /// Helper to get current time as epoch seconds
    #[allow(dead_code)]
    fn current_hb_time() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to get duration since epoch")
            .as_secs() as i64
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_processor_manager_tracks_wmbs() {
        let js_context = setup_nats().await;
        let ot_bucket_name = "test_processor_manager_tracks_wmbs_OT";

        let _ = js_context.delete_key_value(ot_bucket_name).await;

        let ot_bucket = create_kv_bucket(&js_context, ot_bucket_name).await;

        // Create wrapped KV store for ProcessorManager
        let ot_store = create_ot_store(ot_bucket.clone(), ot_bucket_name);

        let bucket_config = BucketConfig {
            vertex: "test",
            ot_bucket: ot_bucket_name,
            partitions: vec![0],
            delay: None,
        };

        let processor_manager =
            ProcessorManager::new(ot_store, &bucket_config, VertexType::MapUDF, 0)
                .await
                .unwrap();

        let processor_name = Bytes::from("processor1");

        // Spawn a task to keep publishing WMBs with hb_time
        let ot_task = tokio::spawn(async move {
            loop {
                let wmb_bytes: BytesMut = WMB {
                    watermark: 200,
                    offset: 1,
                    idle: false,
                    partition: 0,
                    hb_time: current_hb_time(),
                }
                .try_into()
                .unwrap();
                ot_bucket
                    .put("processor1", wmb_bytes.freeze())
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        // Check every 10ms if the processor is added and the WMB is tracked
        let start_time = tokio::time::Instant::now();
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let processors = processor_manager
                .processors
                .read()
                .expect("failed to acquire lock");
            if let Some(processor) = processors.get(&processor_name)
                && processor.status == Status::Active
            {
                let timeline = processor
                    .timelines
                    .get(&0)
                    .expect("Expected timeline 0 to exist");
                if let Some(head_wmb) = timeline.get_head_wmb()
                    && head_wmb.watermark == 200
                    && head_wmb.offset == 1
                {
                    break;
                }
            }
            if start_time.elapsed() > Duration::from_secs(1) {
                panic!(
                    "Test failed: Processor was not added or WMB was not tracked within 1 second"
                );
            }
        }

        // Abort the task
        ot_task.abort();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_processor_manager_tracks_multiple_processors() {
        let js_context = setup_nats().await;
        let ot_bucket_name = "test_processor_manager_multi_ot_bucket_v2";

        let _ = js_context.delete_key_value(ot_bucket_name).await;

        let ot_bucket = create_kv_bucket(&js_context, ot_bucket_name).await;

        // Create wrapped KV store for ProcessorManager
        let ot_store = create_ot_store(ot_bucket.clone(), ot_bucket_name);

        let bucket_config = BucketConfig {
            vertex: "test",
            ot_bucket: ot_bucket_name,
            partitions: vec![0],
            delay: None,
        };

        let processor_manager =
            ProcessorManager::new(ot_store, &bucket_config, VertexType::MapUDF, 0)
                .await
                .unwrap();

        let processor_names = [
            Bytes::from("processor1"),
            Bytes::from("processor2"),
            Bytes::from("processor3"),
        ];

        // Spawn tasks to keep publishing WMBs for each processor
        let ot_tasks: Vec<_> = processor_names
            .iter()
            .map(|processor_name| {
                let ot_bucket = ot_bucket.clone();
                let processor_name = processor_name.clone();
                tokio::spawn(async move {
                    loop {
                        let wmb_bytes: BytesMut = WMB {
                            watermark: 200,
                            offset: 1,
                            idle: false,
                            partition: 0,
                            hb_time: current_hb_time(),
                        }
                        .try_into()
                        .unwrap();
                        ot_bucket
                            .put(
                                String::from_utf8(processor_name.to_vec()).unwrap(),
                                wmb_bytes.freeze(),
                            )
                            .await
                            .unwrap();
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                })
            })
            .collect();

        // Check every 10ms if the processors are added and the WMBs are tracked
        let start_time = tokio::time::Instant::now();
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let all_processors_tracked = {
                let processors = processor_manager
                    .processors
                    .read()
                    .expect("failed to acquire lock");

                processor_names.iter().all(|processor_name| {
                    if let Some(processor) = processors.get(processor_name)
                        && processor.status == Status::Active
                        && let Some(head_wmb) = processor
                            .timelines
                            .get(&0)
                            .expect("failed to get timeline")
                            .get_head_wmb()
                    {
                        head_wmb.watermark == 200 && head_wmb.offset == 1
                    } else {
                        false
                    }
                })
            };

            if all_processors_tracked {
                break;
            }

            if start_time.elapsed() > Duration::from_secs(1) {
                panic!(
                    "Test failed: Processors were not added or WMBs were not tracked within 1 second"
                );
            }
        }
        // Abort the tasks
        for ot_task in ot_tasks {
            ot_task.abort();
        }
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_reduce_udf_partition_filtering() {
        let js_context = setup_nats().await;
        let ot_bucket_name = "test_reduce_udf_filtering_ot_bucket_v2";

        let _ = js_context.delete_key_value(ot_bucket_name).await;

        let ot_bucket = create_kv_bucket(&js_context, ot_bucket_name).await;

        // Create wrapped KV store for ProcessorManager
        let ot_store = create_ot_store(ot_bucket.clone(), ot_bucket_name);

        let bucket_config = BucketConfig {
            vertex: "test",
            ot_bucket: ot_bucket_name,
            partitions: vec![1], // For reduce UDF, WMBs are stored in timeline 1
            delay: None,
        };

        // Create processor manager for reduce UDF with replica 1
        let processor_manager = ProcessorManager::new(
            ot_store,
            &bucket_config,
            VertexType::ReduceUDF,
            1, // vertex replica 1
        )
        .await
        .unwrap();

        let processor_name = Bytes::from("test_processor");

        // Publish WMBs for different partitions - the first one will create the processor
        let wmbs = vec![
            WMB {
                watermark: 100,
                offset: 1,
                idle: false,
                partition: 0, // Should be filtered out (replica is 1)
                hb_time: current_hb_time(),
            },
            WMB {
                watermark: 200,
                offset: 2,
                idle: false,
                partition: 1, // Should be accepted (matches replica 1)
                hb_time: current_hb_time(),
            },
            WMB {
                watermark: 300,
                offset: 3,
                idle: false,
                partition: 2, // Should be filtered out (replica is 1)
                hb_time: current_hb_time(),
            },
        ];

        for wmb in wmbs {
            let wmb_bytes: BytesMut = wmb.try_into().unwrap();
            ot_bucket
                .put(
                    String::from_utf8(processor_name.to_vec()).unwrap(),
                    wmb_bytes.freeze(),
                )
                .await
                .unwrap();
            // Small delay to ensure ordering
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Wait for WMBs to be processed and verify the correct one is stored
        let start_time = tokio::time::Instant::now();
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let processors = processor_manager
                .processors
                .read()
                .expect("failed to acquire lock");

            if let Some(processor) = processors.get(&processor_name)
                && let Some(timeline) = processor.timelines.get(&1)
                && let Some(head_wmb) = timeline.get_head_wmb()
                && head_wmb.watermark == 200
                && head_wmb.partition == 1
            {
                // Also check that other timelines don't have the filtered WMBs
                let timeline_0_wmb = processor.timelines.get(&0);
                assert!(timeline_0_wmb.is_none());

                let timeline_2_wmb = processor.timelines.get(&2);
                assert!(timeline_2_wmb.is_none());
                break;
            }

            if start_time.elapsed() > Duration::from_secs(1) {
                panic!("Test failed: Expected WMB was not processed within 1 second");
            }
        }

        // Final verification
        let processors = processor_manager
            .processors
            .read()
            .expect("failed to acquire lock");

        let processor = processors
            .get(&processor_name)
            .expect("Processor should exist");

        // For reduce UDF, WMBs should be stored in timeline 1 because that is replica
        let timeline_1 = processor
            .timelines
            .get(&1)
            .expect("Expected timeline 1 to exist");
        let head_wmb = timeline_1
            .get_head_wmb()
            .expect("Should have a WMB in timeline 1");

        // Should only have the WMB with partition 1 (watermark 200)
        assert_eq!(head_wmb.watermark, 200);
        assert_eq!(head_wmb.partition, 1);
        assert_eq!(head_wmb.offset, 2);

        // For reduce UDF, we should only have timeline 1
        assert_eq!(
            processor.timelines.len(),
            1,
            "Reduce UDF should only have one timeline"
        );
        assert!(
            processor.timelines.contains_key(&1),
            "Timeline should exist for partition 1"
        );
    }
}
