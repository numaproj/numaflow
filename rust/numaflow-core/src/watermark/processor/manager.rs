//! Manages the processors and their lifecycle. It will keep track of all the active processor by listening
//! to the heartbeat bucket and update their offset timelines by listening to the ot bucket. It will also
//! refresh the active processors if they are not active for a certain time.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use async_nats::jetstream::kv::Watch;
use backoff::retry::Retry;
use backoff::strategy::fixed;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use prost::Message as ProtoMessage;
use std::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::config::pipeline::watermark::BucketConfig;
use crate::error::{Error, Result};
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
#[derive(Clone)]
pub(crate) struct Processor {
    /// Name of the processor.
    pub(crate) name: Bytes,
    /// [Status] of the processor.
    pub(crate) status: Status,
    /// OffsetTimeline for each partition.
    pub(crate) timelines: Vec<OffsetTimeline>,
}

impl Debug for Processor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Processor: {:?}, Status: {:?}, Timelines: ",
            self.name, self.status
        )?;
        for timeline in &self.timelines {
            writeln!(f, "{:?}", timeline)?;
        }
        Ok(())
    }
}

impl Processor {
    pub(crate) fn new(name: Bytes, status: Status, partition_count: usize) -> Self {
        let mut timelines = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            timelines.push(OffsetTimeline::new(10));
        }
        Processor {
            name,
            status,
            timelines,
        }
    }

    /// Set the status of the processor.
    pub(crate) fn set_status(&mut self, status: Status) {
        self.status = status;
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
    /// Handles of ot listener, hb listener and processor refresher tasks
    pub(crate) handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Debug for ProcessorManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProcessorManager: {:?}", self.processors)
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
    /// Creates a new ProcessorManager.
    pub(crate) async fn new(
        js_context: async_nats::jetstream::Context,
        bucket_config: &BucketConfig,
    ) -> Result<Self> {
        let ot_bucket = js_context
            .get_key_value(bucket_config.ot_bucket)
            .await
            .map_err(|e| {
                Error::Watermark(format!(
                    "Failed to get kv bucket {}: {}",
                    bucket_config.ot_bucket, e
                ))
            })?;

        let hb_bucket = js_context
            .get_key_value(bucket_config.hb_bucket)
            .await
            .map_err(|e| {
                Error::Watermark(format!(
                    "Failed to get kv bucket {}: {}",
                    bucket_config.hb_bucket, e
                ))
            })?;

        let (processors_map, heartbeats_map) =
            Self::prepopulate_processors(&hb_bucket, &ot_bucket, bucket_config).await;

        let processors = Arc::new(RwLock::new(processors_map));
        let heartbeats = Arc::new(RwLock::new(heartbeats_map));

        // start the ot watcher, to listen to the OT bucket and update the timelines
        let ot_handle = tokio::spawn(Self::start_ot_watcher(ot_bucket, Arc::clone(&processors)));

        // start the hb watcher, to listen to the HB bucket and update the list of
        // active processors
        let hb_handle = tokio::spawn(Self::start_hb_watcher(
            bucket_config.partitions,
            hb_bucket,
            Arc::clone(&heartbeats),
            Arc::clone(&processors),
        ));

        // start the processor refresher, to update the status of the processors
        // based on the last heartbeat
        let refresh_handle = tokio::spawn(Self::start_refreshing_processors(
            DEFAULT_PROCESSOR_REFRESH_RATE,
            Arc::clone(&processors),
            Arc::clone(&heartbeats),
        ));

        Ok(ProcessorManager {
            processors,
            handles: vec![ot_handle, hb_handle, refresh_handle],
        })
    }

    /// Prepopulate processors and timelines from the hb and ot buckets.
    async fn prepopulate_processors(
        hb_bucket: &async_nats::jetstream::kv::Store,
        ot_bucket: &async_nats::jetstream::kv::Store,
        bucket_config: &BucketConfig,
    ) -> (HashMap<Bytes, Processor>, HashMap<Bytes, i64>) {
        let mut processors = HashMap::new();
        let mut heartbeats = HashMap::new();

        // Get all existing keys from the hb bucket and create processors
        let hb_keys = match hb_bucket.keys().await {
            Ok(keys) => keys
                .try_collect::<Vec<String>>()
                .await
                .unwrap_or_else(|_| Vec::new()),
            Err(e) => {
                error!(?e, "Failed to get keys from hb bucket");
                Vec::new()
            }
        };

        for key in hb_keys {
            let processor_name = Bytes::from(key.clone());
            let processor = Processor::new(
                processor_name.clone(),
                Status::Active,
                bucket_config.partitions as usize,
            );
            processors.insert(processor_name.clone(), processor);

            let Ok(Some(value)) = hb_bucket.get(&key).await else {
                continue;
            };

            let hb = numaflow_pb::objects::watermark::Heartbeat::decode(value)
                .expect("Failed to decode heartbeat")
                .heartbeat;
            heartbeats.insert(processor_name, hb);
        }

        // Get all existing entries from the ot bucket and store them in the timeline
        let ot_keys = match ot_bucket.keys().await {
            Ok(keys) => keys
                .try_collect::<Vec<String>>()
                .await
                .unwrap_or_else(|_| Vec::new()),
            Err(e) => {
                warn!(?e, "Failed to get keys from ot bucket");
                Vec::new()
            }
        };

        for ot_key in ot_keys {
            let processor_name = Bytes::from(ot_key.clone());
            let processor = match processors.get_mut(&processor_name) {
                Some(processor) => processor,
                None => {
                    warn!(?processor_name, "Processor not found, skipping");
                    continue;
                }
            };

            let Ok(Some(ot_value)) = ot_bucket.get(&ot_key).await else {
                continue;
            };
            let wmb: WMB = ot_value.try_into().expect("Failed to decode WMB");

            let timeline = &mut processor.timelines[wmb.partition as usize];
            timeline.put(wmb);
        }

        (processors, heartbeats)
    }

    /// Starts refreshing the processors status based on the last heartbeat, if the last heartbeat
    /// is more than 10 times the refreshing rate, the processor is marked as deleted
    async fn start_refreshing_processors(
        refreshing_processors_rate: u16,
        processors: Arc<RwLock<HashMap<Bytes, Processor>>>,
        heartbeats: Arc<RwLock<HashMap<Bytes, i64>>>,
    ) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(refreshing_processors_rate as u64));
        loop {
            interval.tick().await;
            let current_time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            let heartbeats = heartbeats.read().expect("failed to acquire lock");
            let mut processors = processors.write().expect("failed to acquire lock");

            heartbeats.iter().for_each(|(p_name, &p_time)| {
                if let Some(p) = processors.get_mut(p_name) {
                    let status = match current_time - p_time {
                        diff if diff > 10 * refreshing_processors_rate as i64 => Status::Deleted,
                        diff if diff > refreshing_processors_rate as i64 => Status::InActive,
                        _ => Status::Active,
                    };
                    p.set_status(status);
                }
            });
        }
    }

    /// Starts the ot watcher, to listen to the OT bucket and update the timelines for the
    /// processors.
    async fn start_ot_watcher(
        ot_bucket: async_nats::jetstream::kv::Store,
        processors: Arc<RwLock<HashMap<Bytes, Processor>>>,
    ) {
        let mut ot_watcher = Self::create_watcher(ot_bucket.clone()).await;

        loop {
            let Some(val) = ot_watcher.next().await else {
                warn!("OT watcher stopped, recreating watcher");
                ot_watcher = Self::create_watcher(ot_bucket.clone()).await;
                continue;
            };

            let kv = match val {
                Ok(kv) => kv,
                Err(e) => {
                    warn!(?e, "Failed to get next kv entry, recreating watcher");
                    ot_watcher = Self::create_watcher(ot_bucket.clone()).await;
                    continue;
                }
            };

            match kv.operation {
                async_nats::jetstream::kv::Operation::Put => {
                    let processor_name = Bytes::from(kv.key);
                    let wmb: WMB = kv.value.try_into().expect("Failed to decode WMB");

                    debug!(wmb = ?wmb, processor = ?processor_name, "Received wmb from watcher");
                    if let Some(processor) = processors
                        .write()
                        .expect("failed to acquire lock")
                        .get_mut(&processor_name)
                    {
                        let timeline = &mut processor.timelines[wmb.partition as usize];
                        timeline.put(wmb);
                    } else {
                        debug!(?processor_name, "Processor not found");
                    }
                }
                async_nats::jetstream::kv::Operation::Delete
                | async_nats::jetstream::kv::Operation::Purge => {
                    // we don't care about delete or purge operations
                }
            }
        }
    }

    /// Starts the hb watcher, to listen to the HB bucket, will also create the processor if it
    /// doesn't exist.
    async fn start_hb_watcher(
        partition_count: u16,
        hb_bucket: async_nats::jetstream::kv::Store,
        heartbeats: Arc<RwLock<HashMap<Bytes, i64>>>,
        processors: Arc<RwLock<HashMap<Bytes, Processor>>>,
    ) {
        let mut hb_watcher = Self::create_watcher(hb_bucket.clone()).await;

        loop {
            let Some(val) = hb_watcher.next().await else {
                warn!("HB watcher stopped, recreating watcher");
                hb_watcher = Self::create_watcher(hb_bucket.clone()).await;
                continue;
            };

            let kv = match val {
                Ok(kv) => kv,
                Err(e) => {
                    warn!(?e, "Failed to get next kv entry, recreating watcher");
                    hb_watcher = Self::create_watcher(hb_bucket.clone()).await;
                    continue;
                }
            };

            match kv.operation {
                async_nats::jetstream::kv::Operation::Put => {
                    let processor_name = Bytes::from(kv.key);
                    let hb = numaflow_pb::objects::watermark::Heartbeat::decode(kv.value)
                        .expect("Failed to decode heartbeat")
                        .heartbeat;
                    heartbeats
                        .write()
                        .expect("failed to acquire lock")
                        .insert(processor_name.clone(), hb);

                    debug!(hb = ?hb, processor = ?processor_name, "Received heartbeat from watcher");
                    // if the processor is not in the processors map, add it
                    // or if processor status is not active, set it to active
                    let mut processors = processors.write().expect("failed to acquire lock");
                    if let Some(processor) = processors.get_mut(&processor_name) {
                        if !processor.is_active() {
                            processor.set_status(Status::Active);
                        }
                    } else {
                        info!(processor = ?processor_name, "Processor not found, adding it");
                        let processor = Processor::new(
                            processor_name,
                            Status::Active,
                            partition_count as usize,
                        );
                        processors.insert(processor.name.clone(), processor);
                    }
                }
                async_nats::jetstream::kv::Operation::Delete => {
                    let processor_name = Bytes::from(kv.key);
                    heartbeats
                        .write()
                        .expect("failed to acquire lock")
                        .remove(&processor_name);

                    // update the processor status to deleted
                    if let Some(processor) = processors
                        .write()
                        .expect("failed to acquire lock")
                        .get_mut(&processor_name)
                    {
                        processor.set_status(Status::Deleted);
                    }
                }
                async_nats::jetstream::kv::Operation::Purge => {
                    heartbeats.write().expect("failed to acquire lock").clear();

                    // update the processor status to deleted
                    for (_, processor) in processors
                        .write()
                        .expect("failed to acquire lock")
                        .iter_mut()
                    {
                        processor.set_status(Status::Deleted);
                    }
                }
            }
        }
    }

    /// creates a watcher for the given bucket, will retry infinitely until it succeeds
    async fn create_watcher(bucket: async_nats::jetstream::kv::Store) -> Watch {
        const RECONNECT_INTERVAL: u64 = 1000;

        // infinite retry
        let interval = fixed::Interval::from_millis(RECONNECT_INTERVAL).take(usize::MAX);

        Retry::retry(
            interval,
            || async {
                match bucket.watch_all().await {
                    Ok(w) => Ok(w),
                    Err(e) => {
                        error!(?e, "Failed to create watcher");
                        Err(Error::Watermark(format!("Failed to create watcher: {}", e)))
                    }
                }
            },
            |_: &Error| true,
        )
        .await
        .expect("Failed to create ot watcher")
    }

    /// Delete a processor from the processors map
    pub(crate) fn delete_processor(&self, processor_name: &Bytes) {
        let mut processors = self.processors.write().expect("failed to acquire lock");
        processors.remove(processor_name);
    }
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;
    use async_nats::jetstream::kv::Store;
    use async_nats::jetstream::Context;
    use bytes::{Bytes, BytesMut};
    use prost::Message;

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

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_processor_manager_tracks_heartbeats_and_wmbs() {
        let js_context = setup_nats().await;
        let ot_bucket = create_kv_bucket(&js_context, "ot_bucket").await;
        let hb_bucket = create_kv_bucket(&js_context, "hb_bucket").await;

        let bucket_config = BucketConfig {
            vertex: "test",
            ot_bucket: "ot_bucket",
            hb_bucket: "hb_bucket",
            partitions: 1,
        };

        let processor_manager = ProcessorManager::new(js_context.clone(), &bucket_config)
            .await
            .unwrap();

        let processor_name = Bytes::from("processor1");

        // Spawn a task to keep publishing heartbeats
        let hb_task = tokio::spawn(async move {
            loop {
                let heartbeat = numaflow_pb::objects::watermark::Heartbeat { heartbeat: 100 };
                let mut bytes = BytesMut::new();
                heartbeat
                    .encode(&mut bytes)
                    .expect("Failed to encode heartbeat");
                hb_bucket.put("processor1", bytes.freeze()).await.unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        // Spawn a task to keep publishing WMBs
        let ot_task = tokio::spawn(async move {
            loop {
                let wmb_bytes: BytesMut = WMB {
                    watermark: 200,
                    offset: 1,
                    idle: false,
                    partition: 0,
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
            if let Some(processor) = processors.get(&processor_name) {
                if processor.status == Status::Active {
                    let timeline = &processor.timelines[0];
                    if let Some(head_wmb) = timeline.get_head_wmb() {
                        if head_wmb.watermark == 200 && head_wmb.offset == 1 {
                            break;
                        }
                    }
                }
            }
            if start_time.elapsed() > Duration::from_secs(1) {
                panic!(
                    "Test failed: Processor was not added or WMB was not tracked within 1 second"
                );
            }
        }

        // Abort the tasks
        hb_task.abort();
        ot_task.abort();

        // delete the kv store
        js_context.delete_key_value("ot_bucket").await.unwrap();
        js_context.delete_key_value("hb_bucket").await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_processor_manager_tracks_multiple_processors() {
        let js_context = setup_nats().await;
        let ot_bucket =
            create_kv_bucket(&js_context, "test_processor_manager_multi_ot_bucket").await;
        let hb_bucket =
            create_kv_bucket(&js_context, "test_processor_manager_multi_hb_bucket").await;

        let bucket_config = BucketConfig {
            vertex: "test",
            ot_bucket: "test_processor_manager_multi_ot_bucket",
            hb_bucket: "test_processor_manager_multi_hb_bucket",
            partitions: 1,
        };

        let processor_manager = ProcessorManager::new(js_context.clone(), &bucket_config)
            .await
            .unwrap();

        let processor_names = [
            Bytes::from("processor1"),
            Bytes::from("processor2"),
            Bytes::from("processor3"),
        ];

        // Spawn tasks to keep publishing heartbeats and WMBs for each processor
        let hb_tasks: Vec<_> = processor_names
            .iter()
            .map(|processor_name| {
                let hb_bucket = hb_bucket.clone();
                let processor_name = processor_name.clone();
                tokio::spawn(async move {
                    loop {
                        let heartbeat =
                            numaflow_pb::objects::watermark::Heartbeat { heartbeat: 100 };
                        let mut bytes = BytesMut::new();
                        heartbeat
                            .encode(&mut bytes)
                            .expect("Failed to encode heartbeat");
                        hb_bucket
                            .put(
                                String::from_utf8(processor_name.to_vec()).unwrap(),
                                bytes.freeze(),
                            )
                            .await
                            .unwrap();
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                })
            })
            .collect();

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
            let processors = processor_manager
                .processors
                .read()
                .expect("failed to acquire lock");

            let futures: Vec<_> = processor_names
                .iter()
                .map(|processor_name| {
                    let processor = processors.get(processor_name).cloned();
                    async move {
                        if let Some(processor) = processor {
                            if processor.status == Status::Active {
                                if let Some(head_wmb) = processor.timelines[0].get_head_wmb() {
                                    return head_wmb.watermark == 200 && head_wmb.offset == 1;
                                }
                            }
                        }
                        false
                    }
                })
                .collect();

            let results = futures::future::join_all(futures).await;
            let all_processors_tracked = results.into_iter().all(|tracked| tracked);

            if all_processors_tracked {
                break;
            }

            if start_time.elapsed() > Duration::from_secs(1) {
                panic!("Test failed: Processors were not added or WMBs were not tracked within 1 second");
            }
        }
        // Abort the tasks
        for hb_task in hb_tasks {
            hb_task.abort();
        }
        for ot_task in ot_tasks {
            ot_task.abort();
        }

        // delete the kv store
        js_context
            .delete_key_value("test_processor_manager_multi_ot_bucket")
            .await
            .unwrap();
        js_context
            .delete_key_value("test_processor_manager_multi_hb_bucket")
            .await
            .unwrap();
    }
}
