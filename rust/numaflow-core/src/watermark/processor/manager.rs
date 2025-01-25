use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use bytes::Bytes;
use prost::Message as ProtoMessage;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tracing::{debug, info};

use crate::watermark::processor::timeline::OffsetTimeline;
use crate::watermark::wmb::WMB;

const DEFAULT_PROCESSOR_REFRESH_RATE: u16 = 5;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Status {
    InActive,
    Active,
    Deleted,
}

/// Processor is the smallest unit of entity (from which we fetch data) that does inorder processing
/// or contains inorder data. It tracks OT for all the partitions of the from buffer.
#[derive(Clone, Debug)]
pub(crate) struct Processor {
    /// Name of the processor
    pub(crate) name: Bytes,
    /// Status of the processor (Active, InActive, Deleted)
    pub(crate) status: Status,
    /// OffsetTimeline for each partition
    pub(crate) timelines: Vec<OffsetTimeline>,
}

impl Processor {
    fn new(name: Bytes, status: Status, partition_count: usize) -> Self {
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

    /// Set the status of the processor
    pub(crate) fn set_status(&mut self, status: Status) {
        self.status = status;
    }

    /// Check if the processor is active
    pub(crate) fn is_active(&self) -> bool {
        self.status == Status::Active
    }

    /// Check if the processor is deleted
    pub(crate) fn is_deleted(&self) -> bool {
        self.status == Status::Deleted
    }
}

/// processorManager manages the point of view of Vn-1 from Vn vertex processors (or source processor).
/// The code is running on Vn vertex. It has the mapping of all the processors which in turn has all the
/// information about each processor timelines.
#[derive(Debug)]
pub(crate) struct ProcessorManager {
    /// Mapping of processor name to processor
    processors: Arc<RwLock<HashMap<Bytes, Processor>>>,
    /// Handles of ot listener, hb listener and processor refresher tasks
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for ProcessorManager {
    fn drop(&mut self) {
        for handle in self.handles.drain(..) {
            handle.abort();
        }
    }
}

impl ProcessorManager {
    /// Create a new ProcessorManager
    pub(crate) async fn new(
        partition_count: u16,
        ot_watcher: async_nats::jetstream::kv::Watch,
        hb_watcher: async_nats::jetstream::kv::Watch,
    ) -> Self {
        let processors = Arc::new(RwLock::new(HashMap::new()));
        let heartbeats = Arc::new(RwLock::new(HashMap::new()));

        // start the ot watcher, to listen to the OT bucket and update the timelines
        let ot_handle = tokio::spawn(Self::start_ot_watcher(ot_watcher, Arc::clone(&processors)));

        // start the hb watcher, to listen to the HB bucket and update the list of
        // active processors
        let hb_handle = tokio::spawn(Self::start_hb_watcher(
            partition_count,
            hb_watcher,
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

        ProcessorManager {
            processors,
            handles: vec![ot_handle, hb_handle, refresh_handle],
        }
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

            let heartbeats = heartbeats.read().await;
            let mut processors = processors.write().await;

            for (p_name, &p_time) in heartbeats.iter() {
                if let Some(p) = processors.get_mut(p_name) {
                    if current_time - p_time > 10 * refreshing_processors_rate as i64 {
                        p.set_status(Status::Deleted);
                    } else if current_time - p_time > refreshing_processors_rate as i64 {
                        p.set_status(Status::InActive);
                    } else {
                        p.set_status(Status::Active);
                    }
                }
            }
        }
    }

    /// Starts the ot watcher, to listen to the OT bucket and update the timelines
    /// for the processors
    async fn start_ot_watcher(
        mut ot_watcher: async_nats::jetstream::kv::Watch,
        processors: Arc<RwLock<HashMap<Bytes, Processor>>>,
    ) {
        while let Ok(kv) = ot_watcher
            .next()
            .await
            .expect("Failed to get next kv entry")
        {
            match kv.operation {
                async_nats::jetstream::kv::Operation::Put => {
                    let processor_name = Bytes::from(kv.key);
                    let wmb: WMB = kv.value.try_into().expect("Failed to decode WMB");

                    info!("Got WMB {:?} for processor {:?}", wmb, processor_name);
                    if let Some(processor) = processors.write().await.get_mut(&processor_name) {
                        let timeline = &mut processor.timelines[wmb.partition as usize];
                        timeline.put(wmb).await;
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
        mut hb_watcher: async_nats::jetstream::kv::Watch,
        heartbeats: Arc<RwLock<HashMap<Bytes, i64>>>,
        processors: Arc<RwLock<HashMap<Bytes, Processor>>>,
    ) {
        while let Ok(kv) = hb_watcher
            .next()
            .await
            .expect("Failed to get next kv entry")
        {
            match kv.operation {
                async_nats::jetstream::kv::Operation::Put => {
                    let processor_name = Bytes::from(kv.key);
                    let hb = numaflow_pb::objects::watermark::Heartbeat::decode(kv.value)
                        .expect("Failed to decode heartbeat")
                        .heartbeat;
                    heartbeats.write().await.insert(processor_name.clone(), hb);

                    info!("Got heartbeat {} for processor {:?}", hb, processor_name);

                    // if the processor is not in the processors map, add it
                    // or if processor status is not active, set it to active
                    let mut processors = processors.write().await;
                    if let Some(processor) = processors.get_mut(&processor_name) {
                        if !processor.is_active() {
                            processor.set_status(Status::Active);
                        }
                    } else {
                        info!("Processor {:?} not found, adding it", processor_name);
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
                    heartbeats.write().await.remove(&processor_name);

                    // update the processor status to deleted
                    if let Some(processor) = processors.write().await.get_mut(&processor_name) {
                        processor.set_status(Status::Deleted);
                    }
                }
                async_nats::jetstream::kv::Operation::Purge => {
                    heartbeats.write().await.clear();

                    // update the processor status to deleted
                    for (_, processor) in processors.write().await.iter_mut() {
                        processor.set_status(Status::Deleted);
                    }
                }
            }
        }
    }

    /// Get all the processors, returns a read guard to the processors map
    pub(crate) async fn get_all_processors(
        &self,
    ) -> tokio::sync::RwLockReadGuard<'_, HashMap<Bytes, Processor>> {
        self.processors.read().await
    }

    /// Delete a processor from the processors map
    pub(crate) async fn delete_processor(&self, processor_name: &Bytes) {
        let mut processors = self.processors.write().await;
        processors.remove(processor_name);
    }
}
