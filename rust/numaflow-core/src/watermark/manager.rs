use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use prost::Message as ProtoMessage;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tracing::{debug, info};

use crate::watermark::timeline::OffsetTimeline;
use crate::watermark::WMB;

const DEFAULT_PROCESSOR_REFRESH_RATE: u16 = 5;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Status {
    InActive,
    Active,
    Deleted,
}

#[derive(Clone, Debug)]
pub(crate) struct Processor {
    pub(crate) name: String,
    pub(crate) status: Status,
    pub(crate) timelines: Vec<OffsetTimeline>,
}

impl Processor {
    fn new(name: String, status: Status, partition_count: usize) -> Self {
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

    pub(crate) fn set_status(&mut self, status: Status) {
        self.status = status;
    }

    pub(crate) fn is_active(&self) -> bool {
        self.status == Status::Active
    }

    pub(crate) fn is_deleted(&self) -> bool {
        self.status == Status::Deleted
    }
}

pub(crate) struct ProcessorManager {
    processors: Arc<Mutex<HashMap<String, Processor>>>,
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
    pub(crate) async fn new(
        partition_count: u16,
        ot_watcher: async_nats::jetstream::kv::Watch,
        hb_watcher: async_nats::jetstream::kv::Watch,
    ) -> Self {
        let processors = Arc::new(Mutex::new(HashMap::new()));
        let heartbeats = Arc::new(Mutex::new(HashMap::new()));

        let ot_handle = tokio::spawn(Self::start_ot_watcher(ot_watcher, Arc::clone(&processors)));
        let hb_handle = tokio::spawn(Self::start_hb_watcher(
            partition_count,
            hb_watcher,
            Arc::clone(&heartbeats),
            Arc::clone(&processors),
        ));
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

    async fn start_refreshing_processors(
        refreshing_processors_rate: u16,
        processors: Arc<Mutex<HashMap<String, Processor>>>,
        heartbeats: Arc<Mutex<HashMap<String, i64>>>,
    ) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(refreshing_processors_rate as u64));
        loop {
            interval.tick().await;
            let current_time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            let heartbeats = heartbeats.lock().await;
            let mut processors = processors.lock().await;

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

    async fn start_ot_watcher(
        mut ot_watcher: async_nats::jetstream::kv::Watch,
        processors: Arc<Mutex<HashMap<String, Processor>>>,
    ) {
        while let Ok(kv) = ot_watcher.next().await.unwrap() {
            match kv.operation {
                async_nats::jetstream::kv::Operation::Put => {
                    let processor_name = kv.key;
                    let wmb: WMB = kv.value.try_into().unwrap();

                    info!("Got WMB {:?} for processor {}", wmb, processor_name);
                    if let Some(processor) = processors.lock().await.get_mut(&processor_name) {
                        let timeline = &mut processor.timelines[wmb.partition as usize];
                        if wmb.idle {
                            timeline.put_idle(wmb).await;
                        } else {
                            timeline.put(wmb).await;
                        }
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

    async fn start_hb_watcher(
        partition_count: u16,
        mut hb_watcher: async_nats::jetstream::kv::Watch,
        heartbeats: Arc<Mutex<HashMap<String, i64>>>,
        processors: Arc<Mutex<HashMap<String, Processor>>>,
    ) {
        while let Ok(kv) = hb_watcher.next().await.unwrap() {
            match kv.operation {
                async_nats::jetstream::kv::Operation::Put => {
                    let processor_name = kv.key;
                    // convert Bytes to u32
                    let hb = numaflow_pb::objects::watermark::Heartbeat::decode(kv.value)
                        .expect("Failed to decode heartbeat")
                        .heartbeat;
                    info!("Got heartbeat {} for processor {}", hb, processor_name);
                    heartbeats.lock().await.insert(processor_name.clone(), hb);
                    // if the processor is not in the processors map, add it
                    // or if processor status is not active, set it to active
                    let mut processors = processors.lock().await;
                    if let Some(processor) = processors.get_mut(&processor_name) {
                        if !processor.is_active() {
                            processor.set_status(Status::Active);
                        }
                    } else {
                        info!("Processor {} not found, adding it", processor_name);
                        let processor = Processor::new(
                            processor_name,
                            Status::Active,
                            partition_count as usize,
                        );
                        processors.insert(processor.name.clone(), processor);
                    }
                }
                async_nats::jetstream::kv::Operation::Delete => {
                    let processor_name = kv.key;
                    heartbeats.lock().await.remove(&processor_name);

                    // update the processor status to deleted
                    if let Some(processor) = processors.lock().await.get_mut(&processor_name) {
                        processor.set_status(Status::Deleted);
                    }
                }
                async_nats::jetstream::kv::Operation::Purge => {
                    heartbeats.lock().await.clear();

                    // update the processor status to deleted
                    for (_, processor) in processors.lock().await.iter_mut() {
                        processor.set_status(Status::Deleted);
                    }
                }
            }
        }
    }

    pub(crate) async fn get_all_processors(&self) -> HashMap<String, Processor> {
        let mut processors = HashMap::new();
        for (name, processor) in self.processors.lock().await.iter() {
            processors.insert(name.clone(), processor.clone());
        }
        processors
    }

    pub(crate) async fn delete_processor(&self, processor_name: &str) {
        let mut processors = self.processors.lock().await;
        processors.remove(processor_name);
    }
}
