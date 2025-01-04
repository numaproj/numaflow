use crate::config::pipeline::FromVertexConfig;
use crate::error::{Error, Result};
use crate::watermark::manager::ProcessorManager;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) struct Fetcher {
    processor_managers: Arc<RwLock<HashMap<String, ProcessorManager>>>,
    last_processed_wm: Arc<RwLock<HashMap<String, Vec<i64>>>>,
}

impl Fetcher {
    pub(crate) async fn new(
        from_vertex_configs: Vec<FromVertexConfig>,
        js_context: async_nats::jetstream::Context,
    ) -> Result<Self> {
        let mut processor_managers = HashMap::new();
        let mut last_processed_wm = HashMap::new();
        for from_vertex_config in from_vertex_configs {
            let config = from_vertex_config
                .watermark_config
                .expect("Watermark config not found");

            let ot_bucket = js_context
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket: config.ot_bucket.clone(),
                    ..Default::default()
                })
                .await
                .map_err(|e| Error::Watermark(e.to_string()))?;

            let hb_bucket = js_context
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket: config.hb_bucket.clone(),
                    ..Default::default()
                })
                .await
                .map_err(|e| Error::Watermark(e.to_string()))?;

            let processor_manager = ProcessorManager::new(
                from_vertex_config.partitions,
                ot_bucket
                    .watch_all()
                    .await
                    .map_err(|e| Error::Watermark(e.to_string()))?,
                hb_bucket
                    .watch_all()
                    .await
                    .map_err(|e| Error::Watermark(e.to_string()))?,
            )
            .await;

            processor_managers.insert(from_vertex_config.name.clone(), processor_manager);
            last_processed_wm.insert(
                from_vertex_config.name,
                vec![-1; from_vertex_config.partitions as usize],
            );
        }
        Ok(Fetcher {
            processor_managers: Arc::new(RwLock::new(processor_managers)),
            last_processed_wm: Arc::new(RwLock::new(last_processed_wm)),
        })
    }

    pub(crate) async fn fetch_watermark(&self, offset: u32, partition_idx: u16) -> Result<i64> {
        let all_processors = self.processor_managers.read().await;

        for (edge, processor_manager) in all_processors.iter() {
            let mut epoch = i64::MAX;

            for (name, processor) in processor_manager.get_all_processors().await {
                let mut head_offset = -1;
                for (index, timeline) in processor.timelines.iter().enumerate() {
                    if index == partition_idx as usize {
                        let t = timeline.get_event_time(offset as i64).await;
                        if t < epoch {
                            epoch = t;
                        }
                    }
                    if timeline.get_head_offset().await > head_offset {
                        head_offset = timeline.get_head_offset().await;
                    }
                }

                // if the pod is not active and the head offset of all the timelines is less than the input offset,
                // delete the processor (this means we are processing data later than what the stale processor has processed)
                if processor.is_deleted() && offset > head_offset as u32 {
                    processor_manager.delete_processor(name.as_str()).await;
                }
            }

            // update the last processed watermark for this particular edge and the partition
            let mut last_processed_wm = self.last_processed_wm.write().await;
            last_processed_wm.get_mut(edge).unwrap()[partition_idx as usize] = epoch;
        }

        // iterate over the last processed wm and return the min
        self.get_watermark().await
    }

    pub(crate) async fn fetch_head_idle_watermark(&self, partition_idx: u16) -> Result<i64> {
        let processors = self.processor_managers.read().await;

        let mut min_wm = i64::MAX;
        for processor_manager in processors.values() {
            for processor in processor_manager.get_all_processors().await.values() {
                if !processor.is_active() {
                    continue;
                }

                let head_wmb = processor
                    .timelines
                    .get(partition_idx as usize)
                    .ok_or(Error::Watermark("Partition not found".to_string()))?
                    .get_head_wmb()
                    .await;

                if let Some(wmb) = head_wmb {
                    if !wmb.idle {
                        return Ok(-1);
                    }
                    if wmb.watermark < min_wm {
                        min_wm = wmb.watermark;
                    }
                }
            }
        }

        if min_wm == i64::MAX {
            return Ok(-1);
        }
        Ok(min_wm)
    }

    async fn get_watermark(&self) -> Result<i64> {
        let mut min_wm = i64::MAX;
        let last_processed_wm = self.last_processed_wm.read().await;

        for wm in last_processed_wm.values() {
            for &w in wm {
                if min_wm > w {
                    min_wm = w;
                }
            }
        }

        if min_wm == i64::MAX {
            return Ok(-1);
        }

        Ok(min_wm)
    }
}
