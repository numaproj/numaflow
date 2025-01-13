use crate::config::pipeline::{FromVertexConfig, WatermarkConfig};
use crate::error::{Error, Result};
use crate::watermark::manager::ProcessorManager;
use crate::watermark::Watermark;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[allow(dead_code)]
pub(crate) struct Fetcher {
    processor_managers: HashMap<String, ProcessorManager>,
    last_processed_wm: HashMap<String, Vec<i64>>,
    last_fetched_wm: Vec<(Watermark, DateTime<Utc>)>,
}

impl Fetcher {
    async fn new(
        from_vertex_configs: Vec<FromVertexConfig>,
        js_context: async_nats::jetstream::Context,
        source_watermark_config: Option<WatermarkConfig>,
    ) -> Result<Self> {
        let mut processor_managers = HashMap::new();
        let mut last_processed_wm = HashMap::new();
        let last_fetched_wm = vec![
            (Watermark::from_timestamp_millis(-1).unwrap(), Utc::now());
            from_vertex_configs.first().unwrap().partitions as usize
        ];

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
            processor_managers,
            last_processed_wm,
            last_fetched_wm,
        })
    }

    pub(crate) async fn fetch_watermark(
        &mut self,
        offset: u64,
        partition_idx: u16,
    ) -> Result<Watermark> {
        if Utc::now()
            .signed_duration_since(self.last_fetched_wm[partition_idx as usize].1)
            .num_milliseconds()
            < 100
        {
            return Ok(self.last_fetched_wm[partition_idx as usize].1);
        }

        for (edge, processor_manager) in self.processor_managers.iter() {
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

                if processor.is_deleted() && offset > head_offset as u64 {
                    processor_manager.delete_processor(name.as_str()).await;
                }
            }

            if epoch != i64::MAX {
                self.last_processed_wm.get_mut(edge).unwrap()[partition_idx as usize] = epoch;
            }
        }

        let watermark = self.get_watermark().await?;
        self.last_fetched_wm[partition_idx as usize] = (watermark, Utc::now());
        Ok(watermark)
    }

    pub(crate) async fn fetch_source_watermark(&self) -> Result<Watermark> {
        let mut min_wm = i64::MAX;

        for (edge, processor_manager) in self.processor_managers.iter() {
            for (name, processor) in processor_manager.get_all_processors().await {
                if !processor.is_active() {
                    continue;
                }
                let head_wm = processor
                    .timelines
                    .first()
                    .unwrap()
                    .get_head_watermark()
                    .await;

                if head_wm < min_wm {
                    min_wm = head_wm;
                }
            }
        }

        if min_wm == i64::MAX {
            return Ok(Watermark::from_timestamp_millis(-1).unwrap());
        }

        Ok(Watermark::from_timestamp_millis(min_wm).unwrap())
    }

    pub(crate) async fn fetch_head_idle_watermark(&mut self, partition_idx: u16) -> Result<i64> {
        let mut min_wm = i64::MAX;
        for (edge, processor_manager) in self.processor_managers.iter() {
            let mut epoch = i64::MAX;
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
                    if wmb.watermark < epoch {
                        epoch = wmb.watermark;
                    }
                }
            }

            if epoch < min_wm {
                min_wm = epoch;
            }

            if epoch != i64::MAX {
                // update the last processed watermark for this particular edge and the partition
                self.last_processed_wm.get_mut(edge).unwrap()[partition_idx as usize] = epoch;
            }
        }

        if min_wm == i64::MAX {
            return Ok(-1);
        }

        Ok(min_wm)
    }

    async fn get_watermark(&self) -> Result<Watermark> {
        let mut min_wm = i64::MAX;

        for wm in self.last_processed_wm.values() {
            for &w in wm {
                if min_wm > w {
                    min_wm = w;
                }
            }
        }

        if min_wm == i64::MAX {
            return Ok(Watermark::from_timestamp_millis(-1).unwrap());
        }

        Ok(Watermark::from_timestamp_millis(min_wm).unwrap())
    }
}
