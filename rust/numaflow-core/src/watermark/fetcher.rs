use std::collections::HashMap;

use chrono::{DateTime, Utc};
use tracing::info;

use crate::config::pipeline::watermark::BucketConfig;
use crate::error::{Error, Result};
use crate::watermark::manager::ProcessorManager;
use crate::watermark::Watermark;

pub(crate) struct EdgeFetcher {
    processor_managers: HashMap<&'static str, ProcessorManager>,
    last_processed_wm: HashMap<&'static str, Vec<i64>>,
    last_fetched_wm: Vec<(Watermark, DateTime<Utc>)>,
}

impl EdgeFetcher {
    pub(crate) async fn new(
        js_context: async_nats::jetstream::Context,
        bucket_configs: &[BucketConfig],
    ) -> Result<Self> {
        let mut processor_managers = HashMap::new();
        let mut last_processed_wm = HashMap::new();
        let last_fetched_wm = vec![
            (Watermark::from_timestamp_millis(-1).unwrap(), Utc::now());
            bucket_configs
                .first()
                .expect("one edge should be present")
                .partitions as usize
        ];

        for config in bucket_configs {
            let (processor_manager, processed_wm, _) =
                create_processor_manager(js_context.clone(), config).await?;

            processor_managers.insert(config.vertex, processor_manager);
            last_processed_wm.insert(config.vertex, processed_wm);
        }

        Ok(EdgeFetcher {
            processor_managers,
            last_processed_wm,
            last_fetched_wm,
        })
    }

    pub(crate) async fn fetch_watermark(
        &mut self,
        offset: i64,
        partition_idx: u16,
    ) -> Result<Watermark> {
        if Utc::now()
            .signed_duration_since(self.last_fetched_wm[partition_idx as usize].1)
            .num_milliseconds()
            < 100
        {
            return Ok(self.last_fetched_wm[partition_idx as usize].0);
        }
        for (edge, processor_manager) in self.processor_managers.iter() {
            let mut epoch = i64::MAX;

            for (name, processor) in processor_manager.get_all_processors().await {
                let mut head_offset = -1;
                for (index, timeline) in processor.timelines.iter().enumerate() {
                    if index == partition_idx as usize {
                        let t = timeline.get_event_time(offset).await;
                        if t < epoch {
                            epoch = t;
                        }
                    }
                    if timeline.get_head_offset().await > head_offset {
                        head_offset = timeline.get_head_offset().await;
                    }
                }

                if processor.is_deleted() && offset > head_offset {
                    info!("Processor {} inactive, deleting", name);
                    processor_manager.delete_processor(name.as_str()).await;
                }
            }

            if epoch != i64::MAX {
                self.last_processed_wm.get_mut(edge).unwrap()[partition_idx as usize] = epoch;
            }
        }

        let watermark = self.get_watermark().await?;
        self.last_fetched_wm[partition_idx as usize] = (watermark, Utc::now());
        info!("Fetched watermark: {}", watermark.timestamp_millis());
        Ok(watermark)
    }

    #[allow(dead_code)]
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

pub(crate) struct SourceFetcher {
    processor_manager: ProcessorManager,
}

impl SourceFetcher {
    pub(crate) async fn new(
        js_context: async_nats::jetstream::Context,
        bucket_config: &BucketConfig,
    ) -> Result<Self> {
        let (processor_manager, _last_processed_wm, _last_fetched_wm) =
            create_processor_manager(js_context, bucket_config).await?;

        Ok(SourceFetcher { processor_manager })
    }

    pub(crate) async fn fetch_source_watermark(&self) -> Result<Watermark> {
        let mut min_wm = i64::MAX;

        for (_, processor) in self.processor_manager.get_all_processors().await {
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

        if min_wm == i64::MAX {
            return Ok(Watermark::from_timestamp_millis(-1).unwrap());
        }

        Ok(Watermark::from_timestamp_millis(min_wm).unwrap())
    }
}

async fn create_processor_manager(
    js_context: async_nats::jetstream::Context,
    bucket_config: &BucketConfig,
) -> Result<(ProcessorManager, Vec<i64>, Vec<(Watermark, DateTime<Utc>)>)> {
    let last_fetched_wm = vec![
        (Watermark::from_timestamp_millis(-1).unwrap(), Utc::now());
        bucket_config.partitions as usize
    ];

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

    let processor_manager = ProcessorManager::new(
        bucket_config.partitions,
        ot_bucket
            .watch_all()
            .await
            .map_err(|e| Error::Watermark(format!("Failed to watch ot bucket: {}", e)))?,
        hb_bucket
            .watch_all()
            .await
            .map_err(|e| Error::Watermark(format!("Failed to watch hb bucket: {}", e)))?,
    )
    .await;

    let last_processed_wm = vec![-1; bucket_config.partitions as usize];
    Ok((processor_manager, last_processed_wm, last_fetched_wm))
}
