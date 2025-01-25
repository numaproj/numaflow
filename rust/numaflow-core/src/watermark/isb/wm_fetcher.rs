use std::collections::HashMap;

use chrono::{DateTime, Utc};
use tracing::info;

use crate::config::pipeline::watermark::BucketConfig;
use crate::error::{Error, Result};
use crate::watermark::processor::manager::ProcessorManager;
use crate::watermark::shared::create_processor_manager;
use crate::watermark::wmb::Watermark;

/// EdgeFetcher is the watermark fetcher for the incoming edges.
pub(crate) struct EdgeFetcher {
    /// A map of vertex to its ProcessorManager.
    processor_managers: HashMap<&'static str, ProcessorManager>,
    /// A map of vertex to its last processed watermark for each partition.
    last_processed_wm: HashMap<&'static str, Vec<i64>>,
    /// Vector of (last fetched watermark, last fetched time) for each partition.
    last_fetched_wm: Vec<(Watermark, DateTime<Utc>)>,
}

impl EdgeFetcher {
    /// Creates a new EdgeFetcher.
    pub(crate) async fn new(
        js_context: async_nats::jetstream::Context,
        bucket_configs: &[BucketConfig],
    ) -> Result<Self> {
        let mut processor_managers = HashMap::new();
        let mut last_processed_wm = HashMap::new();
        let last_fetched_wm = vec![
            (
                Watermark::from_timestamp_millis(-1).expect("failed to parse time"),
                Utc::now()
            );
            bucket_configs
                .first()
                .expect("one edge should be present")
                .partitions as usize
        ];

        // Create a ProcessorManager for each edge.
        for config in bucket_configs {
            let processor_manager = create_processor_manager(js_context.clone(), config).await?;
            let processed_wm = vec![-1; config.partitions as usize];

            processor_managers.insert(config.vertex, processor_manager);
            last_processed_wm.insert(config.vertex, processed_wm);
        }

        Ok(EdgeFetcher {
            processor_managers,
            last_processed_wm,
            last_fetched_wm,
        })
    }

    /// Fetches the watermark for the given offset and partition.
    pub(crate) async fn fetch_watermark(
        &mut self,
        offset: i64,
        partition_idx: u16,
    ) -> Result<Watermark> {
        // We fetch the watermark only if we haven't fetched it in the last 100ms.
        if Utc::now()
            .signed_duration_since(self.last_fetched_wm[partition_idx as usize].1)
            .num_milliseconds()
            < 100
        {
            return Ok(self.last_fetched_wm[partition_idx as usize].0);
        }

        // Iterate over all the processor managers and get the smallest watermark. (join case)
        for (edge, processor_manager) in self.processor_managers.iter() {
            let mut epoch = i64::MAX;
            let mut processors_to_delete = Vec::new();

            // iterate over all the timelines of the processor and get the smallest watermark
            for (name, processor) in processor_manager.processors.read().await.iter() {
                // headOffset is used to check whether this pod can be deleted.
                let mut head_offset = -1;

                for (index, timeline) in processor.timelines.iter().enumerate() {
                    // we only need to check the timelines of the partition we are reading from
                    if index == partition_idx as usize {
                        let t = timeline.get_event_time(offset).await;
                        if t < epoch {
                            epoch = t;
                        }
                    }
                    // get the highest head offset among all the partitions of the processor so we can
                    // check later on whether the processor in question is stale (its head is far behind)
                    if timeline.get_head_offset().await > head_offset {
                        head_offset = timeline.get_head_offset().await;
                    }
                }

                // if the pod is not active and the head offset of all the timelines is less than the input offset, delete
                // the processor (this means we are processing data later than what the stale processor has processed)
                if processor.is_deleted() && offset > head_offset {
                    info!("Processor {:?} inactive, deleting", name);
                    processors_to_delete.push(name.clone());
                }
            }

            // delete the processors that are inactive
            for name in processors_to_delete {
                processor_manager.delete_processor(&name).await;
            }

            // if the epoch is not i64::MAX, update the last processed watermark for this particular edge and the partition
            // while fetching watermark we need to consider the smallest last processed watermark among all the partitions
            if epoch != i64::MAX {
                self.last_processed_wm
                    .get_mut(edge)
                    .expect("failed to acquire lock")[partition_idx as usize] = epoch;
            }
        }

        // return the smallest among all the last processed watermarks
        let watermark = self.get_watermark().await?;

        // update the last fetched watermark for this partition
        self.last_fetched_wm[partition_idx as usize] = (watermark, Utc::now());

        info!(
            "Fetched watermark: offset={}, watermark={} partition={}",
            offset,
            watermark.timestamp_millis(),
            partition_idx
        );
        Ok(watermark)
    }

    /// Fetches the latest idle WMB with the smallest watermark for the given partition
    /// Only returns one if all Publishers are idle and if it's the smallest one of any partitions
    #[allow(dead_code)]
    pub(crate) async fn fetch_head_idle_watermark(&mut self, partition_idx: u16) -> Result<i64> {
        let mut min_wm = i64::MAX;
        for (edge, processor_manager) in self.processor_managers.iter() {
            let mut epoch = i64::MAX;
            for processor in processor_manager.processors.read().await.values() {
                // if the processor is not active, skip
                if !processor.is_active() {
                    continue;
                }

                // retrieve the head watermark of the partition, we only care about the head watermark
                // because by looking at the head wmb we will know whether the processor is idle or not
                let head_wmb = processor
                    .timelines
                    .get(partition_idx as usize)
                    .ok_or(Error::Watermark("Partition not found".to_string()))?
                    .get_head_wmb()
                    .await;

                if let Some(wmb) = head_wmb {
                    // if the processor is not idle, return early
                    if !wmb.idle {
                        return Ok(-1);
                    }
                    // consider the smallest watermark among all the partitions
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
                self.last_processed_wm.get_mut(edge).expect("invalid edge")
                    [partition_idx as usize] = epoch;
            }
        }

        if min_wm == i64::MAX {
            return Ok(-1);
        }

        Ok(min_wm)
    }

    // returns the smallest last processed watermark among all the partitions
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
            return Ok(Watermark::from_timestamp_millis(-1).expect("failed to parse time"));
        }
        Ok(Watermark::from_timestamp_millis(min_wm).expect("failed to parse time"))
    }
}
