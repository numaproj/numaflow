//! Fetches watermark for the messages read from the ISB. It keeps track of the previous vertices
//! (could be more than one in case of join vertex) processors and their published watermarks
//! for each partition and fetches the watermark for the given offset and partition by iterating over
//! all the processor managers and getting the smallest watermark. It also deletes the processors that
//! are inactive. Since the vertex could be reading from multiple partitions, it keeps track of the
//! last fetched watermark per partition and returns the smallest watermark among all the last fetched
//! watermarks across the partitions this is to make sure the watermark is min across all the incoming
//! partitions.
use crate::config::pipeline::watermark::BucketConfig;
use crate::error::Result;
use crate::watermark::processor::manager::ProcessorManager;
use crate::watermark::wmb::Watermark;
use std::collections::HashMap;

/// ISBWatermarkFetcher is the watermark fetcher for the incoming edges.
pub(crate) struct ISBWatermarkFetcher {
    /// A map of vertex to its ProcessorManager. Each key represents the incoming vertex, it will
    /// be > 1 only during JOIN.
    processor_managers: HashMap<&'static str, ProcessorManager>,
    /// A map of vertex to its last processed watermark for each partition. Index[0] will be 0th
    /// partition, and so forth.
    last_processed_wm: HashMap<&'static str, Vec<i64>>,
}

impl ISBWatermarkFetcher {
    /// Creates a new ISBWatermarkFetcher.
    pub(crate) async fn new(
        processor_managers: HashMap<&'static str, ProcessorManager>,
        bucket_configs: &[BucketConfig],
    ) -> Result<Self> {
        let mut last_processed_wm = HashMap::new();

        // Create a ProcessorManager for each edge.
        for config in bucket_configs {
            let processed_wm = vec![-1; config.partitions as usize];
            last_processed_wm.insert(config.vertex, processed_wm);
        }

        Ok(ISBWatermarkFetcher {
            processor_managers,
            last_processed_wm,
        })
    }

    /// Fetches the watermark for the given offset and partition.
    pub(crate) fn fetch_watermark(&mut self, offset: i64, partition_idx: u16) -> Result<Watermark> {
        // Iterate over all the processor managers and get the smallest watermark. (join case)
        for (edge, processor_manager) in self.processor_managers.iter() {
            let mut epoch = i64::MAX;
            let mut processors_to_delete = Vec::new();

            // iterate over all the processors and get the smallest watermark
            processor_manager
                .processors
                .read()
                .expect("failed to acquire lock")
                .iter()
                .for_each(|(name, processor)| {
                    // we only need to consider the timeline for the requested partition
                    if let Some(timeline) = processor.timelines.get(partition_idx as usize) {
                        let t = timeline.get_event_time(offset);
                        if t < epoch {
                            epoch = t;
                        }
                    }

                    // if the pod is not active and the head offset of all the timelines is less than the input offset, delete
                    // the processor (this means we are processing data later than what the stale processor has processed)
                    if processor.is_deleted() {
                        // headOffset is used to check whether this pod can be deleted (e.g., dead pod)
                        let head_offset = processor
                            .timelines
                            .iter()
                            .map(|timeline| timeline.get_head_offset())
                            .max()
                            .unwrap_or(-1);

                        if offset > head_offset {
                            processors_to_delete.push(name.clone());
                        }
                    }
                });

            // delete the processors that are inactive
            for name in processors_to_delete {
                processor_manager.delete_processor(&name);
            }

            // if the epoch is not i64::MAX, update the last processed watermark for this particular edge and the partition
            // while fetching watermark we need to consider the smallest last processed watermark among all the partitions
            if epoch != i64::MAX {
                self.last_processed_wm
                    .get_mut(edge)
                    .unwrap_or_else(|| panic!("invalid vertex {}", edge))
                    [partition_idx as usize] = epoch;
            }
        }

        // now we computed and updated for this partition, we just need to compare across partitions.
        self.get_watermark()
    }

    /// Fetches the latest idle WMB with the smallest watermark for the given partition
    /// Only returns one if all Publishers are idle and if it's the smallest one of any partitions
    pub(crate) fn fetch_head_idle_watermark(&mut self) -> Result<Watermark> {
        let mut min_wm = i64::MAX;

        for (edge, processor_manager) in &self.processor_managers {
            let mut epoch = i64::MAX;

            let processors = processor_manager
                .processors
                .read()
                .expect("failed to acquire lock");

            let active_processors = processors
                .values()
                .filter(|processor| processor.is_active());

            for processor in active_processors {
                for timeline in &processor.timelines {
                    if let Some(head_wmb) = timeline.get_head_wmb() {
                        // if the processor is not idle, return early
                        if !head_wmb.idle {
                            return Ok(
                                Watermark::from_timestamp_millis(-1).expect("failed to parse time")
                            );
                        }
                        // consider the smallest watermark among all the partitions
                        epoch = epoch.min(head_wmb.watermark);
                    }
                }
            }

            if epoch < i64::MAX {
                min_wm = min_wm.min(epoch);
                // update the last processed watermark for this particular edge and all the partitions
                self.last_processed_wm
                    .get_mut(edge)
                    .expect(&format!("invalid vertex {}", edge))
                    .iter_mut()
                    .for_each(|partition| *partition = epoch);
            }
        }

        if min_wm == i64::MAX {
            min_wm = -1;
        }

        Ok(Watermark::from_timestamp_millis(min_wm).expect("failed to parse time"))
    }

    /// returns the smallest last processed watermark among all the partitions
    fn get_watermark(&self) -> Result<Watermark> {
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use std::sync::RwLock;

    use super::*;
    use crate::watermark::processor::manager::{Processor, Status};
    use crate::watermark::processor::timeline::OffsetTimeline;
    use crate::watermark::wmb::WMB;

    #[tokio::test]
    async fn test_fetch_watermark_single_edge_single_processor_single_partition() {
        // Create a ProcessorManager with a single Processor and a single OffsetTimeline
        let processor_name = Bytes::from("processor1");
        let mut processor = Processor::new(processor_name.clone(), Status::Active, 1);
        let timeline = OffsetTimeline::new(10);

        // Populate the OffsetTimeline with sorted WMB entries
        let wmb1 = WMB {
            watermark: 100,
            offset: 1,
            idle: false,
            partition: 0,
        };
        let wmb2 = WMB {
            watermark: 200,
            offset: 2,
            idle: false,
            partition: 0,
        };
        let wmb3 = WMB {
            watermark: 300,
            offset: 3,
            idle: false,
            partition: 0,
        };

        timeline.put(wmb1);
        timeline.put(wmb2);
        timeline.put(wmb3);

        processor.timelines[0] = timeline;

        let mut processors = HashMap::new();
        processors.insert(processor_name.clone(), processor);

        let processor_manager = ProcessorManager {
            processors: Arc::new(RwLock::new(processors)),
            handles: vec![],
        };

        let mut processor_managers = HashMap::new();
        processor_managers.insert("from_vtx", processor_manager);

        let bucket_config = BucketConfig {
            vertex: "from_vtx",
            ot_bucket: "ot_bucket",
            hb_bucket: "hb_bucket",
            partitions: 1,
        };

        let mut fetcher = ISBWatermarkFetcher::new(processor_managers, &[bucket_config])
            .await
            .unwrap();

        // Invoke fetch_watermark and verify the result
        let watermark = fetcher.fetch_watermark(2, 0).unwrap();
        assert_eq!(watermark.timestamp_millis(), 100);
    }

    #[tokio::test]
    async fn test_fetch_watermark_single_edge_multi_processor_single_partition() {
        // Create ProcessorManager with multiple Processors and different OffsetTimelines
        let processor_name1 = Bytes::from("processor1");
        let processor_name2 = Bytes::from("processor2");
        let processor_name3 = Bytes::from("processor3");

        let mut processor1 = Processor::new(processor_name1.clone(), Status::Active, 1);
        let mut processor2 = Processor::new(processor_name2.clone(), Status::Active, 1);
        let mut processor3 = Processor::new(processor_name3.clone(), Status::Active, 1);

        let timeline1 = OffsetTimeline::new(10);
        let timeline2 = OffsetTimeline::new(10);
        let timeline3 = OffsetTimeline::new(10);

        // Populate the OffsetTimelines with sorted WMB entries with unique and mixed offsets
        let wmbs1 = vec![
            WMB {
                watermark: 100,
                offset: 5,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 150,
                offset: 10,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 200,
                offset: 15,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 250,
                offset: 20,
                idle: false,
                partition: 0,
            },
        ];
        let wmbs2 = vec![
            WMB {
                watermark: 110,
                offset: 3,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 160,
                offset: 8,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 210,
                offset: 13,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 260,
                offset: 18,
                idle: false,
                partition: 0,
            },
        ];
        let wmbs3 = vec![
            WMB {
                watermark: 120,
                offset: 2,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 170,
                offset: 7,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 220,
                offset: 12,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 270,
                offset: 17,
                idle: false,
                partition: 0,
            },
        ];

        for wmb in wmbs1 {
            timeline1.put(wmb);
        }
        for wmb in wmbs2 {
            timeline2.put(wmb);
        }
        for wmb in wmbs3 {
            timeline3.put(wmb);
        }

        processor1.timelines[0] = timeline1;
        processor2.timelines[0] = timeline2;
        processor3.timelines[0] = timeline3;

        let mut processors = HashMap::new();
        processors.insert(processor_name1.clone(), processor1);
        processors.insert(processor_name2.clone(), processor2);
        processors.insert(processor_name3.clone(), processor3);

        let processor_manager = ProcessorManager {
            processors: Arc::new(RwLock::new(processors)),
            handles: vec![],
        };

        let mut processor_managers = HashMap::new();
        processor_managers.insert("from_vtx", processor_manager);

        let bucket_config = BucketConfig {
            vertex: "from_vtx",
            ot_bucket: "ot_bucket",
            hb_bucket: "hb_bucket",
            partitions: 1,
        };

        let mut fetcher = ISBWatermarkFetcher::new(processor_managers, &[bucket_config])
            .await
            .unwrap();

        // Invoke fetch_watermark and verify the result
        let watermark = fetcher.fetch_watermark(12, 0).unwrap();
        assert_eq!(watermark.timestamp_millis(), 150);
    }

    #[tokio::test]
    async fn test_fetch_watermark_single_edge_multi_processor_multi_partition() {
        // Create ProcessorManager with multiple Processors and different OffsetTimelines
        let processor_name1 = Bytes::from("processor1");
        let processor_name2 = Bytes::from("processor2");
        let processor_name3 = Bytes::from("processor3");

        let mut processor1 = Processor::new(processor_name1.clone(), Status::Active, 2);
        let mut processor2 = Processor::new(processor_name2.clone(), Status::Active, 2);
        let mut processor3 = Processor::new(processor_name3.clone(), Status::Active, 2);

        let timeline1_p0 = OffsetTimeline::new(10);
        let timeline1_p1 = OffsetTimeline::new(10);
        let timeline2_p0 = OffsetTimeline::new(10);
        let timeline2_p1 = OffsetTimeline::new(10);
        let timeline3_p0 = OffsetTimeline::new(10);
        let timeline3_p1 = OffsetTimeline::new(10);

        // Populate the OffsetTimelines with sorted WMB entries with unique and mixed offsets
        let wmbs1_p0 = vec![
            WMB {
                watermark: 100,
                offset: 6,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 150,
                offset: 10,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 200,
                offset: 15,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 250,
                offset: 20,
                idle: false,
                partition: 0,
            },
        ];
        let wmbs1_p1 = vec![
            WMB {
                watermark: 110,
                offset: 25,
                idle: false,
                partition: 1,
            },
            WMB {
                watermark: 160,
                offset: 30,
                idle: false,
                partition: 1,
            },
            WMB {
                watermark: 210,
                offset: 35,
                idle: false,
                partition: 1,
            },
            WMB {
                watermark: 260,
                offset: 40,
                idle: false,
                partition: 1,
            },
        ];
        let wmbs2_p0 = vec![
            WMB {
                watermark: 120,
                offset: 3,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 170,
                offset: 8,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 220,
                offset: 13,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 270,
                offset: 18,
                idle: false,
                partition: 0,
            },
        ];
        let wmbs2_p1 = vec![
            WMB {
                watermark: 130,
                offset: 23,
                idle: false,
                partition: 1,
            },
            WMB {
                watermark: 180,
                offset: 28,
                idle: false,
                partition: 1,
            },
            WMB {
                watermark: 230,
                offset: 33,
                idle: false,
                partition: 1,
            },
            WMB {
                watermark: 280,
                offset: 38,
                idle: false,
                partition: 1,
            },
        ];
        let wmbs3_p0 = vec![
            WMB {
                watermark: 140,
                offset: 2,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 190,
                offset: 7,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 240,
                offset: 12,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 290,
                offset: 17,
                idle: false,
                partition: 0,
            },
        ];
        let wmbs3_p1 = vec![
            WMB {
                watermark: 150,
                offset: 22,
                idle: false,
                partition: 1,
            },
            WMB {
                watermark: 200,
                offset: 27,
                idle: false,
                partition: 1,
            },
            WMB {
                watermark: 250,
                offset: 32,
                idle: false,
                partition: 1,
            },
            WMB {
                watermark: 300,
                offset: 37,
                idle: false,
                partition: 1,
            },
        ];

        for wmb in wmbs1_p0 {
            timeline1_p0.put(wmb);
        }
        for wmb in wmbs1_p1 {
            timeline1_p1.put(wmb);
        }
        for wmb in wmbs2_p0 {
            timeline2_p0.put(wmb);
        }
        for wmb in wmbs2_p1 {
            timeline2_p1.put(wmb);
        }
        for wmb in wmbs3_p0 {
            timeline3_p0.put(wmb);
        }
        for wmb in wmbs3_p1 {
            timeline3_p1.put(wmb);
        }

        processor1.timelines[0] = timeline1_p0;
        processor1.timelines[1] = timeline1_p1;
        processor2.timelines[0] = timeline2_p0;
        processor2.timelines[1] = timeline2_p1;
        processor3.timelines[0] = timeline3_p0;
        processor3.timelines[1] = timeline3_p1;

        let mut processors = HashMap::new();
        processors.insert(processor_name1.clone(), processor1);
        processors.insert(processor_name2.clone(), processor2);
        processors.insert(processor_name3.clone(), processor3);

        let processor_manager = ProcessorManager {
            processors: Arc::new(RwLock::new(processors)),
            handles: vec![],
        };

        let mut processor_managers = HashMap::new();
        processor_managers.insert("from_vtx", processor_manager);

        let bucket_config = BucketConfig {
            vertex: "from_vtx",
            ot_bucket: "ot_bucket",
            hb_bucket: "hb_bucket",
            partitions: 2,
        };

        let mut fetcher = ISBWatermarkFetcher::new(processor_managers, &[bucket_config])
            .await
            .unwrap();

        // Invoke fetch_watermark and verify the result for partition 0, first fetch will be -1 because we have not fetched for other
        // partition (we consider min across the last fetched watermark)
        let watermark_p0 = fetcher.fetch_watermark(12, 0).unwrap();
        assert_eq!(watermark_p0.timestamp_millis(), -1);

        // Invoke fetch_watermark and verify the result for partition 1 (we consider min across the last fetch wm for all partitions)
        let watermark_p1 = fetcher.fetch_watermark(32, 1).unwrap();
        assert_eq!(watermark_p1.timestamp_millis(), 150);
    }

    #[tokio::test]
    async fn test_fetch_watermark_two_edges_multi_processor_multi_partition() {
        // Create ProcessorManagers with multiple Processors and different OffsetTimelines for edge1
        let processor_name1_edge1 = Bytes::from("processor1_edge1");
        let processor_name2_edge1 = Bytes::from("processor2_edge1");

        let mut processor1_edge1 = Processor::new(processor_name1_edge1.clone(), Status::Active, 2);
        let mut processor2_edge1 = Processor::new(processor_name2_edge1.clone(), Status::Active, 2);

        let timeline1_p0_edge1 = OffsetTimeline::new(10);
        let timeline1_p1_edge1 = OffsetTimeline::new(10);
        let timeline2_p0_edge1 = OffsetTimeline::new(10);
        let timeline2_p1_edge1 = OffsetTimeline::new(10);

        // Populate the OffsetTimelines with sorted WMB entries with unique and mixed offsets for edge1
        let wmbs1_p0_edge1 = vec![
            WMB {
                watermark: 100,
                offset: 6,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 150,
                offset: 10,
                idle: false,
                partition: 0,
            },
        ];
        let wmbs1_p1_edge1 = vec![
            WMB {
                watermark: 110,
                offset: 25,
                idle: false,
                partition: 1,
            },
            WMB {
                watermark: 160,
                offset: 30,
                idle: false,
                partition: 1,
            },
        ];
        let wmbs2_p0_edge1 = vec![
            WMB {
                watermark: 120,
                offset: 3,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 170,
                offset: 8,
                idle: false,
                partition: 0,
            },
        ];
        let wmbs2_p1_edge1 = vec![
            WMB {
                watermark: 130,
                offset: 23,
                idle: false,
                partition: 1,
            },
            WMB {
                watermark: 180,
                offset: 28,
                idle: false,
                partition: 1,
            },
        ];

        for wmb in wmbs1_p0_edge1 {
            timeline1_p0_edge1.put(wmb);
        }
        for wmb in wmbs1_p1_edge1 {
            timeline1_p1_edge1.put(wmb);
        }
        for wmb in wmbs2_p0_edge1 {
            timeline2_p0_edge1.put(wmb);
        }
        for wmb in wmbs2_p1_edge1 {
            timeline2_p1_edge1.put(wmb);
        }

        processor1_edge1.timelines[0] = timeline1_p0_edge1;
        processor1_edge1.timelines[1] = timeline1_p1_edge1;
        processor2_edge1.timelines[0] = timeline2_p0_edge1;
        processor2_edge1.timelines[1] = timeline2_p1_edge1;

        let mut processors_edge1 = HashMap::new();
        processors_edge1.insert(processor_name1_edge1.clone(), processor1_edge1);
        processors_edge1.insert(processor_name2_edge1.clone(), processor2_edge1);

        let processor_manager_edge1 = ProcessorManager {
            processors: Arc::new(RwLock::new(processors_edge1)),
            handles: vec![],
        };

        // Create ProcessorManagers with multiple Processors and different OffsetTimelines for edge2
        let processor_name1_edge2 = Bytes::from("processor1_edge2");
        let processor_name2_edge2 = Bytes::from("processor2_edge2");

        let mut processor1_edge2 = Processor::new(processor_name1_edge2.clone(), Status::Active, 2);
        let mut processor2_edge2 = Processor::new(processor_name2_edge2.clone(), Status::Active, 2);

        let timeline1_p0_edge2 = OffsetTimeline::new(10);
        let timeline1_p1_edge2 = OffsetTimeline::new(10);
        let timeline2_p0_edge2 = OffsetTimeline::new(10);
        let timeline2_p1_edge2 = OffsetTimeline::new(10);

        // Populate the OffsetTimelines with sorted WMB entries with unique and mixed offsets for edge2
        let wmbs1_p0_edge2 = vec![
            WMB {
                watermark: 140,
                offset: 2,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 190,
                offset: 7,
                idle: false,
                partition: 0,
            },
        ];
        let wmbs1_p1_edge2 = vec![
            WMB {
                watermark: 150,
                offset: 22,
                idle: false,
                partition: 1,
            },
            WMB {
                watermark: 200,
                offset: 27,
                idle: false,
                partition: 1,
            },
        ];
        let wmbs2_p0_edge2 = vec![
            WMB {
                watermark: 160,
                offset: 4,
                idle: false,
                partition: 0,
            },
            WMB {
                watermark: 210,
                offset: 9,
                idle: false,
                partition: 0,
            },
        ];
        let wmbs2_p1_edge2 = vec![
            WMB {
                watermark: 170,
                offset: 24,
                idle: false,
                partition: 1,
            },
            WMB {
                watermark: 220,
                offset: 29,
                idle: false,
                partition: 1,
            },
        ];

        for wmb in wmbs1_p0_edge2 {
            timeline1_p0_edge2.put(wmb);
        }
        for wmb in wmbs1_p1_edge2 {
            timeline1_p1_edge2.put(wmb);
        }
        for wmb in wmbs2_p0_edge2 {
            timeline2_p0_edge2.put(wmb);
        }
        for wmb in wmbs2_p1_edge2 {
            timeline2_p1_edge2.put(wmb);
        }

        processor1_edge2.timelines[0] = timeline1_p0_edge2;
        processor1_edge2.timelines[1] = timeline1_p1_edge2;
        processor2_edge2.timelines[0] = timeline2_p0_edge2;
        processor2_edge2.timelines[1] = timeline2_p1_edge2;

        let mut processors_edge2 = HashMap::new();
        processors_edge2.insert(processor_name1_edge2.clone(), processor1_edge2);
        processors_edge2.insert(processor_name2_edge2.clone(), processor2_edge2);

        let processor_manager_edge2 = ProcessorManager {
            processors: Arc::new(RwLock::new(processors_edge2)),
            handles: vec![],
        };

        let mut processor_managers = HashMap::new();
        processor_managers.insert("edge1", processor_manager_edge1);
        processor_managers.insert("edge2", processor_manager_edge2);

        let bucket_config1 = BucketConfig {
            vertex: "edge1",
            ot_bucket: "ot_bucket1",
            hb_bucket: "hb_bucket1",
            partitions: 2,
        };
        let bucket_config2 = BucketConfig {
            vertex: "edge2",
            ot_bucket: "ot_bucket2",
            hb_bucket: "hb_bucket2",
            partitions: 2,
        };

        let mut fetcher =
            ISBWatermarkFetcher::new(processor_managers, &[bucket_config1, bucket_config2])
                .await
                .unwrap();

        // Invoke fetch_watermark and verify the result for partition 0
        let watermark_p0 = fetcher.fetch_watermark(12, 0).unwrap();
        assert_eq!(watermark_p0.timestamp_millis(), -1);

        // Invoke fetch_watermark and verify the result for partition 1
        let watermark_p1 = fetcher.fetch_watermark(32, 1).unwrap();
        assert_eq!(watermark_p1.timestamp_millis(), 150);
    }
}
