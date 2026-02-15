//! Fetches watermark for the source, source fetcher will only have one processor manager which tracks
//! all the partition based processors. The fetcher will look at all the active processors and their
//! timelines to determine the watermark. We don't care about offsets here since the watermark starts
//! at source, we only consider the head watermark and consider the minimum watermark of all the active
//! processors.

use std::time::{Duration, Instant};

use tracing::info;

use crate::watermark::processor::manager::ProcessorManager;
use crate::watermark::wmb::Watermark;

/// Interval for logging watermark summary
const WATERMARK_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// SourceWatermarkFetcher is the watermark fetcher for the source.
pub struct SourceWatermarkFetcher {
    processor_manager: ProcessorManager,
    /// Last time the watermark summary was logged
    last_log_time: Instant,
}

impl SourceWatermarkFetcher {
    /// Creates a new [SourceWatermarkFetcher].
    pub(crate) fn new(processor_manager: ProcessorManager) -> Self {
        SourceWatermarkFetcher {
            processor_manager,
            last_log_time: Instant::now(),
        }
    }

    /// Fetches the watermark for the source, which is the minimum watermark of all the active
    /// processors.
    pub(crate) fn fetch_source_watermark(&mut self) -> Watermark {
        let mut min_wm = i64::MAX;

        for (_, processor) in self
            .processor_manager
            .processors
            .read()
            .expect("failed to acquire lock")
            .iter()
        {
            // We only consider active processors.
            if !processor.is_active() {
                continue;
            }

            // only consider the head watermark of the processor (source has only partition 0)
            let head_wm = processor.timelines.get(&0).unwrap().get_head_watermark();

            if head_wm < min_wm {
                min_wm = head_wm;
            }
        }

        if min_wm == i64::MAX {
            min_wm = -1;
        }

        let watermark = Watermark::from_timestamp_millis(min_wm).expect("Failed to parse watermark");

        // Log summary periodically
        self.watermark_log_summary(&watermark);

        watermark
    }

    /// Fetches the head watermark for the source, which is the minimum head watermark of all the active
    /// processors for the specified partition. This method is similar to fetch_source_watermark but provides a dedicated interface
    /// for fetching head watermarks.
    pub(crate) fn fetch_head_watermark(&mut self, partition_idx: u16) -> Watermark {
        let mut min_wm = i64::MAX;

        for (_, processor) in self
            .processor_manager
            .processors
            .read()
            .expect("failed to acquire lock")
            .iter()
        {
            // We only consider active processors.
            if !processor.is_active() {
                continue;
            }

            // Only consider the head watermark of the processor for the requested partition
            // For sources, typically only partition 0 is used, but we check the requested partition
            if let Some(timeline) = processor.timelines.get(&partition_idx) {
                let head_wm = timeline.get_head_watermark();

                if head_wm != -1 && head_wm < min_wm {
                    min_wm = head_wm;
                }
            }
        }

        if min_wm == i64::MAX {
            min_wm = -1;
        }

        Watermark::from_timestamp_millis(min_wm).expect("Failed to parse watermark")
    }

    /// Logs a summary of the watermark state if the log interval has elapsed.
    /// This includes fetched watermark, processors, and their timelines.
    fn watermark_log_summary(&mut self, fetched_wm: &Watermark) {
        if self.last_log_time.elapsed() < WATERMARK_LOG_INTERVAL {
            return;
        }
        self.last_log_time = Instant::now();

        let summary = self.build_summary(fetched_wm);
        info!("{}", summary);
    }

    /// Builds a summary string of the watermark state.
    fn build_summary(&self, fetched_wm: &Watermark) -> String {
        let mut summary = String::new();

        // Add fetched watermark
        summary.push_str(&format!(
            "Source Watermark Summary: fetched_wm={}, ",
            fetched_wm.timestamp_millis()
        ));

        // Add processor information
        let processors = self
            .processor_manager
            .processors
            .read()
            .expect("failed to acquire lock");

        let mut proc_infos: Vec<String> = Vec::new();
        for (name, processor) in processors.iter() {
            let name_str = String::from_utf8_lossy(name);
            let status = if processor.is_active() {
                "active"
            } else if processor.is_deleted() {
                "deleted"
            } else {
                "inactive"
            };

            // Get complete timeline for each partition
            let mut timeline_parts: Vec<String> = Vec::new();
            for (partition, timeline) in &processor.timelines {
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

            proc_infos.push(format!(
                "{}({})[{}]",
                name_str,
                status,
                timeline_parts.join(",")
            ));
        }
        proc_infos.sort();
        summary.push_str(&format!("processors={{{}}}", proc_infos.join(", ")));

        summary
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::RwLock;

    use bytes::Bytes;

    use super::*;
    use crate::watermark::processor::manager::{Processor, Status};
    use crate::watermark::processor::timeline::OffsetTimeline;
    use crate::watermark::wmb::WMB;

    #[tokio::test]
    async fn test_source_watermark_fetcher_single_processor() {
        // Create a ProcessorManager with a single Processor and a single OffsetTimeline
        let processor_name = Bytes::from("processor1");
        let mut processor = Processor::new(processor_name.clone(), Status::Active, &[0]);
        let mut timeline = OffsetTimeline::new(10);

        // Populate the OffsetTimeline with sorted WMB entries
        let wmb1 = WMB {
            watermark: 100,
            offset: 19723492734,
            idle: false,
            partition: 0,
        };
        let wmb2 = WMB {
            watermark: 200,
            offset: 19723492735,
            idle: false,
            partition: 0,
        };
        let wmb3 = WMB {
            watermark: 300,
            offset: 19723492736,
            idle: false,
            partition: 0,
        };

        timeline.put(wmb1);
        timeline.put(wmb2);
        timeline.put(wmb3);

        processor.timelines.insert(0, timeline);

        let mut processors = HashMap::new();
        processors.insert(processor_name.clone(), processor);

        let processor_manager = ProcessorManager {
            processors: Arc::new(RwLock::new(processors)),
            handles: vec![],
        };

        let mut fetcher = SourceWatermarkFetcher::new(processor_manager);

        // Invoke fetch_watermark and verify the result
        let watermark = fetcher.fetch_source_watermark();
        assert_eq!(watermark.timestamp_millis(), 300);
    }

    #[tokio::test]
    async fn test_source_watermark_fetcher_multi_processors() {
        // Create a ProcessorManager with multiple Processors and a single OffsetTimeline
        let processor_name1 = Bytes::from("processor1");
        let mut processor1 = Processor::new(processor_name1.clone(), Status::Active, &[0]);
        let mut timeline1 = OffsetTimeline::new(10);

        // Populate the OffsetTimeline with sorted WMB entries
        let wmb1 = WMB {
            watermark: 100,
            offset: 19723492734,
            idle: false,
            partition: 0,
        };
        let wmb2 = WMB {
            watermark: 200,
            offset: 19723492735,
            idle: false,
            partition: 0,
        };
        let wmb3 = WMB {
            watermark: 323,
            offset: 19723492736,
            idle: false,
            partition: 0,
        };

        timeline1.put(wmb1);
        timeline1.put(wmb2);
        timeline1.put(wmb3);

        processor1.timelines.insert(0, timeline1);

        let processor_name2 = Bytes::from("processor2");
        let mut processor2 = Processor::new(processor_name2.clone(), Status::Active, &[0]);
        let mut timeline2 = OffsetTimeline::new(10);

        // Populate the OffsetTimeline with sorted WMB entries
        let wmb4 = WMB {
            watermark: 150,
            offset: 19723492734,
            idle: false,
            partition: 0,
        };
        let wmb5 = WMB {
            watermark: 250,
            offset: 19723492735,
            idle: false,
            partition: 0,
        };
        let wmb6 = WMB {
            watermark: 350,
            offset: 19723492736,
            idle: false,
            partition: 0,
        };

        timeline2.put(wmb4);
        timeline2.put(wmb5);
        timeline2.put(wmb6);

        processor2.timelines.insert(0, timeline2);

        let mut processors = HashMap::new();
        processors.insert(processor_name1.clone(), processor1);
        processors.insert(processor_name2.clone(), processor2);

        let processor_manager = ProcessorManager {
            processors: Arc::new(RwLock::new(processors)),
            handles: vec![],
        };

        let mut fetcher = SourceWatermarkFetcher::new(processor_manager);

        // Invoke fetch_watermark and verify the result
        let watermark = fetcher.fetch_source_watermark();
        assert_eq!(watermark.timestamp_millis(), 323);
    }

    #[tokio::test]
    async fn test_source_watermark_fetcher_fetch_head_watermark_single_processor() {
        // Create a ProcessorManager with a single Processor and a single OffsetTimeline
        let processor_name = Bytes::from("processor1");
        let mut processor = Processor::new(processor_name.clone(), Status::Active, &[0]);
        let mut timeline = OffsetTimeline::new(10);

        // Populate the OffsetTimeline with sorted WMB entries
        let wmb1 = WMB {
            watermark: 100,
            offset: 19723492734,
            idle: false,
            partition: 0,
        };
        let wmb2 = WMB {
            watermark: 200,
            offset: 19723492735,
            idle: false,
            partition: 0,
        };
        let wmb3 = WMB {
            watermark: 300,
            offset: 19723492736,
            idle: false,
            partition: 0,
        };

        timeline.put(wmb1);
        timeline.put(wmb2);
        timeline.put(wmb3);

        processor.timelines.insert(0, timeline);

        let mut processors = HashMap::new();
        processors.insert(processor_name.clone(), processor);

        let processor_manager = ProcessorManager {
            processors: Arc::new(RwLock::new(processors)),
            handles: vec![],
        };

        let mut fetcher = SourceWatermarkFetcher::new(processor_manager);

        // Invoke fetch_head_watermark and verify the result
        let watermark = fetcher.fetch_head_watermark(0);
        assert_eq!(watermark.timestamp_millis(), 300);
    }

    #[tokio::test]
    async fn test_source_watermark_fetcher_fetch_head_watermark_multi_processors() {
        // Create a ProcessorManager with multiple Processors and a single OffsetTimeline
        let processor_name1 = Bytes::from("processor1");
        let mut processor1 = Processor::new(processor_name1.clone(), Status::Active, &[0]);
        let mut timeline1 = OffsetTimeline::new(10);

        // Populate the OffsetTimeline with sorted WMB entries
        let wmb1 = WMB {
            watermark: 100,
            offset: 19723492734,
            idle: false,
            partition: 0,
        };
        let wmb2 = WMB {
            watermark: 200,
            offset: 19723492735,
            idle: false,
            partition: 0,
        };
        let wmb3 = WMB {
            watermark: 323,
            offset: 19723492736,
            idle: false,
            partition: 0,
        };

        timeline1.put(wmb1);
        timeline1.put(wmb2);
        timeline1.put(wmb3);

        processor1.timelines.insert(0, timeline1);

        let processor_name2 = Bytes::from("processor2");
        let mut processor2 = Processor::new(processor_name2.clone(), Status::Active, &[0]);
        let mut timeline2 = OffsetTimeline::new(10);

        // Populate the OffsetTimeline with sorted WMB entries
        let wmb4 = WMB {
            watermark: 150,
            offset: 19723492734,
            idle: false,
            partition: 0,
        };
        let wmb5 = WMB {
            watermark: 250,
            offset: 19723492735,
            idle: false,
            partition: 0,
        };
        let wmb6 = WMB {
            watermark: 350,
            offset: 19723492736,
            idle: false,
            partition: 0,
        };

        timeline2.put(wmb4);
        timeline2.put(wmb5);
        timeline2.put(wmb6);

        processor2.timelines.insert(0, timeline2);

        let mut processors = HashMap::new();
        processors.insert(processor_name1.clone(), processor1);
        processors.insert(processor_name2.clone(), processor2);

        let processor_manager = ProcessorManager {
            processors: Arc::new(RwLock::new(processors)),
            handles: vec![],
        };

        let mut fetcher = SourceWatermarkFetcher::new(processor_manager);

        // Invoke fetch_head_watermark and verify the result (should be minimum of head watermarks)
        let watermark = fetcher.fetch_head_watermark(0);
        assert_eq!(watermark.timestamp_millis(), 323);
    }

    #[tokio::test]
    async fn test_source_watermark_fetcher_fetch_head_watermark_inactive_processor() {
        // Create a ProcessorManager with one active and one inactive processor
        let processor_name1 = Bytes::from("processor1");
        let mut processor1 = Processor::new(processor_name1.clone(), Status::Active, &[0]);
        let mut timeline1 = OffsetTimeline::new(10);

        // Populate the OffsetTimeline with sorted WMB entries
        let wmb1 = WMB {
            watermark: 100,
            offset: 19723492734,
            idle: false,
            partition: 0,
        };

        timeline1.put(wmb1);
        processor1.timelines.insert(0, timeline1);

        let processor_name2 = Bytes::from("processor2");
        let mut processor2 = Processor::new(processor_name2.clone(), Status::InActive, &[0]);
        let mut timeline2 = OffsetTimeline::new(10);

        // Populate the OffsetTimeline with sorted WMB entries (should be ignored)
        let wmb2 = WMB {
            watermark: 50,
            offset: 19723492734,
            idle: false,
            partition: 0,
        };

        timeline2.put(wmb2);
        processor2.timelines.insert(0, timeline2);

        let mut processors = HashMap::new();
        processors.insert(processor_name1.clone(), processor1);
        processors.insert(processor_name2.clone(), processor2);

        let processor_manager = ProcessorManager {
            processors: Arc::new(RwLock::new(processors)),
            handles: vec![],
        };

        let mut fetcher = SourceWatermarkFetcher::new(processor_manager);

        // Invoke fetch_head_watermark and verify the result (should only consider active processor)
        let watermark = fetcher.fetch_head_watermark(0);
        assert_eq!(watermark.timestamp_millis(), 100);
    }
}
