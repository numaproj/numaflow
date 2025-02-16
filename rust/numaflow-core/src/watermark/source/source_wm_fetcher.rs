//! Fetches watermark for the source, source fetcher will only have one processor manager which tracks
//! all the partition based processors. The fetcher will look at all the active processors and their
//! timelines to determine the watermark. We don't care about offsets here since the watermark starts
//! at source, we only consider the head watermark and consider the minimum watermark of all the active
//! processors.
use crate::watermark::processor::manager::ProcessorManager;
use crate::watermark::wmb::Watermark;

/// SourceWatermarkFetcher is the watermark fetcher for the source.
pub struct SourceWatermarkFetcher {
    processor_manager: ProcessorManager,
}

impl SourceWatermarkFetcher {
    /// Creates a new [SourceWatermarkFetcher].
    pub(crate) fn new(processor_manager: ProcessorManager) -> Self {
        SourceWatermarkFetcher { processor_manager }
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

            // only consider the head watermark of the processor
            let head_wm = processor.timelines.first().unwrap().get_head_watermark();

            if head_wm < min_wm {
                min_wm = head_wm;
            }
        }

        if min_wm == i64::MAX {
            min_wm = -1;
        }

        Watermark::from_timestamp_millis(min_wm).expect("Failed to parse watermark")
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
        let mut processor = Processor::new(processor_name.clone(), Status::Active, 1);
        let timeline = OffsetTimeline::new(10);

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

        processor.timelines[0] = timeline;

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
        let mut processor1 = Processor::new(processor_name1.clone(), Status::Active, 1);
        let timeline1 = OffsetTimeline::new(10);

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

        processor1.timelines[0] = timeline1;

        let processor_name2 = Bytes::from("processor2");
        let mut processor2 = Processor::new(processor_name2.clone(), Status::Active, 1);
        let timeline2 = OffsetTimeline::new(10);

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

        processor2.timelines[0] = timeline2;

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
}
