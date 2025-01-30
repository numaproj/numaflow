use crate::error::Result;
use crate::watermark::processor::manager::ProcessorManager;
use crate::watermark::wmb::Watermark;

/// SourceWatermarkFetcher is the watermark fetcher for the source.
pub struct SourceWatermarkFetcher {
    processor_manager: ProcessorManager,
}

impl SourceWatermarkFetcher {
    /// Creates a new [SourceWatermarkFetcher].
    pub(crate) async fn new(processor_manager: ProcessorManager) -> Result<Self> {
        Ok(SourceWatermarkFetcher { processor_manager })
    }

    /// Fetches the watermark for the source, which is the minimum watermark of all the active
    /// processors.
    pub(crate) async fn fetch_source_watermark(&mut self) -> Result<Watermark> {
        let mut min_wm = i64::MAX;

        for (_, processor) in self.processor_manager.processors.read().await.iter() {
            // We only consider active processors.
            if !processor.is_active() {
                continue;
            }

            // only consider the head watermark of the processor
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
            min_wm = -1;
        }

        Ok(Watermark::from_timestamp_millis(min_wm).expect("Invalid watermark"))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use tokio::sync::RwLock;

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

        timeline.put(wmb1).await;
        timeline.put(wmb2).await;
        timeline.put(wmb3).await;

        processor.timelines[0] = timeline;

        let mut processors = HashMap::new();
        processors.insert(processor_name.clone(), processor);

        let processor_manager = ProcessorManager {
            processors: Arc::new(RwLock::new(processors)),
            handles: vec![],
        };

        let mut fetcher = SourceWatermarkFetcher::new(processor_manager)
            .await
            .unwrap();

        // Invoke fetch_watermark and verify the result
        let watermark = fetcher.fetch_source_watermark().await.unwrap();
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

        timeline1.put(wmb1).await;
        timeline1.put(wmb2).await;
        timeline1.put(wmb3).await;

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

        timeline2.put(wmb4).await;
        timeline2.put(wmb5).await;
        timeline2.put(wmb6).await;

        processor2.timelines[0] = timeline2;

        let mut processors = HashMap::new();
        processors.insert(processor_name1.clone(), processor1);
        processors.insert(processor_name2.clone(), processor2);

        let processor_manager = ProcessorManager {
            processors: Arc::new(RwLock::new(processors)),
            handles: vec![],
        };

        let mut fetcher = SourceWatermarkFetcher::new(processor_manager)
            .await
            .unwrap();

        // Invoke fetch_watermark and verify the result
        let watermark = fetcher.fetch_source_watermark().await.unwrap();
        assert_eq!(watermark.timestamp_millis(), 323);
    }
}
