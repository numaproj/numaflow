use crate::config::pipeline::watermark::BucketConfig;
use crate::error::Result;
use crate::watermark::processor::manager::ProcessorManager;
use crate::watermark::shared::create_processor_manager;
use crate::watermark::wmb::Watermark;

/// SourceFetcher is the watermark fetcher for the source.
pub struct SourceFetcher {
    processor_manager: ProcessorManager,
}

impl SourceFetcher {
    /// Creates a new SourceFetcher.
    pub(crate) async fn new(
        js_context: async_nats::jetstream::Context,
        bucket_config: &BucketConfig,
    ) -> Result<Self> {
        let processor_manager = create_processor_manager(js_context, bucket_config).await?;
        Ok(SourceFetcher { processor_manager })
    }

    /// Fetches the watermark for the source, which is the minimum watermark of all the active
    /// processors.
    pub(crate) async fn fetch_source_watermark(&self) -> Result<Watermark> {
        let mut min_wm = i64::MAX;

        for (_, processor) in self.processor_manager.get_all_processors().await {
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
            return Ok(Watermark::from_timestamp_millis(-1).unwrap());
        }

        Ok(Watermark::from_timestamp_millis(min_wm).unwrap())
    }
}
