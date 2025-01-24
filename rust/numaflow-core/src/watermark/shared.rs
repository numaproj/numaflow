use crate::config::pipeline::watermark::BucketConfig;
use crate::error::Error;
use crate::watermark::processor::manager::ProcessorManager;

/// Creates a ProcessorManager for the given bucket config, also creates the last
pub(super) async fn create_processor_manager(
    js_context: async_nats::jetstream::Context,
    bucket_config: &BucketConfig,
) -> crate::error::Result<ProcessorManager> {
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

    Ok(processor_manager)
}
