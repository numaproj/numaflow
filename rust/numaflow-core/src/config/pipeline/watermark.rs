use std::time::Duration;

/// Watermark config for different types of Vertex.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WatermarkConfig {
    Source(SourceWatermarkConfig),
    Edge(EdgeWatermarkConfig),
}

/// Source's Watermark configuration is different because it does publish/fetch/publish because
/// Watermark starts at Source.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SourceWatermarkConfig {
    pub(crate) max_delay: Duration,
    pub(crate) source_bucket_config: BucketConfig,
    pub(crate) to_vertex_bucket_config: Vec<BucketConfig>,
    pub(crate) idle_config: Option<IdleConfig>,
}

/// Idle configuration for detecting idleness when there is no data
/// from source and publish the Watermark.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IdleConfig {
    pub(crate) increment_by: Duration,
    pub(crate) step_interval: Duration,
    pub(crate) threshold: Duration,
}

impl Default for IdleConfig {
    fn default() -> Self {
        IdleConfig {
            increment_by: Duration::from_millis(0),
            step_interval: Duration::from_millis(0),
            threshold: Duration::from_millis(0),
        }
    }
}

/// Watermark movements are captured via a Key/Value bucket.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BucketConfig {
    pub(crate) vertex: &'static str,
    pub(crate) partitions: u16,
    /// Offset Timeline (OT) bucket.
    pub(crate) ot_bucket: &'static str,
    /// Heartbeat bucket for processor heartbeats.
    pub(crate) hb_bucket: &'static str,
    /// Optional delay to publish watermark, to reduce the number of writes to the kv bucket.
    pub(crate) delay: Option<Duration>,
}

/// Edge's Watermark is purely based on the previous vertex and the next vertex. It only has to
/// implement fetch/publish.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EdgeWatermarkConfig {
    pub(crate) from_vertex_config: Vec<BucketConfig>,
    pub(crate) to_vertex_config: Vec<BucketConfig>,
}
