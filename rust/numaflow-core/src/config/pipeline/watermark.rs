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
    pub(crate) source_bucket_config: BucketConfig,
    pub(crate) to_vertex_bucket_config: Vec<BucketConfig>,
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
}

/// Edge's Watermark is purely based on the previous vertex and the next vertex. It only has to
/// implement fetch/publish.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EdgeWatermarkConfig {
    pub(crate) from_vertex_config: Vec<BucketConfig>,
    pub(crate) to_vertex_config: Vec<BucketConfig>,
}
