#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WatermarkConfig {
    Source(SourceWatermarkConfig),
    Edge(EdgeWatermarkConfig),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SourceWatermarkConfig {
    pub(crate) source_bucket_config: BucketConfig,
    pub(crate) to_vertex_bucket_config: Vec<BucketConfig>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BucketConfig {
    pub(crate) vertex: &'static str,
    pub(crate) partitions: u16,
    pub(crate) ot_bucket: &'static str,
    pub(crate) hb_bucket: &'static str,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EdgeWatermarkConfig {
    pub(crate) from_vertex_config: Vec<BucketConfig>,
    pub(crate) to_vertex_config: Vec<BucketConfig>,
}
