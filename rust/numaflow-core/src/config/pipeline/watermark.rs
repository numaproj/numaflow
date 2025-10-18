use crate::config::get_vertex_replica;
use crate::config::pipeline::{FromVertexConfig, ToVertexConfig, VertexConfig};
use numaflow_models::models::Watermark;
use std::time::Duration;

/// Watermark config for different types of Vertex.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WatermarkConfig {
    Source(SourceWatermarkConfig),
    Edge(EdgeWatermarkConfig),
}

impl WatermarkConfig {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        watermark_spec: Option<Box<Watermark>>,
        namespace: &str,
        pipeline_name: &str,
        vertex_name: &str,
        vertex: &VertexConfig,
        from_vertex_config: &[FromVertexConfig],
        to_vertex_config: &[ToVertexConfig],
        delay_in_millis: u64,
    ) -> Option<WatermarkConfig> {
        let max_delay = watermark_spec
            .as_ref()
            .and_then(|w| w.max_delay.map(|x| Duration::from(x).as_millis() as u64))
            .unwrap_or(0);

        let idle_config = watermark_spec
            .as_ref()
            .and_then(|w| w.idle_source.as_ref())
            .map(|idle| IdleConfig {
                increment_by: idle.increment_by.map(Duration::from).unwrap_or_default(),
                step_interval: idle.step_interval.map(Duration::from).unwrap_or_default(),
                threshold: idle.threshold.map(Duration::from).unwrap_or_default(),
                init_source_delay: idle.init_source_delay.map(Duration::from),
            });

        // Helper function to create bucket config for to_vertex
        let create_to_vertex_bucket_config = |to: &ToVertexConfig| BucketConfig {
            vertex: to.name,
            partitions: (0..to.partitions).collect(),
            ot_bucket: Box::leak(
                format!(
                    "{}-{}-{}-{}_OT",
                    namespace, pipeline_name, vertex_name, &to.name
                )
                .into_boxed_str(),
            ),
            hb_bucket: Box::leak(
                format!(
                    "{}-{}-{}-{}_PROCESSORS",
                    namespace, pipeline_name, vertex_name, &to.name
                )
                .into_boxed_str(),
            ),
            delay: Some(Duration::from_millis(delay_in_millis)),
        };

        // Helper function to create bucket config for from_vertex
        let create_from_vertex_bucket_config =
            |from: &FromVertexConfig, partitions: Vec<u16>| BucketConfig {
                vertex: from.name,
                partitions,
                ot_bucket: Box::leak(
                    format!(
                        "{}-{}-{}-{}_OT",
                        namespace, pipeline_name, &from.name, vertex_name
                    )
                    .into_boxed_str(),
                ),
                hb_bucket: Box::leak(
                    format!(
                        "{}-{}-{}-{}_PROCESSORS",
                        namespace, pipeline_name, &from.name, vertex_name
                    )
                    .into_boxed_str(),
                ),
                delay: Some(Duration::from_millis(delay_in_millis)),
            };

        match vertex {
            VertexConfig::Source(_) => Some(WatermarkConfig::Source(SourceWatermarkConfig {
                max_delay: Duration::from_millis(max_delay),
                source_bucket_config: BucketConfig {
                    vertex: Box::leak(vertex_name.to_string().into_boxed_str()),
                    partitions: vec![0], // source will have only one partition
                    ot_bucket: Box::leak(
                        format!("{namespace}-{pipeline_name}-{vertex_name}_SOURCE_OT")
                            .into_boxed_str(),
                    ),
                    hb_bucket: Box::leak(
                        format!("{namespace}-{pipeline_name}-{vertex_name}_SOURCE_PROCESSORS")
                            .into_boxed_str(),
                    ),
                    delay: Some(Duration::from_millis(delay_in_millis)),
                },
                to_vertex_bucket_config: to_vertex_config
                    .iter()
                    .map(create_to_vertex_bucket_config)
                    .collect(),
                idle_config,
            })),
            VertexConfig::Sink(_) | VertexConfig::Map(_) => {
                Some(WatermarkConfig::Edge(EdgeWatermarkConfig {
                    from_vertex_config: from_vertex_config
                        .iter()
                        .map(|from| {
                            create_from_vertex_bucket_config(from, (0..from.partitions).collect())
                        })
                        .collect(),
                    to_vertex_config: to_vertex_config
                        .iter()
                        .map(create_to_vertex_bucket_config)
                        .collect(),
                }))
            }
            VertexConfig::Reduce(_) => {
                Some(WatermarkConfig::Edge(EdgeWatermarkConfig {
                    from_vertex_config: from_vertex_config
                        .iter()
                        .map(|from| {
                            // reduce will have only one partition which is the same as the vertex replica
                            create_from_vertex_bucket_config(from, vec![*get_vertex_replica()])
                        }) // reduce will have only one partition
                        .collect(),
                    to_vertex_config: to_vertex_config
                        .iter()
                        .map(create_to_vertex_bucket_config)
                        .collect(),
                }))
            }
        }
    }
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
    pub(crate) init_source_delay: Option<Duration>,
}

impl Default for IdleConfig {
    fn default() -> Self {
        IdleConfig {
            increment_by: Duration::from_millis(0),
            step_interval: Duration::from_millis(0),
            threshold: Duration::from_millis(0),
            init_source_delay: None,
        }
    }
}

/// Watermark movements are captured via a Key/Value bucket.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BucketConfig {
    pub(crate) vertex: &'static str,
    pub(crate) partitions: Vec<u16>,
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
