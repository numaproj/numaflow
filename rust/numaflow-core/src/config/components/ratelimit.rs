use crate::config::{get_pipeline_name, get_vertex_name, get_vertex_replica, is_mono_vertex};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RateLimitConfig {
    /// Key prefix for the rate limiter. This is fixed for a given Vertex/MonoVertex.
    pub(crate) key_prefix: &'static str,
    /// Processor ID for the given replica for the Vertex in the Pipeline or MonoVertex.
    pub(crate) processor_id: &'static str,
    /// Maximum allowed TPS (per pod if distributed store is not configured).
    pub(crate) max: usize,
    /// Minimum TPS during ramp up (per pod if distributed store is not configured).
    pub(crate) min: usize,
    /// Ramp up duration in seconds with minimum of 1 per second.
    pub(crate) ramp_up_duration: std::time::Duration,
    pub(crate) store: Option<Box<numaflow_models::models::RateLimiterStore>>,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            key_prefix: "default",
            processor_id: "default-processor-0",
            max: 1,
            min: 1,
            ramp_up_duration: std::time::Duration::from_secs(1),
            store: None,
        }
    }
}

impl From<Box<numaflow_models::models::RateLimit>> for RateLimitConfig {
    fn from(value: Box<numaflow_models::models::RateLimit>) -> Self {
        Self {
            key_prefix: if is_mono_vertex() {
                Box::leak(format!("{}-mv", get_vertex_name()).into_boxed_str())
            } else {
                Box::leak(
                    format!("{}-{}-pl", get_pipeline_name(), get_vertex_name()).into_boxed_str(),
                )
            },
            processor_id: Box::leak(
                format!("{}-{}", get_vertex_name(), get_vertex_replica()).into_boxed_str(),
            ),
            max: value.max.map(|x| x as usize).unwrap_or_default(),
            min: value.min.map(|x| x as usize).unwrap_or_default(),
            ramp_up_duration: value
                .ramp_up_duration
                .map(std::time::Duration::from)
                .unwrap_or_default(),
            store: value.store,
        }
    }
}
