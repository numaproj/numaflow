use crate::config::{get_pipeline_name, get_vertex_name, get_vertex_replica, is_mono_vertex};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RateLimitConfig {
    pub(crate) key_prefix: &'static str,
    pub(crate) processor_id: &'static str,
    pub(crate) max: usize,
    pub(crate) min: usize,
    pub(crate) duration: std::time::Duration,
    pub(crate) store: Option<Box<numaflow_models::models::Store>>,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            key_prefix: "default",
            processor_id: "default-processor-0",
            max: 1,
            min: 1,
            duration: std::time::Duration::from_secs(1),
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
            min: value.burst.map(|x| x as usize).unwrap_or_default(),
            duration: value
                .duration
                .map(std::time::Duration::from)
                .unwrap_or_default(),
            store: value.store,
        }
    }
}
