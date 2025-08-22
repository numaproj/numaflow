#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RateLimitConfig {
    pub(crate) max: usize,
    pub(crate) min: usize,
    pub(crate) duration: std::time::Duration,
    pub(crate) store: Option<Box<numaflow_models::models::Store>>,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
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
