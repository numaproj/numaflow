use crate::config::{get_pipeline_name, get_vertex_name, get_vertex_replica, is_mono_vertex};

#[derive(Clone, PartialEq)]
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
    /// Optional store for distributed rate limiting.
    pub(crate) store: Option<Box<numaflow_models::models::RateLimiterStore>>,
    /// Optional modes for rate limiting.
    pub(crate) modes: Option<Box<numaflow_models::models::RateLimiterModes>>,
    /// Resume ramp up for a processor after pause.
    pub(crate) resume: bool,
    /// TTL for the rate limiter state in seconds.
    pub(crate) ttl: usize,
}

impl std::fmt::Debug for RateLimitConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimitConfig")
            .field("key_prefix", &self.key_prefix)
            .field("processor_id", &self.processor_id)
            .field("max", &self.max)
            .field("min", &self.min)
            .field("ramp_up_duration", &self.ramp_up_duration)
            .field("store", &self.store.as_deref().map(RedactedStoreDebug))
            .field("modes", &self.modes)
            .field("resume", &self.resume)
            .field("ttl", &self.ttl)
            .finish()
    }
}

/// Debug wrapper stripping userinfo from the Redis URL, which may embed
/// credentials (`redis://user:password@host`). The generated model types
/// derive Debug and cannot be changed.
struct RedactedStoreDebug<'a>(&'a numaflow_models::models::RateLimiterStore);

impl std::fmt::Debug for RedactedStoreDebug<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiterStore")
            .field("in_memory_store", &self.0.in_memory_store)
            .field(
                "redis_store",
                &self.0.redis_store.as_deref().map(RedactedRedisStoreDebug),
            )
            .finish()
    }
}

struct RedactedRedisStoreDebug<'a>(&'a numaflow_models::models::RateLimiterRedisStore);

impl std::fmt::Debug for RedactedRedisStoreDebug<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiterRedisStore")
            .field("db", &self.0.db)
            .field("mode", &self.0.mode)
            .field("sentinel", &self.0.sentinel)
            .field(
                "url",
                &self.0.url.as_deref().map(redact_url_userinfo),
            )
            .finish()
    }
}

/// Strips any `user:password@` userinfo from the authority of a URL, so
/// credentials embedded in connection URLs never reach logs.
fn redact_url_userinfo(url: &str) -> String {
    if let Some(scheme_end) = url.find("://") {
        let rest = &url[scheme_end + 3..];
        if let Some(at) = rest.find('@') {
            return format!("{}://***@{}", &url[..scheme_end], &rest[at + 1..]);
        }
    }
    url.to_string()
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
            modes: None,
            resume: false,
            ttl: 180,
        }
    }
}

impl RateLimitConfig {
    pub(crate) fn new(
        batch_size: usize,
        is_source: bool,
        rate_limit: numaflow_models::models::RateLimit,
    ) -> Self {
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
            max: if is_source {
                // for source, we throttle based on number of read invocations so we divide by batch
                // size
                rate_limit
                    .max
                    .map(|x| x as usize)
                    .unwrap_or_default()
                    .div_ceil(batch_size)
            } else {
                rate_limit.max.map(|x| x as usize).unwrap_or_default()
            },
            min: if is_source {
                // for source, we throttle based on number of read invocations so we divide by batch
                // size
                rate_limit
                    .min
                    .map(|x| x as usize)
                    .unwrap_or_default()
                    .div_ceil(batch_size)
            } else {
                rate_limit.min.map(|x| x as usize).unwrap_or_default()
            },
            ramp_up_duration: rate_limit
                .ramp_up_duration
                .map(std::time::Duration::from)
                .unwrap_or_default(),
            store: rate_limit.store,
            modes: rate_limit.modes,
            resume: rate_limit.resumed_ramp_up.unwrap_or_default(),
            ttl: rate_limit
                .ttl
                .map(std::time::Duration::from)
                .unwrap_or(std::time::Duration::from_secs(180))
                .as_secs() as usize,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use numaflow_models::models::{RateLimiterRedisStore, RateLimiterStore};

    #[test]
    fn test_rate_limit_config_debug_redacts_redis_url_credentials() {
        let mut redis_store = RateLimiterRedisStore::new("single".to_string());
        redis_store.url = Some("redis://redis-user:secret-redis-password@redis-host:6379/0".into());
        let mut store = RateLimiterStore::new();
        store.redis_store = Some(Box::new(redis_store));

        let config = RateLimitConfig {
            store: Some(Box::new(store)),
            ..Default::default()
        };
        let debug_output = format!("{:?}", config);
        assert!(!debug_output.contains("secret-redis-password"));
        assert!(!debug_output.contains("redis-user"));
        assert!(debug_output.contains("redis-host:6379"));
        assert!(debug_output.contains("***"));
    }

    #[test]
    fn test_rate_limit_config_debug_keeps_clean_redis_url() {
        let mut redis_store = RateLimiterRedisStore::new("single".to_string());
        redis_store.url = Some("redis://redis-host:6379/0".into());
        let mut store = RateLimiterStore::new();
        store.redis_store = Some(Box::new(redis_store));

        let config = RateLimitConfig {
            store: Some(Box::new(store)),
            ..Default::default()
        };
        let debug_output = format!("{:?}", config);
        assert!(debug_output.contains("redis://redis-host:6379/0"));
    }

    #[test]
    fn test_redact_url_userinfo() {
        assert_eq!(
            redact_url_userinfo("redis://user:pass@host:6379/0"),
            "redis://***@host:6379/0"
        );
        assert_eq!(
            redact_url_userinfo("rediss://user@host:6379"),
            "rediss://***@host:6379"
        );
        assert_eq!(redact_url_userinfo("redis://host:6379"), "redis://host:6379");
    }
}
