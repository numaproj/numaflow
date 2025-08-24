use tokio_util::sync::CancellationToken;

use crate::config::pipeline::PipelineConfig;
use crate::shared::create_components::create_rate_limiter;
use crate::{Result, error};
use numaflow_throttling::state::store::in_memory_store::InMemoryStore;
use numaflow_throttling::state::store::redis_store::{RedisMode, RedisStore};
use numaflow_throttling::{RateLimit, RateLimiter, WithDistributedState};

pub trait NumaflowTypeConfig: Send + Sync + 'static {
    type RateLimiter: RateLimiter + Clone + Send + Sync + 'static;

    // async fn build_rate_limiter(
    //     config: &PipelineConfig,
    //     cln_token: CancellationToken,
    // ) -> Result<Option<Self::RateLimiter>>;
}

pub(crate) async fn build_rate_limiter<C: NumaflowTypeConfig>(
    config: &PipelineConfig,
    cln_token: CancellationToken,
) -> Result<C::RateLimiter> {
    let rate_limit_config = config.rate_limit.as_ref().ok_or_else(|| {
        error::Error::Config("Rate limit config is required for RedisRateLimiter".to_string())
    })?;

    let redis_store_config = rate_limit_config
        .store
        .as_ref()
        .and_then(|s| s.redis_store.as_ref())
        .ok_or_else(|| error::Error::Config("Redis store config is required".to_string()))?;

    // Create Redis store with the configured URL
    let redis_url = redis_store_config.url.clone();
    let store = RedisStore::new("rate_limiter", RedisMode::SingleUrl { url: redis_url })
        .await
        .map_err(|e| error::Error::Config(format!("Failed to create Redis store: {}", e)))?;

    let limiter = create_rate_limiter(rate_limit_config, store, cln_token).await?;
    Ok(limiter)
}

pub struct WithRedisRateLimiter;

impl NumaflowTypeConfig for WithRedisRateLimiter {
    type RateLimiter = RateLimit<WithDistributedState<RedisStore>>;

    // async fn build_rate_limiter(
    //     config: &PipelineConfig,
    //     cln_token: CancellationToken,
    // ) -> Result<Option<Self::RateLimiter>> {
    //     todo!()
    // }
}

pub struct WithInMemoryRateLimiter;

impl NumaflowTypeConfig for WithInMemoryRateLimiter {
    type RateLimiter = RateLimit<WithDistributedState<InMemoryStore>>;

    // async fn build_rate_limiter(
    //     config: &PipelineConfig,
    //     cln_token: CancellationToken,
    // ) -> Result<Option<Self::RateLimiter>> {
    //     if let Some(rate_limit_config) = &config.rate_limit {
    //         // Create in-memory store
    //         let store = InMemoryStore::new();
    //         let limiter = create_rate_limiter(rate_limit_config, store, cln_token).await?;
    //         Ok(Some(limiter))
    //     } else {
    //         // No rate limiting configured
    //         Ok(None)
    //     }
    // }
}

pub fn should_use_redis_rate_limiter(config: &PipelineConfig) -> bool {
    config
        .rate_limit
        .as_ref()
        .and_then(|rlc| rlc.store.as_ref())
        .and_then(|s| s.redis_store.as_ref())
        .is_some()
}
