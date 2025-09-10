//! Type configuration trait for Numaflow components.

use crate::config::components::ratelimit::RateLimitConfig;
use crate::error::Error;
use crate::{Result, error};
use numaflow_throttling::state::OptimisticValidityUpdateSecs;
use numaflow_throttling::state::store::in_memory_store::InMemoryStore;
use numaflow_throttling::state::store::redis_store::{RedisMode, RedisStore};
use numaflow_throttling::{NoOpRateLimiter, RateLimit, RateLimiter, TokenCalcBounds, WithState};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub trait NumaflowTypeConfig: Send + Sync + Clone + 'static {
    type RateLimiter: RateLimiter + Clone + Sync + 'static;
}

#[derive(Clone)]
pub struct WithRedisRateLimiter {
    pub throttling_config: RateLimit<WithState<RedisStore>>,
}
impl NumaflowTypeConfig for WithRedisRateLimiter {
    type RateLimiter = RateLimit<WithState<RedisStore>>;
}

#[derive(Clone)]
pub struct WithInMemoryRateLimiter {
    pub throttling_config: RateLimit<WithState<InMemoryStore>>,
}
impl NumaflowTypeConfig for WithInMemoryRateLimiter {
    type RateLimiter = RateLimit<WithState<InMemoryStore>>;
}

#[derive(Clone)]
pub struct WithoutRateLimiter {}
impl NumaflowTypeConfig for WithoutRateLimiter {
    type RateLimiter = NoOpRateLimiter;
}

/// Build a Redis-backed rate limiter from rate limit config
pub async fn build_redis_rate_limiter(
    rate_limit_config: &RateLimitConfig,
    cln_token: CancellationToken,
) -> Result<RateLimit<WithState<RedisStore>>> {
    let redis_store_config = rate_limit_config
        .store
        .as_ref()
        .and_then(|s| s.redis_store.as_ref())
        .ok_or_else(|| Error::Config("Redis store config is required".to_string()))?;

    // Create Redis mode based on configuration
    let redis_mode = RedisMode::new(redis_store_config)
        .map_err(|e| Error::Config(format!("Failed to create Redis mode: {}", e)))?;

    let store = RedisStore::new(rate_limit_config.key_prefix, redis_mode)
        .await
        .map_err(|e| error::Error::Config(format!("Failed to create Redis store: {}", e)))?;

    let limiter = create_rate_limiter(rate_limit_config, store, cln_token).await?;
    Ok(limiter)
}

/// Build a Redis-backed rate limiter configuration.
pub async fn build_redis_rate_limiter_config(
    rate_limit_config: &RateLimitConfig,
    cln_token: CancellationToken,
) -> Result<WithRedisRateLimiter> {
    let limiter = build_redis_rate_limiter(rate_limit_config, cln_token).await?;
    Ok(WithRedisRateLimiter {
        throttling_config: limiter,
    })
}

/// Build an in-memory rate limiter from rate limit config
pub async fn build_in_memory_rate_limiter(
    rate_limit_config: &RateLimitConfig,
    cln_token: CancellationToken,
) -> Result<RateLimit<WithState<InMemoryStore>>> {
    // Create in-memory store
    let store = InMemoryStore::new();
    let limiter = create_rate_limiter(rate_limit_config, store, cln_token).await?;
    Ok(limiter)
}

/// Build an in-memory rate limiter configuration.
pub async fn build_in_memory_rate_limiter_config(
    rate_limit_config: &RateLimitConfig,
    cln_token: CancellationToken,
) -> Result<WithInMemoryRateLimiter> {
    let limiter = build_in_memory_rate_limiter(rate_limit_config, cln_token).await?;
    Ok(WithInMemoryRateLimiter {
        throttling_config: limiter,
    })
}

/// Helper function to determine which rate limiter to use based on the rate limit configuration.
pub fn should_use_redis_rate_limiter(rate_limit_config: &RateLimitConfig) -> bool {
    rate_limit_config
        .store
        .as_ref()
        .and_then(|s| s.redis_store.as_ref())
        .is_some()
}

/// Creates rate limiter for a given store and rate limit configuration.
pub async fn create_rate_limiter<S>(
    rate_limit_config: &RateLimitConfig,
    store: S,
    cancel_token: CancellationToken,
) -> Result<RateLimit<WithState<S>>>
where
    S: numaflow_throttling::state::Store + Sync + 'static,
{
    let bounds = TokenCalcBounds::new(
        rate_limit_config.max,
        rate_limit_config.min,
        rate_limit_config.ramp_up_duration,
    );

    let refresh_interval = Duration::from_millis(100);
    let runway_update = OptimisticValidityUpdateSecs::default();

    RateLimit::<WithState<S>>::new(
        bounds,
        store,
        rate_limit_config.processor_id,
        cancel_token,
        refresh_interval,
        runway_update,
    )
    .await
    .map_err(|e| Error::Config(format!("Failed to create rate limiter: {}", e)))
}
