//! Type configuration trait for Numaflow components.
//!
//! This module provides a simple TypeConfig trait that allows the application logic to be generic
//! over the configuration of its components, rather than their concrete types. This approach
//! uses concrete builder functions for clarity and maintainability.

use crate::config::pipeline::PipelineConfig;
use crate::shared::create_components::create_rate_limiter;
use crate::{Result, error};
use numaflow_throttling::state::store::in_memory_store::InMemoryStore;
use numaflow_throttling::state::store::redis_store::{RedisMode, RedisStore};
use numaflow_throttling::{NoOpRateLimiter, RateLimit, RateLimiter, WithDistributedState};
use tokio_util::sync::CancellationToken;

pub trait NumaflowTypeConfig: Send + Sync + Clone + 'static {
    type RateLimiter: RateLimiter + Clone + Sync + 'static;
}

#[derive(Clone)]
pub struct WithRedisRateLimiter {
    pub throttling_config: RateLimit<WithDistributedState<RedisStore>>,
}
impl NumaflowTypeConfig for WithRedisRateLimiter {
    type RateLimiter = RateLimit<WithDistributedState<RedisStore>>;
}

#[derive(Clone)]
pub struct WithInMemoryRateLimiter {
    pub throttling_config: RateLimit<WithDistributedState<InMemoryStore>>,
}
impl NumaflowTypeConfig for WithInMemoryRateLimiter {
    type RateLimiter = RateLimit<WithDistributedState<InMemoryStore>>;
}

#[derive(Clone)]
pub struct WithoutRateLimiter {
    pub throttling_config: NoOpRateLimiter,
}
impl NumaflowTypeConfig for WithoutRateLimiter {
    type RateLimiter = NoOpRateLimiter;
}

/// Build a Redis-backed rate limiter
pub async fn build_redis_rate_limiter(
    config: &PipelineConfig,
    cln_token: CancellationToken,
) -> Result<RateLimit<WithDistributedState<RedisStore>>> {
    let rate_limit_config = config.rate_limit.as_ref().ok_or_else(|| {
        error::Error::Config("Rate limit config is required for Redis rate limiter".to_string())
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

/// Build a Redis-backed rate limiter configuration (returns struct)
pub async fn build_redis_rate_limiter_config(
    config: &PipelineConfig,
    cln_token: CancellationToken,
) -> Result<WithRedisRateLimiter> {
    let limiter = build_redis_rate_limiter(config, cln_token).await?;
    Ok(WithRedisRateLimiter {
        throttling_config: limiter,
    })
}

/// Build an in-memory rate limiter
pub async fn build_in_memory_rate_limiter(
    config: &PipelineConfig,
    cln_token: CancellationToken,
) -> Result<RateLimit<WithDistributedState<InMemoryStore>>> {
    let rate_limit_config = config.rate_limit.as_ref().ok_or_else(|| {
        error::Error::Config("Rate limit config is required for in-memory rate limiter".to_string())
    })?;

    // Create in-memory store
    let store = InMemoryStore::new();
    let limiter = create_rate_limiter(rate_limit_config, store, cln_token).await?;
    Ok(limiter)
}

/// Build an in-memory rate limiter configuration (returns struct)
pub async fn build_in_memory_rate_limiter_config(
    config: &PipelineConfig,
    cln_token: CancellationToken,
) -> Result<WithInMemoryRateLimiter> {
    let limiter = build_in_memory_rate_limiter(config, cln_token).await?;
    Ok(WithInMemoryRateLimiter {
        throttling_config: limiter,
    })
}

/// Build a no-op rate limiter configuration (returns struct)
pub async fn build_noop_rate_limiter_config(
    _config: &PipelineConfig,
    _cln_token: CancellationToken,
) -> Result<WithoutRateLimiter> {
    Ok(WithoutRateLimiter {
        throttling_config: NoOpRateLimiter,
    })
}

/// Helper function to determine which rate limiter to use based on the pipeline configuration.
pub fn should_use_redis_rate_limiter(config: &PipelineConfig) -> bool {
    config
        .rate_limit
        .as_ref()
        .and_then(|rlc| rlc.store.as_ref())
        .and_then(|s| s.redis_store.as_ref())
        .is_some()
}
