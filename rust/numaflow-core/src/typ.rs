//! Type configuration trait for Numaflow components.

use crate::config::components::ratelimit::RateLimitConfig;
use crate::error::Error;
use crate::shared::create_components::get_secret_from_volume;
use crate::{Result, error};
use numaflow_throttling::state::OptimisticValidityUpdateSecs;
use numaflow_throttling::state::store::in_memory_store::InMemoryStore;
use numaflow_throttling::state::store::redis_store::{RedisAuth, RedisMode, RedisStore};
use numaflow_throttling::{
    NoOpRateLimiter, RateLimit, RateLimiter, TokenCalcBounds, WithDistributedState,
};
use redis::TlsMode;
use std::time::Duration;
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
pub struct WithoutRateLimiter {}
impl NumaflowTypeConfig for WithoutRateLimiter {
    type RateLimiter = NoOpRateLimiter;
}

/// Build a Redis-backed rate limiter from rate limit config
pub async fn build_redis_rate_limiter(
    rate_limit_config: &crate::config::components::ratelimit::RateLimitConfig,
    cln_token: CancellationToken,
) -> Result<RateLimit<WithDistributedState<RedisStore>>> {
    let redis_store_config = rate_limit_config
        .store
        .as_ref()
        .and_then(|s| s.redis_store.as_ref())
        .ok_or_else(|| error::Error::Config("Redis store config is required".to_string()))?;

    // Create Redis mode based on configuration
    let redis_mode = create_redis_mode(redis_store_config)?;

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
) -> Result<RateLimit<WithDistributedState<InMemoryStore>>> {
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
) -> error::Result<RateLimit<WithDistributedState<S>>>
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

    RateLimit::<WithDistributedState<S>>::new(
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

/// Create RedisMode from the rate limiter Redis store configuration using builder pattern
fn create_redis_mode(
    redis_store_config: &numaflow_models::models::RateLimiterRedisStore,
) -> Result<RedisMode> {
    match redis_store_config.mode.as_str() {
        "single" => {
            let url = redis_store_config
                .url
                .as_ref()
                .ok_or_else(|| Error::Config("URL is required for Single mode".to_string()))?
                .clone();

            let mut builder = RedisMode::single_url(url);

            if let Some(db) = redis_store_config.db {
                builder = builder.db(db);
            }

            builder
                .build()
                .map_err(|e| Error::Config(format!("Failed to create Single Redis mode: {}", e)))
        }
        "sentinel" => {
            let sentinel_config = redis_store_config.sentinel.as_ref().ok_or_else(|| {
                Error::Config("Sentinel config is required for Sentinel mode".to_string())
            })?;

            let mut builder = RedisMode::sentinel(
                sentinel_config.master_name.clone(),
                sentinel_config.endpoints.clone(),
            );

            if let Some(role) = &sentinel_config.role {
                builder = builder.role(role.clone());
            }

            if let Some(sentinel_auth) = &sentinel_config.sentinel_auth {
                let auth = parse_redis_auth(sentinel_auth.as_ref())?;
                builder = builder.sentinel_auth(auth);
            }

            if let Some(redis_auth) = &sentinel_config.redis_auth {
                let auth = parse_redis_auth(redis_auth.as_ref())?;
                builder = builder.redis_auth(auth);
            }

            if sentinel_config.sentinel_tls.is_some() {
                builder = builder.sentinel_tls(TlsMode::Secure);
            }

            if sentinel_config.redis_tls.is_some() {
                builder = builder.redis_tls(TlsMode::Secure);
            }

            if let Some(db) = redis_store_config.db {
                builder = builder.db(db);
            }

            builder
                .build()
                .map_err(|e| Error::Config(format!("Failed to create Sentinel Redis mode: {}", e)))
        }
        _ => Err(Error::Config(format!(
            "Unsupported Redis mode: {}",
            redis_store_config.mode
        ))),
    }
}

/// Parse Redis authentication from the CRD model
fn parse_redis_auth(auth: &numaflow_models::models::RedisAuth) -> Result<RedisAuth> {
    let username = auth
        .username
        .as_ref()
        .map(|secret_ref| get_secret_from_volume(&secret_ref.name, &secret_ref.key))
        .transpose()
        .map_err(|e| Error::Config(format!("Failed to get username secret: {}", e)))?;

    let password = auth
        .password
        .as_ref()
        .map(|secret_ref| get_secret_from_volume(&secret_ref.name, &secret_ref.key))
        .transpose()
        .map_err(|e| Error::Config(format!("Failed to get password secret: {}", e)))?;

    Ok(RedisAuth { username, password })
}
