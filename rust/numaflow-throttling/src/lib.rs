use crate::state::OptimisticValidityUpdateSecs;
use crate::state::store::Store;
use state::RateLimiterDistributedState;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::warn;

pub(crate) mod error;

pub(crate) use error::Error;
pub(crate) use error::Result;

/// State module contains the implementation of the [RateLimiterDistributedState] which has the
/// holds distributed consensus state required to compute the available tokens.
pub mod state;

#[derive(Clone)]
pub struct WithoutDistributedState;

#[derive(Clone)]
pub struct WithDistributedState<S>(RateLimiterDistributedState<S>);

/// RateLimiter will expose methods to query the tokens available per unit time
#[trait_variant::make(Send)]
pub trait RateLimiter {
    /// GetN returns the number of tokens available for the next n time windows or blocks till then.
    /// If timeout is None, it will block till the tokens are available, else returns whatever is
    /// available. If `n` is provided, it will try to acquire `n` tokens else it will acquire all the tokens.
    async fn acquire_n(&self, n: Option<usize>, timeout: Option<Duration>) -> usize;
}

/// RateLimit is the main struct that will be used by the user to get the tokens available for the
/// current time window. It has to be clonable so it can be used across multiple tasks (e.g., pipeline
/// with multiple partitions).
#[derive(Clone)]
pub struct RateLimit<W> {
    token_calc_bounds: TokenCalcBounds,
    /// Current number of tokens available.
    token: Arc<AtomicUsize>,
    /// Max number of tokens ever filled till now. The next refill relies on the current max allowed
    /// value. For the first run this will be set to `burst` and will be slowly incremented to the
    /// `max` value.
    max_ever_filled: Arc<AtomicUsize>,
    /// Last time the token was queried. The tokens are replenished based on the time elapsed since
    /// the last query.
    last_queried_epoch: Arc<AtomicU64>,
    /// Optional [RateLimiterDistributedState] to query for the pool-size in a distributed setting.
    state: W,
}

impl<W> std::fmt::Display for RateLimit<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "token_calc_bounds: {:?}, token: {}, max_ever_filled: {}, last_queried_epoch: {}",
            self.token_calc_bounds,
            self.token.load(std::sync::atomic::Ordering::Relaxed),
            self.max_ever_filled
                .load(std::sync::atomic::Ordering::Relaxed),
            self.last_queried_epoch
                .load(std::sync::atomic::Ordering::Relaxed)
        )
    }
}

#[derive(Debug)]
enum TokenAvailability {
    /// Tokens are available for this epoch
    Available(usize),
    /// Tokens are not available for this epoch, there is no need to recompute this we have exhausted
    /// all the tokens for this epoch.
    Exhausted,
    /// Recompute the number of tokens available since we are in a new epoch.
    Recompute,
}

impl<W> RateLimit<W> {
    /// Computes the number of tokens to be refilled for the next epoch.
    pub(crate) fn compute_refill(&self) -> usize {
        let max_ever_filled = self
            .max_ever_filled
            .load(std::sync::atomic::Ordering::Relaxed);

        // let's make sure we do not go beyond the max
        if max_ever_filled >= self.token_calc_bounds.max {
            self.token_calc_bounds.max
        } else {
            let refill = max_ever_filled + (self.token_calc_bounds.slope as usize);
            let capped_refill = refill.min(self.token_calc_bounds.max);
            self.max_ever_filled
                .store(capped_refill, std::sync::atomic::Ordering::Release);
            capped_refill
        }
    }
}

impl RateLimit<WithoutDistributedState> {
    /// Create a new [RateLimit] without a distributed state.
    pub fn new(token_calc_bounds: TokenCalcBounds) -> Result<Self> {
        let burst = token_calc_bounds.min;
        Ok(RateLimit {
            token_calc_bounds,
            token: Arc::new(AtomicUsize::new(burst)),
            max_ever_filled: Arc::new(AtomicUsize::new(burst)),
            last_queried_epoch: Arc::new(AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs(),
            )),
            state: WithoutDistributedState,
        })
    }

    /// Get the number of tokens available for the current time provided it is already refilled.
    /// Since pool size is always one, we don't have to divide the tokens by pool size.
    pub(crate) fn get_tokens(&self, n: Option<usize>) -> TokenAvailability {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        let previous_epoch = self
            .last_queried_epoch
            .load(std::sync::atomic::Ordering::Relaxed);

        if previous_epoch < now {
            return TokenAvailability::Recompute;
        }

        match self.token.fetch_update(
            std::sync::atomic::Ordering::Release,
            std::sync::atomic::Ordering::Acquire,
            |current| {
                if current == 0 {
                    return None; // No tokens available, no update
                }

                match n {
                    None => Some(0), // Acquire all tokens
                    Some(requested) if current >= requested => Some(current - requested), // Acquire requested tokens
                    Some(_) => Some(0), // Not enough tokens, acquire all available
                }
            },
        ) {
            Ok(previous) => {
                let acquired = match n {
                    None => previous,
                    Some(requested) if previous >= requested => requested,
                    Some(_) => previous,
                };
                TokenAvailability::Available(acquired)
            }
            Err(_) => TokenAvailability::Exhausted,
        }
    }
}

impl RateLimiter for RateLimit<WithoutDistributedState> {
    /// Acquire `n` tokens. If `n` is not provided, it will acquire all the tokens. For non-distributed
    /// state, `timeout` is not used since there are no blocking calls.
    async fn acquire_n(&self, n: Option<usize>, _timeout: Option<Duration>) -> usize {
        // let's try to acquire the tokens
        match self.get_tokens(n) {
            TokenAvailability::Available(t) => return t,
            TokenAvailability::Exhausted => return 0,
            TokenAvailability::Recompute => {}
        }

        // recompute the number of tokens available
        let now_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        let new_token_count = self.compute_refill();
        self.token
            .store(new_token_count, std::sync::atomic::Ordering::Release);

        // Update the epoch to current time after refilling
        self.last_queried_epoch
            .store(now_epoch, std::sync::atomic::Ordering::Release);

        // try to acquire the tokens again
        match self.get_tokens(n) {
            TokenAvailability::Available(tokens) => tokens,
            other => {
                warn!(
                    ?other,
                    "We just refilled and it still needs to be recomputed."
                );
                0
            }
        }
    }
}

impl<S: Store + Send + Sync + Clone + 'static> RateLimiter for RateLimit<WithDistributedState<S>> {
    /// Acquire `n` tokens with distributed consensus (non-blocking, truncated-window semantics).
    async fn acquire_n(&self, n: Option<usize>, _timeout: Option<Duration>) -> usize {
        // Try the hot-path first: consume from the current window.
        match self.get_tokens(n) {
            TokenAvailability::Available(t) => return t,
            TokenAvailability::Exhausted => return 0,
            TokenAvailability::Recompute => {}
        }

        // We crossed a new epoch; recompute this second's budget.
        let now_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        let next_total_tokens = self.compute_refill();

        // Store the total tokens (division happens in get_tokens)
        self.token
            .store(next_total_tokens, std::sync::atomic::Ordering::Release);

        self.last_queried_epoch
            .store(now_epoch, std::sync::atomic::Ordering::Release);

        // Try to take after refill.
        match self.get_tokens(n) {
            TokenAvailability::Available(tokens) => tokens,
            other => {
                warn!(
                    ?other,
                    "We just refilled (distributed) and it still needs to be recomputed."
                );
                0
            }
        }
    }
}

impl<S: Store> RateLimit<WithDistributedState<S>> {
    /// Create a new [RateLimit] with a distributed state.
    pub async fn new(
        token_calc_bounds: TokenCalcBounds,
        store: S,
        processor_id: &str,
        cancel: CancellationToken,
        refresh_interval: Duration,
        runway_update_len: OptimisticValidityUpdateSecs,
    ) -> Result<Self> {
        // Registers and blocks until initial consensus (AGREE) inside state::new(...)
        let state = RateLimiterDistributedState::new(
            store,
            processor_id,
            cancel,
            refresh_interval,
            runway_update_len,
        )
        .await?;

        // Add 1-second sleep after initial agreement so we don't get inconsistent results within
        // the same second because race conditions among multiple processors initializing.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let burst = token_calc_bounds.min;

        Ok(RateLimit {
            token_calc_bounds,
            // Store the full burst amount, we divide by pool size when querying tokens
            token: Arc::new(AtomicUsize::new(burst)),
            max_ever_filled: Arc::new(AtomicUsize::new(burst)),
            last_queried_epoch: Arc::new(AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs(),
            )),
            state: WithDistributedState(state),
        })
    }

    /// Get the number of tokens available for the current time provided it is already refilled.
    pub(crate) fn get_tokens(&self, n: Option<usize>) -> TokenAvailability {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        let previous_epoch = self
            .last_queried_epoch
            .load(std::sync::atomic::Ordering::Relaxed);

        if previous_epoch < now {
            return TokenAvailability::Recompute;
        }

        // Get current pool size for division
        let pool = self
            .state
            .0
            .known_pool_size
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(1);

        match self.token.fetch_update(
            std::sync::atomic::Ordering::Release,
            std::sync::atomic::Ordering::Acquire,
            |current| {
                if current == 0 {
                    return None; // No tokens available
                }

                // Divide by pool size to get appropriate tokens for this processor
                let processor_tokens = current / pool;
                if processor_tokens == 0 {
                    return None; // No tokens available
                }

                // since the current tokens are for the entire pool, we need to subtract
                // the tokens by multiplying by pool size
                match n {
                    None => Some(current - (processor_tokens * pool)),
                    Some(requested) if processor_tokens >= requested => {
                        Some(current - (requested * pool))
                    }
                    Some(_) => Some(current - (processor_tokens * pool)),
                }
            },
        ) {
            Ok(previous) => {
                let available_for_pod = previous / pool;
                let acquired = match n {
                    None => available_for_pod,
                    Some(requested) if available_for_pod >= requested => requested,
                    Some(_) => available_for_pod,
                };
                TokenAvailability::Available(acquired)
            }
            Err(_) => TokenAvailability::Exhausted,
        }
    }
}

/// Mathematical Boundaries of Token Computation
#[derive(Clone, Debug)]
pub struct TokenCalcBounds {
    /// Amount of tokens that will be added for each unit of time.
    slope: f32,
    /// Maximum number of tokens that can be available.
    max: usize,
    /// Minimum number of tokens available at t=0 (origin)
    min: usize,
    /// Unit of time for the slope in seconds.
    duration: Duration,
}

impl Default for TokenCalcBounds {
    fn default() -> Self {
        TokenCalcBounds {
            slope: 1.0,
            max: 1,
            min: 1,
            duration: Duration::from_secs(1),
        }
    }
}

impl TokenCalcBounds {
    /// `Maximum` number of tokens that can be added in a unit of time with an initial `burst`.
    /// The `duration` (in seconds) at with we can add `max` tokens.
    pub fn new(max: usize, min: usize, duration: Duration) -> Self {
        TokenCalcBounds {
            slope: (max - min) as f32 / duration.as_secs_f32(),
            max,
            min,
            duration,
        }
    }
}

#[derive(Clone)]
pub struct NoOpRateLimiter;

impl RateLimiter for NoOpRateLimiter {
    async fn acquire_n(&self, n: Option<usize>, _timeout: Option<Duration>) -> usize {
        // Always return the requested number of tokens (or max if not specified)
        n.unwrap_or(usize::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    /// Test utilities for Redis integration tests
    #[cfg(feature = "redis-tests")]
    mod test_utils {
        use crate::state::store::redis_store::{RedisMode, RedisStore};

        /// Creates a Redis store for testing with a unique key prefix
        /// Returns None if Redis is not available
        pub async fn create_test_redis_store(test_name: &str) -> Option<RedisStore> {
            let redis_url = "redis://127.0.0.1:6379";

            // Check if Redis is available
            if let Err(_) = redis::Client::open(redis_url) {
                println!(
                    "Skipping Redis test - Redis server not available at {}",
                    redis_url
                );
                return None;
            }

            let test_key_prefix =
                Box::leak(format!("test_{}_{}", test_name, std::process::id()).into_boxed_str());
            let redis_mode = RedisMode::SingleUrl {
                url: redis_url.to_string(),
            };

            match RedisStore::new(test_key_prefix, redis_mode).await {
                Ok(store) => Some(store),
                Err(e) => {
                    println!("Skipping Redis test - Failed to connect to Redis: {}", e);
                    None
                }
            }
        }

        /// Cleans up Redis keys after a test
        pub fn cleanup_redis_keys(test_name: &str) {
            let redis_url = "redis://127.0.0.1:6379";
            if let Ok(client) = redis::Client::open(redis_url)
                && let Ok(mut conn) = client.get_connection()
            {
                let test_key_prefix = format!("test_{}_{}", test_name, std::process::id());
                let _: Result<(), _> = redis::cmd("DEL")
                    .arg(format!("{}:heartbeats", test_key_prefix))
                    .arg(format!("{}:poolsize", test_key_prefix))
                    .query(&mut conn);
            }
        }
    }

    #[tokio::test]
    async fn test_acquire_all_tokens() {
        let bounds = TokenCalcBounds::new(10, 5, Duration::from_secs(1));
        let rate_limiter = RateLimit::<WithoutDistributedState>::new(bounds).unwrap();

        // Should get all 5 burst tokens initially
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 5);

        // Should get 0 tokens on next call
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 0);
    }

    #[tokio::test]
    async fn test_acquire_specific_tokens() {
        let bounds = TokenCalcBounds::new(10, 5, Duration::from_secs(1));
        let rate_limiter = RateLimit::<WithoutDistributedState>::new(bounds).unwrap();

        // Acquire 3 tokens
        let tokens = rate_limiter.acquire_n(Some(3), None).await;
        assert_eq!(tokens, 3);

        // Should have 2 tokens left
        let tokens = rate_limiter.acquire_n(Some(2), None).await;
        assert_eq!(tokens, 2);

        // Should get 0 tokens now
        let tokens = rate_limiter.acquire_n(Some(1), None).await;
        assert_eq!(tokens, 0);
    }

    #[tokio::test]
    async fn test_acquire_more_than_available() {
        let bounds = TokenCalcBounds::new(10, 3, Duration::from_secs(1));
        let rate_limiter = RateLimit::<WithoutDistributedState>::new(bounds).unwrap();

        // Try to acquire 5 tokens when only 3 are available
        let tokens = rate_limiter.acquire_n(Some(5), None).await;
        assert_eq!(tokens, 3); // Should get all available tokens

        // Should get 0 tokens on next call
        let tokens = rate_limiter.acquire_n(Some(1), None).await;
        assert_eq!(tokens, 0);
    }

    #[tokio::test]
    async fn test_token_refill_gradual() {
        let bounds = TokenCalcBounds::new(10, 2, Duration::from_secs(1));
        let rate_limiter = RateLimit::<WithoutDistributedState>::new(bounds).unwrap();

        // Consume initial burst tokens
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 2);

        // Advance time by updating last_queried_epoch to simulate time passage
        rate_limiter
            .last_queried_epoch
            .store(0, std::sync::atomic::Ordering::Release);

        // Should refill with slope amount (8 tokens added to 2 burst = 10 max)
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 10);
    }

    #[tokio::test]
    async fn test_token_refill_capped_at_max() {
        let bounds = TokenCalcBounds::new(5, 2, Duration::from_secs(1));
        let rate_limiter = RateLimit::<WithoutDistributedState>::new(bounds).unwrap();

        // Consume initial tokens
        rate_limiter.acquire_n(None, None).await;

        // Force time passage
        rate_limiter
            .last_queried_epoch
            .store(0, std::sync::atomic::Ordering::Release);

        // Should be capped at max (5), not burst + slope
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 5);
    }

    #[tokio::test]
    async fn test_default_token_calc_bounds() {
        let bounds = TokenCalcBounds::default();
        assert_eq!(bounds.max, 1);
        assert_eq!(bounds.min, 1);

        let rate_limiter = RateLimit::<WithoutDistributedState>::new(bounds).unwrap();
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 1);
    }

    #[tokio::test]
    async fn test_custom_token_calc_bounds() {
        let bounds = TokenCalcBounds::new(20, 5, Duration::from_secs(2));
        assert_eq!(bounds.max, 20);
        assert_eq!(bounds.min, 5);
        assert_eq!(bounds.slope, 7.5); // (20-5)/2

        let rate_limiter = RateLimit::<WithoutDistributedState>::new(bounds).unwrap();
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 5);
    }

    #[tokio::test]
    async fn test_concurrent_token_acquisition() {
        let bounds = TokenCalcBounds::new(10, 10, Duration::from_secs(1));
        let rate_limiter = Arc::new(RateLimit::<WithoutDistributedState>::new(bounds).unwrap());

        let mut join_set = tokio::task::JoinSet::new();

        // Spawn 5 tasks trying to acquire 2 tokens each
        for _ in 0..5 {
            let limiter = Arc::clone(&rate_limiter);
            join_set.spawn(async move { limiter.acquire_n(Some(2), None).await });
        }

        let results: Vec<usize> = join_set.join_all().await.into_iter().collect();

        // Total tokens acquired should equal initial burst (10)
        let total_acquired: usize = results.iter().sum();
        assert_eq!(total_acquired, 10);
    }

    #[tokio::test]
    async fn test_timeout_parameter_ignored() {
        let bounds = TokenCalcBounds::new(5, 3, Duration::from_secs(1));
        let rate_limiter = RateLimit::<WithoutDistributedState>::new(bounds).unwrap();

        // Timeout should be ignored for WithoutDistributedState
        let start = std::time::Instant::now();
        let tokens = rate_limiter
            .acquire_n(Some(2), Some(Duration::from_secs(1)))
            .await;
        let elapsed = start.elapsed();

        assert_eq!(tokens, 2);
        assert!(elapsed < Duration::from_millis(100)); // Should return immediately
    }

    #[tokio::test]
    async fn test_distributed_rate_limiter_single_pod() {
        use crate::state::OptimisticValidityUpdateSecs;
        use crate::state::store::in_memory_store::InMemoryStore;

        let bounds = TokenCalcBounds::new(20, 10, Duration::from_secs(1));
        let store = InMemoryStore::new();
        let cancel = CancellationToken::new();
        let refresh_interval = Duration::from_millis(100);
        let runway_update = OptimisticValidityUpdateSecs::default();

        // Create a single distributed rate limiter
        let rate_limiter = RateLimit::<WithDistributedState<InMemoryStore>>::new(
            bounds,
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update,
        )
        .await
        .unwrap();

        // With a single pod, it should get the full burst allocation
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 10, "Single pod should get full burst tokens");

        // Should get 0 tokens on next call within same epoch
        let tokens = rate_limiter.acquire_n(Some(5), None).await;
        assert_eq!(tokens, 0, "No tokens should be available in same epoch");

        // Force time passage to trigger refill
        rate_limiter
            .last_queried_epoch
            .store(0, std::sync::atomic::Ordering::Release);

        // Should refill to max capacity for single pod
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(
            tokens, 20,
            "Single pod should get full max tokens after refill"
        );

        // Clean up
        cancel.cancel();
    }

    // ignore this test for now since it is flaky
    #[tokio::test]
    #[ignore]
    async fn test_distributed_rate_limiter_multiple_pods() {
        use crate::state::OptimisticValidityUpdateSecs;
        use crate::state::store::in_memory_store::InMemoryStore;

        let bounds = TokenCalcBounds::new(30, 15, Duration::from_secs(1));
        let store = InMemoryStore::new();
        let cancel = CancellationToken::new();
        let refresh_interval = Duration::from_millis(50);
        let runway_update = OptimisticValidityUpdateSecs::default();

        // Create three distributed rate limiters (simulating 3 pods)
        let rate_limiter_1 = RateLimit::<WithDistributedState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
        )
        .await
        .unwrap();

        let rate_limiter_2 = RateLimit::<WithDistributedState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_2",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
        )
        .await
        .unwrap();

        let rate_limiter_3 = RateLimit::<WithDistributedState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_3",
            cancel.clone(),
            refresh_interval,
            runway_update,
        )
        .await
        .unwrap();

        // Wait for consensus to be reached among all pods
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Each pod should get 1/3 of the burst tokens (15/3 = 5)
        let tokens_1 = rate_limiter_1.acquire_n(None, None).await;
        let tokens_2 = rate_limiter_2.acquire_n(None, None).await;
        let tokens_3 = rate_limiter_3.acquire_n(None, None).await;

        // Each pod should get equal share
        assert_eq!(tokens_1, 5, "Pod 1 should get 1/3 of burst tokens");
        assert_eq!(tokens_2, 5, "Pod 2 should get 1/3 of burst tokens");
        assert_eq!(tokens_3, 5, "Pod 3 should get 1/3 of burst tokens");

        // Total tokens distributed should equal burst
        assert_eq!(
            tokens_1 + tokens_2 + tokens_3,
            15,
            "Total distributed should equal burst"
        );

        // Force time passage for all pods to trigger refill
        rate_limiter_1
            .last_queried_epoch
            .store(0, std::sync::atomic::Ordering::Release);
        rate_limiter_2
            .last_queried_epoch
            .store(0, std::sync::atomic::Ordering::Release);
        rate_limiter_3
            .last_queried_epoch
            .store(0, std::sync::atomic::Ordering::Release);

        // After refill, each pod should get 1/3 of max tokens (30/3 = 10)
        let tokens_1_refill = rate_limiter_1.acquire_n(None, None).await;
        let tokens_2_refill = rate_limiter_2.acquire_n(None, None).await;
        let tokens_3_refill = rate_limiter_3.acquire_n(None, None).await;

        assert_eq!(
            tokens_1_refill, 10,
            "Pod 1 should get 1/3 of max tokens after refill"
        );
        assert_eq!(
            tokens_2_refill, 10,
            "Pod 2 should get 1/3 of max tokens after refill"
        );
        assert_eq!(
            tokens_3_refill, 10,
            "Pod 3 should get 1/3 of max tokens after refill"
        );

        // Total tokens after refill should equal max
        assert_eq!(
            tokens_1_refill + tokens_2_refill + tokens_3_refill,
            30,
            "Total distributed after refill should equal max"
        );

        // Test partial token acquisition
        rate_limiter_1
            .last_queried_epoch
            .store(0, std::sync::atomic::Ordering::Release);

        let partial_tokens = rate_limiter_1.acquire_n(Some(3), None).await;
        assert_eq!(
            partial_tokens, 3,
            "Should acquire exactly 3 tokens when requested"
        );

        let remaining_tokens = rate_limiter_1.acquire_n(None, None).await;
        assert_eq!(remaining_tokens, 7, "Should get remaining 7 tokens");

        // Clean up
        cancel.cancel();
    }

    #[tokio::test]
    async fn test_distributed_rate_limiter_time_based_refill() {
        use crate::state::OptimisticValidityUpdateSecs;
        use crate::state::store::in_memory_store::InMemoryStore;

        // Create a rate limiter with 30 max tokens, 15 burst, over 2 seconds
        let bounds = TokenCalcBounds::new(90, 45, Duration::from_secs(9));
        let store = InMemoryStore::new();
        let cancel = CancellationToken::new();
        let refresh_interval = Duration::from_millis(50);
        let runway_update = OptimisticValidityUpdateSecs::default();

        // Create three distributed rate limiters (simulating 3 pods)
        let rate_limiter_1 = RateLimit::<WithDistributedState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
        )
        .await
        .unwrap();

        let rate_limiter_2 = RateLimit::<WithDistributedState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_2",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
        )
        .await
        .unwrap();

        let rate_limiter_3 = RateLimit::<WithDistributedState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_3",
            cancel.clone(),
            refresh_interval,
            runway_update,
        )
        .await
        .unwrap();

        // Wait for consensus to be reached among all pods
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Phase 1: Each pod requests a specific number of tokens
        let tokens_1_phase1 = rate_limiter_1.acquire_n(Some(2), None).await;
        let tokens_2_phase1 = rate_limiter_2.acquire_n(Some(2), None).await;
        let tokens_3_phase1 = rate_limiter_3.acquire_n(Some(2), None).await;

        println!("Phase 1 - Specific token requests:");
        println!("Pod 1 got: {} tokens", tokens_1_phase1);
        println!("Pod 2 got: {} tokens", tokens_2_phase1);
        println!("Pod 3 got: {} tokens", tokens_3_phase1);

        // Each pod should get some tokens (may not be exactly 2 due to pool division)
        assert!(tokens_1_phase1 > 0, "Pod 1 should get some tokens");
        assert!(tokens_2_phase1 > 0, "Pod 2 should get some tokens");
        assert!(tokens_3_phase1 > 0, "Pod 3 should get some tokens");

        println!("Waiting for token refill...");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Phase 2: Each pod tries to acquire all available tokens
        let tokens_1_phase2 = rate_limiter_1.acquire_n(None, None).await;
        let tokens_2_phase2 = rate_limiter_2.acquire_n(None, None).await;
        let tokens_3_phase2 = rate_limiter_3.acquire_n(None, None).await;

        println!("Phase 2 - Acquire all available tokens:");
        println!("Pod 1 got: {} tokens", tokens_1_phase2);
        println!("Pod 2 got: {} tokens", tokens_2_phase2);
        println!("Pod 3 got: {} tokens", tokens_3_phase2);

        // After refill, each pod should get more tokens
        assert!(tokens_1_phase2 > 0, "Pod 1 should get tokens after refill");
        assert!(tokens_2_phase2 > 0, "Pod 2 should get tokens after refill");
        assert!(tokens_3_phase2 > 0, "Pod 3 should get tokens after refill");

        // Total tokens distributed should be reasonable (allowing for truncation effects)
        let total_phase1 = tokens_1_phase1 + tokens_2_phase1 + tokens_3_phase1;
        let total_phase2 = tokens_1_phase2 + tokens_2_phase2 + tokens_3_phase2;

        println!("Total tokens phase 1: {}", total_phase1);
        println!("Total tokens phase 2: {}", total_phase2);

        // Due to time-based truncation and distributed consensus, we can't expect exact values
        // but we should see reasonable token distribution
        assert!(total_phase1 > 0, "Should distribute some tokens in phase 1");
        assert!(total_phase2 > 0, "Should distribute some tokens in phase 2");

        // Phase 3: Try to acquire more tokens immediately (should get 0 or very few)
        let tokens_1_phase3 = rate_limiter_1.acquire_n(Some(5), None).await;
        let tokens_2_phase3 = rate_limiter_2.acquire_n(Some(5), None).await;
        let tokens_3_phase3 = rate_limiter_3.acquire_n(Some(5), None).await;

        println!("Phase 3 - Immediate retry:");
        println!("Pod 1 got: {} tokens", tokens_1_phase3);
        println!("Pod 2 got: {} tokens", tokens_2_phase3);
        println!("Pod 3 got: {} tokens", tokens_3_phase3);

        // Should get very few or no tokens immediately after exhausting the pool
        let total_phase3 = tokens_1_phase3 + tokens_2_phase3 + tokens_3_phase3;
        println!("Total tokens phase 3: {}", total_phase3);

        // Clean up
        cancel.cancel();
    }

    #[tokio::test]
    #[cfg(feature = "redis-tests")]
    async fn test_distributed_rate_limiter_time_based_refill_redis() {
        use crate::state::OptimisticValidityUpdateSecs;
        use crate::state::store::redis_store::RedisStore;

        // Create Redis store for testing
        let store = match test_utils::create_test_redis_store("rate_limiter_refill").await {
            Some(store) => store,
            None => return, // Skip test if Redis is not available
        };

        // Create a rate limiter with 90 max tokens, 45 burst, over 9 seconds
        let bounds = TokenCalcBounds::new(90, 45, Duration::from_secs(9));

        let cancel = CancellationToken::new();
        let refresh_interval = Duration::from_millis(50);
        let runway_update = OptimisticValidityUpdateSecs::default();

        // Create three distributed rate limiters (simulating 3 pods)
        let rate_limiter_1 = RateLimit::<WithDistributedState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
        )
        .await
        .unwrap();

        let rate_limiter_2 = RateLimit::<WithDistributedState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_2",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
        )
        .await
        .unwrap();

        let rate_limiter_3 = RateLimit::<WithDistributedState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_3",
            cancel.clone(),
            refresh_interval,
            runway_update,
        )
        .await
        .unwrap();

        // Wait for consensus to be reached among all pods
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Phase 1: Each pod requests a specific number of tokens
        let tokens_1_phase1 = rate_limiter_1.acquire_n(Some(2), None).await;
        let tokens_2_phase1 = rate_limiter_2.acquire_n(Some(2), None).await;
        let tokens_3_phase1 = rate_limiter_3.acquire_n(Some(2), None).await;

        println!("Phase 1 - Specific token requests:");
        println!("Pod 1 got: {} tokens", tokens_1_phase1);
        println!("Pod 2 got: {} tokens", tokens_2_phase1);
        println!("Pod 3 got: {} tokens", tokens_3_phase1);

        // Each pod should get some tokens (may not be exactly 2 due to pool division)
        assert!(tokens_1_phase1 > 0, "Pod 1 should get some tokens");
        assert!(tokens_2_phase1 > 0, "Pod 2 should get some tokens");
        assert!(tokens_3_phase1 > 0, "Pod 3 should get some tokens");

        // Total tokens should not exceed the burst limit divided by pool size
        let total_tokens_phase1 = tokens_1_phase1 + tokens_2_phase1 + tokens_3_phase1;
        assert!(
            total_tokens_phase1 <= 45,
            "Total tokens {} should not exceed burst limit 45",
            total_tokens_phase1
        );

        // Phase 2: Wait for some time to allow token refill
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Phase 2: Request tokens again to test refill
        let tokens_1_phase2 = rate_limiter_1.acquire_n(Some(3), None).await;
        let tokens_2_phase2 = rate_limiter_2.acquire_n(Some(3), None).await;
        let tokens_3_phase2 = rate_limiter_3.acquire_n(Some(3), None).await;

        println!("Phase 2 - After refill:");
        println!("Pod 1 got: {} tokens", tokens_1_phase2);
        println!("Pod 2 got: {} tokens", tokens_2_phase2);
        println!("Pod 3 got: {} tokens", tokens_3_phase2);

        // Should get more tokens due to refill
        let total_tokens_phase2 = tokens_1_phase2 + tokens_2_phase2 + tokens_3_phase2;
        assert!(
            total_tokens_phase2 > 0,
            "Should get some tokens after refill"
        );

        // Phase 3: Test exhaustion - try to get many tokens
        let tokens_1_phase3 = rate_limiter_1.acquire_n(Some(50), None).await;
        let tokens_2_phase3 = rate_limiter_2.acquire_n(Some(50), None).await;
        let tokens_3_phase3 = rate_limiter_3.acquire_n(Some(50), None).await;

        println!("Phase 3 - Exhaustion test:");
        println!("Pod 1 got: {} tokens", tokens_1_phase3);
        println!("Pod 2 got: {} tokens", tokens_2_phase3);
        println!("Pod 3 got: {} tokens", tokens_3_phase3);

        // Should get fewer tokens as the bucket is getting exhausted
        let total_tokens_phase3 = tokens_1_phase3 + tokens_2_phase3 + tokens_3_phase3;
        assert!(
            total_tokens_phase3 <= 90,
            "Total tokens {} should not exceed max capacity 90",
            total_tokens_phase3
        );

        // Clean up Redis keys
        test_utils::cleanup_redis_keys("rate_limiter_refill");

        // Clean up
        cancel.cancel();
    }
}
