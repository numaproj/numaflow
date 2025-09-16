use crate::state::OptimisticValidityUpdateSecs;
use crate::state::store::Store;
use state::RateLimiterState;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex};
use std::time;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::warn;

pub(crate) mod error;

pub(crate) use error::Error;
pub(crate) use error::Result;

/// State module contains the implementation of the [RateLimiterState] which has the
/// holds distributed consensus state required to compute the available tokens.
pub mod state;

#[derive(Clone)]
pub struct WithoutState;

#[derive(Clone)]
pub struct WithState<S>(RateLimiterState<S>);

/// RateLimiter will expose methods to query the tokens available per unit time
#[trait_variant::make(Send)]
pub trait RateLimiter {
    /// GetN returns the number of tokens available for the next n time windows or blocks till then.
    /// If timeout is None, it will block till the tokens are available, else returns whatever is
    /// available. If `n` is provided, it will try to acquire `n` tokens else it will acquire all the tokens.
    async fn acquire_n(&self, n: Option<usize>, timeout: Option<Duration>) -> usize;

    /// Shutdown the rate limiter and clean up resources.
    /// This will deregister the processor from the distributed store and stop any background tasks.
    async fn shutdown(&self) -> Result<()>;
}

/// Mode of calculation of tokens to give out
///
/// Todo: docs for definition/usage
#[derive(Clone, Debug)]
pub enum Mode {
    OnlyIfUsed,
    Scheduled,
    Relaxed,
}

// change to default
impl Default for Mode {
    fn default() -> Self {
        Mode::Relaxed
    }
}

/// RateLimit is the main struct that will be used by the user to get the tokens available for the
/// current time window. It has to be clonable so it can be used across multiple process-a-stream
/// tasks (e.g., pipeline with multiple partitions).
#[derive(Clone)]
pub struct RateLimit<W> {
    token_calc_bounds: TokenCalcBounds,
    /// Current number of tokens available.
    token: Arc<AtomicUsize>,
    /// Max number of tokens ever filled till now. The next refill relies on the current max allowed
    /// value. For the first run this will be set to `burst` and will be slowly incremented to the
    /// `max` value. This has to be float because we support fractional slope. E.g., one could
    /// say ramp up from 10 to 20 in 60 seconds, which means we add 1/6 tokens per second.
    max_ever_filled: Arc<Mutex<f32>>,
    /// Last time the token was queried. The tokens are replenished based on the time elapsed since
    /// the last query.
    last_queried_epoch: Arc<AtomicU64>,
    /// Optional [RateLimiterState] to query for the pool-size in a distributed setting.
    state: W,
}

impl<W> std::fmt::Display for RateLimit<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "token_calc_bounds: {:?}, token: {}, max_ever_filled: {}, last_queried_epoch: {}",
            self.token_calc_bounds,
            self.token.load(std::sync::atomic::Ordering::Relaxed),
            self.max_ever_filled.lock().unwrap(),
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
    fn default_slope_increase(&self, max_ever_filled: &mut f32) -> usize {
        // let's make sure we do not go beyond the max
        if *max_ever_filled >= self.token_calc_bounds.max as f32 {
            self.token_calc_bounds.max
        } else {
            let refill = *max_ever_filled + self.token_calc_bounds.slope;
            let capped_refill = refill.min(self.token_calc_bounds.max as f32);

            // Update the fractional value
            *max_ever_filled = capped_refill;

            capped_refill as usize
        }
    }

    /// Computes the number of tokens to be refilled for the next epoch.
    ///
    /// TODO: refactor duplicate code
    pub(crate) fn compute_refill(
        &self,
        requested_token_size: Option<usize>,
        cur_epoch: u64,
    ) -> usize {
        let mut max_ever_filled = self.max_ever_filled.lock().unwrap();

        match self.token_calc_bounds.mode {
            Mode::OnlyIfUsed => {
                match requested_token_size {
                    None => {
                        self.default_slope_increase(&mut max_ever_filled)
                    }
                    Some(tokens_to_acquire) => {
                        if tokens_to_acquire <= *max_ever_filled as usize {
                            *max_ever_filled as usize
                        } else {
                            self.default_slope_increase(&mut max_ever_filled)
                        }
                    }
                }
            }
            Mode::Scheduled => {
                // let's make sure we do not go beyond the max
                if *max_ever_filled >= self.token_calc_bounds.max as f32 {
                    self.token_calc_bounds.max
                } else {
                    let prev_epoch = self
                        .last_queried_epoch
                        .load(std::sync::atomic::Ordering::Relaxed);
                    let time_diff = cur_epoch.checked_sub(prev_epoch)
                        .expect("Previous epoch should be smaller than current epoch \
                        when calculating scheduled refill") as f32;
                    let refill = *max_ever_filled + self.token_calc_bounds.slope * (time_diff);
                    let capped_refill = refill.min(self.token_calc_bounds.max as f32);

                    // Update the fractional value
                    *max_ever_filled = capped_refill;

                    capped_refill as usize
                }
            }
            Mode::Relaxed => {
                self.default_slope_increase(&mut max_ever_filled)
            }
        }
    }
}

impl RateLimit<WithoutState> {
    /// Create a new [RateLimit] without a distributed state.
    pub fn new(token_calc_bounds: TokenCalcBounds) -> Result<Self> {
        let burst = token_calc_bounds.min;
        Ok(RateLimit {
            token_calc_bounds,
            token: Arc::new(AtomicUsize::new(burst)),
            max_ever_filled: Arc::new(Mutex::new(burst as f32)),
            last_queried_epoch: Arc::new(AtomicU64::new(0)),
            state: WithoutState,
        })
    }

    /// Get the number of tokens available for the current time provided it is already refilled.
    /// Since pool size is always one, we don't have to divide the tokens by pool size.
    pub(crate) fn get_tokens(&self, n: Option<usize>, cur_epoch: u64) -> TokenAvailability {
        let previous_epoch = self
            .last_queried_epoch
            .load(std::sync::atomic::Ordering::Relaxed);

        if previous_epoch < cur_epoch {
            return TokenAvailability::Recompute;
        }

        let fetched = self.token.fetch_update(
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
        );

        match fetched {
            Ok(previous) => {
                let acquired = match n {
                    None => previous,
                    Some(requested) => previous.min(requested),
                };
                TokenAvailability::Available(acquired)
            }
            Err(_) => TokenAvailability::Exhausted,
        }
    }
}

/// Helper to sleep until the start of the next second.
async fn sleep_until_next_sec() -> u64 {
    let now = std::time::SystemTime::now();
    let since_epoch = now
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards");

    // Calculate the start of the next second truncated to the next second so we sleep only
    // till the top of the second
    let start_of_next_sec = since_epoch.as_secs() + 1;

    // delta till the top of the next second
    let delta_till_next_sec = Duration::from_secs(start_of_next_sec) - since_epoch;

    tokio::time::sleep_until(tokio::time::Instant::from_std(
        std::time::Instant::now() + (delta_till_next_sec),
    ))
    .await;

    start_of_next_sec
}

impl RateLimit<WithoutState> {
    /// Tries to acquire tokens(non-blocking)
    async fn attempt_acquire_n(&self, n: Option<usize>, cur_epoch: u64) -> usize {
        // let's try to acquire the tokens
        match self.get_tokens(n, cur_epoch) {
            TokenAvailability::Available(t) => return t,
            TokenAvailability::Exhausted => return 0,
            TokenAvailability::Recompute => {}
        }

        let new_token_count = self.compute_refill(n, cur_epoch);
        self.token
            .store(new_token_count, std::sync::atomic::Ordering::Release);

        // Update the epoch to current time after refilling
        self.last_queried_epoch
            .store(cur_epoch, std::sync::atomic::Ordering::Release);

        // try to acquire the tokens again
        match self.get_tokens(n, cur_epoch) {
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

impl RateLimiter for RateLimit<WithoutState> {
    /// Acquire `n` tokens. If `n` is not provided, it will acquire all the tokens.
    /// If timeout is None, returns immediately with available tokens (non-blocking).
    /// If timeout is Some, will wait up to the specified duration for some tokens to be available.
    /// Timeout is only considered when token size is zero.
    async fn acquire_n(&self, n: Option<usize>, timeout: Option<Duration>) -> usize {
        let cur_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        // First attempt - try to get tokens immediately
        let tokens = self.attempt_acquire_n(n, cur_epoch).await;
        if tokens > 0 {
            return tokens;
        }

        // If no timeout specified, return immediately
        let Some(duration) = timeout else {
            return 0;
        };

        // With timeout, wait for tokens to become available
        let acquisition_loop = async {
            loop {
                // Wait for the next epoch to try again
                let cur_epoch = sleep_until_next_sec().await;
                let tokens = self.attempt_acquire_n(n, cur_epoch).await;
                if tokens > 0 {
                    return tokens;
                }
            }
        };

        tokio::time::timeout(duration, acquisition_loop)
            .await
            .unwrap_or(0)
    }

    async fn shutdown(&self) -> crate::Result<()> {
        Ok(())
    }
}

impl<S: Store + Send + Sync + Clone + 'static> RateLimit<WithState<S>> {
    /// Tries to acquire tokens (non-blocking)
    async fn attempt_acquire_n(&self, n: Option<usize>, cur_epoch: u64) -> usize {
        // Try the hot-path first: consume from the current window.
        match self.get_tokens(n, cur_epoch) {
            TokenAvailability::Available(t) => return t,
            TokenAvailability::Exhausted => return 0,
            TokenAvailability::Recompute => {}
        }

        // We crossed a new epoch; we gave to recompute the budget i.e., recompute the number of
        // new tokens available
        let next_total_tokens = self.compute_refill(n, cur_epoch);

        // Store the total tokens (division happens in get_tokens)
        self.token
            .store(next_total_tokens, std::sync::atomic::Ordering::Release);

        self.last_queried_epoch
            .store(cur_epoch, std::sync::atomic::Ordering::Release);

        // Try to take after refill.
        match self.get_tokens(n, cur_epoch) {
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

impl<S: Store + Send + Sync + Clone + 'static> RateLimiter for RateLimit<WithState<S>> {
    /// Acquire `n` tokens. If `n` is not provided, it will acquire all the tokens.
    /// If timeout is None, returns immediately with available tokens (non-blocking).
    /// If timeout is Some, will wait up to the specified duration for some tokens to be available.
    /// Timeout is only considered when token size is zero.
    async fn acquire_n(&self, n: Option<usize>, timeout: Option<Duration>) -> usize {
        let mut cur_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        // First attempt - try to get tokens immediately
        let tokens = self.attempt_acquire_n(n, cur_epoch).await;
        if tokens > 0 {
            return tokens;
        }

        // If no timeout specified, return immediately (original behavior)
        let Some(duration) = timeout else {
            return 0;
        };

        // With timeout, wait for tokens to become available
        let acquisition_loop = async {
            loop {
                // Wait for the next epoch to try again
                cur_epoch = sleep_until_next_sec().await;
                let tokens = self.attempt_acquire_n(n, cur_epoch).await;
                if tokens > 0 {
                    return tokens;
                }
            }
        };

        tokio::time::timeout(duration, acquisition_loop)
            .await
            .unwrap_or(0)
    }

    async fn shutdown(&self) -> Result<()> {
        self.state.0.shutdown().await
    }
}

impl<S: Store> RateLimit<WithState<S>> {
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
        let state = RateLimiterState::new(
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
            max_ever_filled: Arc::new(Mutex::new(burst as f32)),
            last_queried_epoch: Arc::new(AtomicU64::new(0)),
            state: WithState(state),
        })
    }

    /// Get the number of tokens available for the current time provided it is already refilled.
    pub(crate) fn get_tokens(&self, n: Option<usize>, cur_epoch: u64) -> TokenAvailability {
        let previous_epoch = self
            .last_queried_epoch
            .load(std::sync::atomic::Ordering::Relaxed);

        if previous_epoch < cur_epoch {
            return TokenAvailability::Recompute;
        }

        // Get current pool size for division
        let pool = self
            .state
            .0
            .known_pool_size
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(1);

        let fetched = self.token.fetch_update(
            std::sync::atomic::Ordering::Release,
            std::sync::atomic::Ordering::Acquire,
            |current| {
                if current == 0 {
                    return None; // No tokens available
                }

                // Divide by pool size to get appropriate tokens for this processor
                let processor_tokens = current / pool;
                if processor_tokens == 0 {
                    return None;
                }

                // if n is not provided, acquire all available tokens, else min of requested and
                // available tokens.
                let tokens_to_acquire = match n {
                    None => processor_tokens,
                    Some(requested) => processor_tokens.min(requested),
                };

                // since the tokens are not stored at processor level, we need to subtract the tokens
                // by pool size to get the right number.
                current.checked_sub(tokens_to_acquire * pool)
            },
        );

        match fetched {
            Ok(previous) => {
                let available_for_pod = previous / pool;
                let acquired = match n {
                    None => available_for_pod,
                    Some(requested) => available_for_pod.min(requested),
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
    /// Mode of operation
    mode: Mode,
}

impl Default for TokenCalcBounds {
    fn default() -> Self {
        TokenCalcBounds {
            slope: 1.0,
            max: 1,
            min: 1,
            mode: Mode::Relaxed,
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
            // TODO: Get this from config
            mode: Mode::Relaxed,
        }
    }
}

/// Added TokenCalcBoundsBuilder to allow setting mode for testing purposes
/// without changing original TokenCalcBounds constructor
///
/// TODO: Remove this once throttling implementation is finalized and tokenCalcBounds' new() api is changed
struct TokenCalcBoundsBuilder {
    max: Option<usize>,
    min: Option<usize>,
    duration: Option<Duration>,
    mode: Option<Mode>,
}

impl Default for TokenCalcBoundsBuilder {
    fn default() -> Self {
        TokenCalcBoundsBuilder {
            duration: None,
            max: None,
            min: None,
            mode: None,
        }
    }
}

impl TokenCalcBoundsBuilder {
    pub fn new(max: usize, min: usize, duration: Duration) -> Self {
        TokenCalcBoundsBuilder {
            max: Some(max),
            min: Some(min),
            duration: Some(duration),
            mode: None,
        }
    }

    pub fn max(&mut self, max: usize) -> &mut Self {
        self.max = Some(max);
        self
    }

    pub fn min(&mut self, min: usize) -> &mut Self {
        self.min = Some(min);
        self
    }

    pub fn duration(&mut self, duration: Duration) -> &mut Self {
        self.duration = Some(duration);
        self
    }

    pub fn mode(&mut self, mode: Mode) -> &mut Self {
        self.mode = Some(mode);
        self
    }

    pub fn build(&self) -> TokenCalcBounds {
        let max = self.max.expect("max is required");
        let min = self.min.expect("min is required");
        let duration = self.duration.expect("duration is required");
        TokenCalcBounds {
            slope: (max - min) as f32 / duration.as_secs_f32(),
            max,
            min,
            mode: self.mode.clone().expect("Mode is required"),
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

    async fn shutdown(&self) -> crate::Result<()> {
        // No-op for NoOpRateLimiter as there are no resources to clean up
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use tokio::time::Duration;

    /// Test utilities for integration tests
    mod test_utils {
        use std::ops::Deref;
        use crate::state::store::in_memory_store::InMemoryStore;
        use crate::state::store::redis_store::{RedisMode, RedisStore};
        use crate::state::{OptimisticValidityUpdateSecs, Store};
        use crate::{RateLimit, RateLimiter, TokenCalcBounds, WithState, Mode, TokenCalcBoundsBuilder};
        use std::time::Duration;
        use tokio_util::sync::CancellationToken;

        #[derive(Clone)]
        pub(super) enum StoreType {
            InMemory,
            Redis,
        }

        /// Test case struct for distributed rate limiter tests
        /// Takes in the test parameters to be used for each test case.
        ///
        /// - [crate::tests::test_distributed_rate_limiter_multiple_pods_in_memory]
        /// - [crate::tests::test_distributed_rate_limiter_multiple_pods_redis]
        ///
        pub(super) struct TestCase {
            /// The maximum number of tokens that can be stored in the bucket
            pub(super) max_tokens: usize,
            /// The number of tokens that can be burst in a single second
            pub(super) burst_tokens: usize,
            /// The duration of the bucket
            pub(super) duration: Duration,
            /// The number of pods
            pub(super) pod_count: usize,
            /// The store name (String) to initialize for the test
            pub(super) store_type: StoreType,
            /// The name of the test
            pub(super) test_name: String,
            /// Tuple of tokens asked in each iteration by *each* pod and count of epochs after which
            /// the next set of tokens are asked.
            /// Second item in the tuple refers to the number of epochs after which the next set of
            /// tokens are going to be fetched, not the tokens specified in the current tuple.
            /// Eg:
            /// This test case: [(None, 3), (Some(2), 1)], specifies that fetch all tokens at 0th
            /// epoch and fetch 2 tokens after 3 epochs.
            pub(super) asked_tokens: Vec<(Option<usize>, usize)>,
            // Tokens expected to be returned by rate limiter in each iteration to *each* pod
            pub(super) expected_tokens: Vec<usize>,
            // The throttling mode of the rate limiter
            // defaults to Relaxed
            pub(super) mode: Mode,
        }

        impl TestCase {
            // Creates a new test case with certain default values
            // Created to make it easier to add new params to test cases
            // without changing initialization of it in existing tests.
            pub(super) fn new(max_tokens: usize,
                              burst_tokens: usize,
                              duration: Duration,
                              pod_count: usize,
                              asked_tokens: Vec<(Option<usize>, usize)>,
                              expected_tokens: Vec<usize>) -> Self {
                TestCase {
                    max_tokens,
                    burst_tokens,
                    duration,
                    pod_count,
                    asked_tokens,
                    expected_tokens,
                    store_type: StoreType::InMemory,
                    test_name: String::new(),
                    mode: Mode::Relaxed,
                }
            }

            pub(super) fn store_type(mut self, store_type: StoreType) -> Self {
                self.store_type = store_type;
                self
            }

            pub(super) fn test_name(mut self, test_name: String) -> Self {
                self.test_name = test_name;
                self
            }

            pub(super) fn mode(&mut self, mode: Mode) -> &mut Self {
                self.mode = mode;
                self
            }
        }

        /// A generic utility function to test rate limiter with generic state
        /// Test runner for:
        /// - [crate::tests::test_distributed_rate_limiter_multiple_pods_in_memory]
        /// - [crate::tests::test_distributed_rate_limiter_multiple_pods_redis]
        ///
        async fn test_rate_limiter_with_state<S: Store + Sync>(
            // A trait object that implements the [Store] trait
            store: S,
            // A struct containing the test parameters
            test_case: TestCase,
        ) {
            let TestCase {
                max_tokens,
                burst_tokens,
                duration,
                pod_count,
                store_type: _store_type,
                test_name,
                asked_tokens,
                expected_tokens,
                mode,
            } = test_case;
            let cancel = CancellationToken::new();
            let refresh_interval = Duration::from_millis(50);
            let runway_update = OptimisticValidityUpdateSecs::default();
            let bounds = TokenCalcBoundsBuilder::new(max_tokens, burst_tokens, duration)
                                                                .mode(mode)
                                                                .build();

            assert_eq!(
                asked_tokens.len(),
                expected_tokens.len(),
                "asked_tokens and expected_tokens should have same length for test: {}",
                test_name
            );

            // create pod_count number of rate limiters with passed store
            let mut rate_limiters = Vec::with_capacity(pod_count);
            for i in 0..pod_count {
                rate_limiters.push(
                    RateLimit::<WithState<S>>::new(
                        bounds.clone(),
                        store.clone(),
                        &format!("processor_{}", i),
                        cancel.clone(),
                        refresh_interval,
                        runway_update.clone(),
                    )
                    .await
                    .expect("Failed to create rate limiters"),
                );
            }

            let iterations = asked_tokens.len();
            let mut cur_epoch = 0;
            for i in 0..iterations {
                let mut total_got_tokens = 0;
                let mut total_expected_tokens = 0;
                for rate_limiter in rate_limiters.iter() {
                    let tokens = rate_limiter
                        .attempt_acquire_n(asked_tokens[i].0, cur_epoch)
                        .await;
                    assert_eq!(
                        tokens, expected_tokens[i],
                        "Number of tokens fetched in each iteration \
                should increase by slope/pod_count until ramp up",
                    );
                    total_got_tokens += tokens;
                    total_expected_tokens += expected_tokens[i];
                }
                assert_eq!(
                    total_got_tokens, total_expected_tokens,
                    "Total number of tokens fetched in each iteration \
                should be less than or equal to total expected tokens for each processor",
                );

                // Determines after how many epochs next pull is going to be made.
                cur_epoch += asked_tokens[i].1 as u64;
            }

            for rate_limiter in rate_limiters.iter() {
                rate_limiter
                    .shutdown()
                    .await
                    .expect("Rate limiter failed to shutdownj");
            }
        }

        /// Utility function to run distributed rate limiter multiple pods test cases
        /// Only here to iterate over the different test cases and initialize stores to
        /// be used by the test cases.
        ///
        pub(super) async fn run_distributed_rate_limiter_multiple_pods_test_cases(
            // The test cases to run
            test_cases: Vec<TestCase>,
        ) {
            for test_case in test_cases {
                let store_type = &test_case.store_type;
                let test_name = &test_case.test_name;

                // Determining/initializing the store based on the store type here
                // instead of passing a cloned store so that each test case gets its own store
                // Necessary for redis store test cases.
                match store_type {
                    StoreType::InMemory => {
                        let store = InMemoryStore::new();
                        test_rate_limiter_with_state(store, test_case).await;
                    }
                    StoreType::Redis => {
                        let store = match create_test_redis_store(test_name).await {
                            Some(store) => store,
                            None => return, // Skip test if Redis is not available
                        };

                        test_rate_limiter_with_state(store, test_case).await;
                    }
                }
            }
        }

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
            let redis_mode = RedisMode::single_url(redis_url.to_string())
                .build()
                .unwrap();

            match RedisStore::new(test_key_prefix, redis_mode).await {
                Ok(store) => Some(store),
                Err(e) => {
                    println!("Skipping Redis test - Failed to connect to Redis: {}", e);
                    None
                }
            }
        }

        /// Cleans up Redis keys after a test
        #[cfg(feature = "redis-tests")]
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
        let rate_limiter = RateLimit::<WithoutState>::new(bounds).unwrap();
        rate_limiter.last_queried_epoch.store(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            std::sync::atomic::Ordering::Release,
        );
        // Should get all 5 burst tokens initially
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 5);

        // Should get 0 tokens on next call
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 0);
    }

    #[tokio::test]
    async fn test_sleep_until_next_sec() {
        let start = std::time::Instant::now();
        sleep_until_next_sec().await;
        let elapsed = start.elapsed();
        // < 5 to make tests are not flaky
        assert!(elapsed < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_acquire_specific_tokens() {
        let bounds = TokenCalcBounds::new(10, 5, Duration::from_secs(1));
        let rate_limiter = RateLimit::<WithoutState>::new(bounds).unwrap();
        rate_limiter.last_queried_epoch.store(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            std::sync::atomic::Ordering::Release,
        );

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
        let rate_limiter = RateLimit::<WithoutState>::new(bounds).unwrap();
        rate_limiter.last_queried_epoch.store(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            std::sync::atomic::Ordering::Release,
        );

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
        let rate_limiter = RateLimit::<WithoutState>::new(bounds).unwrap();
        rate_limiter.last_queried_epoch.store(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            std::sync::atomic::Ordering::Release,
        );

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
        let rate_limiter = RateLimit::<WithoutState>::new(bounds).unwrap();

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

        let rate_limiter = RateLimit::<WithoutState>::new(bounds).unwrap();
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 1);
    }

    #[tokio::test]
    async fn test_custom_token_calc_bounds() {
        let bounds = TokenCalcBounds::new(20, 5, Duration::from_secs(2));
        assert_eq!(bounds.max, 20);
        assert_eq!(bounds.min, 5);
        assert_eq!(bounds.slope, 7.5); // (20-5)/2

        let rate_limiter = RateLimit::<WithoutState>::new(bounds).unwrap();
        rate_limiter.last_queried_epoch.store(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            std::sync::atomic::Ordering::Release,
        );
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 5);
    }

    #[tokio::test]
    async fn test_fractional_slope_accumulation() {
        // Test case: max=2, min=1, ramp_up=10s
        // slope = (2-1)/10 = 0.1 tokens per second
        let bounds = TokenCalcBounds::new(2, 1, Duration::from_secs(10));
        assert_eq!(bounds.max, 2);
        assert_eq!(bounds.min, 1);
        assert_eq!(bounds.slope, 0.1); // (2-1)/10

        let rate_limiter = RateLimit::<WithoutState>::new(bounds).unwrap();
        rate_limiter.last_queried_epoch.store(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            std::sync::atomic::Ordering::Release,
        );

        // Initially should have 1 token (burst)
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 1);

        // Force time passage to trigger refill
        rate_limiter
            .last_queried_epoch
            .store(0, std::sync::atomic::Ordering::Release);

        // After first refill: 1.0 + 0.1 = 1.1, but tokens returned as usize = 1
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 1);

        // Check that max_ever_filled is accumulating fractionally
        let max_filled = *rate_limiter.max_ever_filled.lock().unwrap();
        assert_eq!(max_filled, 1.1);

        // Force another time passage
        rate_limiter
            .last_queried_epoch
            .store(0, std::sync::atomic::Ordering::Release);

        // After second refill: 1.1 + 0.1 = 1.2, tokens = 1
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 1);
        let max_filled = *rate_limiter.max_ever_filled.lock().unwrap();
        assert_eq!(max_filled, 1.2);

        // Continue until we reach 2.0 (after 10 refills total)
        for _ in 0..8 {
            rate_limiter
                .last_queried_epoch
                .store(0, std::sync::atomic::Ordering::Release);
            rate_limiter.acquire_n(None, None).await;
        }

        let max_filled = *rate_limiter.max_ever_filled.lock().unwrap();
        assert_eq!(max_filled, 2.0);

        // Force one more time passage - should be capped at max (2)
        rate_limiter
            .last_queried_epoch
            .store(0, std::sync::atomic::Ordering::Release);
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 2);
        let max_filled = *rate_limiter.max_ever_filled.lock().unwrap();
        assert_eq!(max_filled, 2.0); // Should not exceed max
    }

    #[tokio::test]
    async fn test_concurrent_token_acquisition() {
        let bounds = TokenCalcBounds::new(10, 10, Duration::from_secs(1));
        let rate_limiter = Arc::new(RateLimit::<WithoutState>::new(bounds).unwrap());

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
    async fn test_timeout_with_available_tokens() {
        let bounds = TokenCalcBounds::new(5, 3, Duration::from_secs(1));
        let rate_limiter = RateLimit::<WithoutState>::new(bounds).unwrap();

        // Should return immediately when tokens are available
        let start = std::time::Instant::now();
        let tokens = rate_limiter
            .acquire_n(Some(2), Some(Duration::from_secs(1)))
            .await;
        let elapsed = start.elapsed();

        assert_eq!(tokens, 2);
        assert!(elapsed < Duration::from_millis(100)); // Should return immediately
    }

    #[tokio::test]
    async fn test_timeout_when_tokens_exhausted() {
        let bounds = TokenCalcBounds::new(5, 2, Duration::from_secs(1));
        let rate_limiter = RateLimit::<WithoutState>::new(bounds).unwrap();

        rate_limiter.last_queried_epoch.store(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            std::sync::atomic::Ordering::Release,
        );

        // Consume all available tokens
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 2);

        // Try to acquire more tokens
        let tokens = rate_limiter.acquire_n(Some(1), None).await;
        assert_eq!(tokens, 0);
    }

    #[tokio::test]
    async fn test_no_timeout_returns_immediately() {
        let bounds = TokenCalcBounds::new(5, 2, Duration::from_secs(1));
        let rate_limiter = RateLimit::<WithoutState>::new(bounds).unwrap();
        rate_limiter.last_queried_epoch.store(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            std::sync::atomic::Ordering::Release,
        );

        // Consume all available tokens
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 2);

        // Should return immediately with 0 tokens when none available and no timeout
        let start = std::time::Instant::now();
        let tokens = rate_limiter.acquire_n(Some(1), None).await;
        let elapsed = start.elapsed();

        assert_eq!(tokens, 0);
        // Should return immediately
        assert!(elapsed < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_timeout_waits_for_next_epoch() {
        let bounds = TokenCalcBounds::new(5, 2, Duration::from_secs(1));
        let rate_limiter = RateLimit::<WithoutState>::new(bounds).unwrap();
        rate_limiter.last_queried_epoch.store(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            std::sync::atomic::Ordering::Release,
        );

        // Consume all available tokens
        let tokens = rate_limiter.acquire_n(None, None).await;
        assert_eq!(tokens, 2);

        // Force time passage by resetting epoch
        rate_limiter
            .last_queried_epoch
            .store(0, std::sync::atomic::Ordering::Release);

        // Should wait and get tokens from next epoch when timeout is provided
        let start = std::time::Instant::now();
        let tokens = rate_limiter
            .acquire_n(Some(1), Some(Duration::from_secs(2)))
            .await;
        let elapsed = start.elapsed();

        assert_eq!(tokens, 1);
        // Should return quickly since we forced epoch reset
        assert!(elapsed < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_attempt_acquire_n_with_state_rate_limiter_single_pod() {
        use crate::state::OptimisticValidityUpdateSecs;
        use crate::state::store::in_memory_store::InMemoryStore;

        let bounds = TokenCalcBounds::new(20, 10, Duration::from_secs(10));
        // time 0 -> 10
        // time 1 -> 11
        // time 2 -> 12
        // time 3 -> 13
        // time 10 -> 20
        let store = InMemoryStore::new();
        let cancel = CancellationToken::new();
        let refresh_interval = Duration::from_millis(100);
        let runway_update = OptimisticValidityUpdateSecs::default();

        // expected = [10,11,12 .. .20]
        // input = for from=time to=time+end
        // result = [] populated while running for loop
        // assert_eq!(result, expected)

        // Create a single distributed rate limiter
        let rate_limiter = RateLimit::<WithState<InMemoryStore>>::new(
            bounds,
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update,
        )
        .await
        .unwrap();

        // initial state(0th second) we can acquire min number of tokens.
        let mut cur_epoch = 0;
        let attempt = rate_limiter.attempt_acquire_n(None, cur_epoch).await;
        assert_eq!(attempt, 10, "Single pod should get full burst tokens");

        // Since the slope is 1, we should get 1 more token each second until we reach
        // the max rate limit (20).
        for i in 1..10 {
            cur_epoch += 1;
            let attempt = rate_limiter.attempt_acquire_n(None, cur_epoch).await;
            assert_eq!(
                attempt,
                10 + i,
                "No tokens should be available in same epoch"
            );
        }

        let attempt = rate_limiter.attempt_acquire_n(None, cur_epoch).await;
        assert_eq!(attempt, 0, "No tokens should be available in same epoch");

        let attempt = rate_limiter.attempt_acquire_n(None, cur_epoch + 1).await;
        assert_eq!(attempt, 20, "No tokens should be available in same epoch");

        // Clean up
        cancel.cancel();
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
        let rate_limiter = RateLimit::<WithState<InMemoryStore>>::new(
            bounds,
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update,
        )
        .await
        .unwrap();
        rate_limiter.last_queried_epoch.store(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
            std::sync::atomic::Ordering::Release,
        );

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

    /// Test distributed rate limiter with multiple pods (1 or more)
    /// using InMemoryStore as the state store
    #[tokio::test]
    async fn test_distributed_rate_limiter_multiple_pods_in_memory() {
        let test_cases = vec![
            // Fractional slope (>1) with multiple pods
            // Acquire all tokens at each epoch
            // Immediately ask for tokens after first epoch
            test_utils::TestCase::new(
                60, 15, Duration::from_secs(10), 2,
                vec![(None,1), (None,0), (None, 1), (None, 1), (None, 1), (None, 1), (None, 1), (None, 1), (None, 1), (None, 1), (None, 1), (None, 1)],
                vec![7, 9, 0, 12, 14, 16, 18, 21, 23, 25, 27, 30],
            ),
            // Fractional slope (>1) with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(
                60,
                15,
                Duration::from_secs(10),
                2,
                vec![(None,1); 11],
                vec![7, 9, 12, 14, 16, 18, 21, 23, 25, 27, 30],
            ),
            // Fractional slope (>1) with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                60,
                15,
                Duration::from_secs(10),
                2,
                vec![(Some(20),1); 11],
                vec![7, 9, 12, 14, 16, 18, 20, 20, 20, 20, 20],
            ),
            // Fractional slope (<1) with multiple pods
            // Acquire all tokens
            // Immediately ask for tokens in the same epoch after receiving 1 token
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                2,
                vec![(None,1), (None, 1), (None, 1), (None,1), (None, 1), (None, 1), (None,1), (None, 1), (None, 1), (None,1), (None, 0), (None, 1)],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
            ),
            // Fractional slope (<1) with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                2,
                vec![(None,1); 11],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            ),
            // Fractional slope (<1) with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                2,
                vec![(Some(1),1); 11],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            ),
            // Integer slope with multiple pods
            // Acquire tokens more than max
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(Some(30),1); 11],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10],
            ),
            // Integer slope with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None,1); 11],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10],
            ),
            // Integer slope with multiple pods
            // Acquire tokens less than max
            // Combination of immediate and non-immediate token requests
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(Some(1),0), (Some(2),1), (Some(1),0), (Some(3),1), (Some(1),0), (Some(4),2), (Some(1),0), (Some(10),1), (Some(2),0), (None,1), (None,1), (None,1), (None,1), (None,1), (None,1), (Some(5),1), (None,1)],
                vec![1, 2, 1, 3, 1, 4, 1, 5, 2, 5, 7, 8, 8, 9, 9, 5, 10],
            ),
            // Integer slope with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(Some(1),1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            ),
            // Fractional slope with single pod
            // Acquire all tokens
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                1,
                vec![(None,1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2],
            ),
            // Fractional slope with single pod
            // Acquire tokens more than max
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                1,
                vec![(Some(5),1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2],
            ),
            // Fractional slope with single pod
            // Acquire tokens less than max
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                1,
                vec![(Some(1),1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            ),
        ];
        test_utils::run_distributed_rate_limiter_multiple_pods_test_cases(test_cases).await;
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
        let rate_limiter_1 = RateLimit::<WithState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
        )
        .await
        .unwrap();

        let rate_limiter_2 = RateLimit::<WithState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_2",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
        )
        .await
        .unwrap();

        let rate_limiter_3 = RateLimit::<WithState<InMemoryStore>>::new(
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

        // Due to time-based truncation and distributed consensus, we can't expect exact values,
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
        let rate_limiter_1 = RateLimit::<WithState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
        )
        .await
        .unwrap();

        let rate_limiter_2 = RateLimit::<WithState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_2",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
        )
        .await
        .unwrap();

        let rate_limiter_3 = RateLimit::<WithState<RedisStore>>::new(
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

    /// Test distributed rate limiter with multiple pods (1 or more) using Redis as the state store
    #[tokio::test]
    #[cfg(feature = "redis-tests")]
    async fn test_distributed_rate_limiter_multiple_pods_redis() {
        let test_cases = vec![
            // Fractional slope (>1) with multiple pods
            // Acquire all tokens at each epoch
            // Immediately ask for tokens after first epoch
            test_utils::TestCase::new(
                60, 15, Duration::from_secs(10), 2,
                vec![(None,1), (None,0), (None, 1), (None, 1), (None, 1), (None, 1), (None, 1), (None, 1), (None, 1), (None, 1), (None, 1), (None, 1)],
                vec![7, 9, 0, 12, 14, 16, 18, 21, 23, 25, 27, 30])
                .store_type(test_utils::StoreType::Redis)
                .test_name("RedisStore Test params: max_tokens=60, burst_tokens=15, duration=10s, pod_count=2".to_string()),
            // Fractional slope (>1) with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(60, 15, Duration::from_secs(10), 2,
                vec![(None,1); 11],
                vec![7, 9, 12, 14, 16, 18, 21, 23, 25, 27, 30])
                .store_type(test_utils::StoreType::Redis)
                .test_name("RedisStore Test params: max_tokens=60, burst_tokens=15, duration=10s, pod_count=2".to_string()),
            // Fractional slope (>1) with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                60,
                15,
                Duration::from_secs(10),
                2,
                vec![(Some(20),1); 11],
                vec![7, 9, 12, 14, 16, 18, 20, 20, 20, 20, 20],
            ).store_type(test_utils::StoreType::Redis)
                .test_name("RedisStore Test params: max_tokens=60, burst_tokens=15, duration=10s, pod_count=2".to_string()),
            // Fractional slope (<1) with multiple pods
            // Acquire all tokens
            // Immediately ask for tokens in the same epoch after receiving 1 token
            test_utils::TestCase::new(2, 1, Duration::from_secs(10), 2,
                vec![(None,1), (None, 1), (None, 1), (None,1), (None, 1), (None, 1), (None,1), (None, 1), (None, 1), (None,1), (None, 0), (None, 1)],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0])
                .store_type(test_utils::StoreType::Redis)
                .test_name("RedisStore Test params: max_tokens=2, burst_tokens=1, duration=10s, pod_count=2".to_string()),
            // Fractional slope (<1) with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(2, 1, Duration::from_secs(10), 2,
                vec![(None,1); 11],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
                .store_type(test_utils::StoreType::Redis)
                .test_name("RedisStore Test params: max_tokens=2, burst_tokens=1, duration=10s, pod_count=2".to_string()),
            // Fractional slope (<1) with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(2, 1, Duration::from_secs(10), 2,
                vec![(Some(1),1); 11],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
                .store_type(test_utils::StoreType::Redis)
                .test_name("RedisStore Test params: max_tokens=2, burst_tokens=1, duration=10s, pod_count=2".to_string()),
            // Integer slope with multiple pods
            // Acquire tokens more than max
            test_utils::TestCase::new(20, 10, Duration::from_secs(10), 2,
                vec![(Some(30),1); 11],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10])
                .store_type(test_utils::StoreType::Redis)
                .test_name("RedisStore Test params: max_tokens=20, burst_tokens=10, duration=10s, pod_count=2".to_string()),
            // Integer slope with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(20, 10, Duration::from_secs(10), 2,
                vec![(None,1); 11],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10])
                .store_type(test_utils::StoreType::Redis)
                .test_name("RedisStore Test params: max_tokens=20, burst_tokens=10, duration=10s, pod_count=2".to_string()),
            // Integer slope with multiple pods
            // Acquire tokens less than max
            // Combination of immediate and non-immediate token requests
            test_utils::TestCase::new(20, 10, Duration::from_secs(10), 2,
                vec![(Some(1),0), (Some(2),1), (Some(1),0), (Some(3),1), (Some(1),0), (Some(4),2), (Some(1),0), (Some(10),1), (Some(2),0), (None,1), (None,1), (None,1), (None,1), (None,1), (None,1), (Some(5),1), (None,1)],
                vec![1, 2, 1, 3, 1, 4, 1, 5, 2, 5, 7, 8, 8, 9, 9, 5, 10])
                .store_type(test_utils::StoreType::Redis)
                .test_name("RedisStore Test params: max_tokens=20, burst_tokens=10, duration=10s, pod_count=2".to_string()),
            // Integer slope with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(20, 10, Duration::from_secs(10), 2,
                vec![(Some(1),1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
                .store_type(test_utils::StoreType::Redis)
                .test_name("RedisStore Test params: max_tokens=20, burst_tokens=10, duration=10s, pod_count=2".to_string()),
            // Fractional slope with single pod
            // Acquire all tokens
            test_utils::TestCase::new(2, 1, Duration::from_secs(10), 1,
                vec![(None,1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2])
                .store_type(test_utils::StoreType::Redis)
                .test_name("RedisStore Test params: max_tokens=2, burst_tokens=1, duration=10s, pod_count=1".to_string()),
            // Fractional slope with single pod
            // Acquire tokens more than max
            test_utils::TestCase::new(2, 1, Duration::from_secs(10), 1,
                vec![(Some(5),1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2])
                .store_type(test_utils::StoreType::Redis)
                .test_name("RedisStore Test params: max_tokens=2, burst_tokens=1, duration=10s, pod_count=1".to_string()),
            // Fractional slope with single pod
            // Acquire tokens less than max
            test_utils::TestCase::new(2,1,Duration::from_secs(10),1,
                vec![(Some(1),1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
            .store_type(test_utils::StoreType::Redis)
            .test_name("RedisStore Test params: max_tokens=2, burst_tokens=1, duration=10s, pod_count=1".to_string()),
        ];

        test_utils::run_distributed_rate_limiter_multiple_pods_test_cases(test_cases).await;
    }
}
