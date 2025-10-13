use crate::state::OptimisticValidityUpdateSecs;
use crate::state::store::Store;
use state::RateLimiterState;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex};
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

    /// Deposit tokens into the rate limiter.
    /// This will be used to deposit tokens into the rate limiter.
    async fn deposit_unused(&self, n: usize);

    /// Shutdown the rate limiter and clean up resources.
    /// This will deregister the processor from the distributed store and stop any background tasks.
    async fn shutdown(&self) -> Result<()>;
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
    /// Computes the number of tokens to be refilled for the next epoch based on various modes
    /// and returns the number of tokens to be used for refilling token pool.
    /// <br>
    /// Takes following arguments:
    /// * `cur_epoch` - Current epoch
    pub(crate) fn compute_refill(&self, cur_epoch: u64) -> usize {
        // The lock needs to be held for the entire duration of the refill computation
        // since we can't lock the max_ever_filled within each utility function as it can lead to
        // deadlocks, due to utility functions calling each other.
        let mut max_ever_filled = self.max_ever_filled.lock().unwrap();

        // Whatever remains in the token pool after previous epoch are the unused tokens,
        // irrespective of whether the tokens were deposited back or never asked for.
        let unused_tokens = self.token.load(std::sync::atomic::Ordering::Relaxed) as f32;
        // Calculate the percentage of tokens that were used in previous epoch
        let used_token_percentage =
            ((1.0 - unused_tokens / *max_ever_filled) * 100.0).round() as usize;

        // We need to compensate for the unusable tokens in the pool that the rate limiter cannot give out.
        // This is because the rate limiter cannot give out fractional tokens.
        // E.g., given the following scenario:
        // min=10, max=20, slope=1, pods=2
        // After the first refill, the max_ever_refill will be 11,
        // but the rate limiter can only give out 10 tokens in total, across the 2 processors.
        // Thus, we need to account for unused token in the pool that the processor cannot give out.
        let floor_normalization =
            ((self.token_calc_bounds.slope / *max_ever_filled) * 100.0).round() as usize;

        // The rate limiter cannot give out all the tokens only when the max_ever_filled is indivisible
        // between the known_pool_size, but since the known_pool_size is not available here we'll just
        // add the error_percentage to the used_token_percentage.
        let used_token_percentage = (used_token_percentage + floor_normalization).clamp(0, 100);

        match &self.token_calc_bounds.mode {
            Mode::Relaxed => self.relaxed_slope_increase(&mut max_ever_filled),
            Mode::Scheduled => self.scheduled_slope_increase(cur_epoch, &mut max_ever_filled),
            Mode::OnlyIfUsed(threshold_percentage) => {
                let threshold_percentage = (*threshold_percentage).clamp(0, 100);
                self.only_if_used_slope_increase(
                    &mut max_ever_filled,
                    used_token_percentage,
                    threshold_percentage,
                )
            }
            Mode::GoBackN(go_back_n_config) => self.go_back_n_slope_increase(
                &mut max_ever_filled,
                used_token_percentage,
                go_back_n_config,
                cur_epoch,
            ),
        }
    }

    /// Computes the number of tokens to be refilled for the next epoch for relaxed mode.
    ///
    /// If there is some traffic, then release the max possible tokens
    fn relaxed_slope_increase(&self, max_ever_filled: &mut f32) -> usize {
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

    /// Computes the number of tokens to be refilled for the next epoch for scheduled mode.
    ///
    /// We will release/increase tokens on a schedule even if it is not used
    fn scheduled_slope_increase(&self, cur_epoch: u64, max_ever_filled: &mut f32) -> usize {
        // let's make sure we do not go beyond the max
        if *max_ever_filled >= self.token_calc_bounds.max as f32 {
            self.token_calc_bounds.max
        } else {
            let prev_epoch = self
                .last_queried_epoch
                .load(std::sync::atomic::Ordering::Relaxed);
            let time_diff = cur_epoch.saturating_sub(prev_epoch) as f32;
            let refill = *max_ever_filled + self.token_calc_bounds.slope * (time_diff);
            let capped_refill = refill.min(self.token_calc_bounds.max as f32);

            // Update the fractional value
            *max_ever_filled = capped_refill;

            capped_refill as usize
        }
    }

    /// Computes the number of tokens to be refilled for the next epoch for only-if-used mode.
    ///
    /// If the max available tokens are not used, then we will refill the token pool size by the same
    /// amount as max_ever_filled.
    /// If the tokens used are within the threshold % of max_ever_filled, then we’ll increase by slope
    fn only_if_used_slope_increase(
        &self,
        max_ever_filled: &mut f32,
        used_token_percentage: usize,
        used_threshold_percentage: usize,
    ) -> usize {
        // If the used token percentage is greater than the threshold provided in config, then
        // we'll increase the tokens by slope. Otherwise, we'll keep the tokens as it is.
        if used_token_percentage >= used_threshold_percentage {
            self.relaxed_slope_increase(max_ever_filled)
        } else {
            *max_ever_filled as usize
        }
    }

    /// Computes the number of tokens to be refilled for the next epoch for go-back-n mode.
    ///
    /// If the tokens aren't used within the last n epochs
    /// - then we will reduce the token pool size by slope * (n - 1) before calculating the next increase.
    ///
    /// If the tokens are used within next epoch,
    /// - then we'll check if the tokens used are lower than threshold % of max_ever_filled,
    ///     - then we’ll reduce the token pool size by slope for that iteration,
    /// - otherwise we'll increase the token pool size by slope.
    ///
    /// Penalty for delayed usage <br>
    /// Penalty for underutilization
    fn go_back_n_slope_increase(
        &self,
        max_ever_filled: &mut f32,
        used_token_percentage: usize,
        go_back_n_config: &GoBackNConfig,
        cur_epoch: u64,
    ) -> usize {
        let prev_epoch = self
            .last_queried_epoch
            .load(std::sync::atomic::Ordering::Relaxed);
        let time_diff = cur_epoch.saturating_sub(prev_epoch) as f32;

        let GoBackNConfig {
            cool_down_period,
            ramp_down_percentage,
            utilization_threshold,
        } = go_back_n_config;

        // if the call is being made after a gap of more than cool down period seconds
        // then we'll reduce the amount to be refilled
        //
        // - penalize for delay in usage
        if time_diff > *cool_down_period as f32 {
            // If the tokens haven't been refilled for a while then reduce the amount to be refilled
            // equivalent to slope * (time_diff - 1)
            // We're using (time_diff - 1) here to make sure we don't decrease the amount to be
            // refilled if the calls were made between subsequent epochs
            let reduced_refill = *max_ever_filled
                - (*ramp_down_percentage as f32 / 100.0)
                    * self.token_calc_bounds.slope
                    * (time_diff - 1.0);
            // Make sure we do not go below the min or above the max
            let capped_refill = reduced_refill
                .max(self.token_calc_bounds.min as f32)
                .min(self.token_calc_bounds.max as f32);
            *max_ever_filled = capped_refill;
            capped_refill as usize
        } else if used_token_percentage >= *utilization_threshold {
            // If consecutive calls have been made and the used token percentage is greater than
            // the threshold provided in config, then we'll increase the tokens according to relaxed mode.
            self.relaxed_slope_increase(max_ever_filled)
        } else {
            // If consecutive calls have been made and the used token percentage is less than
            // the threshold provided in config, then we'll reduce the token pool by slope.
            // - penalize for underutilization
            //
            // Reduce the amount of tokens to refill by slope
            let reduced_refill = *max_ever_filled - self.token_calc_bounds.slope;
            // Make sure we do not go below the min
            *max_ever_filled = reduced_refill.max(self.token_calc_bounds.min as f32);
            *max_ever_filled as usize
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

        let new_token_count = self.compute_refill(cur_epoch);
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

    /// Tries to deposit unused tokens (non-blocking), if the tokens don't belong to the current
    /// epoch's budget, then it is a no-op.
    async fn attempt_deposit_unused(&self, n: usize, cur_epoch: u64) {
        if cur_epoch
            != self
                .last_queried_epoch
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }

        // Track the unused tokens
        self.token
            .fetch_add(n, std::sync::atomic::Ordering::Release);
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

    async fn deposit_unused(&self, n: usize) {
        let cur_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        self.attempt_deposit_unused(n, cur_epoch).await;
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
        let next_total_tokens = self.compute_refill(cur_epoch);

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

    /// Tries to deposit unused tokens (non-blocking), if the tokens don't belong to the current
    /// epoch, then it is a no-op.
    async fn attempt_deposit_unused(&self, n: usize, cur_epoch: u64) {
        if cur_epoch
            != self
                .last_queried_epoch
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }

        let known_pool_size = self
            .state
            .0
            .known_pool_size
            .load(std::sync::atomic::Ordering::Acquire);

        // Track the unused tokens. We multiply by pool size because while grabbing tokens,
        // we divide by pool size thinking every other processor will be grabbing the same amount
        // of tokens.
        self.token
            .fetch_add(n * known_pool_size, std::sync::atomic::Ordering::Release);
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

    /// Deposit unused tokens back into the rate limiter only if the tokens were acquired in the
    /// same epoch used to acquire the tokens.
    async fn deposit_unused(&self, n: usize) {
        let cur_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        self.attempt_deposit_unused(n, cur_epoch).await;
    }

    async fn shutdown(&self) -> Result<()> {
        let max_ever_filled = *self.max_ever_filled.lock().unwrap();
        self.state.0.shutdown(max_ever_filled).await
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
        resume_ramp_up: bool,
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

        // If resume ramp up is enabled, use the max filled from the recently deregistered processor
        // with same name, otherwise, use the min tokens
        let burst = if resume_ramp_up {
            (*state.prev_max_filled as usize).clamp(token_calc_bounds.min, token_calc_bounds.max)
        } else {
            token_calc_bounds.min
        };

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

/// Configuration for GoBackN mode
/// This needs to be pub since
#[derive(Clone, Debug)]
pub struct GoBackNConfig {
    /// Cool down period in seconds
    /// Time in seconds for which, if no call is made to the rate_limiter,
    /// the token pool size will be ramped down
    pub(crate) cool_down_period: usize,
    /// Ramp down percentage is the percentage of slope by which the token pool size will be ramped down.
    /// The amount by which the token pool size will be ramped down. Ranges between 0 and 100.
    /// 0 means no ramp down, else token pool is reduced by (ramp_down_strength / 100) * slope * (cool_down_period - 1)
    pub(crate) ramp_down_percentage: usize,
    /// Utilization threshold
    /// The capacity utilization percentage that should be consumed before ramping down the token pool size
    /// by ramp_down_strength
    pub(crate) utilization_threshold: usize,
}

impl GoBackNConfig {
    pub fn new(
        cool_down_period: usize,
        ramp_down_percentage: usize,
        utilization_threshold: usize,
    ) -> Self {
        GoBackNConfig {
            cool_down_period,
            ramp_down_percentage,
            utilization_threshold,
        }
    }
}

/// Rate limiting/Throttling mode
#[derive(Clone, Debug)]
pub enum Mode {
    /// We will release/increase tokens on a schedule even if it is not used
    Scheduled,
    /// If there is some traffic, then release the max possible tokens
    Relaxed,
    /// If the max available tokens are not used, then we will refill the token pool size by the same
    /// amount as max_ever_filled.
    ///
    /// If the tokens used are within the threshold % of max_ever_filled, then we’ll increase by slope
    OnlyIfUsed(usize),
    /// If the tokens aren't used within the last n epochs
    /// - then we will reduce the token pool size by slope * (n - 1) before calculating the next increase.
    ///
    /// If the tokens are used within next epoch,
    /// - then we'll check if the tokens used are lower than threshold % of max_ever_filled,
    ///     - then we’ll reduce the token pool size by slope for that iteration,
    /// - otherwise we'll increase the token pool size by slope.
    ///
    /// Penalty for delayed usage
    ///
    /// Penalty for underutilization
    GoBackN(GoBackNConfig),
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
    pub fn new(max: usize, min: usize, duration: Duration, mode: Mode) -> Self {
        TokenCalcBounds {
            slope: (max - min) as f32 / duration.as_secs_f32(),
            max,
            min,
            mode,
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

    async fn deposit_unused(&self, _: usize) {}

    async fn shutdown(&self) -> crate::Result<()> {
        // No-op for NoOpRateLimiter as there are no resources to clean up
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::store::in_memory_store::InMemoryStore;
    use std::time::SystemTime;
    use tokio::time::Duration;

    /// Test utilities for integration tests
    mod test_utils {
        use crate::state::store::in_memory_store::InMemoryStore;
        use crate::state::store::redis_store::{RedisMode, RedisStore};
        use crate::state::{OptimisticValidityUpdateSecs, Store};
        use crate::{Mode, RateLimit, RateLimiter, TokenCalcBounds, WithState};
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
            /// Tokens expected to be returned by rate limiter in each iteration to *each* pod
            pub(super) expected_tokens: Vec<usize>,
            /// Vector of tokens that will be deposited back by the caller to each processor
            /// Since each caller deposits tokens back to the rate limiter, this vector should
            /// have the same length as asked_tokens.
            pub(super) deposited_tokens: Vec<usize>,
            /// The throttling mode of the rate limiter
            /// defaults to Relaxed
            pub(super) mode: Mode,
        }

        impl TestCase {
            // Creates a new test case with certain default values
            // Created to make it easier to add new params to test cases
            // without changing initialization of it in existing tests.
            pub(super) fn new(
                max_tokens: usize,
                burst_tokens: usize,
                duration: Duration,
                pod_count: usize,
                asked_tokens: Vec<(Option<usize>, usize)>,
                expected_tokens: Vec<usize>,
            ) -> Self {
                let asked_tokens_len = asked_tokens.len();
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
                    deposited_tokens: vec![0; asked_tokens_len],
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

            pub(super) fn mode(mut self, mode: Mode) -> Self {
                self.mode = mode;
                self
            }

            pub(super) fn deposited_tokens(mut self, deposited_tokens: Vec<usize>) -> Self {
                self.deposited_tokens = deposited_tokens;
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
                store_type: _,
                test_name,
                asked_tokens,
                expected_tokens,
                mode,
                deposited_tokens,
            } = test_case;
            let cancel = CancellationToken::new();
            let refresh_interval = Duration::from_millis(50);
            let runway_update = OptimisticValidityUpdateSecs::default();
            let bounds = TokenCalcBounds::new(max_tokens, burst_tokens, duration, mode);

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
                        false,
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
                        tokens,
                        expected_tokens[i],
                        "Test Name: {}, Iteration: {}\n\
                        Number of tokens fetched in each iteration \
                should increase by slope/pod_count until ramp up",
                        test_name,
                        i + 1
                    );
                    total_got_tokens += tokens;
                    total_expected_tokens += expected_tokens[i];

                    assert!(
                        deposited_tokens[i] <= tokens,
                        "Test Name: {}, Iteration: {}\n\
                        Invalid test case. Number of tokens deposited back ({}) can't \
                        be greater that tokens given by the rate limiter ({})",
                        test_name,
                        i + 1,
                        deposited_tokens[i],
                        tokens
                    );
                    rate_limiter
                        .attempt_deposit_unused(deposited_tokens[i], cur_epoch)
                        .await;
                }

                assert_eq!(
                    total_got_tokens,
                    total_expected_tokens,
                    "Test Name: {}, Iteration: {}\n\
                    Total number of tokens fetched in each iteration \
                should be less than or equal to total expected tokens for each processor",
                    test_name,
                    i + 1
                );

                // Determines after how many epochs next pull is going to be made.
                cur_epoch += asked_tokens[i].1 as u64;
            }

            for rate_limiter in rate_limiters.iter() {
                rate_limiter
                    .shutdown()
                    .await
                    .expect("Rate limiter failed to shutdown");
            }
        }

        /// Utility function to run distributed rate limiter multiple pods test cases
        /// Only here to iterate over the different test cases and initialize stores to
        /// be used by the test cases.
        pub(super) async fn run_distributed_rate_limiter_multiple_pods_test_cases(
            // The test cases to run
            test_cases: Vec<TestCase>,
        ) {
            for test_case in test_cases {
                let store_type = test_case.store_type.clone();
                let test_name = test_case.test_name.clone();
                let temp_test_name = test_name.clone();

                // Determining/initializing the store based on the store type here
                // instead of passing a cloned store so that each test case gets its own store
                // Necessary for redis store test cases.
                match store_type {
                    StoreType::InMemory => {
                        let store = InMemoryStore::new(180);
                        test_rate_limiter_with_state(store, test_case).await;
                    }
                    StoreType::Redis => {
                        let store =
                            match create_test_redis_store(temp_test_name.as_str(), 180).await {
                                Some(store) => store,
                                None => return, // Skip test if Redis is not available
                            };

                        test_rate_limiter_with_state(store, test_case).await;
                        cleanup_redis_keys(test_name.as_str());
                    }
                }
            }
        }

        /// Creates a Redis store for testing with a unique key prefix
        /// Returns None if Redis is not available
        pub async fn create_test_redis_store(
            test_name: &str,
            stale_age: usize,
        ) -> Option<RedisStore> {
            let redis_url = "redis://127.0.0.1:6379";

            // Check if Redis is available
            if redis::Client::open(redis_url).is_err() {
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

            match RedisStore::new(test_key_prefix, stale_age, redis_mode).await {
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
                    .arg(format!("{}:prev_max_filled", test_key_prefix))
                    .arg(format!("{}:prev_max_filled_ttl", test_key_prefix))
                    .query(&mut conn);
            }
        }
    }

    #[tokio::test]
    async fn test_acquire_all_tokens() {
        let bounds = TokenCalcBounds::new(10, 5, Duration::from_secs(1), Mode::Relaxed);
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
        let bounds = TokenCalcBounds::new(10, 5, Duration::from_secs(1), Mode::Relaxed);
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
        let bounds = TokenCalcBounds::new(10, 3, Duration::from_secs(1), Mode::Relaxed);
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
        let bounds = TokenCalcBounds::new(10, 2, Duration::from_secs(1), Mode::Relaxed);
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
        let bounds = TokenCalcBounds::new(5, 2, Duration::from_secs(1), Mode::Relaxed);
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
        let bounds = TokenCalcBounds::new(20, 5, Duration::from_secs(2), Mode::Relaxed);
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
        let bounds = TokenCalcBounds::new(2, 1, Duration::from_secs(10), Mode::Relaxed);
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
        let bounds = TokenCalcBounds::new(10, 10, Duration::from_secs(1), Mode::Relaxed);
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
        let bounds = TokenCalcBounds::new(5, 3, Duration::from_secs(1), Mode::Relaxed);
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
        let bounds = TokenCalcBounds::new(5, 2, Duration::from_secs(1), Mode::Relaxed);
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
        let bounds = TokenCalcBounds::new(5, 2, Duration::from_secs(1), Mode::Relaxed);
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
        let bounds = TokenCalcBounds::new(5, 2, Duration::from_secs(1), Mode::Relaxed);
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

        let bounds = TokenCalcBounds::new(20, 10, Duration::from_secs(10), Mode::Relaxed);
        // time 0 -> 10
        // time 1 -> 11
        // time 2 -> 12
        // time 3 -> 13
        // time 10 -> 20
        let store = InMemoryStore::default();
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
            false,
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

        let bounds = TokenCalcBounds::new(20, 10, Duration::from_secs(1), Mode::Relaxed);
        let store = InMemoryStore::default();
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
            false,
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
                60,
                15,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 0),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![7, 9, 0, 12, 14, 16, 18, 21, 23, 25, 27, 30],
            ),
            // Fractional slope (>1) with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(
                60,
                15,
                Duration::from_secs(10),
                2,
                vec![(None, 1); 11],
                vec![7, 9, 12, 14, 16, 18, 21, 23, 25, 27, 30],
            ),
            // Fractional slope (>1) with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                60,
                15,
                Duration::from_secs(10),
                2,
                vec![(Some(20), 1); 11],
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
                vec![
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 0),
                    (None, 1),
                ],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
            ),
            // Fractional slope (<1) with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                2,
                vec![(None, 1); 11],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            ),
            // Fractional slope (<1) with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                2,
                vec![(Some(1), 1); 11],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            ),
            // Integer slope with multiple pods
            // Acquire tokens more than max
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(Some(30), 1); 11],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10],
            ),
            // Integer slope with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 1); 11],
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
                vec![
                    (Some(1), 0),
                    (Some(2), 1),
                    (Some(1), 0),
                    (Some(3), 1),
                    (Some(1), 0),
                    (Some(4), 2),
                    (Some(1), 0),
                    (Some(10), 1),
                    (Some(2), 0),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (Some(5), 1),
                    (None, 1),
                ],
                vec![1, 2, 1, 3, 1, 4, 1, 5, 2, 5, 7, 8, 8, 9, 9, 5, 10],
            ),
            // Integer slope with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(Some(1), 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            ),
            // Fractional slope with single pod
            // Acquire all tokens
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                1,
                vec![(None, 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2],
            ),
            // Fractional slope with single pod
            // Acquire tokens more than max
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                1,
                vec![(Some(5), 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2],
            ),
            // Fractional slope with single pod
            // Acquire tokens less than max
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                1,
                vec![(Some(1), 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            ),
        ];
        test_utils::run_distributed_rate_limiter_multiple_pods_test_cases(test_cases).await;
    }

    #[tokio::test]
    async fn test_distributed_rate_limiter_scheduled_mode() {
        let test_cases = vec![
            // Integer slope with multiple pods
            // Keep acquiring min tokens with regular gaps in epochs between calls
            // Acquire None tokens here and there to check the max tokens shelled out.
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (Some(2), 2),
                    (Some(2), 2),
                    (None, 2),
                    (Some(2), 2),
                    (None, 2),
                    (Some(2), 2),
                    (None, 2),
                    (None, 2),
                ],
                vec![2, 2, 7, 2, 9, 2, 10, 10],
            )
            .test_name("test_distributed_rate_limiter_scheduled_mode_1".to_string())
            .mode(Mode::Scheduled),
            // Integer slope with multiple pods
            // Acquire tokens more than max tokens after extended gaps between epochs
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (Some(30), 1),
                    (Some(30), 3),
                    (Some(30), 2),
                    (Some(30), 4),
                    (Some(30), 1),
                    (Some(30), 1),
                ],
                vec![5, 5, 7, 8, 10, 10],
            )
            .test_name("test_distributed_rate_limiter_scheduled_mode_2".to_string())
            .mode(Mode::Scheduled),
            // Fractional slope (>1) with multiple pods
            // Acquire all tokens at each epoch
            // Immediately ask for tokens after first epoch
            test_utils::TestCase::new(
                60,
                15,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 0),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![7, 9, 0, 12, 14, 16, 18, 21, 23, 25, 27, 30],
            )
            .test_name("test_distributed_rate_limiter_scheduled_mode_3".to_string())
            .mode(Mode::Scheduled),
            // Fractional slope (>1) with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(
                60,
                15,
                Duration::from_secs(10),
                2,
                vec![(None, 1); 11],
                vec![7, 9, 12, 14, 16, 18, 21, 23, 25, 27, 30],
            )
            .test_name("test_distributed_rate_limiter_scheduled_mode_4".to_string())
            .mode(Mode::Scheduled),
            // Fractional slope (>1) with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                60,
                15,
                Duration::from_secs(10),
                2,
                vec![(Some(20), 1); 11],
                vec![7, 9, 12, 14, 16, 18, 20, 20, 20, 20, 20],
            )
            .test_name("test_distributed_rate_limiter_scheduled_mode_5".to_string())
            .mode(Mode::Scheduled),
            // Fractional slope (<1) with multiple pods
            // Acquire all tokens
            // Immediately ask for tokens in the same epoch after receiving 1 token
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 0),
                    (None, 1),
                ],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
            )
            .test_name("test_distributed_rate_limiter_scheduled_mode_6".to_string())
            .mode(Mode::Scheduled),
            // Fractional slope (<1) with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                2,
                vec![(None, 1); 11],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            )
            .test_name("test_distributed_rate_limiter_scheduled_mode_7".to_string())
            .mode(Mode::Scheduled),
            // Fractional slope (<1) with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                2,
                vec![(Some(1), 1); 11],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            )
            .test_name("test_distributed_rate_limiter_scheduled_mode_8".to_string())
            .mode(Mode::Scheduled),
            // Integer slope with multiple pods
            // Acquire tokens more than max
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(Some(30), 1); 11],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10],
            )
            .test_name("test_distributed_rate_limiter_scheduled_mode_9".to_string())
            .mode(Mode::Scheduled),
            // Integer slope with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 1); 11],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10],
            )
            .test_name("test_distributed_rate_limiter_scheduled_mode_10".to_string())
            .mode(Mode::Scheduled),
            // Integer slope with multiple pods
            // Acquire tokens less than max
            // Combination of immediate and non-immediate token requests
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (Some(1), 0),
                    (Some(2), 1),
                    (Some(1), 0),
                    (Some(3), 1),
                    (Some(1), 0),
                    (Some(4), 2),
                    (Some(1), 0),
                    (Some(10), 1),
                    (Some(2), 0),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (Some(5), 1),
                    (None, 1),
                ],
                vec![1, 2, 1, 3, 1, 4, 1, 6, 2, 5, 8, 8, 9, 9, 10, 5, 10],
            )
            .test_name("test_distributed_rate_limiter_scheduled_mode_11".to_string())
            .mode(Mode::Scheduled),
            // Integer slope with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(Some(1), 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            )
            .test_name("test_distributed_rate_limiter_scheduled_mode_12".to_string())
            .mode(Mode::Scheduled),
            // Fractional slope with single pod
            // Acquire all tokens
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                1,
                vec![(None, 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2],
            )
            .test_name("test_distributed_rate_limiter_scheduled_mode_13".to_string())
            .mode(Mode::Scheduled),
            // Fractional slope with single pod
            // Acquire tokens more than max
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                1,
                vec![(Some(5), 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2],
            )
            .test_name("test_distributed_rate_limiter_scheduled_mode_14".to_string())
            .mode(Mode::Scheduled),
            // Fractional slope with single pod
            // Acquire tokens less than max
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                1,
                vec![(Some(1), 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            )
            .test_name("test_distributed_rate_limiter_scheduled_mode_15".to_string())
            .mode(Mode::Scheduled),
        ];
        test_utils::run_distributed_rate_limiter_multiple_pods_test_cases(test_cases).await;
    }

    #[tokio::test]
    async fn test_distributed_rate_limiter_go_back_n_mode() {
        use test_utils::StoreType;

        let default_go_back_n_config = GoBackNConfig {
            cool_down_period: 1,
            ramp_down_percentage: 100,
            utilization_threshold: 100,
        };

        let test_cases = vec![
            // Integer slope with multiple pods
            // Keep acquiring min tokens without any gaps
            // The fetched tokens should follow relaxed mode
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![2, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_1".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // Refill amount should not increase when token utilization is less than threshold
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 1), (Some(2), 1), (Some(5), 1), (None, 1), (None, 1)],
                vec![5, 2, 5, 5, 6],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_2".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // Increase in slope is delayed when there is a gap in epochs > 1
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 1), (None, 2), (None, 1), (None, 1)],
                vec![5, 5, 5, 5],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_3".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // The max_ever_filled should be not go below burst
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 3),
                    (None, 2),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 5, 5, 5, 5, 6],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_4".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // Drop in max_ever_filled should be large for large gaps in epochs
            // then continue where it left off
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 5),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 5, 6, 6, 7, 7, 8, 6, 6, 7],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_5".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // Keep acquiring max tokens without any gaps
            // The fetched tokens should follow relaxed mode if all tokens are used
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_6".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase in max_ever_filled should stall whenever max tokens aren't acquired.
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 5, 6, 6, 7, 7, 8, 8, 9],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_7".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase in max_ever_filled should stall whenever usage threshold isn't crossed.
            // Usage threshold here isn't crossed because we're constantly depositing tokens back.
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 5, 5, 5, 5, 5, 5, 5, 5],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_8".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .deposited_tokens(vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase in max_ever_filled should decrease whenever usage threshold isn't crossed.
            // Since, we can't go below the burst, the max_ever_filled stays at the burst here.
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 5, 5, 5, 5, 5, 5, 5, 5],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_9".to_string())
            .mode(Mode::GoBackN(GoBackNConfig {
                cool_down_period: 1,
                ramp_down_percentage: 100,
                utilization_threshold: 90,
            }))
            .deposited_tokens(vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase max_ever_filled whenever usage threshold is crossed
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 5, 6, 6, 7, 7, 8, 8, 9],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_10".to_string())
            .mode(Mode::GoBackN(GoBackNConfig {
                cool_down_period: 1,
                ramp_down_percentage: 100,
                utilization_threshold: 80,
            }))
            .deposited_tokens(vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // max_ever_filled should decrease whenever the usage threshold is not crossed
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 5, 5, 5, 6, 5, 6, 6, 7],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_11".to_string())
            .mode(Mode::GoBackN(GoBackNConfig {
                cool_down_period: 1,
                ramp_down_percentage: 100,
                utilization_threshold: 80,
            }))
            .deposited_tokens(vec![1, 1, 1, 1, 2, 1, 1, 2, 1, 1, 1, 1])
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // max_ever_filled should decrease whenever the usage threshold is not crossed
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 9, 8, 8, 7, 7, 6],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_12".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .deposited_tokens(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8, 7, 7, 6])
            .store_type(StoreType::InMemory),
        ];
        test_utils::run_distributed_rate_limiter_multiple_pods_test_cases(test_cases).await;
    }

    #[tokio::test]
    async fn test_distributed_rate_limiter_only_if_used_mode() {
        use test_utils::StoreType;

        let test_cases = vec![
            // Integer slope with multiple pods
            // Keep acquiring max tokens without any gaps
            // The fetched tokens should follow relaxed mode if all tokens are used
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10],
            )
            .test_name("test_distributed_rate_limiter_only_if_used_mode_1".to_string())
            .mode(Mode::OnlyIfUsed(100))
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase in max_ever_filled should stall whenever max tokens aren't acquired.
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 6, 6, 7, 7, 8, 8, 9, 9],
            )
            .test_name("test_distributed_rate_limiter_only_if_used_mode_2".to_string())
            .mode(Mode::OnlyIfUsed(100))
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase in max_ever_filled should stall whenever usage threshold isn't crossed.
            // Usage threshold here isn't crossed because we're constantly depositing tokens back.
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 5, 5, 5, 5, 5, 5, 5, 5],
            )
            .test_name("test_distributed_rate_limiter_only_if_used_mode_3".to_string())
            .mode(Mode::OnlyIfUsed(100))
            .deposited_tokens(vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase in max_ever_filled should stall whenever usage threshold isn't crossed.
            // Even though the threshold percentage is 90%, since we're depositing tokens back,
            // the actual usage threshold doesn't get crossed (hovers around 82%).
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 5, 5, 5, 5, 5, 5, 5, 5],
            )
            .test_name("test_distributed_rate_limiter_only_if_used_mode_4".to_string())
            .mode(Mode::OnlyIfUsed(90))
            .deposited_tokens(vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase max_ever_filled whenever usage threshold is crossed
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 6, 6, 7, 7, 8, 8, 9, 9],
            )
            .test_name("test_distributed_rate_limiter_only_if_used_mode_5".to_string())
            .mode(Mode::OnlyIfUsed(80))
            .deposited_tokens(vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
            .store_type(StoreType::InMemory),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase max_ever_filled should stall whenever the usage threshold is not crossed
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 6, 6, 6, 6, 7, 7, 8, 8],
            )
            .test_name("test_distributed_rate_limiter_only_if_used_mode_6".to_string())
            .mode(Mode::OnlyIfUsed(80))
            .deposited_tokens(vec![1, 1, 1, 1, 2, 1, 2, 1, 1, 1, 1, 1])
            .store_type(StoreType::InMemory),
        ];
        test_utils::run_distributed_rate_limiter_multiple_pods_test_cases(test_cases).await;
    }

    #[tokio::test]
    async fn test_distributed_rate_limiter_deposit_logic() {
        use test_utils::StoreType;

        let test_cases = vec![
            // Integer slope with multiple pods
            // Keep acquiring min tokens with regular gaps in epochs between calls
            // Acquire None tokens here and there to check the max tokens shelled out.
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(Some(2), 0), (None, 0), (None, 1), (None, 0), (Some(3), 1)],
                vec![2, 5, 2, 5, 3],
            )
            .test_name("test_distributed_rate_limiter_deposit_logic_1".to_string())
            .mode(Mode::Relaxed)
            .store_type(StoreType::InMemory)
            .deposited_tokens(vec![2, 2, 0, 3, 0]),
            // Integer slope with multiple pods
            // Keep depositing all the tokens back
            // We'll get burst tokens in each pull within the same epoch
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 0), (None, 0), (None, 0), (None, 0), (None, 1)],
                vec![5, 5, 5, 5, 5],
            )
            .test_name("test_distributed_rate_limiter_deposit_logic_2".to_string())
            .mode(Mode::Relaxed)
            .deposited_tokens(vec![5, 5, 5, 5, 5]),
            // Integer slope with multiple pods
            // keep depositing same amount of tokens in each epoch
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 1), (None, 0), (None, 1), (None, 0), (None, 1)],
                vec![5, 5, 5, 6, 5],
            )
            .test_name("test_distributed_rate_limiter_deposit_logic_3".to_string())
            .mode(Mode::Relaxed)
            .deposited_tokens(vec![5, 5, 5, 5, 5]),
            // Integer slope with multiple pods
            // keep depositing same amount of tokens in each epoch
            // The tokens fetched in next epoch should be independent of deposited tokens
            // in relaxed mode
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 1), (None, 1), (None, 1), (None, 1), (None, 1)],
                vec![5, 5, 6, 6, 7],
            )
            .test_name("test_distributed_rate_limiter_deposit_logic_4".to_string())
            .mode(Mode::Relaxed)
            .deposited_tokens(vec![5, 5, 5, 5, 5]),
            // Integer slope with multiple pods
            // keep depositing same amount of tokens in each epoch
            // The tokens fetched in next epoch should be independent of deposited tokens
            // in scheduled mode
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 1), (None, 1), (None, 1), (None, 1), (None, 1)],
                vec![5, 5, 6, 6, 7],
            )
            .test_name("test_distributed_rate_limiter_deposit_logic_5".to_string())
            .mode(Mode::Scheduled)
            .deposited_tokens(vec![5, 5, 5, 5, 5]),
        ];
        test_utils::run_distributed_rate_limiter_multiple_pods_test_cases(test_cases).await;
    }

    #[tokio::test]
    async fn test_distributed_rate_limiter_time_based_refill() {
        use crate::state::OptimisticValidityUpdateSecs;
        use crate::state::store::in_memory_store::InMemoryStore;

        // Create a rate limiter with 30 max tokens, 15 burst, over 2 seconds
        let bounds = TokenCalcBounds::new(90, 45, Duration::from_secs(9), Mode::Relaxed);
        let store = InMemoryStore::default();
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
            false,
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
            false,
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
            false,
        )
        .await
        .unwrap();

        let mut cur_epoch = 1;

        // Phase 1: Each pod requests a specific number of tokens
        let tokens_1_phase1 = rate_limiter_1.attempt_acquire_n(Some(2), cur_epoch).await;
        let tokens_2_phase1 = rate_limiter_2.attempt_acquire_n(Some(2), cur_epoch).await;
        let tokens_3_phase1 = rate_limiter_3.attempt_acquire_n(Some(2), cur_epoch).await;

        // Each pod should get some tokens (may not be exactly 2 due to pool division)
        assert!(tokens_1_phase1 > 0, "Pod 1 should get some tokens");
        assert!(tokens_2_phase1 > 0, "Pod 2 should get some tokens");
        assert!(tokens_3_phase1 > 0, "Pod 3 should get some tokens");

        cur_epoch += 2;

        // Phase 2: Each pod tries to acquire all available tokens
        let tokens_1_phase2 = rate_limiter_1.attempt_acquire_n(None, cur_epoch).await;
        let tokens_2_phase2 = rate_limiter_2.attempt_acquire_n(None, cur_epoch).await;
        let tokens_3_phase2 = rate_limiter_3.attempt_acquire_n(None, cur_epoch).await;

        // After refill, each pod should get more tokens
        assert!(tokens_1_phase2 > 0, "Pod 1 should get tokens after refill");
        assert!(tokens_2_phase2 > 0, "Pod 2 should get tokens after refill");
        assert!(tokens_3_phase2 > 0, "Pod 3 should get tokens after refill");

        // Total tokens distributed should be reasonable (allowing for truncation effects)
        let total_phase1 = tokens_1_phase1 + tokens_2_phase1 + tokens_3_phase1;
        let total_phase2 = tokens_1_phase2 + tokens_2_phase2 + tokens_3_phase2;

        // Due to time-based truncation and distributed consensus, we can't expect exact values,
        // but we should see reasonable token distribution
        assert!(total_phase1 > 0, "Should distribute some tokens in phase 1");
        assert!(total_phase2 > 0, "Should distribute some tokens in phase 2");

        // Phase 3: Try to acquire more tokens immediately (should get 0 or very few)
        let tokens_1_phase3 = rate_limiter_1.attempt_acquire_n(Some(5), cur_epoch).await;
        let tokens_2_phase3 = rate_limiter_2.attempt_acquire_n(Some(5), cur_epoch).await;
        let tokens_3_phase3 = rate_limiter_3.attempt_acquire_n(Some(5), cur_epoch).await;

        // Should get very few or no tokens immediately after exhausting the pool
        let total_phase3 = tokens_1_phase3 + tokens_2_phase3 + tokens_3_phase3;

        assert_eq!(total_phase3, 0, "Should distribute no tokens in phase 3");

        // Clean up
        cancel.cancel();
    }

    // Simple test for acquire_n with in-memory store
    #[tokio::test]
    async fn test_distributed_rate_limiter_simple_acquire_n_in_memory_store() {
        use crate::state::OptimisticValidityUpdateSecs;
        use crate::state::store::in_memory_store::InMemoryStore;

        // Create a rate limiter with 30 max tokens, 15 burst, over 2 seconds
        let bounds = TokenCalcBounds::new(90, 45, Duration::from_secs(9), Mode::Relaxed);
        let store = InMemoryStore::default();
        let cancel = CancellationToken::new();
        let refresh_interval = Duration::from_millis(50);
        let runway_update = OptimisticValidityUpdateSecs::default();
        let pod_count = 3;

        // create pod_count number of rate limiters with in memory store
        let mut rate_limiters = Vec::with_capacity(pod_count);
        for i in 0..pod_count {
            rate_limiters.push(
                RateLimit::<WithState<InMemoryStore>>::new(
                    bounds.clone(),
                    store.clone(),
                    &format!("processor_{}", i),
                    cancel.clone(),
                    refresh_interval,
                    runway_update.clone(),
                    false,
                )
                .await
                .expect("Failed to create rate limiters"),
            );
        }

        // Phase 1: Each pod requests all tokens
        let mut total_got_tokens_phase1 = 0;
        let mut total_expected_tokens_phase1 = 0;
        for rate_limiter in rate_limiters.iter() {
            let tokens = rate_limiter.acquire_n(None, None).await;
            assert_eq!(
                tokens, 16,
                "Number of tokens fetched in each iteration \
                should increase by slope/pod_count until ramp up",
            );
            total_got_tokens_phase1 += tokens;
            total_expected_tokens_phase1 += 16;
        }
        assert_eq!(
            total_got_tokens_phase1, total_expected_tokens_phase1,
            "Total number of tokens fetched in each iteration \
                should be equal to total expected tokens for each processor",
        );

        // Phase 2: Wait for some time to allow token refill
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Phase 2: Each pod tries to acquire all available tokens
        let mut total_got_tokens_phase2 = 0;
        let mut total_expected_tokens_phase2 = 0;
        for rate_limiter in rate_limiters.iter() {
            let tokens = rate_limiter.acquire_n(None, None).await;
            assert_eq!(
                tokens, 18,
                "Number of tokens fetched in each iteration \
                should increase by slope/pod_count until ramp up",
            );
            total_got_tokens_phase2 += tokens;
            total_expected_tokens_phase2 += 18;
        }
        assert_eq!(
            total_got_tokens_phase2, total_expected_tokens_phase2,
            "Total number of tokens fetched in each iteration \
                should be equal to total expected tokens for each processor",
        );

        // Phase 2 should have more tokens than phase 1
        assert!(total_got_tokens_phase2 > total_got_tokens_phase1);

        // Phase 3: Wait for some time to allow token refill
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Phase 3: Each pod tries to acquire some available tokens
        let mut total_got_tokens_phase3 = 0;
        let mut total_expected_tokens_phase3 = 0;
        for rate_limiter in rate_limiters.iter() {
            let tokens = rate_limiter.acquire_n(Some(2), None).await;
            assert_eq!(
                tokens, 2,
                "Number of tokens fetched in each iteration \
                should be exactly what we asked for",
            );
            total_got_tokens_phase3 += tokens;
            total_expected_tokens_phase3 += 2;
        }
        assert_eq!(
            total_got_tokens_phase3, total_expected_tokens_phase3,
            "Total number of tokens fetched in each iteration \
                should be equal to total expected tokens for each processor",
        );

        for rate_limiter in rate_limiters.iter() {
            rate_limiter
                .shutdown()
                .await
                .expect("Rate limiter failed to shutdown");
        }

        // Clean up
        cancel.cancel();
    }

    #[tokio::test]
    async fn test_resume_throttling_in_memory() {
        let cancel = CancellationToken::new();
        let bounds = TokenCalcBounds::new(20, 10, Duration::from_secs(10), Mode::Scheduled);
        let ttl = 2;
        let store = InMemoryStore::new(ttl);
        let refresh_interval = Duration::from_millis(100);
        let runway_update = OptimisticValidityUpdateSecs::default();

        // Create a single distributed rate limiter
        let rate_limiter = RateLimit::<WithState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        // With a single pod, it should get the full burst allocation
        let tokens = rate_limiter.attempt_acquire_n(None, 5).await;
        assert_eq!(
            tokens, 15,
            "The single pod should get all the tokens in scheduled fashion"
        );

        rate_limiter.shutdown().await.unwrap();

        let rate_limiter = RateLimit::<WithState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        let tokens = rate_limiter.attempt_acquire_n(None, 0).await;
        assert_eq!(
            tokens, 15,
            "The processor should start from where it left off"
        );

        rate_limiter.shutdown().await.unwrap();

        // Clean up
        cancel.cancel();
    }

    #[tokio::test]
    async fn test_resume_throttling_different_processors_in_memory() {
        let cancel = CancellationToken::new();
        let bounds = TokenCalcBounds::new(20, 10, Duration::from_secs(10), Mode::Scheduled);
        let ttl = 10;
        let store = InMemoryStore::new(ttl);
        let refresh_interval = Duration::from_millis(100);
        let runway_update = OptimisticValidityUpdateSecs::default();

        // Create a single distributed rate limiter
        let rate_limiter1 = RateLimit::<WithState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        let rate_limiter2 = RateLimit::<WithState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_2",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        // With a single pod, it should get the full burst allocation
        let tokens = rate_limiter1.attempt_acquire_n(None, 5).await;
        assert_eq!(tokens, 7);
        let tokens = rate_limiter2.attempt_acquire_n(None, 2).await;
        assert_eq!(tokens, 6);

        rate_limiter1.shutdown().await.unwrap();
        rate_limiter2.shutdown().await.unwrap();

        cancel.cancel();

        let cancel = CancellationToken::new();

        let rate_limiter1 = RateLimit::<WithState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();
        let rate_limiter2 = RateLimit::<WithState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_2",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        let tokens = rate_limiter1.attempt_acquire_n(None, 0).await;
        assert_eq!(tokens, 7);
        let tokens = rate_limiter2.attempt_acquire_n(None, 0).await;
        assert_eq!(tokens, 6);

        rate_limiter1.shutdown().await.unwrap();
        rate_limiter2.shutdown().await.unwrap();

        // Clean up
        cancel.cancel();
    }

    #[tokio::test]
    async fn test_resume_throttling_different_processors_expire_in_memory() {
        let cancel = CancellationToken::new();
        let bounds = TokenCalcBounds::new(20, 10, Duration::from_secs(10), Mode::Scheduled);
        let ttl = 1;
        let store = InMemoryStore::new(ttl);
        let refresh_interval = Duration::from_millis(100);
        let runway_update = OptimisticValidityUpdateSecs::default();

        // Create a single distributed rate limiter
        let rate_limiter1 = RateLimit::<WithState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        let rate_limiter2 = RateLimit::<WithState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_2",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        // With a single pod, it should get the full burst allocation
        let tokens = rate_limiter1.attempt_acquire_n(None, 5).await;
        assert_eq!(tokens, 7);
        let tokens = rate_limiter2.attempt_acquire_n(None, 2).await;
        assert_eq!(tokens, 6);

        rate_limiter1.shutdown().await.unwrap();
        rate_limiter2.shutdown().await.unwrap();

        cancel.cancel();

        tokio::time::sleep(Duration::from_millis(1500)).await;

        let cancel = CancellationToken::new();

        let rate_limiter1 = RateLimit::<WithState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();
        let rate_limiter2 = RateLimit::<WithState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_2",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        let tokens = rate_limiter1.attempt_acquire_n(None, 0).await;
        assert_eq!(tokens, 5);
        let tokens = rate_limiter2.attempt_acquire_n(None, 0).await;
        assert_eq!(tokens, 5);

        rate_limiter1.shutdown().await.unwrap();
        rate_limiter2.shutdown().await.unwrap();

        // Clean up
        cancel.cancel();
    }

    #[tokio::test]
    async fn test_resume_throttling_different_processors_reboot_in_memory() {
        let cancel = CancellationToken::new();
        let bounds = TokenCalcBounds::new(20, 10, Duration::from_secs(10), Mode::Scheduled);
        let ttl = 10;
        let store = InMemoryStore::new(ttl);
        let refresh_interval = Duration::from_millis(100);
        let runway_update = OptimisticValidityUpdateSecs::default();

        // Create a single distributed rate limiter
        let rate_limiter1 = RateLimit::<WithState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        let rate_limiter2 = RateLimit::<WithState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_2",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        // With a single pod, it should get the full burst allocation
        let tokens = rate_limiter1.attempt_acquire_n(None, 5).await;
        assert_eq!(tokens, 7);
        let tokens = rate_limiter2.attempt_acquire_n(None, 2).await;
        assert_eq!(tokens, 6);

        rate_limiter1.shutdown().await.unwrap();
        rate_limiter2.shutdown().await.unwrap();

        // Cancel the CancellationToken to stop the sync loop otherwise state of previously
        // registered processor will be used
        cancel.cancel();

        let cancel = CancellationToken::new();

        let rate_limiter1 = RateLimit::<WithState<InMemoryStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        let tokens = rate_limiter1.attempt_acquire_n(None, 0).await;
        // Since only rate_limiter1 is registered, it should get the full burst allocation as it
        // continues from where it left off.
        assert_eq!(tokens, 15);

        rate_limiter1.shutdown().await.unwrap();

        // Clean up
        cancel.cancel();
    }

    #[tokio::test]
    #[cfg(feature = "redis-tests")]
    async fn test_distributed_rate_limiter_time_based_refill_redis() {
        use crate::state::OptimisticValidityUpdateSecs;
        use crate::state::store::redis_store::RedisStore;

        // Create Redis store for testing
        let store = match test_utils::create_test_redis_store("rate_limiter_refill", 180).await {
            Some(store) => store,
            None => return, // Skip test if Redis is not available
        };

        // Create a rate limiter with 90 max tokens, 45 burst, over 9 seconds
        let bounds = TokenCalcBounds::new(90, 45, Duration::from_secs(9), Mode::Relaxed);

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
            false,
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
            false,
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
            false,
        )
        .await
        .unwrap();

        // Wait for consensus to be reached among all pods
        let mut cur_epoch = 1;

        // Phase 1: Each pod requests a specific number of tokens
        let tokens_1_phase1 = rate_limiter_1.attempt_acquire_n(Some(2), cur_epoch).await;
        let tokens_2_phase1 = rate_limiter_2.attempt_acquire_n(Some(2), cur_epoch).await;
        let tokens_3_phase1 = rate_limiter_3.attempt_acquire_n(Some(2), cur_epoch).await;

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
        cur_epoch += 2;

        // Phase 2: Request tokens again to test refill
        let tokens_1_phase2 = rate_limiter_1.attempt_acquire_n(Some(3), cur_epoch).await;
        let tokens_2_phase2 = rate_limiter_2.attempt_acquire_n(Some(3), cur_epoch).await;
        let tokens_3_phase2 = rate_limiter_3.attempt_acquire_n(Some(3), cur_epoch).await;

        // Should get more tokens due to refill
        let total_tokens_phase2 = tokens_1_phase2 + tokens_2_phase2 + tokens_3_phase2;
        assert!(
            total_tokens_phase2 > 0,
            "Should get some tokens after refill"
        );

        // Phase 3: Test exhaustion - try to get many tokens
        let tokens_1_phase3 = rate_limiter_1.attempt_acquire_n(Some(50), cur_epoch).await;
        let tokens_2_phase3 = rate_limiter_2.attempt_acquire_n(Some(50), cur_epoch).await;
        let tokens_3_phase3 = rate_limiter_3.attempt_acquire_n(Some(50), cur_epoch).await;

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

    // Simple test for acquire_n with redis store
    #[tokio::test]
    #[cfg(feature = "redis-tests")]
    async fn test_distributed_rate_limiter_simple_acquire_n_redis_store() {
        use crate::state::OptimisticValidityUpdateSecs;
        use crate::state::store::redis_store::RedisStore;

        // Create a rate limiter with 30 max tokens, 15 burst, over 2 seconds
        let bounds = TokenCalcBounds::new(90, 45, Duration::from_secs(9), Mode::Relaxed);
        // Create Redis store for testing
        let store =
            match test_utils::create_test_redis_store("simple_acquire_n_rate_limiter_test", 180)
                .await
            {
                Some(store) => store,
                None => return, // Skip test if Redis is not available
            };
        let cancel = CancellationToken::new();
        let refresh_interval = Duration::from_millis(50);
        let runway_update = OptimisticValidityUpdateSecs::default();
        let pod_count = 3;

        // create pod_count number of rate limiters with in memory store
        let mut rate_limiters = Vec::with_capacity(pod_count);
        for i in 0..pod_count {
            rate_limiters.push(
                RateLimit::<WithState<RedisStore>>::new(
                    bounds.clone(),
                    store.clone(),
                    &format!("processor_{}", i),
                    cancel.clone(),
                    refresh_interval,
                    runway_update.clone(),
                    false,
                )
                .await
                .expect("Failed to create rate limiters"),
            );
        }

        // Phase 1: Each pod requests all tokens
        let mut total_got_tokens_phase1 = 0;
        let mut total_expected_tokens_phase1 = 0;
        for rate_limiter in rate_limiters.iter() {
            let tokens = rate_limiter.acquire_n(None, None).await;
            assert_eq!(
                tokens, 16,
                "Number of tokens fetched in each iteration \
                should increase by slope/pod_count until ramp up",
            );
            total_got_tokens_phase1 += tokens;
            total_expected_tokens_phase1 += 16;
        }
        assert_eq!(
            total_got_tokens_phase1, total_expected_tokens_phase1,
            "Total number of tokens fetched in each iteration \
                should be equal to total expected tokens for each processor",
        );

        // Phase 2: Wait for some time to allow token refill
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Phase 2: Each pod tries to acquire all available tokens
        let mut total_got_tokens_phase2 = 0;
        let mut total_expected_tokens_phase2 = 0;
        for rate_limiter in rate_limiters.iter() {
            let tokens = rate_limiter.acquire_n(None, None).await;
            assert_eq!(
                tokens, 18,
                "Number of tokens fetched in each iteration \
                should increase by slope/pod_count until ramp up",
            );
            total_got_tokens_phase2 += tokens;
            total_expected_tokens_phase2 += 18;
        }
        assert_eq!(
            total_got_tokens_phase2, total_expected_tokens_phase2,
            "Total number of tokens fetched in each iteration \
                should be equal to total expected tokens for each processor",
        );

        // Phase 2 should have more tokens than phase 1
        assert!(total_got_tokens_phase2 > total_got_tokens_phase1);

        // Phase 3: Wait for some time to allow token refill
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Phase 3: Each pod tries to acquire some available tokens
        let mut total_got_tokens_phase3 = 0;
        let mut total_expected_tokens_phase3 = 0;
        for rate_limiter in rate_limiters.iter() {
            let tokens = rate_limiter.acquire_n(Some(2), None).await;
            assert_eq!(
                tokens, 2,
                "Number of tokens fetched in each iteration \
                should be exactly what we asked for",
            );
            total_got_tokens_phase3 += tokens;
            total_expected_tokens_phase3 += 2;
        }
        assert_eq!(
            total_got_tokens_phase3, total_expected_tokens_phase3,
            "Total number of tokens fetched in each iteration \
                should be equal to total expected tokens for each processor",
        );

        for rate_limiter in rate_limiters.iter() {
            rate_limiter
                .shutdown()
                .await
                .expect("Rate limiter failed to shutdownj");
        }

        test_utils::cleanup_redis_keys("simple_acquire_n_rate_limiter_test");
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
                60,
                15,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 0),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![7, 9, 0, 12, 14, 16, 18, 21, 23, 25, 27, 30],
            )
            .store_type(test_utils::StoreType::Redis)
            .test_name(
                "RedisStore Test params: max_tokens=60, burst_tokens=15, duration=10s, pod_count=2"
                    .to_string(),
            )
            .mode(Mode::Relaxed),
            // Fractional slope (>1) with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(
                60,
                15,
                Duration::from_secs(10),
                2,
                vec![(None, 1); 11],
                vec![7, 9, 12, 14, 16, 18, 21, 23, 25, 27, 30],
            )
            .store_type(test_utils::StoreType::Redis)
            .test_name(
                "RedisStore Test params: max_tokens=60, burst_tokens=15, duration=10s, pod_count=2"
                    .to_string(),
            ),
            // Fractional slope (>1) with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                60,
                15,
                Duration::from_secs(10),
                2,
                vec![(Some(20), 1); 11],
                vec![7, 9, 12, 14, 16, 18, 20, 20, 20, 20, 20],
            )
            .store_type(test_utils::StoreType::Redis)
            .test_name(
                "RedisStore Test params: max_tokens=60, burst_tokens=15, duration=10s, pod_count=2"
                    .to_string(),
            ),
            // Fractional slope (<1) with multiple pods
            // Acquire all tokens
            // Immediately ask for tokens in the same epoch after receiving 1 token
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 0),
                    (None, 1),
                ],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
            )
            .store_type(test_utils::StoreType::Redis)
            .test_name(
                "RedisStore Test params: max_tokens=2, burst_tokens=1, duration=10s, pod_count=2"
                    .to_string(),
            ),
            // Fractional slope (<1) with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                2,
                vec![(None, 1); 11],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            )
            .store_type(test_utils::StoreType::Redis)
            .test_name(
                "RedisStore Test params: max_tokens=2, burst_tokens=1, duration=10s, pod_count=2"
                    .to_string(),
            ),
            // Fractional slope (<1) with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                2,
                vec![(Some(1), 1); 11],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            )
            .store_type(test_utils::StoreType::Redis)
            .test_name(
                "RedisStore Test params: max_tokens=2, burst_tokens=1, duration=10s, pod_count=2"
                    .to_string(),
            ),
            // Integer slope with multiple pods
            // Acquire tokens more than max
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(Some(30), 1); 11],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10],
            )
            .store_type(test_utils::StoreType::Redis)
            .test_name(
                "RedisStore Test params: max_tokens=20, burst_tokens=10, duration=10s, pod_count=2"
                    .to_string(),
            ),
            // Integer slope with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 1); 11],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10],
            )
            .store_type(test_utils::StoreType::Redis)
            .test_name(
                "RedisStore Test params: max_tokens=20, burst_tokens=10, duration=10s, pod_count=2"
                    .to_string(),
            ),
            // Integer slope with multiple pods
            // Acquire tokens less than max
            // Combination of immediate and non-immediate token requests
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (Some(1), 0),
                    (Some(2), 1),
                    (Some(1), 0),
                    (Some(3), 1),
                    (Some(1), 0),
                    (Some(4), 2),
                    (Some(1), 0),
                    (Some(10), 1),
                    (Some(2), 0),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (Some(5), 1),
                    (None, 1),
                ],
                vec![1, 2, 1, 3, 1, 4, 1, 5, 2, 5, 7, 8, 8, 9, 9, 5, 10],
            )
            .store_type(test_utils::StoreType::Redis)
            .test_name(
                "RedisStore Test params: max_tokens=20, burst_tokens=10, duration=10s, pod_count=2"
                    .to_string(),
            ),
            // Integer slope with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(Some(1), 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            )
            .store_type(test_utils::StoreType::Redis)
            .test_name(
                "RedisStore Test params: max_tokens=20, burst_tokens=10, duration=10s, pod_count=2"
                    .to_string(),
            ),
            // Fractional slope with single pod
            // Acquire all tokens
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                1,
                vec![(None, 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2],
            )
            .store_type(test_utils::StoreType::Redis)
            .test_name(
                "RedisStore Test params: max_tokens=2, burst_tokens=1, duration=10s, pod_count=1"
                    .to_string(),
            ),
            // Fractional slope with single pod
            // Acquire tokens more than max
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                1,
                vec![(Some(5), 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2],
            )
            .store_type(test_utils::StoreType::Redis)
            .test_name(
                "RedisStore Test params: max_tokens=2, burst_tokens=1, duration=10s, pod_count=1"
                    .to_string(),
            ),
            // Fractional slope with single pod
            // Acquire tokens less than max
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                1,
                vec![(Some(1), 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            )
            .store_type(test_utils::StoreType::Redis)
            .test_name(
                "RedisStore Test params: max_tokens=2, burst_tokens=1, duration=10s, pod_count=1"
                    .to_string(),
            ),
        ];

        test_utils::run_distributed_rate_limiter_multiple_pods_test_cases(test_cases).await;
    }

    #[tokio::test]
    #[cfg(feature = "redis-tests")]
    async fn test_distributed_rate_limiter_only_scheduled_mode_redis() {
        use test_utils::StoreType::Redis;
        let test_cases = vec![
            // Integer slope with multiple pods
            // Keep acquiring min tokens with regular gaps in epochs between calls
            // Acquire None tokens here and there to check the max tokens shelled out.
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (Some(2), 2),
                    (Some(2), 2),
                    (None, 2),
                    (Some(2), 2),
                    (None, 2),
                    (Some(2), 2),
                    (None, 2),
                    (None, 2),
                ],
                vec![2, 2, 7, 2, 9, 2, 10, 10],
            )
            .test_name("test_distributed_rate_limiter_only_scheduled_mode_redis1".to_string())
            .mode(Mode::Scheduled)
            .store_type(Redis),
            // Integer slope with multiple pods
            // Acquire tokens more than max tokens after extended gaps between epochs
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (Some(30), 1),
                    (Some(30), 3),
                    (Some(30), 2),
                    (Some(30), 4),
                    (Some(30), 1),
                    (Some(30), 1),
                ],
                vec![5, 5, 7, 8, 10, 10],
            )
            .test_name("test_distributed_rate_limiter_only_scheduled_mode_redis2".to_string())
            .mode(Mode::Scheduled)
            .store_type(Redis),
            // Fractional slope (>1) with multiple pods
            // Acquire all tokens at each epoch
            // Immediately ask for tokens after first epoch
            test_utils::TestCase::new(
                60,
                15,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 0),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![7, 9, 0, 12, 14, 16, 18, 21, 23, 25, 27, 30],
            )
            .test_name("test_distributed_rate_limiter_only_scheduled_mode_redis3".to_string())
            .mode(Mode::Scheduled)
            .store_type(Redis),
            // Fractional slope (>1) with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(
                60,
                15,
                Duration::from_secs(10),
                2,
                vec![(None, 1); 11],
                vec![7, 9, 12, 14, 16, 18, 21, 23, 25, 27, 30],
            )
            .test_name("test_distributed_rate_limiter_only_scheduled_mode_redis4".to_string())
            .mode(Mode::Scheduled)
            .store_type(Redis),
            // Fractional slope (>1) with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                60,
                15,
                Duration::from_secs(10),
                2,
                vec![(Some(20), 1); 11],
                vec![7, 9, 12, 14, 16, 18, 20, 20, 20, 20, 20],
            )
            .test_name("test_distributed_rate_limiter_only_scheduled_mode_redis5".to_string())
            .mode(Mode::Scheduled)
            .store_type(Redis),
            // Fractional slope (<1) with multiple pods
            // Acquire all tokens
            // Immediately ask for tokens in the same epoch after receiving 1 token
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 0),
                    (None, 1),
                ],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
            )
            .test_name("test_distributed_rate_limiter_only_scheduled_mode_redis6".to_string())
            .mode(Mode::Scheduled)
            .store_type(Redis),
            // Fractional slope (<1) with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                2,
                vec![(None, 1); 11],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            )
            .test_name("test_distributed_rate_limiter_only_scheduled_mode_redis7".to_string())
            .mode(Mode::Scheduled)
            .store_type(Redis),
            // Fractional slope (<1) with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                2,
                vec![(Some(1), 1); 11],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            )
            .test_name("test_distributed_rate_limiter_only_scheduled_mode_redis8".to_string())
            .mode(Mode::Scheduled)
            .store_type(Redis),
            // Integer slope with multiple pods
            // Acquire tokens more than max
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(Some(30), 1); 11],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10],
            )
            .test_name("test_distributed_rate_limiter_only_scheduled_mode_redis9".to_string())
            .mode(Mode::Scheduled)
            .store_type(Redis),
            // Integer slope with multiple pods
            // Acquire all tokens
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 1); 11],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10],
            )
            .test_name("test_distributed_rate_limiter_only_scheduled_mode_redis10".to_string())
            .mode(Mode::Scheduled)
            .store_type(Redis),
            // Integer slope with multiple pods
            // Acquire tokens less than max
            // Combination of immediate and non-immediate token requests
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (Some(1), 0),
                    (Some(2), 1),
                    (Some(1), 0),
                    (Some(3), 1),
                    (Some(1), 0),
                    (Some(4), 2),
                    (Some(1), 0),
                    (Some(10), 1),
                    (Some(2), 0),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (Some(5), 1),
                    (None, 1),
                ],
                vec![1, 2, 1, 3, 1, 4, 1, 6, 2, 5, 8, 8, 9, 9, 10, 5, 10],
            )
            .test_name("test_distributed_rate_limiter_only_scheduled_mode_redis11".to_string())
            .mode(Mode::Scheduled)
            .store_type(Redis),
            // Integer slope with multiple pods
            // Acquire tokens less than max
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(Some(1), 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            )
            .test_name("test_distributed_rate_limiter_only_scheduled_mode_redis12".to_string())
            .mode(Mode::Scheduled)
            .store_type(Redis),
            // Fractional slope with single pod
            // Acquire all tokens
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                1,
                vec![(None, 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2],
            )
            .test_name("test_distributed_rate_limiter_only_scheduled_mode_redis13".to_string())
            .mode(Mode::Scheduled)
            .store_type(Redis),
            // Fractional slope with single pod
            // Acquire tokens more than max
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                1,
                vec![(Some(5), 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2],
            )
            .test_name("test_distributed_rate_limiter_only_scheduled_mode_redis14".to_string())
            .mode(Mode::Scheduled)
            .store_type(Redis),
            // Fractional slope with single pod
            // Acquire tokens less than max
            test_utils::TestCase::new(
                2,
                1,
                Duration::from_secs(10),
                1,
                vec![(Some(1), 1); 11],
                vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            )
            .test_name("test_distributed_rate_limiter_only_scheduled_mode_redis15".to_string())
            .mode(Mode::Scheduled)
            .store_type(Redis),
        ];
        test_utils::run_distributed_rate_limiter_multiple_pods_test_cases(test_cases).await;
    }

    #[tokio::test]
    #[cfg(feature = "redis-tests")]
    async fn test_distributed_rate_limiter_go_back_n_mode_redis() {
        use test_utils::StoreType;

        let default_go_back_n_config = GoBackNConfig {
            cool_down_period: 1,
            ramp_down_percentage: 100,
            utilization_threshold: 100,
        };

        let test_cases = vec![
            // Integer slope with multiple pods
            // Keep acquiring min tokens without any gaps
            // The fetched tokens should follow relaxed mode
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![2, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_redis_1".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // Refill amount should not increase when token utilization is less than threshold
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 1), (Some(2), 1), (Some(5), 1), (None, 1), (None, 1)],
                vec![5, 2, 5, 5, 6],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_redis_2".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // Increase in slope is delayed when there is a gap in epochs > 1
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 1), (None, 2), (None, 1), (None, 1)],
                vec![5, 5, 5, 5],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_redis_3".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // The max_ever_filled should be not go below burst
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 3),
                    (None, 2),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 5, 5, 5, 5, 6],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_redis_4".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // Drop in max_ever_filled should be large for large gaps in epochs
            // then continue where it left off
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 5),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 5, 6, 6, 7, 7, 8, 6, 6, 7],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_redis_5".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // Keep acquiring max tokens without any gaps
            // The fetched tokens should follow relaxed mode if all tokens are used
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_redis_6".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase in max_ever_filled should stall whenever max tokens aren't acquired.
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 5, 6, 6, 7, 7, 8, 8, 9],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_redis_7".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase in max_ever_filled should stall whenever usage threshold isn't crossed.
            // Usage threshold here isn't crossed because we're constantly depositing tokens back.
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 5, 5, 5, 5, 5, 5, 5, 5],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_redis_8".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .deposited_tokens(vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase in max_ever_filled should decrease whenever usage threshold isn't crossed.
            // Since, we can't go below the burst, the max_ever_filled stays at the burst here.
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 5, 5, 5, 5, 5, 5, 5, 5],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_redis_9".to_string())
            .mode(Mode::GoBackN(GoBackNConfig {
                cool_down_period: 1,
                ramp_down_percentage: 100,
                utilization_threshold: 90,
            }))
            .deposited_tokens(vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase max_ever_filled whenever usage threshold is crossed
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 5, 6, 6, 7, 7, 8, 8, 9],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_redis_10".to_string())
            .mode(Mode::GoBackN(GoBackNConfig {
                cool_down_period: 1,
                ramp_down_percentage: 100,
                utilization_threshold: 80,
            }))
            .deposited_tokens(vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // max_ever_filled should decrease whenever the usage threshold is not crossed
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 5, 5, 5, 6, 5, 6, 6, 7],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_redis_11".to_string())
            .mode(Mode::GoBackN(GoBackNConfig {
                cool_down_period: 1,
                ramp_down_percentage: 100,
                utilization_threshold: 80,
            }))
            .deposited_tokens(vec![1, 1, 1, 1, 2, 1, 1, 2, 1, 1, 1, 1])
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // max_ever_filled should decrease whenever the usage threshold is not crossed
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 9, 8, 8, 7, 7, 6],
            )
            .test_name("test_distributed_rate_limiter_go_back_n_mode_redis_12".to_string())
            .mode(Mode::GoBackN(default_go_back_n_config.clone()))
            .deposited_tokens(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8, 7, 7, 6])
            .store_type(StoreType::Redis),
        ];
        test_utils::run_distributed_rate_limiter_multiple_pods_test_cases(test_cases).await;
    }

    #[tokio::test]
    #[cfg(feature = "redis-tests")]
    async fn test_distributed_rate_limiter_only_if_used_mode_redis() {
        use test_utils::StoreType;

        let test_cases = vec![
            // Integer slope with multiple pods
            // Keep acquiring max tokens without any gaps
            // The fetched tokens should follow relaxed mode if all tokens are used
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10],
            )
            .test_name("test_distributed_rate_limiter_only_if_used_mode_redis_1".to_string())
            .mode(Mode::OnlyIfUsed(100))
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase in max_ever_filled should stall whenever max tokens aren't acquired.
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 6, 6, 7, 7, 8, 8, 9, 9],
            )
            .test_name("test_distributed_rate_limiter_only_if_used_mode_redis_2".to_string())
            .mode(Mode::OnlyIfUsed(100))
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase in max_ever_filled should stall whenever usage threshold isn't crossed.
            // Usage threshold here isn't crossed because we're constantly depositing tokens back.
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 5, 5, 5, 5, 5, 5, 5, 5],
            )
            .test_name("test_distributed_rate_limiter_only_if_used_mode_redis_3".to_string())
            .mode(Mode::OnlyIfUsed(100))
            .deposited_tokens(vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase in max_ever_filled should stall whenever usage threshold isn't crossed.
            // Even though the threshold percentage is 90%, since we're depositing tokens back,
            // the actual usage threshold doesn't get crossed (hovers around 82%).
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 5, 5, 5, 5, 5, 5, 5, 5],
            )
            .test_name("test_distributed_rate_limiter_only_if_used_mode_redis_4".to_string())
            .mode(Mode::OnlyIfUsed(90))
            .deposited_tokens(vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase max_ever_filled whenever usage threshold is crossed
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 6, 6, 7, 7, 8, 8, 9, 9],
            )
            .test_name("test_distributed_rate_limiter_only_if_used_mode_redis_5".to_string())
            .mode(Mode::OnlyIfUsed(80))
            .deposited_tokens(vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
            .store_type(StoreType::Redis),
            // Integer slope with multiple pods
            // Keep acquiring tokens without any gaps
            // Increase max_ever_filled should stall whenever the usage threshold is not crossed
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![
                    (None, 1),
                    (Some(4), 1),
                    (Some(2), 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                    (None, 1),
                ],
                vec![5, 4, 2, 5, 6, 6, 6, 6, 7, 7, 8, 8],
            )
            .test_name("test_distributed_rate_limiter_only_if_used_mode_redis_6".to_string())
            .mode(Mode::OnlyIfUsed(80))
            .deposited_tokens(vec![1, 1, 1, 1, 2, 1, 2, 1, 1, 1, 1, 1])
            .store_type(StoreType::Redis),
        ];
        test_utils::run_distributed_rate_limiter_multiple_pods_test_cases(test_cases).await;
    }

    #[tokio::test]
    #[cfg(feature = "redis-tests")]
    async fn test_distributed_rate_limiter_deposit_logic_redis() {
        use test_utils::StoreType;
        let test_cases = vec![
            // Integer slope with multiple pods
            // Keep acquiring min tokens with regular gaps in epochs between calls
            // Acquire None tokens here and there to check the max tokens shelled out.
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(Some(2), 0), (None, 0), (None, 1), (None, 0), (Some(3), 1)],
                vec![2, 5, 2, 5, 3],
            )
            .test_name("test_distributed_rate_limiter_deposit_logic_redis_1".to_string())
            .mode(Mode::Relaxed)
            .store_type(StoreType::Redis)
            .deposited_tokens(vec![2, 2, 0, 3, 0]),
            // Integer slope with multiple pods
            // Keep depositing all the tokens back
            // We'll get burst tokens in each pull within the same epoch
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 0), (None, 0), (None, 0), (None, 0), (None, 1)],
                vec![5, 5, 5, 5, 5],
            )
            .test_name("test_distributed_rate_limiter_deposit_logic_redis_2".to_string())
            .mode(Mode::Relaxed)
            .store_type(StoreType::Redis)
            .deposited_tokens(vec![5, 5, 5, 5, 5]),
            // Integer slope with multiple pods
            // keep depositing same amount of tokens in each epoch
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 1), (None, 0), (None, 1), (None, 0), (None, 1)],
                vec![5, 5, 5, 6, 5],
            )
            .test_name("test_distributed_rate_limiter_deposit_logic_redis_3".to_string())
            .mode(Mode::Relaxed)
            .store_type(StoreType::Redis)
            .deposited_tokens(vec![5, 5, 5, 5, 5]),
            // Integer slope with multiple pods
            // keep depositing same amount of tokens in each epoch
            // The tokens fetched in next epoch should be independent of deposited tokens
            // in relaxed mode
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 1), (None, 1), (None, 1), (None, 1), (None, 1)],
                vec![5, 5, 6, 6, 7],
            )
            .test_name("test_distributed_rate_limiter_deposit_logic_redis_4".to_string())
            .mode(Mode::Relaxed)
            .store_type(StoreType::Redis)
            .deposited_tokens(vec![5, 5, 5, 5, 5]),
            // Integer slope with multiple pods
            // keep depositing same amount of tokens in each epoch
            // The tokens fetched in next epoch should be independent of deposited tokens
            // in scheduled mode
            test_utils::TestCase::new(
                20,
                10,
                Duration::from_secs(10),
                2,
                vec![(None, 1), (None, 1), (None, 1), (None, 1), (None, 1)],
                vec![5, 5, 6, 6, 7],
            )
            .test_name("test_distributed_rate_limiter_deposit_logic_redis_5".to_string())
            .mode(Mode::Scheduled)
            .store_type(StoreType::Redis)
            .deposited_tokens(vec![5, 5, 5, 5, 5]),
        ];
        test_utils::run_distributed_rate_limiter_multiple_pods_test_cases(test_cases).await;
    }

    #[tokio::test]
    #[cfg(feature = "redis-tests")]
    async fn test_resume_throttling_redis() {
        use crate::state::store::redis_store::RedisStore;

        let test_name = "test_resume_throttling_redis";
        let cancel = CancellationToken::new();
        let bounds = TokenCalcBounds::new(20, 10, Duration::from_secs(10), Mode::Scheduled);
        let ttl = 2;
        // Create Redis store for testing
        let store = match test_utils::create_test_redis_store(test_name, ttl).await {
            Some(store) => store,
            None => return, // Skip test if Redis is not available
        };
        let refresh_interval = Duration::from_millis(100);
        let runway_update = OptimisticValidityUpdateSecs::default();

        // Create a single distributed rate limiter
        let rate_limiter = RateLimit::<WithState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        // With a single pod, it should get the full burst allocation
        let tokens = rate_limiter.attempt_acquire_n(None, 5).await;
        assert_eq!(
            tokens, 15,
            "The single pod should get all the tokens in scheduled fashion"
        );

        rate_limiter.shutdown().await.unwrap();

        // Cancel the CancellationToken to stop the sync loop otherwise state of previously
        // registered processor will be used
        cancel.cancel();

        let cancel = CancellationToken::new();

        let rate_limiter = RateLimit::<WithState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        let tokens = rate_limiter.attempt_acquire_n(None, 0).await;
        assert_eq!(
            tokens, 15,
            "The processor should start from where it left off"
        );

        rate_limiter.shutdown().await.unwrap();

        // Clean up Redis keys
        test_utils::cleanup_redis_keys(test_name);

        // Clean up
        cancel.cancel();
    }

    #[tokio::test]
    #[cfg(feature = "redis-tests")]
    async fn test_resume_throttling_different_processors_redis() {
        use crate::state::store::redis_store::RedisStore;

        let test_name = "test_resume_throttling_different_processors_redis";
        let cancel = CancellationToken::new();
        let bounds = TokenCalcBounds::new(20, 10, Duration::from_secs(10), Mode::Scheduled);
        let ttl = 10;
        // Create Redis store for testing
        let store = match test_utils::create_test_redis_store(test_name, ttl).await {
            Some(store) => store,
            None => return, // Skip test if Redis is not available
        };
        let refresh_interval = Duration::from_millis(100);
        let runway_update = OptimisticValidityUpdateSecs::default();

        // Create a single distributed rate limiter
        let rate_limiter1 = RateLimit::<WithState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        let rate_limiter2 = RateLimit::<WithState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_2",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        // With a single pod, it should get the full burst allocation
        let tokens = rate_limiter1.attempt_acquire_n(None, 5).await;
        assert_eq!(tokens, 7);
        let tokens = rate_limiter2.attempt_acquire_n(None, 2).await;
        assert_eq!(tokens, 6);

        rate_limiter1.shutdown().await.unwrap();
        rate_limiter2.shutdown().await.unwrap();

        // Cancel the CancellationToken to stop the sync loop otherwise state of previously
        // registered processor will be used
        cancel.cancel();

        let cancel = CancellationToken::new();

        let rate_limiter1 = RateLimit::<WithState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();
        let rate_limiter2 = RateLimit::<WithState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_2",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        let tokens = rate_limiter1.attempt_acquire_n(None, 0).await;
        assert_eq!(tokens, 7);
        let tokens = rate_limiter2.attempt_acquire_n(None, 0).await;
        assert_eq!(tokens, 6);

        rate_limiter1.shutdown().await.unwrap();
        rate_limiter2.shutdown().await.unwrap();

        // Clean up Redis keys
        test_utils::cleanup_redis_keys(test_name);

        // Clean up
        cancel.cancel();
    }

    #[tokio::test]
    #[cfg(feature = "redis-tests")]
    async fn test_resume_throttling_different_processors_expire_redis() {
        use crate::state::store::redis_store::RedisStore;

        let test_name = "test_resume_throttling_different_processors_expire_redis";
        let cancel = CancellationToken::new();
        let bounds = TokenCalcBounds::new(20, 10, Duration::from_secs(10), Mode::Scheduled);
        let ttl = 1;
        // Create Redis store for testing
        let store = match test_utils::create_test_redis_store(test_name, ttl).await {
            Some(store) => store,
            None => return, // Skip test if Redis is not available
        };
        let refresh_interval = Duration::from_millis(100);
        let runway_update = OptimisticValidityUpdateSecs::default();

        // Create a single distributed rate limiter
        let rate_limiter1 = RateLimit::<WithState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        let rate_limiter2 = RateLimit::<WithState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_2",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        // With a single pod, it should get the full burst allocation
        let tokens = rate_limiter1.attempt_acquire_n(None, 5).await;
        assert_eq!(tokens, 7);
        let tokens = rate_limiter2.attempt_acquire_n(None, 2).await;
        assert_eq!(tokens, 6);

        rate_limiter1.shutdown().await.unwrap();
        rate_limiter2.shutdown().await.unwrap();

        // Cancel the CancellationToken to stop the sync loop otherwise state of previously
        // registered processor will be used
        cancel.cancel();

        tokio::time::sleep(Duration::from_secs(2)).await;

        let cancel = CancellationToken::new();

        let rate_limiter1 = RateLimit::<WithState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();
        let rate_limiter2 = RateLimit::<WithState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_2",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        let tokens = rate_limiter1.attempt_acquire_n(None, 0).await;
        assert_eq!(tokens, 5);
        let tokens = rate_limiter2.attempt_acquire_n(None, 0).await;
        assert_eq!(tokens, 5);

        rate_limiter1.shutdown().await.unwrap();
        rate_limiter2.shutdown().await.unwrap();

        // Clean up Redis keys
        test_utils::cleanup_redis_keys(test_name);

        // Clean up
        cancel.cancel();
    }

    #[tokio::test]
    #[cfg(feature = "redis-tests")]
    async fn test_resume_throttling_different_processors_reboot_redis() {
        use crate::state::store::redis_store::RedisStore;

        let test_name = "test_resume_throttling_different_processors_reboot_redis";
        let cancel = CancellationToken::new();
        let bounds = TokenCalcBounds::new(20, 10, Duration::from_secs(10), Mode::Scheduled);
        let ttl = 10;
        // Create Redis store for testing
        let store = match test_utils::create_test_redis_store(test_name, ttl).await {
            Some(store) => store,
            None => return, // Skip test if Redis is not available
        };
        // Increase the refresh interval to avoid flakiness
        let refresh_interval = Duration::from_millis(100);
        let runway_update = OptimisticValidityUpdateSecs::default();

        // Create a single distributed rate limiter
        let rate_limiter1 = RateLimit::<WithState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        let rate_limiter2 = RateLimit::<WithState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_2",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        // With a single pod, it should get the full burst allocation
        let tokens = rate_limiter1.attempt_acquire_n(None, 5).await;
        assert_eq!(tokens, 7);
        let tokens = rate_limiter2.attempt_acquire_n(None, 2).await;
        assert_eq!(tokens, 6);

        rate_limiter1.shutdown().await.unwrap();
        rate_limiter2.shutdown().await.unwrap();

        // Cancel the CancellationToken to stop the sync loop otherwise state of previously
        // registered processor will be used
        cancel.cancel();

        let cancel = CancellationToken::new();

        let rate_limiter1 = RateLimit::<WithState<RedisStore>>::new(
            bounds.clone(),
            store.clone(),
            "processor_1",
            cancel.clone(),
            refresh_interval,
            runway_update.clone(),
            true,
        )
        .await
        .unwrap();

        let tokens = rate_limiter1.attempt_acquire_n(None, 0).await;
        // Since only rate_limiter1 is registered, it should get the full burst allocation as it
        // continues from where it left off.
        assert_eq!(tokens, 15);

        rate_limiter1.shutdown().await.unwrap();

        // Clean up Redis keys
        test_utils::cleanup_redis_keys(test_name);

        // Clean up
        cancel.cancel();
    }
}
