pub use crate::state::store::Store;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Store module contains the implementation of the external store which may or may not be distributed.
pub mod store;

/// Consensus represents the consensus reached by the distributed processors.
#[derive(Debug)]
pub enum Consensus {
    /// All active processors agree on the pool size.
    Agree(usize),
    /// Active processors do not agree on the pool size.
    Disagree(usize),
}

/// The state the [RateLimiter] always relies on to compute the available tokens.
#[derive(Clone, Debug)]
pub struct RateLimiterState<S> {
    /// Timestamp (epoch) for which we have the pool size computed and beyond this timestamp the state is
    /// invalid. This is a moving timestamp and will be updated as we get updated data from the
    /// external store.
    // FIXME: use me
    valid_till_epoch: Arc<AtomicU64>,
    /// Pool size as known to this Consumer.
    pub(super) known_pool_size: Arc<AtomicUsize>,
    /// Pool size this Consumer should eventually converge to. This is based on the pool-size
    /// reported by other external store.
    desired_pool_size: Arc<AtomicUsize>,
    /// Processor ID for this rate limiter instance
    processor_id: String,
    /// Cancellation token for background tasks
    cancel_token: CancellationToken,
    /// Previously utilized max tokens by the processor
    /// TODO: Find a way to remove this as it is only being used to pass value from
    /// internal store to RateLimit<W> struct during initialization.
    pub(super) prev_max_filled: Arc<f32>,
    /// Handle to the background task updating the pool size
    /// Arc to update the background task across tokio tasks
    /// Mutex to ensure thread safety for the rlds object
    background_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    store: S,
}

/// Defines what is the optimistic forward-looking updates we can make to the [RateLimiterState].
/// This should be tweaked based on the refresh-interval.
#[derive(Clone)]
pub struct OptimisticValidityUpdateSecs {
    /// on agree, how many runway slots we should update.
    on_agree: u8,
    /// On disagree, how many runway slots we should update. It usually will be less that `on_agree`.
    on_disagree: u8,
}

impl Default for OptimisticValidityUpdateSecs {
    fn default() -> Self {
        OptimisticValidityUpdateSecs {
            on_agree: 10,
            on_disagree: 2,
        }
    }
}

impl<S: Store> RateLimiterState<S> {
    pub(crate) async fn new(
        s: S,
        processor_id: &str,
        cancel: CancellationToken,
        refresh_interval: Duration,
        runway_update_len: OptimisticValidityUpdateSecs,
    ) -> crate::Result<Self> {
        // Register with the external store.
        let (initial_pool_size, prev_max_filled) = s.register(processor_id, cancel.clone()).await?;
        let pool_size =
            Self::wait_for_consensus(&s, processor_id, initial_pool_size, &cancel).await?;

        let mut rlds = RateLimiterState {
            valid_till_epoch: Arc::new(AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs()
                    + runway_update_len.on_agree as u64,
            )),
            known_pool_size: Arc::new(AtomicUsize::new(pool_size)),
            desired_pool_size: Arc::new(AtomicUsize::new(pool_size)),
            processor_id: processor_id.to_string(),
            cancel_token: cancel.clone(),
            prev_max_filled: Arc::new(prev_max_filled),
            store: s,
            // Placeholder for the background task
            background_task: Arc::new(Mutex::new(None)),
        };

        // Spawn a background task to update the pool size
        let background_task_handle = {
            let processor_id = processor_id.to_string();
            let mut rlds = rlds.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(refresh_interval);
                loop {
                    // The interval tick is where this background task yields.
                    // Calling abort on the corresponding JoinHandle for this task
                    // will cancel the task on next interval tick.
                    interval.tick().await;

                    // sent the desired pool size to converge faster even though we might
                    // be using known_pool_size due to DISAGREEMENT
                    let pool_size = rlds
                        .desired_pool_size
                        .load(std::sync::atomic::Ordering::Relaxed);

                    let consensus = match rlds
                        .store
                        .sync_pool_size(processor_id.clone().as_str(), pool_size, cancel.clone())
                        .await
                    {
                        Ok(consensus) => consensus,
                        Err(err) => {
                            warn!("Error updating pool size: {err}");
                            continue;
                        }
                    };

                    rlds.update_state(runway_update_len.clone(), consensus);

                    if cancel.is_cancelled() {
                        info!("RateLimiterDistributedState::update: Cancelled");
                        break;
                    }
                }
            })
        };

        // Set the background task for the state
        rlds.set_background_task(background_task_handle).await;

        Ok(rlds)
    }

    /// Get the initial pool size from the external store. It will only return if there is AGREEMENT,
    /// else it will block till it gets AGREEMENT. It is important to block because if we start too
    /// early, we might end up with wrong pool size, and we might end up consuming more tokens than
    /// what is available.
    async fn wait_for_consensus(
        s: &S,
        processor_id: &str,
        initial_pool_size: usize,
        cancel: &CancellationToken,
    ) -> crate::Result<usize> {
        let mut desired_pool_size = initial_pool_size;
        loop {
            let state = s
                .sync_pool_size(processor_id, desired_pool_size, cancel.clone())
                .await?;
            match state {
                Consensus::Agree(pool_size) => return Ok(pool_size),
                Consensus::Disagree(pool_size) => {
                    info!(
                        pool_size,
                        "RateLimiterDistributedState::wait_for_consensus: DISAGREEMENT (will retry)"
                    );
                    desired_pool_size = pool_size;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            };
        }
    }

    /// Update the state based on the consensus reached.
    fn update_state(
        &mut self,
        runway_update_len: OptimisticValidityUpdateSecs,
        consensus: Consensus,
    ) {
        match consensus {
            Consensus::Agree(pool_size) => {
                self.desired_pool_size
                    .store(pool_size, std::sync::atomic::Ordering::Relaxed);

                self.known_pool_size
                    .store(pool_size, std::sync::atomic::Ordering::Relaxed);

                // Update the state validity to current time + runway_update_len.on_agree
                self.valid_till_epoch.store(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_secs()
                        + runway_update_len.on_agree as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            Consensus::Disagree(pool_size) => {
                // on DISAGREEMENT, we will use the update the desired_pool_size
                self.desired_pool_size
                    .store(pool_size, std::sync::atomic::Ordering::Relaxed);

                // Update the state validity to current time + runway_update_len.on_disagree
                self.valid_till_epoch.store(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_secs()
                        + runway_update_len.on_disagree as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        }
    }

    /// Shutdown the distributed state by deregistering from the store.
    pub(crate) async fn shutdown(&self, max_ever_filled: f32) -> crate::Result<()> {
        // Abort the background task
        // Note: The mutex held here is the std::sync::Mutex, and it is ok to
        // hold it here before the store deregistration (await call) since the mutexGuard will be
        // dropped by compiler before the await call, as bt is not used after this point.
        if let Some(ref bt) = *self.background_task.lock().unwrap() {
            bt.abort();
        }
        // Deregister from the store
        self.store
            .deregister(
                &self.processor_id,
                max_ever_filled,
                self.cancel_token.clone(),
            )
            .await
    }

    /// Set the background task for the state.
    async fn set_background_task(&mut self, background_task: JoinHandle<()>) {
        let mut bt = self.background_task.lock().unwrap();
        *bt = Some(background_task);
    }
}
