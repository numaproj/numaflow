use crate::state::store::Store;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Store module contains the implementation of the external store.
pub mod store;

/// Consensus represents the consensus reached by the distributed processors.
#[derive(Debug)]
pub(crate) enum Consensus {
    /// All active processors agree on the pool size.
    Agree(usize),
    /// Active processors do not agree on the pool size.
    Disagree(usize),
}

/// The state the [RateLimiter] always relies on to compute the available tokens.
#[derive(Clone, Debug)]
pub struct RateLimiterDistributedState<S> {
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
    store: S,
}

/// Defines what is the optimistic forward-looking updates we can make to the [RateLimiterDistributedState].
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

impl<S: Store> RateLimiterDistributedState<S> {
    pub(crate) async fn new(
        s: S,
        processor_id: &str,
        cancel: CancellationToken,
        refresh_interval: Duration,
        runway_update_len: OptimisticValidityUpdateSecs,
    ) -> crate::Result<Self> {
        // Register with the external store.
        let initial_pool_size = s.register(processor_id, cancel.clone()).await?;
        let pool_size =
            Self::wait_for_consensus(&s, processor_id, initial_pool_size, &cancel).await?;

        let rlds = RateLimiterDistributedState {
            valid_till_epoch: Arc::new(AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs()
                    + runway_update_len.on_agree as u64,
            )),
            known_pool_size: Arc::new(AtomicUsize::new(pool_size)),
            desired_pool_size: Arc::new(AtomicUsize::new(pool_size)),
            store: s,
        };

        // Spawn a background task to update the pool size
        {
            let processor_id = processor_id.to_string();
            let mut rlds = rlds.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(refresh_interval);
                loop {
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
            });
        }

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
}
