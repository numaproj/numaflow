use crate::state::Consensus;
use tokio_util::sync::CancellationToken;

pub mod in_memory_store;
pub mod redis_store;

/// Store is the trait that defines the interface for the external store. It is
/// responsible updating the [RateLimiterDistributedState] and for publishing the heartbeats.
#[trait_variant::make(Send)]
pub trait Store: Clone + 'static {
    /// Register the consumer with the external store. It increments the pool size by 1, and returns
    /// the initial pool size, and the max_filled from the previous processor with the same name
    /// if it exists.
    /// Consensus cannot be reached during registration.
    async fn register(
        &self,
        processor_id: &str,
        cancel: CancellationToken,
    ) -> crate::Result<(usize, f32)>;

    /// Deregister the consumer with the external store. It decrements the pool size by 1.
    async fn deregister(
        &self,
        processor_id: &str,
        prev_max_filled: f32,
        cancel: CancellationToken,
    ) -> crate::Result<()>;

    /// Synchronize the pool size with the external store. It also updates the heartbeat to show that
    /// the processor is still alive. Returns the [Consensus] state which can be used to determine if
    /// the pool size has reached consensus.
    /// This function is invoked periodically by the consumer to update the pool size and heartbeat.
    async fn sync_pool_size(
        &self,
        processor_id: &str,
        pool_size: usize,
        cancel: CancellationToken,
    ) -> crate::Result<Consensus>;
}
