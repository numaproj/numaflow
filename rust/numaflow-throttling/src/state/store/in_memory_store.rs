//! In-memory implementation of the Store trait for simple use cases and testing.
//! We can use in-memory if the user does not want to use an external store.

use crate::state::Consensus;
use crate::state::store::Store;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// InMemoryStore implements distributed consensus logic for multiple processors.
///
/// Tracks per-processor heartbeats and reported pool sizes, prunes stale processors
/// by TTL, and computes consensus:
/// - AGREE(K) if all active processors reported the same pool size K and K == active_count
/// - Otherwise DISAGREE(max_reported) for conservative behavior
#[derive(Clone)]
pub struct InMemoryStore {
    inner: Arc<Mutex<ProcessorsTimeline>>,
    stale_age: Duration,
}

#[derive(Default)]
struct ProcessorsTimeline {
    /// processor_id -> last heartbeat time
    heartbeats: HashMap<String, Instant>,
    /// processor_id -> reported pool size
    reported_pool: HashMap<String, usize>,
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::with_ttl(Duration::from_secs(180))
    }

    /// Create a new in-memory store with custom TTL for heartbeats
    pub(crate) fn with_ttl(stale_age: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ProcessorsTimeline::default())),
            stale_age,
        }
    }

    /// Remove stale processors based on heartbeat TTL
    fn prune_stale(inner: &mut ProcessorsTimeline, ttl: Duration) {
        let now = Instant::now();
        let mut stale_ids = Vec::new();

        for (id, last_heartbeat) in &inner.heartbeats {
            if now.duration_since(*last_heartbeat) > ttl {
                stale_ids.push(id.clone());
            }
        }

        for id in stale_ids {
            inner.heartbeats.remove(&id);
            inner.reported_pool.remove(&id);
        }
    }

    /// Compute consensus using only active processors
    fn compute_consensus(inner: &ProcessorsTimeline) -> Consensus {
        let active_count = inner.heartbeats.len();
        if active_count == 0 {
            return Consensus::Disagree(0);
        }

        // Gather reported pools for active processors only
        let mut min_size = usize::MAX;
        let mut max_size = 0;
        let mut reported_count = 0;

        for processor_id in inner.heartbeats.keys() {
            if let Some(&reported_size) = inner.reported_pool.get(processor_id) {
                reported_count += 1;
                min_size = min_size.min(reported_size);
                max_size = max_size.max(reported_size);
            }
        }

        if reported_count != active_count {
            return Consensus::Disagree(max_size);
        }

        // All active processors agree on the same size
        if min_size == max_size {
            Consensus::Agree(max_size)
        } else {
            Consensus::Disagree(max_size)
        }
    }
}

impl Store for InMemoryStore {
    async fn register(
        &self,
        processor_id: &str,
        _cancel: CancellationToken,
    ) -> crate::Result<usize> {
        info!("Registering processor: {processor_id}");

        let mut inner = self.inner.lock().await;
        Self::prune_stale(&mut inner, self.stale_age);
        inner
            .heartbeats
            .insert(processor_id.to_string(), Instant::now());

        // Initialize reported pool to 1 so consensus intentionally starts in DISAGREE
        let pool_size = inner.heartbeats.len();
        inner
            .reported_pool
            .entry(processor_id.to_string())
            .and_modify(|e| *e += 1)
            .or_insert(1);

        Ok(pool_size)
    }

    /// Deregister a processor from the store.
    async fn deregister(
        &self,
        processor_id: &str,
        _cancel: CancellationToken,
    ) -> crate::Result<()> {
        info!("Deregistering processor: {processor_id}");

        let mut inner = self.inner.lock().await;
        inner.heartbeats.remove(processor_id);
        inner.reported_pool.remove(processor_id);

        Ok(())
    }

    /// Update processor's heartbeat and reported pool size, then compute consensus.
    async fn sync_pool_size(
        &self,
        processor_id: &str,
        pool_size: usize,
        cancel: CancellationToken,
    ) -> crate::Result<Consensus> {
        if cancel.is_cancelled() {
            return Err(crate::Error::Cancellation);
        }

        let mut inner = self.inner.lock().await;

        // Update heartbeat and reported pool for this processor
        inner
            .heartbeats
            .insert(processor_id.to_string(), Instant::now());

        // update the reported pool size for the requesting processor
        inner
            .reported_pool
            .insert(processor_id.to_string(), pool_size);

        // Prune stale processors and compute consensus among active ones
        Self::prune_stale(&mut inner, self.stale_age);

        Ok(Self::compute_consensus(&inner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_consensus_agree_when_all_report_active_count() {
        let store = InMemoryStore::new();
        let cancel = CancellationToken::new();

        store.register("processor_a", cancel.clone()).await.unwrap();
        store.register("processor_b", cancel.clone()).await.unwrap();

        // Before reports, should disagree (max=0)
        let consensus = store
            .sync_pool_size("processor_a", 2, cancel.clone())
            .await
            .unwrap();
        assert!(matches!(consensus, Consensus::Disagree(2)));

        let consensus = store
            .sync_pool_size("processor_b", 2, cancel.clone())
            .await
            .unwrap();

        // Should agree since all active processors (2) reported the same size (2)
        assert!(matches!(consensus, Consensus::Agree(2)));
    }

    #[tokio::test]
    async fn test_consensus_disagree_when_reports_differ() {
        let store = InMemoryStore::new();
        let cancel = CancellationToken::new();

        store.register("processor_a", cancel.clone()).await.unwrap();
        store.register("processor_b", cancel.clone()).await.unwrap();

        let _ = store
            .sync_pool_size("processor_a", 1, cancel.clone())
            .await
            .unwrap();
        let consensus = store
            .sync_pool_size("processor_b", 3, cancel.clone())
            .await
            .unwrap();

        assert!(matches!(consensus, Consensus::Disagree(3)));
    }

    #[tokio::test]
    async fn test_consensus_agree_when_same_size_reported() {
        let store = InMemoryStore::new();
        let cancel = CancellationToken::new();

        store.register("processor_a", cancel.clone()).await.unwrap();
        store.register("processor_b", cancel.clone()).await.unwrap();

        let _ = store
            .sync_pool_size("processor_a", 5, cancel.clone())
            .await
            .unwrap();
        let consensus = store
            .sync_pool_size("processor_b", 5, cancel.clone())
            .await
            .unwrap();

        assert!(matches!(consensus, Consensus::Agree(5)));
    }

    #[tokio::test]
    async fn test_stale_processor_pruning() {
        let store = InMemoryStore::with_ttl(Duration::from_millis(50));
        let cancel = CancellationToken::new();

        store.register("processor_a", cancel.clone()).await.unwrap();
        store.register("processor_b", cancel.clone()).await.unwrap();

        // Only processor_b reports, processor_a becomes stale
        let _ = store
            .sync_pool_size("processor_b", 1, cancel.clone())
            .await
            .unwrap();

        // Wait for processor_a to become stale
        tokio::time::sleep(Duration::from_millis(80)).await;

        let consensus = store
            .sync_pool_size("processor_b", 1, cancel.clone())
            .await
            .unwrap();

        // Only processor_b remains active; it reported 1 and active count is 1 => Agree(1)
        assert!(matches!(consensus, Consensus::Agree(1)));
    }

    #[tokio::test]
    async fn test_deregister_removes_processor() {
        let store = InMemoryStore::new();
        let cancel = CancellationToken::new();

        store.register("processor_a", cancel.clone()).await.unwrap();
        store.register("processor_b", cancel.clone()).await.unwrap();

        let _ = store
            .sync_pool_size("processor_a", 2, cancel.clone())
            .await
            .unwrap();
        let _ = store
            .sync_pool_size("processor_b", 2, cancel.clone())
            .await
            .unwrap();

        // Should agree initially
        let consensus = store
            .sync_pool_size("processor_a", 2, cancel.clone())
            .await
            .unwrap();
        assert!(matches!(consensus, Consensus::Agree(2)));

        // Deregister one processor
        store
            .deregister("processor_a", cancel.clone())
            .await
            .unwrap();

        // Now only processor_b is active, and it reported 2 but active count is 1
        let consensus = store
            .sync_pool_size("processor_b", 1, cancel.clone())
            .await
            .unwrap();

        assert!(matches!(consensus, Consensus::Agree(1)));
    }
}
