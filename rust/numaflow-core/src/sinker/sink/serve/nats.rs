use std::sync::Arc;

use chrono::Utc;
use numaflow_shared::kv::KVStore;
use tokio::task::JoinSet;
use tracing::trace;

use crate::sinker::sink::serve::StoreEntry;

/// Serving store backed by an ISB-neutral KV store (historically NATS JetStream KV).
#[derive(Clone)]
pub(crate) struct NatsServingStore {
    store: Arc<dyn KVStore>,
}

impl NatsServingStore {
    /// Create a new serving store from a pre-built KV store handle.
    pub(crate) fn new(store: Arc<dyn KVStore>) -> Self {
        Self { store }
    }

    /// Puts multiple data items into the serving store concurrently.
    pub(crate) async fn put_datum(
        &mut self,
        origin: &str,
        payloads: Vec<StoreEntry>,
    ) -> crate::Result<()> {
        let mut jhset = JoinSet::new();

        for payload in payloads {
            let id = format!(
                "rs.{}.{}.{}.{}",
                payload.pod_hash,
                payload.id,
                origin,
                Utc::now()
                    .timestamp_nanos_opt()
                    .unwrap_or(Utc::now().timestamp_micros())
            );

            trace!(?id, length = ?payload.value.len(), "Putting datum");

            let store = Arc::clone(&self.store);
            jhset.spawn(async move {
                store
                    .put(&id, payload.value)
                    .await
                    .map_err(|e| crate::Error::Sink(format!("Failed to put datum: {e:?}")))
            });
        }

        while let Some(task) = jhset.join_next().await {
            let result = task.map_err(|e| crate::Error::Sink(format!("Task failed: {e:?}")))?;
            result?; // Propagate the first error, if any
        }
        Ok(())
    }
}
