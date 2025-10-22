use crate::config::pipeline::NatsStoreConfig;
use crate::sinker::sink::serve::StoreEntry;
use async_nats::jetstream::Context;
use async_nats::jetstream::kv::Store;
use chrono::Utc;
use tokio::task::JoinSet;
use tracing::trace;

/// Nats serving store to store the serving responses.
#[derive(Clone)]
pub(crate) struct NatsServingStore {
    store: Store,
}

impl NatsServingStore {
    /// Create a new Nats serving store.
    pub(crate) async fn new(
        js_context: Context,
        nats_store_config: NatsStoreConfig,
    ) -> crate::Result<Self> {
        let store = js_context
            .get_key_value(nats_store_config.rs_store_name.as_str())
            .await
            .map_err(|e| crate::Error::Connection(format!("Failed to get kv store: {e:?}")))?;
        Ok(Self { store })
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

            let store = self.store.clone();
            jhset.spawn(async move {
                store
                    .put(id, payload.value)
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
