use crate::config::pipeline::NatsStoreConfig;
use async_nats::jetstream::kv::Store;
use async_nats::jetstream::Context;
use bytes::Bytes;
use chrono::Utc;
use tracing::info;

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
        payloads: Vec<(String, String, Bytes)>,
    ) -> crate::Result<()> {
        let mut tasks = Vec::new();

        for payload in payloads {
            let id = format!(
                "rs.{}.{}.{}.{}",
                payload.1,
                payload.0,
                origin,
                Utc::now()
                    .timestamp_nanos_opt()
                    .unwrap_or(Utc::now().timestamp_micros())
            );

            info!("Putting datum with id {} and payload {:?}", id, payload);

            let store = self.store.clone();
            let task = tokio::spawn(async move {
                store
                    .put(id, payload.2)
                    .await
                    .map_err(|e| crate::Error::Sink(format!("Failed to put datum: {e:?}")))
            });

            tasks.push(task);
        }

        for task in tasks {
            let result = task
                .await
                .map_err(|e| crate::Error::Sink(format!("Task failed: {e:?}")))?;
            result?; // Propagate the first error, if any
        }
        Ok(())
    }
}
