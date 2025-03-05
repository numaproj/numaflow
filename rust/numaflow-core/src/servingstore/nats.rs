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
            .get_key_value(nats_store_config.name.as_str())
            .await
            .map_err(|e| crate::Error::Connection(format!("Failed to get kv store: {e:?}")))?;
        info!("Jetstream serving store created");
        Ok(Self { store })
    }

    /// Puts a datum into the serving store.
    pub(crate) async fn put_datum(
        &mut self,
        id: &str,
        origin: &str,
        payload: Vec<u8>,
    ) -> crate::Result<()> {
        let id = format!(
            "response.{id}.{}.{}",
            origin,
            Utc::now().timestamp_nanos_opt().unwrap()
        );

        info!(?id, "Putting datum in Jetstream serving store");
        self.store
            .put(id, Bytes::from(payload))
            .await
            .map_err(|e| crate::Error::Sink(format!("Failed to put datum: {e:?}")))?;
        Ok(())
    }
}
