use std::sync::Arc;

use async_nats::jetstream::Context;
use async_nats::jetstream::kv::Store;
use backoff::retry::Retry;
use backoff::strategy::fixed;
use bytes::Bytes;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::{sync::Semaphore, task::JoinHandle};
use tracing::{error, warn};

use crate::Error;

/// As message passes through each component (map, transformer, sink, etc.). it emits a beacon via callback
/// to inform that message has been processed by this component.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct Callback {
    pub(crate) id: String,
    pub(crate) vertex: String,
    pub(crate) cb_time: u64,
    pub(crate) from_vertex: String,
    /// Due to flat-map operation, we can have 0 or more responses.
    pub(crate) responses: Vec<Response>,
}

impl TryInto<Bytes> for Callback {
    type Error = Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let value = serde_json::to_string(&self)
            .map_err(|e| Error::Other(format!("Failed to serialize callback: {e}")))?;
        Ok(Bytes::from(value))
    }
}

impl TryFrom<Bytes> for Callback {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice(&value)
            .map_err(|e| Error::Other(format!("Failed to deserialize callback: {e}")))
    }
}

/// It contains details about the `To` vertex via tags (conditional forwarding).
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct Response {
    /// If tags is None, the message is forwarded to all vertices, if len(Vec) == 0, it means that
    /// the message has been dropped.
    pub(crate) tags: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct CallbackHandler {
    semaphore: Arc<Semaphore>,
    store: Arc<Store>,
    /// the client to callback to the request originating pod/container
    vertex_name: &'static str,
}

impl CallbackHandler {
    pub async fn new(
        vertex_name: &'static str,
        js_context: Context,
        store_name: &'static str,
        concurrency_limit: usize,
    ) -> Self {
        let store = js_context
            .get_key_value(store_name)
            .await
            .unwrap_or_else(|_| panic!("Failed to get kv store '{store_name}'"));
        let semaphore = Arc::new(Semaphore::new(concurrency_limit));
        Self {
            semaphore,
            vertex_name,
            store: Arc::new(store),
        }
    }

    /// Sends the callback request in a background task.
    pub async fn callback(
        &self,
        id: String,
        pod_hash: String,
        previous_vertex: String,
        responses: Vec<Option<Vec<String>>>,
    ) -> crate::Result<JoinHandle<()>> {
        let cb_time = Utc::now().timestamp_millis() as u64;

        let responses = responses
            .into_iter()
            .map(|tags| Response { tags })
            .collect();

        let callback_payload = Callback {
            vertex: self.vertex_name.to_string(),
            id: id.clone(),
            cb_time,
            responses,
            from_vertex: previous_vertex,
        };
        let vertex_name = self.vertex_name;

        let permit = Arc::clone(&self.semaphore).acquire_owned().await.unwrap();
        let store = Arc::clone(&self.store);
        let handle = tokio::spawn(async move {
            let timestamp = Utc::now().timestamp_nanos_opt().unwrap();
            let callbacks_key = format!("cb.{pod_hash}.{id}.{vertex_name}.{timestamp}");
            let interval = fixed::Interval::from_millis(1000).take(2);

            let _permit = permit;
            let value = serde_json::to_string(&callback_payload).expect("Failed to serialize");
            let result = Retry::new(
                interval,
                async || match store.put(&callbacks_key, Bytes::from(value.clone())).await {
                    Ok(resp) => Ok(resp),
                    Err(e) => {
                        warn!(error = ?e, "Failed to write callback to store, retrying..");
                        Err(Error::Other(format!(
                            "Failed to write callback to store: {e}"
                        )))
                    }
                },
                |_: &Error| true,
            )
            .await;
            if let Err(e) = result {
                error!(?e, "Failed to write callback to store");
            }
        });

        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use tokio_stream::StreamExt;

    use crate::callback::{Callback, CallbackHandler};

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_successful_callback() -> Result<()> {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let store_name = "test_successful_callback_store";
        let _ = context.delete_key_value(store_name).await;

        let kv_store = context
            .create_key_value(jetstream::kv::Config {
                bucket: store_name.to_string(),
                history: 5,
                ..Default::default()
            })
            .await
            .unwrap();

        let id = "test_id";
        // Create a callback payload
        let callback_payload = Callback {
            id: id.to_string(),
            vertex: "test_vertex".to_string(),
            cb_time: 12345,
            from_vertex: "test_from_vertex".to_string(),
            responses: vec![],
        };

        let callback_handler =
            CallbackHandler::new("test_vertex", context.clone(), store_name, 1).await;

        callback_handler
            .callback(
                id.to_string(),
                "0".to_string(),
                "test_from_vertex".to_string(),
                vec![],
            )
            .await
            .unwrap()
            .await
            .unwrap();

        let mut keys = kv_store.keys().await.unwrap();
        let value = kv_store
            .get(keys.next().await.unwrap().unwrap())
            .await
            .unwrap()
            .unwrap();

        let callback: Callback = serde_json::from_slice(&value).unwrap();
        assert_eq!(callback.id, callback_payload.id);
        Ok(())
    }
}
