use async_nats::jetstream::kv::Store;
use async_nats::jetstream::Context;
use bytes::Bytes;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::{debug, info};

use crate::app::callback::datumstore::{DatumStore, Error as StoreError, Result as StoreResult};

#[derive(Clone)]
pub(crate) struct JetStreamDatumStore {
    kv_store: Store,
}

impl JetStreamDatumStore {
    pub(crate) async fn new(js_context: Context, datum_store_name: &str) -> StoreResult<Self> {
        let kv_store = js_context
            .get_key_value(datum_store_name)
            .await
            .map_err(|e| StoreError::Connection(format!("Failed to get datum kv store: {e:?}")))?;
        Ok(Self { kv_store })
    }
}

impl DatumStore for JetStreamDatumStore {
    async fn retrieve_datum(&mut self, id: &str) -> StoreResult<Option<Vec<Vec<u8>>>> {
        let watch_key = format!("response.{id}.*");
        let mut watcher = self
            .kv_store
            .watch_from_revision(&watch_key, 1)
            .await
            .map_err(|e| {
                StoreError::StoreRead(format!("Failed to watch request id in kv store: {e:?}"))
            })?;

        let response_init_key = format!("response.{id}.init");
        let response_done_key = format!("response.{id}.done");

        let mut results = Vec::new();
        while let Some(watch_event) = watcher.next().await {
            info!(?watch_event, "Received watch event");
            let entry = match watch_event {
                Ok(event) => event,
                Err(e) => {
                    tracing::error!(?e, "Received error from Jetstream KV watcher");
                    continue;
                }
            };

            if entry.key == response_init_key {
                continue;
            }

            if entry.key == response_done_key {
                info!("Received done event, stopping watcher");
                break;
            }

            results.push(entry.value.to_vec());
        }

        if results.is_empty() {
            Ok(None)
        } else {
            Ok(Some(results))
        }
    }

    async fn stream_response(
        &mut self,
        id: &str,
    ) -> StoreResult<(ReceiverStream<Arc<Bytes>>, JoinHandle<()>)> {
        let watch_key = format!("response.{id}.*");

        let mut watcher = self
            .kv_store
            .watch_from_revision(&watch_key, 1)
            .await
            .map_err(|e| {
                StoreError::StoreRead(format!("Failed to watch request id in kv store: {e:?}"))
            })?;

        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let response_init_key = format!("response.{id}.init");
        let response_done_key = format!("response.{id}.done");
        let handle = tokio::spawn(async move {
            while let Some(watch_event) = watcher.next().await {
                let entry = match watch_event {
                    Ok(event) => event,
                    Err(e) => {
                        tracing::error!(?e, "Received error from Jetstream KV watcher");
                        continue;
                    }
                };

                if entry.key == response_init_key {
                    continue;
                }

                if entry.key == response_done_key {
                    break;
                }

                if !entry.value.is_empty() {
                    tx.send(Arc::new(entry.value))
                        .await
                        .expect("Failed to send response");
                }
            }
        });

        Ok((ReceiverStream::new(rx), handle))
    }

    async fn ready(&mut self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;
    use bytes::Bytes;
    use tokio_stream::StreamExt;

    use super::*;
    use crate::app::callback::datumstore::DatumStore;

    #[tokio::test]
    async fn test_retrieve_datum() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let datum_store_name = "test_retrieve_datum_store";

        let _ = context.delete_key_value(datum_store_name).await;

        context
            .create_key_value(Config {
                bucket: datum_store_name.to_string(),
                history: 5,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut store = JetStreamDatumStore::new(context.clone(), datum_store_name)
            .await
            .unwrap();

        let id = "test-id";
        let key = format!("response.{id}.0");
        store
            .kv_store
            .put(key, Bytes::from_static(b"test_payload"))
            .await
            .unwrap();

        let done_key = format!("response.{id}.done");
        store.kv_store.put(done_key, Bytes::new()).await.unwrap();

        let result = store.retrieve_datum(id).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap()[0], b"test_payload");

        // delete store
        context.delete_key_value(datum_store_name).await.unwrap();
    }

    #[tokio::test]
    async fn test_stream_response() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let datum_store_name = "test_stream_response_store";

        context
            .create_key_value(Config {
                bucket: datum_store_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut store = JetStreamDatumStore::new(context.clone(), datum_store_name)
            .await
            .unwrap();

        let id = "test_stream_id";
        // Simulate a response being added to the store
        let key = format!("response.{id}.0");
        let payload = Bytes::from_static(b"test_payload");
        store.kv_store.put(key, payload.clone()).await.unwrap();

        let (mut rx, handle) = store.stream_response(id).await.unwrap();

        // Verify that the response is received
        let received_response = rx.next().await.unwrap();
        assert_eq!(received_response, Arc::new(payload));

        handle.abort();

        // delete store
        context.delete_key_value(datum_store_name).await.unwrap();
    }
}
