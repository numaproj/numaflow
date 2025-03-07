use std::sync::Arc;

use async_nats::jetstream::kv::Store;
use async_nats::jetstream::Context;
use bytes::Bytes;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::app::callback::datastore::{DataStore, Error as StoreError, Result as StoreResult};

/// A JetStream implementation of the data store.
#[derive(Clone)]
pub(crate) struct JetStreamDataStore {
    kv_store: Store,
}

impl JetStreamDataStore {
    pub(crate) async fn new(js_context: Context, datum_store_name: &str) -> StoreResult<Self> {
        let kv_store = js_context
            .get_key_value(datum_store_name)
            .await
            .map_err(|e| StoreError::Connection(format!("Failed to get datum kv store: {e:?}")))?;
        Ok(Self { kv_store })
    }
}

impl DataStore for JetStreamDataStore {
    /// Retrieve a data from the store. If the datum is not found, `None` is returned.
    async fn retrieve_data(&mut self, id: &str) -> StoreResult<Option<Vec<Vec<u8>>>> {
        // the responses in kv bucket are stored in the format rs.{id}.{vertex_name}.{timestamp}
        // so we should watch for all keys that start with rs.{id}
        let watch_key = format!("rs.{id}.*.*");
        let mut watcher = self
            .kv_store
            .watch_from_revision(&watch_key, 1)
            .await
            .map_err(|e| {
                StoreError::StoreRead(format!("Failed to watch request id in kv store: {e:?}"))
            })?;

        let response_init_key = format!("rs.{id}.start.processing");
        let response_done_key = format!("rs.{id}.done.processing");

        let mut results = Vec::new();
        while let Some(watch_event) = watcher.next().await {
            let entry = match watch_event {
                Ok(event) => event,
                Err(e) => {
                    tracing::error!(?e, "Received error from Jetstream KV watcher");
                    continue;
                }
            };

            // init key is used to signal the start of processing, the value will be empty
            // we can skip the init key
            if entry.key == response_init_key {
                continue;
            }

            // done key is used to signal the end of processing, we can break the loop
            if entry.key == response_done_key {
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

    /// Stream the response from the store. The response is streamed as it is added to the store.
    async fn stream_data(
        &mut self,
        id: &str,
    ) -> StoreResult<(ReceiverStream<Arc<Bytes>>, JoinHandle<()>)> {
        // the responses in kv bucket are stored in the format rs.{id}.{vertex_name}.{timestamp}
        // so we should watch for all keys that start with rs.{id}
        let watch_key = format!("rs.{id}.*.*");

        let mut watcher = self
            .kv_store
            .watch_from_revision(&watch_key, 1)
            .await
            .map_err(|e| {
                StoreError::StoreRead(format!("Failed to watch request id in kv store: {e:?}"))
            })?;

        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let response_init_key = format!("rs.{id}.start.processing");
        let response_done_key = format!("rs.{id}.done.processing");

        let handle = tokio::spawn(async move {
            while let Some(watch_event) = watcher.next().await {
                let entry = match watch_event {
                    Ok(event) => event,
                    Err(e) => {
                        tracing::error!(?e, "Received error from Jetstream KV watcher");
                        continue;
                    }
                };

                // init key is used to signal the start of processing, the value will be empty
                // we can skip the init key
                if entry.key == response_init_key {
                    continue;
                }

                // done key is used to signal the end of processing, we can break the loop
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
    use chrono::Utc;
    use tokio_stream::StreamExt;

    use super::*;
    use crate::app::callback::datastore::DataStore;

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

        let mut store = JetStreamDataStore::new(context.clone(), datum_store_name)
            .await
            .unwrap();

        let id = "test-id";
        let key = format!("rs.{id}.0.{}", Utc::now().timestamp());
        store
            .kv_store
            .put(key, Bytes::from_static(b"test_payload"))
            .await
            .unwrap();

        let done_key = format!("rs.{id}.done.processing");
        store.kv_store.put(done_key, Bytes::new()).await.unwrap();

        let result = store.retrieve_data(id).await.unwrap();
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

        let mut store = JetStreamDataStore::new(context.clone(), datum_store_name)
            .await
            .unwrap();

        let id = "test_stream_id";
        // Simulate a response being added to the store
        let key = format!("rs.{id}.0.1234");
        let payload = Bytes::from_static(b"test_payload");
        store.kv_store.put(key, payload.clone()).await.unwrap();

        let (mut rx, handle) = store.stream_data(id).await.unwrap();

        // Verify that the response is received
        let received_response = rx.next().await.unwrap();
        assert_eq!(received_response, Arc::new(payload));

        handle.abort();

        // delete store
        context.delete_key_value(datum_store_name).await.unwrap();
    }
}
