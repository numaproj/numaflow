//! TODO(doc)
//!

use std::sync::Arc;

use async_nats::jetstream::kv::{CreateErrorKind, Store};
use async_nats::jetstream::Context;
use bytes::Bytes;
use chrono::Utc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::info;

use crate::app::store::cbstore::ProcessingStatus;
use crate::app::store::datastore::{Error as StoreError, Result as StoreResult};
use crate::callback::Callback;

/// JetStream implementation of the callback store. JetStream KV store is used to store the
/// callback and status information. We use the Watch feature of JetStream to monitor the status
/// of the processing.
#[derive(Clone)]
pub(crate) struct JetStreamCallbackStore {
    kv_store: Store,
}

impl JetStreamCallbackStore {
    pub(crate) async fn new(js_context: Context, bucket_name: &str) -> StoreResult<Self> {
        let kv_store = js_context
            .get_key_value(bucket_name)
            .await
            .map_err(|e| StoreError::Connection(format!("Failed to get kv store: {e:?}")))?;
        Ok(Self { kv_store })
    }
}

impl super::CallbackStore for JetStreamCallbackStore {
    /// registers a request id in the store. If the `id` already exists in the store,
    /// `StoreError::DuplicateRequest` error is returned.
    async fn register(&mut self, id: &str) -> StoreResult<()> {
        // status.{id} is the key for the status of the request, we set it to InProgress
        let status_key = format!("status.{id}");
        info!(id, "Registering key in Jetstream KV store");

        self.kv_store
            .create(&status_key, ProcessingStatus::InProgress.into())
            .await
            .map_err(|e| {
                if e.kind() == CreateErrorKind::AlreadyExists {
                    StoreError::DuplicateRequest(id.to_string())
                } else {
                    StoreError::StoreWrite(format!(
                        "Failed to register request id {status_key} in kv store: {e:?}"
                    ))
                }
            })?;

        let current_timestamp = Utc::now().timestamp().to_string();
        let response_key = format!("rs.{id}.start.processing");
        self.kv_store
            // we start with some key so that JetStream won't return early thinking there is
            // nothing to watch.
            .create(&response_key, Bytes::from(current_timestamp.clone()))
            .await
            .map_err(|e| {
                if e.kind() == CreateErrorKind::AlreadyExists {
                    StoreError::DuplicateRequest(id.to_string())
                } else {
                    StoreError::StoreWrite(format!(
                        "Failed to register request id {response_key} in kv store: {e:?}"
                    ))
                }
            })?;

        let callbacks_key = format!("cb.{id}.start.processing");
        self.kv_store
            // same as above
            .create(&callbacks_key, Bytes::from(current_timestamp))
            .await
            .map_err(|e| {
                if e.kind() == CreateErrorKind::AlreadyExists {
                    StoreError::DuplicateRequest(id.to_string())
                } else {
                    StoreError::StoreWrite(format!(
                        "Failed to register request id {callbacks_key} in kv store: {e:?}"
                    ))
                }
            })?;

        Ok(())
    }
    /// de-registers a request id from the store. Updates the status of the request to completed and
    /// stores the subgraph of the request.
    async fn deregister(&mut self, id: &str, sub_graph: &str) -> StoreResult<()> {
        let key = format!("status.{id}");
        self.kv_store
            .put(
                key,
                ProcessingStatus::Completed(sub_graph.to_string()).into(),
            )
            .await
            .map_err(|e| {
                StoreError::StoreWrite(format!("Failed to mark request as done in kv store: {e:?}"))
            })?;

        // we need to update the callbacks and response keys to signal the end of processing so that
        // the watchers can stop watching for new callbacks and responses.
        let current_timestamp = Utc::now().timestamp().to_string();
        let callbacks_key = format!("cb.{id}.done.processing");
        self.kv_store
            .put(callbacks_key, Bytes::from(current_timestamp.clone()))
            .await
            .map_err(|e| {
                StoreError::StoreWrite(format!("Failed to mark request as done in kv store: {e:?}"))
            })?;

        let response_key = format!("rs.{id}.done.processing");
        self.kv_store
            .put(response_key, Bytes::from(current_timestamp.clone()))
            .await
            .map_err(|e| {
                StoreError::StoreWrite(format!("Failed to mark request as done in kv store: {e:?}"))
            })?;

        Ok(())
    }

    /// updates the status of a request in the store to failed and stores the error message.
    async fn mark_as_failed(&mut self, id: &str, error: &str) -> StoreResult<()> {
        let key = format!("status.{id}");
        self.kv_store
            .put(key, ProcessingStatus::Failed(error.to_string()).into())
            .await
            .map_err(|e| {
                StoreError::StoreWrite(format!(
                    "Failed to mark request as failed in kv store: {e:?}"
                ))
            })?;
        Ok(())
    }

    /// watches for callbacks in the store for a given request id.
    async fn watch_callbacks(
        &mut self,
        id: &str,
    ) -> StoreResult<(ReceiverStream<Arc<Callback>>, JoinHandle<()>)> {
        // the callbacks are stored in the format cb.{id}.{vertex_name}.{timestamp}
        // so we can watch for all keys that start with cb.{id}.*
        let callbacks_key = format!("cb.{id}.*.*");
        let mut watcher = self
            .kv_store
            .watch_from_revision(&callbacks_key, 1)
            .await
            .map_err(|e| {
                StoreError::StoreRead(format!("Failed to watch request id in kv store: {e:?}"))
            })?;

        let (tx, rx) = tokio::sync::mpsc::channel(10);

        let callbacks_init_key = format!("cb.{id}.start.processing");
        let callbacks_done_key = format!("cb.{id}.done.processing");

        let handle = tokio::spawn(async move {
            // FIXME(FMEA): Handle errors from the watcher.
            while let Some(watch_event) = watcher.next().await {
                let entry = match watch_event {
                    Ok(event) => event,
                    Err(e) => {
                        tracing::error!(?e, "Received error from Jetstream KV watcher");
                        continue;
                    }
                };

                // init key is used to signal the start of processing, the value will be the timestamp.
                if entry.key == callbacks_init_key {
                    continue;
                }

                // done key is used to signal the end of processing, we can break the loop
                if entry.key == callbacks_done_key {
                    break;
                }

                let cbr: Callback = entry
                    .value
                    .try_into()
                    .expect("Failed to deserialize callback");

                tx.send(Arc::new(cbr))
                    .await
                    .expect("Failed to send callback");
            }
        });

        Ok((ReceiverStream::new(rx), handle))
    }

    /// returns the status of a request in the store.
    async fn status(&mut self, id: &str) -> StoreResult<ProcessingStatus> {
        let key = format!("status.{id}");
        let status = self.kv_store.get(&key).await.map_err(|e| {
            StoreError::StoreRead(format!("Failed to get status for request id: {e:?}"))
        })?;
        let Some(status) = status else {
            return Err(StoreError::InvalidRequestId(id.to_string()));
        };
        Ok(status.into())
    }

    async fn ready(&mut self) -> bool {
        // we do not need to implement a health check for the JetStream connection because it is the
        // ISB and it has to be up.
        true
    }
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;
    use bytes::Bytes;
    use chrono::Utc;

    use super::*;
    use crate::app::store::cbstore::CallbackStore;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_register() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let serving_store = "test_serving_store";

        context
            .create_key_value(Config {
                bucket: serving_store.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut store = JetStreamCallbackStore::new(context.clone(), serving_store)
            .await
            .unwrap();

        let id = "AFA7E0A1-3F0A-4C1B-AB94-BDA57694648D";
        let result = store.register(id).await;
        assert!(result.is_ok());

        // delete store
        context.delete_key_value(serving_store).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_watch_callbacks() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let serving_store = "test_watch_callbacks";

        // Delete bucket so that re-running the test won't fail
        let _ = context.delete_key_value(serving_store).await;

        context
            .create_key_value(Config {
                bucket: serving_store.to_string(),
                description: "test_description".to_string(),
                history: 15,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut store = JetStreamCallbackStore::new(context.clone(), serving_store)
            .await
            .unwrap();

        let id = "test_watch_id_two";
        store.register(id).await.unwrap();

        let (mut rx, handle) = store.watch_callbacks(id).await.unwrap();

        // Simulate a callback being added to the store
        let callback = Callback {
            id: id.to_string(),
            vertex: "test_vertex".to_string(),
            cb_time: 12345,
            from_vertex: "test_from_vertex".to_string(),
            responses: vec![],
        };
        let key = format!("cb.{id}.0.{}", Utc::now().timestamp());
        store
            .kv_store
            .put(key, Bytes::from(serde_json::to_vec(&callback).unwrap()))
            .await
            .unwrap();

        // Verify that the callback is received
        let received_callback = rx.next().await.unwrap();
        assert_eq!(received_callback.id, callback.id);
        assert_eq!(received_callback.vertex, callback.vertex);
        assert_eq!(received_callback.cb_time, callback.cb_time);
        assert_eq!(received_callback.from_vertex, callback.from_vertex);
        assert_eq!(received_callback.responses.len(), callback.responses.len());

        handle.abort();

        // delete store
        context.delete_key_value(serving_store).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_deregister() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let serving_store = "test_deregister_store";

        // Delete bucket so that re-running the test won't fail
        let _ = context.delete_key_value(serving_store).await;

        context
            .create_key_value(Config {
                bucket: serving_store.to_string(),
                description: "test_description".to_string(),
                history: 15,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut store = JetStreamCallbackStore::new(context.clone(), serving_store)
            .await
            .unwrap();

        let id = "test_deregister_id";
        store.register(id).await.unwrap();

        let sub_graph = "test_sub_graph";
        let result = store.deregister(id, sub_graph).await;
        assert!(result.is_ok());

        // Verify that the status is marked as completed
        let status = store.status(id).await.unwrap();
        assert!(matches!(status, ProcessingStatus::Completed(_)));

        // delete store
        context.delete_key_value(serving_store).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_mark_as_failed() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let serving_store = "test_mark_as_failed_store";

        // Delete bucket so that re-running the test won't fail
        let _ = context.delete_key_value(serving_store).await;

        context
            .create_key_value(Config {
                bucket: serving_store.to_string(),
                description: "test_description".to_string(),
                history: 15,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut store = JetStreamCallbackStore::new(context.clone(), serving_store)
            .await
            .unwrap();

        assert!(store.ready().await);

        let id = "test_mark_as_failed_id";
        store.register(id).await.unwrap();

        let error_message = "test_error_message";
        let result = store.mark_as_failed(id, error_message).await;
        assert!(result.is_ok());

        // Verify that the status is marked as failed
        let status = store.status(id).await.unwrap();
        assert!(matches!(status, ProcessingStatus::Failed(_)));

        // delete store
        context.delete_key_value(serving_store).await.unwrap();
    }
}
