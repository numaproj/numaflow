use std::fmt::format;
use std::sync::Arc;

use async_nats::jetstream::kv::{CreateErrorKind, Store};
use async_nats::jetstream::Context;
use bytes::Bytes;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::info;

use crate::app::callback::cbstore::ProcessingStatus;
use crate::app::callback::datumstore::{Error as StoreError, Result as StoreResult};
use crate::callback::Callback;

#[derive(Clone)]
pub(crate) struct JSCallbackStore {
    kv_store: Store,
}

impl JSCallbackStore {
    pub(crate) async fn new(js_context: Context, bucket_name: &str) -> StoreResult<Self> {
        let kv_store = js_context
            .get_key_value(bucket_name)
            .await
            .map_err(|e| StoreError::Connection(format!("Failed to get kv store: {e:?}")))?;
        Ok(Self { kv_store })
    }
}

impl super::CallbackStore for JSCallbackStore {
    async fn register(&mut self, id: &str) -> StoreResult<()> {
        let key = format!("{id}=status");
        tracing::info!(key, "Registering key in Jetstream KV store");

        self.kv_store
            .create(&key, ProcessingStatus::InProgress.into())
            .await
            .map_err(|e| {
                if e.kind() == CreateErrorKind::AlreadyExists {
                    StoreError::DuplicateRequest(id.to_string())
                } else {
                    StoreError::StoreWrite(format!(
                        "Failed to register request id {key} in kv store: {e:?}"
                    ))
                }
            })?;

        let response_key = format!("{id}=response");
        // FIXME: we write empty data so keys are created
        self.kv_store
            .put(&response_key, Bytes::from_static(b""))
            .await
            .map_err(|e| {
                StoreError::StoreWrite(format!(
                    "Failed to register request id {id} in kv store: {e:?}"
                ))
            })?;
        // FIXME: we write empty data so keys are created
        self.kv_store
            .put(id, Bytes::from_static(b""))
            .await
            .map_err(|e| {
                StoreError::StoreWrite(format!(
                    "Failed to register request id {id} in kv store: {e:?}"
                ))
            })?;

        Ok(())
    }

    async fn deregister(&mut self, id: &str, sub_graph: &str) -> StoreResult<()> {
        info!(?id, "Unregistering key in Jetstream KV store");
        let key = format!("{}=status", id);
        let completed_value = format!("completed:{}", sub_graph);
        self.kv_store
            .put(
                key,
                ProcessingStatus::Completed(completed_value.to_string()).into(),
            )
            .await
            .map_err(|e| {
                StoreError::StoreWrite(format!("Failed to mark request as done in kv store: {e:?}"))
            })?;

        // FIXME: use types or watch from revision
        let response_key = format!("{id}=response");
        self.kv_store
            .put(&response_key, Bytes::from_static(b"deleted"))
            .await
            .map_err(|e| {
                StoreError::StoreWrite(format!("Failed to mark request as done in kv store: {e:?}"))
            })?;

        Ok(())
    }

    async fn mark_as_failed(&mut self, id: &str, error: &str) -> StoreResult<()> {
        let key = format!("{}=status", id);
        let failed_value = format!("failed:{}", error);
        self.kv_store
            .put(
                key,
                ProcessingStatus::Failed(failed_value.to_string()).into(),
            )
            .await
            .map_err(|e| {
                StoreError::StoreWrite(format!(
                    "Failed to mark request as failed in kv store: {e:?}"
                ))
            })?;
        Ok(())
    }

    async fn watch_callbacks(
        &mut self,
        id: &str,
    ) -> StoreResult<(ReceiverStream<Arc<Callback>>, JoinHandle<()>)> {
        let mut watcher = self.kv_store.watch_with_history(id).await.map_err(|e| {
            StoreError::StoreRead(format!("Failed to watch request id in kv store: {e:?}"))
        })?;
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        let handle = tokio::spawn(async move {
            // TODO: handle watch errors
            while let Some(watch_event) = watcher.next().await {
                let entry = match watch_event {
                    Ok(event) => event,
                    Err(e) => {
                        tracing::error!(?e, "Received error from Jetstream KV watcher");
                        continue;
                    }
                };
                // all callbacks received
                if entry.operation == async_nats::jetstream::kv::Operation::Delete {
                    break;
                }

                if entry.value.is_empty() {
                    continue;
                }

                let cbr: Callback = serde_json::from_slice(entry.value.as_ref())
                    .map_err(|e| {
                        StoreError::StoreRead(format!("Parsing payload from bytes - {}", e))
                    })
                    .expect("Failed to parse callback from bytes");
                tx.send(Arc::new(cbr))
                    .await
                    .expect("Failed to send callback");
            }
        });

        Ok((ReceiverStream::new(rx), handle))
    }

    async fn status(&mut self, id: &str) -> StoreResult<ProcessingStatus> {
        let key = format!("{}=status", id);
        let status = self.kv_store.get(&key).await.map_err(|e| {
            StoreError::StoreRead(format!("Failed to get status for request id: {e:?}"))
        })?;
        let Some(status) = status else {
            return Err(StoreError::InvalidRequestId(id.to_string()));
        };
        Ok(status.into())
    }

    async fn ready(&mut self) -> bool {
        // Implement a health check for the JetStream connection if possible
        true
    }
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;
    use bytes::Bytes;

    use super::*;
    use crate::app::callback::cbstore::CallbackStore;

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

        let mut store = JSCallbackStore::new(context.clone(), serving_store)
            .await
            .unwrap();

        let id = "AFA7E0A1-3F0A-4C1B-AB94-BDA57694648D";
        let result = store.register(id).await;
        assert!(result.is_ok());

        // delete store
        context.delete_key_value(serving_store).await.unwrap();
    }

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
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut store = JSCallbackStore::new(context.clone(), serving_store)
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
        store
            .kv_store
            .put(id, Bytes::from(serde_json::to_vec(&callback).unwrap()))
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
}
