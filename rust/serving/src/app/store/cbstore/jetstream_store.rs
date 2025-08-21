//! Stores the watches the callbacks published for the request when it travels through the vertices
//! A central watcher task per pod monitors callbacks for that pod and sends them to the appropriate
//! sender.
//!
//! JetStream Callback Entry Format is as follows:
//!  - Callback key - `cb.{pod-hash}.{id}.{vertex_name}.{timestamp}`
//!  - Callback value - JSON serialized Callback struct
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::app::store::datastore::{Error as StoreError, Result as StoreResult};
use crate::callback::Callback;
use async_nats::jetstream::Context;
use async_nats::jetstream::kv::{Store, Watch};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, warn};

const CALLBACK_KEY_PREFIX: &str = "cb";

/// JetStream implementation of the callback store.
#[derive(Clone)]
pub(crate) struct JetStreamCallbackStore {
    callback_kv: Store,
    callback_senders: Arc<Mutex<HashMap<String, mpsc::Sender<Arc<Callback>>>>>,
    cln_token: CancellationToken,
}

impl JetStreamCallbackStore {
    pub(crate) async fn new(
        js_context: Context,
        pod_hash: &'static str,
        callback_bucket: &str,
        cln_token: CancellationToken,
    ) -> StoreResult<Self> {
        info!(
            pod_hash,
            callback_bucket, "Initializing JetStreamCallbackStore"
        );

        let callback_kv = js_context
            .get_key_value(callback_bucket)
            .await
            .map_err(|e| {
                StoreError::Connection(format!(
                    "Failed to get callback kv store '{callback_bucket}': {e:?}"
                ))
            })?;

        let callback_senders = Arc::new(Mutex::new(HashMap::new()));

        // start the central watcher for this pod
        Self::spawn_central_watcher(
            pod_hash.to_string(),
            callback_kv.clone(),
            Arc::clone(&callback_senders),
            cln_token.clone(),
        )
        .await;

        Ok(Self {
            callback_kv,
            callback_senders,
            cln_token,
        })
    }

    /// spawns the central task that watches callback keys and manages callback collection for this
    /// pod.
    async fn spawn_central_watcher(
        pod_hash: String,
        kv_store: Store,
        senders: Arc<Mutex<HashMap<String, mpsc::Sender<Arc<Callback>>>>>,
        cln_token: CancellationToken,
    ) {
        let span = tracing::info_span!("central_callback_watcher", pod_hash = ?pod_hash);

        // we should start from the first revision when the pod comes up, it will watch for all the
        // callbacks for the requests that are originated from this pod.
        let mut latest_revision = 0;

        // callback key format is cb.{pod_hash}.{request_id}.{vertex_name}.{timestamp}, so we should
        // watch for all the callbacks with prefix cb.{pod_hash}.*
        let watch_key_pattern = format!("{CALLBACK_KEY_PREFIX}.{pod_hash}.*.*.*");
        let mut watcher = Self::create_watcher(
            &kv_store,
            &watch_key_pattern,
            latest_revision + 1,
            cln_token.clone(),
        )
        .await
        .expect("Failed to create central watcher");

        tokio::spawn(async move {
            // Main watch loop
            loop {
                tokio::select! {
                // Stop watching if the cancellation token is triggered, its the callee's responsibility
                // to cancel the token after all the inflight messages are processed (after server shutdown)
                _ = cln_token.cancelled() => {
                    info!("Cancellation token triggered. Stopping central watcher.");
                    break;
                }

                // Process the next entry from the watcher
                maybe_entry = watcher.next() => {
                    match maybe_entry {
                        Some(Ok(entry)) => {
                            latest_revision = entry.revision;
                            debug!(key = ?entry.key, operation = ?entry.operation, revision = entry.revision, "Callback central watcher received entry");

                            // Skip deletions/purges, only process Put operations for new callbacks
                            if entry.operation != async_nats::jetstream::kv::Operation::Put {
                                continue;
                            }

                            // Parse key: cb.{pod_hash}.{id}.{vertex_name}.{timestamp}
                            let parts: Vec<&str> = entry.key.splitn(5, '.').collect();
                            let cb_pod_hash = parts[1];
                            let request_id = parts[2];

                            if parts.len() < 5 || parts[0] != CALLBACK_KEY_PREFIX || cb_pod_hash != pod_hash {
                                error!(?entry, "Received unexpected key format in central watcher");
                                continue;
                            }

                            let callback: Arc<Callback> = Arc::new(entry.value.try_into().expect("Failed to deserialize callback"));
                            // we need to send the callback to appropriate sender based on the id
                            let senders_guard = senders.lock().await;
                            if let Some(cb_sender) = senders_guard.get(request_id) {
                                cb_sender.send(callback).await.expect("Failed to send callback");
                            } else {
                                error!(id = ?callback.id, "No active sender found for request id. Callback not sent.");
                            }
                        }
                        Some(Err(e)) => {
                            error!(error = ?e, "Error from central watcher stream. Re-establishing watch...");
                            // when we are recreating the watcher we should start watching from the latest revision+1
                            // because we would have successfully read till latest revision, duplicate callbacks
                            // will result in tracker not able to decide if the request has been completed.
                            watcher = match Self::create_watcher(&kv_store, &watch_key_pattern, latest_revision + 1, cln_token.clone()).await {
                                Ok(stream) => stream,
                                Err(e) => {
                                    error!(error = ?e, "Failed to recreate watcher. Stopping scan.");
                                    break;
                                }
                            };
                        }
                        None => {
                            error!("Central watcher stream ended unexpectedly. Attempting to restart.");
                            // when we are recreating the watcher we should start watching from the latest revision+1
                            // because we would have successfully read till latest revision, duplicate callbacks
                            // will result in tracker not able to decide if the request has been completed.
                            watcher = match Self::create_watcher(&kv_store, &watch_key_pattern, latest_revision + 1, cln_token.clone()).await {
                                Ok(stream) => stream,
                                Err(e) => {
                                    error!(?e, "Failed to recreate watcher. Stopping scan.");
                                    break;
                                }
                            };
                        }
                    }
                }
            }
            }
        }.instrument(span));
    }

    /// Watch for all the callbacks for a given request id that was getting processed by a different pod.
    async fn watch_historical_callbacks(
        callback_kv: Store,
        previous_pod_hash: String,
        id: String,
        tx: mpsc::Sender<Arc<Callback>>,
        cln_token: CancellationToken,
    ) {
        let key_pattern = format!("{CALLBACK_KEY_PREFIX}.{previous_pod_hash}.{id}.*.*");
        let mut latest_revision = 0;

        info!(request_id = ?id, ?previous_pod_hash, key_pattern, "Historical scan task started");

        let mut entries_stream = match Self::create_watcher(
            &callback_kv,
            &key_pattern,
            latest_revision + 1,
            cln_token.clone(),
        )
        .await
        {
            Ok(stream) => stream,
            Err(e) => {
                error!(request_id = ?id, error = ?e, "Failed to initiate historical callback scan");
                return;
            }
        };

        loop {
            match entries_stream.next().await {
                Some(Ok(entry)) => {
                    latest_revision = entry.revision;
                    if entry.operation == async_nats::jetstream::kv::Operation::Delete {
                        return;
                    }
                    match entry.value.try_into() {
                        Ok(callback_obj) => {
                            let callback = Arc::new(callback_obj);
                            if tx.send(callback).await.is_err() {
                                warn!(
                                    request_id = %id,
                                    "Receiver dropped during historical send. Stopping scan stream."
                                );
                                break;
                            }
                        }
                        Err(e) => {
                            error!(request_id = ?id, key = ?entry.key, error = ?e, "Failed to deserialize historical callback during scan");
                        }
                    }
                }
                Some(Err(e)) => {
                    error!(request_id = ?id, error = ?e, "Error iterating historical callback scan stream. Recreating watcher...");
                    entries_stream = match Self::create_watcher(
                        &callback_kv,
                        &key_pattern,
                        latest_revision + 1,
                        cln_token.clone(),
                    )
                    .await
                    {
                        Ok(stream) => stream,
                        Err(re_e) => {
                            error!(request_id = ?id, error = ?re_e, "Failed to recreate watcher. Stopping scan.");
                            break;
                        }
                    };
                }
                None => {
                    error!(request_id = ?id, "Historical callback stream ended unexpectedly. Recreating watcher...");
                    entries_stream = match Self::create_watcher(
                        &callback_kv,
                        &key_pattern,
                        latest_revision + 1,
                        cln_token.clone(),
                    )
                    .await
                    {
                        Ok(stream) => stream,
                        Err(re_e) => {
                            error!(request_id = ?id, error = ?re_e, "Failed to recreate watcher. Stopping scan.");
                            break;
                        }
                    };
                }
            }
        }
        info!(?id, "Finished streaming historical callbacks.");
    }

    /// creates a kv watcher for the given key pattern and revision, and it keeps retrying until it's
    /// successful.
    async fn create_watcher(
        callback_kv: &Store,
        key_pattern: &str,
        start_revision: u64,
        cln_token: CancellationToken,
    ) -> StoreResult<Watch> {
        loop {
            match callback_kv
                .watch_from_revision(key_pattern, start_revision)
                .await
            {
                Ok(watcher) => {
                    info!(key_pattern, start_revision, "Watcher created successfully");
                    return Ok(watcher);
                }
                Err(e) => {
                    error!(error = ?e, "Failed to create watcher. Retrying...");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            if cln_token.is_cancelled() {
                error!("Cancellation token triggered, exiting watcher creation retry loop.");
                return Err(StoreError::Connection(
                    "Cancellation token triggered".to_string(),
                ));
            }
        }
    }
}

impl super::CallbackStore for JetStreamCallbackStore {
    /// Register the request and watch for callbacks.
    async fn register_and_watch(
        &mut self,
        id: &str,
        failed_pod_hash: Option<String>,
    ) -> StoreResult<ReceiverStream<Arc<Callback>>> {
        let (tx, rx) = mpsc::channel(10);

        // if the pod_hash is same as the current pod, we can directly register the sender because
        // central watcher is already watching the callbacks for this pod. If not we will have to
        // create a new watcher for watching callbacks of the different pod.
        if let Some(failed_pod_hash) = failed_pod_hash {
            let callback_kv_clone = self.callback_kv.clone();
            let id_clone = id.to_string();
            let tx_clone = tx.clone();
            let previous_pod_hash = failed_pod_hash.clone();
            let cln_token = self.cln_token.clone();

            let mut senders_guard = self.callback_senders.lock().await;
            senders_guard.insert(id.to_string(), tx);
            // Spawn a separate task for historical callbacks
            tokio::spawn(async move {
                JetStreamCallbackStore::watch_historical_callbacks(
                    callback_kv_clone,
                    previous_pod_hash,
                    id_clone,
                    tx_clone,
                    cln_token,
                )
                .await;
            });
            Ok(ReceiverStream::new(rx))
        } else {
            let mut senders_guard = self.callback_senders.lock().await;
            senders_guard.insert(id.to_string(), tx);
            Ok(ReceiverStream::new(rx))
        }
    }

    /// deregister the request and remove the callback sender from the map.
    async fn deregister(&mut self, id: &str) -> StoreResult<()> {
        let mut senders_guard = self.callback_senders.lock().await;
        if senders_guard.remove(id).is_none() {
            error!(?id, "No active sender found during deregistration");
        }
        Ok(())
    }

    async fn ready(&mut self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::store::cbstore::CallbackStore;
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;
    use bytes::Bytes;
    use chrono::Utc;

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

        let mut store = JetStreamCallbackStore::new(
            context.clone(),
            "0",
            serving_store,
            CancellationToken::new(),
        )
        .await
        .unwrap();

        let id = "AFA7E0A1-3F0A-4C1B-AB94-BDA57694648D";
        let result = store.register_and_watch(id, None).await;
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
        let pod_hash = "0";

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

        let mut store = JetStreamCallbackStore::new(
            context.clone(),
            pod_hash,
            serving_store,
            CancellationToken::new(),
        )
        .await
        .unwrap();

        let id = "test_watch_id_two";
        let mut stream = store.register_and_watch(id, None).await.unwrap();

        // Simulate a callback being added to the store
        let callback = Callback {
            id: id.to_string(),
            vertex: "test_vertex".to_string(),
            cb_time: 12345,
            from_vertex: "test_from_vertex".to_string(),
            responses: vec![],
        };
        let key = format!("cb.{pod_hash}.{id}.input.{}", Utc::now().timestamp());
        store
            .callback_kv
            .put(key, Bytes::from(serde_json::to_vec(&callback).unwrap()))
            .await
            .unwrap();

        // Verify that the callback is received
        let received_callback = stream.next().await.unwrap();
        assert_eq!(received_callback.id, callback.id);
        assert_eq!(received_callback.vertex, callback.vertex);
        assert_eq!(received_callback.cb_time, callback.cb_time);
        assert_eq!(received_callback.from_vertex, callback.from_vertex);
        assert_eq!(received_callback.responses.len(), callback.responses.len());

        // delete store
        context.delete_key_value(serving_store).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_watch_historical_callbacks() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let serving_store = "test_watch_historical_callbacks";
        let current_pod_hash = "xxba";
        let previous_pod_hash = "xbzb";

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

        let mut store = JetStreamCallbackStore::new(
            context.clone(),
            current_pod_hash,
            serving_store,
            CancellationToken::new(),
        )
        .await
        .unwrap();

        let id = "test_historical_id";

        // Simulate historical callbacks in the KV store
        let callback = Callback {
            id: id.to_string(),
            vertex: "test_vertex".to_string(),
            cb_time: 12345,
            from_vertex: "test_from_vertex".to_string(),
            responses: vec![],
        };
        let callback_key = format!(
            "cb.{}.{}.input.{}",
            previous_pod_hash,
            id,
            Utc::now().timestamp()
        );
        store
            .callback_kv
            .put(
                callback_key,
                Bytes::from(serde_json::to_vec(&callback).unwrap()),
            )
            .await
            .unwrap();

        // Register and watch the callbacks
        let mut stream = store
            .register_and_watch(id, Some(previous_pod_hash.to_string()))
            .await
            .unwrap();

        // Verify that the historical callback is received
        let received_callback = stream.next().await.unwrap();
        assert_eq!(received_callback.id, callback.id);
        assert_eq!(received_callback.vertex, callback.vertex);
        assert_eq!(received_callback.cb_time, callback.cb_time);
        assert_eq!(received_callback.from_vertex, callback.from_vertex);
        assert_eq!(received_callback.responses.len(), callback.responses.len());

        // Clean up the KV store
        context.delete_key_value(serving_store).await.unwrap();
    }
}
