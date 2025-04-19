//! Stores the callback and status information in JetStream KV stores.
//! A central watcher task per instance monitors callbacks for that instance.
//! Status includes the processing replica ID and is stored as a serialized enum.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::app::store::cbstore::ProcessingStatus;
use crate::app::store::datastore::{Error as StoreError, Result as StoreResult};
use crate::callback::Callback;
use crate::config::RequestType;
use async_nats::jetstream::kv::{CreateErrorKind, Store};
use async_nats::jetstream::Context;
use bytes::Bytes;
use chrono::Utc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn, Instrument};

const CALLBACK_KEY_PREFIX: &str = "cb";
const STATUS_KEY_PREFIX: &str = "status";
const RESPONSE_KEY_PREFIX: &str = "rs";
const START_PROCESSING_MARKER: &str = "start.processing";
const DONE_PROCESSING_MARKER: &str = "done.processing";

/// JetStream implementation of the callback store.
#[derive(Clone)]
pub(crate) struct JetStreamCallbackStore {
    pod_replica_id: String,
    callback_kv: Store,
    status_kv: Store,
    response_kv: Store,
    callback_senders: Arc<Mutex<HashMap<String, mpsc::Sender<Arc<Callback>>>>>,
}

impl JetStreamCallbackStore {
    pub(crate) async fn new(
        js_context: Context,
        pod_replica_id: String,
        callback_bucket: &str,
        status_bucket: &str,
        response_bucket: &str,
    ) -> StoreResult<Self> {
        info!(
            pod_replica_id,
            callback_bucket, response_bucket, "Initializing JetStreamCallbackStore"
        );

        let callback_kv = js_context
            .get_key_value(callback_bucket)
            .await
            .map_err(|e| {
                StoreError::Connection(format!(
                    "Failed to get callback kv store '{callback_bucket}': {e:?}"
                ))
            })?;

        let status_kv = js_context.get_key_value(status_bucket).await.map_err(|e| {
            StoreError::Connection(format!(
                "Failed to get status kv store '{status_bucket}': {e:?}"
            ))
        })?;

        let response_kv = js_context
            .get_key_value(response_bucket)
            .await
            .map_err(|e| {
                StoreError::Connection(format!(
                    "Failed to get status kv store '{response_bucket}': {e:?}"
                ))
            })?;

        let callback_senders = Arc::new(Mutex::new(HashMap::new()));
        Self::spawn_central_watcher(
            pod_replica_id.clone(),
            callback_kv.clone(),
            Arc::clone(&callback_senders),
        );

        Ok(Self {
            pod_replica_id,
            callback_kv,
            status_kv,
            response_kv,
            callback_senders,
        })
    }

    fn spawn_central_watcher(
        pod_replica_id: String,
        kv_store: Store,
        senders: Arc<Mutex<HashMap<String, mpsc::Sender<Arc<Callback>>>>>,
    ) -> JoinHandle<()> {
        let span = tracing::info_span!("central_callback_watcher", replica_id = ?pod_replica_id);
        let mut latest_revision = 1;

        tokio::spawn(async move {
            // Watch key specific to this replica
            let watch_key_pattern = format!("{}.{}.*.*.*", CALLBACK_KEY_PREFIX, pod_replica_id);
            info!(watch_key_pattern, "Starting central callback watcher");

            let mut watcher = loop {
                // Use watch_any for robustness against missed updates if watcher restarts
                match kv_store.watch(&watch_key_pattern).await {
                    Ok(w) => {
                        info!(watch_key_pattern, "Central watcher established");
                        break w;
                    },
                    Err(e) => {
                        error!(error = ?e, "Failed to establish initial watch for {}. Retrying...", watch_key_pattern);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                };
            };

            // Main watch loop
            loop {
                match watcher.next().await {
                    Some(Ok(entry)) => {
                        let cb_process_time = Instant::now();
                        latest_revision = entry.revision;
                        debug!(key = ?entry.key, operation = ?entry.operation, revision = entry.revision, "Central watcher received entry");

                        // Skip deletions/purges, only process Put operations for new callbacks
                        if entry.operation != async_nats::jetstream::kv::Operation::Put {
                            continue;
                        }

                        // Parse key: cb.{pod_replica_id}.{id}.{vertex_name}.{timestamp}
                        let parts: Vec<&str> = entry.key.splitn(5, '.').collect();
                        let pod_replica = parts[1];
                        let request_id = parts[2];
                        if parts.len() < 5 || parts[0] != CALLBACK_KEY_PREFIX || pod_replica != pod_replica_id {
                            error!(key = ?entry.key, "Received unexpected key format in central watcher");
                            continue;
                        }

                        match entry.value.try_into() {
                            Ok(callback_obj) => {
                                let callback: Arc<Callback> = Arc::new(callback_obj);
                                let current_time = Utc::now().timestamp_millis() as u64;
                                info!(id = ?callback.id, "Received cb with time diff={:?}", current_time - callback.cb_time);
                                let mut sender = None;
                                {
                                    let senders_guard = senders.lock().expect("Failed to acquire lock");
                                    if let Some(cb_sender) = senders_guard.get(request_id) {
                                        sender = Some(cb_sender.clone());
                                    }
                                }
                                if let Some(sender) = sender {
                                    if sender.send(callback).await.is_err() {
                                        warn!(request_id = ?request_id, "Receiver dropped during send. Stopping watch.");
                                        break;
                                    }
                                } else {
                                    warn!(request_id = ?request_id, "No active sender found for received callback");
                                }
                            }
                            Err(e) => {
                                error!(key = ?entry.key, error = ?e, "Failed to deserialize callback in central watcher");
                            }
                        }
                        info!("Time taken to process a callback={:?}", cb_process_time.elapsed().as_millis());
                    }
                    Some(Err(e)) => {
                        error!(error = ?e, "Error from central watcher stream. Re-establishing watch...");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        watcher = loop {
                            match kv_store.watch_from_revision(&watch_key_pattern, latest_revision+1).await {
                                Ok(w) => {
                                    info!("Central watcher re-established after error.");
                                    break w;
                                },
                                Err(re_e) => {
                                    error!(error = ?re_e, "Failed to re-establish watch. Trying again after delay.");
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                }
                            }
                        };
                    }
                    None => {
                        error!("Central watcher stream ended unexpectedly. Attempting to restart.");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        watcher = loop {
                            match kv_store.watch_from_revision(&watch_key_pattern, latest_revision+1).await {
                                Ok(w) => {
                                    info!("Central watcher restarted after stream ended.");
                                    break w;
                                },
                                Err(e) => {
                                    error!(?e, "Failed to restart watch. Trying again after delay.");
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                }
                            }
                        };
                    }
                }
            }
        }.instrument(span))
    }

    async fn update_status(&self, id: &str, status: ProcessingStatus) -> StoreResult<()> {
        let key = format!("{}.{}", STATUS_KEY_PREFIX, id);

        let status_bytes: Bytes = status.try_into().map_err(|e| {
            StoreError::StoreWrite(format!("Failed to serialize status for {id}: {e:?}"))
        })?;

        self.status_kv.put(key, status_bytes).await.map_err(|e| {
            StoreError::StoreWrite(format!("Failed to update status for {id}: {e:?}"))
        })?;
        Ok(())
    }

    async fn watch_historical_callbacks(
        callback_kv: Store,
        previous_replica_id: String,
        id: String,
        tx: mpsc::Sender<Arc<Callback>>,
    ) {
        let key_pattern = format!("{}.{}.{}.*.*", CALLBACK_KEY_PREFIX, previous_replica_id, id);
        info!(request_id = ?id, ?previous_replica_id, key_pattern, "Historical scan task started");

        let mut entries_stream = match callback_kv.watch(&key_pattern).await {
            Ok(stream) => stream,
            Err(e) => {
                error!(request_id = ?id, error = ?e, "Failed to initiate historical callback scan");
                return;
            }
        };

        let mut count = 0;
        while let Some(entry_result) = entries_stream.next().await {
            match entry_result {
                Ok(entry) => {
                    if entry.operation == async_nats::jetstream::kv::Operation::Delete {
                        return;
                    }
                    match entry.value.try_into() {
                        Ok(callback_obj) => {
                            let callback = Arc::new(callback_obj);
                            if tx.send(callback).await.is_err() {
                                warn!(request_id = ?id, "Receiver dropped during historical send. Stopping scan stream.");
                                break;
                            }
                            count += 1;
                        }
                        Err(e) => {
                            error!(request_id = ?id, key = ?entry.key, error = ?e, "Failed to deserialize historical callback during scan");
                        }
                    }
                }
                Err(e) => {
                    error!(request_id = ?id, error = ?e, "Error iterating historical callback scan stream. Stopping scan.");
                    break;
                }
            }
        }
        info!(?id, count, "Finished streaming historical callbacks.");
    }
}

impl super::CallbackStore for JetStreamCallbackStore {
    async fn deregister(&mut self, id: &str, sub_graph: &str) -> StoreResult<()> {
        let completed_status = ProcessingStatus::Completed {
            subgraph: sub_graph.to_string(),
            replica_id: self.pod_replica_id.clone(),
        };

        self.update_status(id, completed_status).await?;
        {
            // Remove sender from the map *after* successfully updating status
            let mut senders_guard = self
                .callback_senders
                .lock()
                .expect("Failed to acquire lock");
            if senders_guard.remove(id).is_some() {
                debug!(id, "Removed active sender during deregistration.");
            } else {
                debug!(id, "No active sender found during deregistration (might not have been watched or already removed).");
            }
        }

        debug!(?id, replica_id = ?self.pod_replica_id, "De-registering request for data collection.");
        let done_key = format!(
            "{}.{}.{}.{}",
            RESPONSE_KEY_PREFIX, self.pod_replica_id, id, DONE_PROCESSING_MARKER
        );
        match self.response_kv.put(done_key.clone(), Bytes::new()).await {
            Ok(_) => {
                debug!(?id, ?done_key, "Successfully wrote done processing marker.");
            }
            Err(e) => {
                error!(?id, ?done_key, error = ?e, "Failed to write done processing marker.");
                return Err(StoreError::StoreWrite(format!(
                    "Failed to write done marker {done_key}: {e:?}"
                )));
            }
        }
        Ok(())
    }

    async fn mark_as_failed(&mut self, id: &str, error: &str) -> StoreResult<()> {
        info!(id, replica_id = ?self.pod_replica_id, error, "Marking request as failed");
        let failed_status = ProcessingStatus::Failed {
            error: error.to_string(),
            replica_id: self.pod_replica_id.clone(),
        };
        self.update_status(id, failed_status).await?;

        // Remove sender from the map *after* successfully updating status
        let mut senders_guard = self
            .callback_senders
            .lock()
            .expect("Failed to lock senders");
        if senders_guard.remove(id).is_some() {
            debug!(id, "Removed active sender during failure marking.");
        } else {
            debug!(id, "No active sender found during failure marking (might not have been watched or already removed).");
        }
        Ok(())
    }

    async fn register_and_watch(
        &mut self,
        id: &str,
        request_type: RequestType,
    ) -> StoreResult<ReceiverStream<Arc<Callback>>> {
        let start_time = Instant::now();
        let status_key = format!("{}.{}", STATUS_KEY_PREFIX, id);
        debug!(id, replica_id = ?self.pod_replica_id, "Registering request");

        let initial_status = ProcessingStatus::InProgress {
            replica_id: self.pod_replica_id.clone(),
        };
        let status_bytes: Bytes = initial_status.clone().try_into().map_err(|e| {
            StoreError::StoreWrite(format!("Failed to serialize status for {id}: {e:?}"))
        })?;

        let current_status = match self
            .callback_kv
            .create(&status_key, status_bytes)
            .await
        {
            Ok(_) => {
                info!(id, status_key, "Request registered successfully.");
                initial_status
            }
            Err(e) => {
                if e.kind() == CreateErrorKind::AlreadyExists {
                    warn!(
                        id,
                        status_key, "Registration attempt failed: Request ID already exists."
                    );
                    let status_entry = self.callback_kv.get(&status_key).await.map_err(|e| {
                        StoreError::StoreRead(format!(
                            "Failed to get status for watch check {id}: {e:?}"
                        ))
                    })?;
                    ProcessingStatus::try_from(status_entry.expect("status should be present"))
                        .map_err(|e| {
                            StoreError::StoreRead(format!(
                                "Failed to deserialize status for watch check {id}: {e:?}"
                            ))
                        })?
                } else {
                    error!(id, status_key, error = ?e, "Failed to create status entry during registration.");
                    return Err(StoreError::StoreWrite(format!(
                        "Failed to register request id {status_key} in status kv store: {e:?}"
                    )));
                }
            }
        };

        let (tx, rx) = mpsc::channel(10);
        match &current_status {
            ProcessingStatus::InProgress {
                replica_id: processing_replica_id,
            } => {
                if processing_replica_id != &self.pod_replica_id {
                    warn!(current_replica = ?self.pod_replica_id, previous_replica = ?processing_replica_id, "Fail over detected. Taking over request processing.");
                    let callback_kv_clone = self.callback_kv.clone();
                    let id_clone = id.to_string();
                    let tx_clone = tx.clone();
                    let previous_replica_id = processing_replica_id.clone();

                    // Spawn a separate task for historical callbacks
                    tokio::spawn(async move {
                        JetStreamCallbackStore::watch_historical_callbacks(
                            callback_kv_clone,
                            previous_replica_id,
                            id_clone,
                            tx_clone,
                        )
                        .await;
                    });
                } else {
                    debug!("Already processing on this replica.");
                }
            }
            ProcessingStatus::Completed { .. } | ProcessingStatus::Failed { .. } => {
                error!(status = ?current_status, "Attempted to watch callbacks for a completed/failed request.");
                return Err(StoreError::InvalidRequestId(format!(
                    "Request {id} already finished: {:?}",
                    current_status
                )));
            }
        }

        {
            let mut senders_guard = self
                .callback_senders
                .lock()
                .expect("Failed to lock senders");
            senders_guard.insert(id.to_string(), tx);
        }

        let response_key = format!(
            "{}.{}.{}.{}",
            RESPONSE_KEY_PREFIX, self.pod_replica_id, id, START_PROCESSING_MARKER
        );

        let request_type_bytes: Bytes = request_type.try_into().map_err(|e| {
            StoreError::StoreWrite(format!("Failed to serialize request type for {id}: {e:?}"))
        })?;

        self.response_kv
            .put(&response_key, request_type_bytes)
            .await
            .map_err(|e| {
                StoreError::StoreWrite(format!("Failed to create response entry for {id}: {e:?}"))
            })?;

        info!(
            "Time taken to register={:?}",
            start_time.elapsed().as_millis()
        );
        Ok(ReceiverStream::new(rx))
    }

    async fn status(&mut self, id: &str) -> StoreResult<ProcessingStatus> {
        let key = format!("{}.{}", STATUS_KEY_PREFIX, id);
        let entry = self.callback_kv.get(&key).await.map_err(|e| {
            StoreError::StoreRead(format!("Failed to get status for request id {id}: {e:?}"))
        })?;
        match entry {
            Some(status) => ProcessingStatus::try_from(status).map_err(|e| {
                StoreError::StoreRead(format!("Failed to deserialize status for {id}: {e:?}"))
            }),
            None => Err(StoreError::InvalidRequestId(id.to_string())),
        }
    }

    async fn ready(&mut self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_watch_revision() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let serving_store = "test_watch_callbacks";

        // Delete bucket so that re-running the test won't fail
        let _ = context.delete_key_value(serving_store).await;

        let store = context
            .create_key_value(Config {
                bucket: serving_store.to_string(),
                description: "test_description".to_string(),
                history: 15,
                ..Default::default()
            })
            .await
            .unwrap();

        // insert 10 dummy values to the store with keys "x.0" to "x.9"
        for i in 0..10 {
            let key = format!("x.{i}");
            store.put(key, format!("value {i}").into()).await.unwrap();
            let key = format!("y.{i}");
            store.put(key, format!("value {i}").into()).await.unwrap();
        }

        for i in 0..10 {
            let key = format!("y.{i}");
            store.put(key, format!("value {i}").into()).await.unwrap();
        }

        let mut latest_revision = 1;
        // watch for the 5 values, and create a new watch with revision and watch another 5
        let mut watcher = store
            .watch_from_revision("x.*", latest_revision)
            .await
            .unwrap();
        let mut count = 0;
        while let Some(entry) = watcher.next().await {
            match entry {
                Ok(entry) => {
                    println!(
                        "Key: {}, Value: {}, Revision: {}",
                        entry.key,
                        String::from_utf8_lossy(&entry.value),
                        entry.revision
                    );
                    count += 1;
                    latest_revision = entry.revision;
                }
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
            if count == 5 {
                break;
            }
        }

        for i in 0..1000 {
            let key = format!("z.{i}");
            store.put(key, format!("value {i}").into()).await.unwrap();
        }

        // Create a new watch with revision
        let mut watcher = store
            .watch_from_revision("x.*", latest_revision + 1)
            .await
            .unwrap();
        count = 0;
        while let Some(entry) = watcher.next().await {
            match entry {
                Ok(entry) => {
                    println!(
                        "Key: {}, Value: {}, Revision: {}",
                        entry.key,
                        String::from_utf8_lossy(&entry.value),
                        entry.revision
                    );
                    count += 1;
                }
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
            if count == 5 {
                break;
            }
        }
    }
}
