//! Stores the response from the processing in a JetStream key-value store. Each response is stored
//! as a new key-value pair in the store, so that no responses are lost. We cannot use the same key
//! and overwrite the value, since the history of the kv store is not per key, but for the entire
//! store.
//!
//! **JetStream Response Entry Format**
//!
//! Response Key - rs.{id}.{vertex_name}.{timestamp}
//!
//! Response Value - response_payload
use crate::app::store::datastore::{DataStore, Error as StoreError, Result as StoreResult};
use crate::config::RequestType;
use async_nats::jetstream::kv::Store;
use async_nats::jetstream::Context;
use bytes::{Buf, BufMut, Bytes};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::{error, info, warn};

const RESPONSE_KEY_PREFIX: &str = "rs";
const FINAL_RESULT_KEY_SUFFIX: &str = "final.result.processed";
const START_PROCESSING_MARKER: &str = "start.processing";
const DONE_PROCESSING_MARKER: &str = "done.processing";

/// Merges a list of Bytes using u64 length prefixing (Big Endian).
fn merge_bytes_list(list: &[Bytes]) -> Bytes {
    let mut accumulator = Vec::new();
    for b in list {
        let len = b.len() as u64;
        accumulator.put_u64(len);
        accumulator.put_slice(b);
    }
    Bytes::from(accumulator)
}

/// Extracts a list of Bytes merged with u64 length prefixing.
fn extract_bytes_list(merged: &Bytes) -> Result<Vec<Bytes>, String> {
    let mut extracted = Vec::new();
    let mut cursor = Cursor::new(merged);

    while cursor.has_remaining() {
        if cursor.remaining() < size_of::<u64>() {
            if cursor.remaining() == 0 {
                break;
            } else {
                return Err(format!(
                    "Truncated data: not enough bytes for length prefix. Remaining: {}",
                    cursor.remaining()
                ));
            }
        }
        let len = cursor.get_u64();
        if cursor.remaining() < len as usize {
            return Err(format!(
                "Truncated data: declared payload length {} exceeds remaining bytes {}",
                len,
                cursor.remaining()
            ));
        }
        let current_pos = cursor.position() as usize;
        let end_pos = current_pos + (len as usize);
        extracted.push(merged.slice(current_pos..end_pos));
        cursor.advance(len as usize);
    }
    Ok(extracted)
}

/// A JetStream implementation of the data store.
#[derive(Clone)]
pub(crate) struct JetStreamDataStore {
    kv_store: Store,
    pod_replica_id: String,
    responses_map: Arc<Mutex<HashMap<String, ResponseMode>>>,
}

impl JetStreamDataStore {
    pub(crate) async fn new(
        js_context: Context,
        datum_store_name: &str,
        pod_replica_id: String,
    ) -> StoreResult<Self> {
        let kv_store = js_context
            .get_key_value(datum_store_name)
            .await
            .map_err(|e| StoreError::Connection(format!("Failed to get datum kv store: {e:?}")))?;

        let responses_map = Arc::new(Mutex::new(HashMap::new()));
        // start the central watcher
        Self::spawn_central_watcher(
            pod_replica_id.clone(),
            kv_store.clone(),
            Arc::clone(&responses_map),
        );
        info!(
            "Central watcher spawned for pod replica ID: {}",
            pod_replica_id
        );
        Ok(Self {
            kv_store,
            pod_replica_id,
            responses_map,
        })
    }

    /// Spawns the central task that watches response keys and manages data collection.
    fn spawn_central_watcher(
        pod_replica_id: String,
        kv_store: Store,
        responses_map: Arc<Mutex<HashMap<String, ResponseMode>>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let watch_pattern = format!("{}.{}.*.*.*", RESPONSE_KEY_PREFIX, pod_replica_id);
            info!(watch_pattern, "Starting central data watcher");
            let mut latest_revision = 1;

            let mut watcher = loop {
                match kv_store.watch(&watch_pattern).await {
                    Ok(w) => {
                        info!(watch_pattern, "Central watcher established");
                        break w;
                    }
                    Err(e) => {
                        error!(error = ?e, "Failed to establish initial watch for {}. Retrying...", watch_pattern);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                };
            };

            loop {
                match watcher.next().await {
                    Some(Ok(entry)) => {
                        latest_revision = entry.revision;
                        if entry.operation != async_nats::jetstream::kv::Operation::Put {
                            continue;
                        }

                        if entry.key.contains(FINAL_RESULT_KEY_SUFFIX) {
                            continue;
                        }

                        // Parse key: rs.{pod_replica_id}.{id}.{vertex_name}.{timestamp}
                        let parts: Vec<&str> = entry.key.splitn(5, '.').collect();

                        let pod_replica_id = parts[1];
                        let request_id = parts[2];
                        let key_suffix = format!("{}.{}", parts[3], parts[4]);

                        if parts.len() < 5
                            || parts[0] != RESPONSE_KEY_PREFIX
                            || parts[1] != pod_replica_id
                        {
                            error!(key = ?entry.key, "Received unexpected key format in central watcher");
                            continue;
                        }

                        if key_suffix == START_PROCESSING_MARKER {
                            let request_type = RequestType::try_from(entry.value.clone())
                                .expect("Failed to convert request type");
                            match request_type {
                                RequestType::Sse => {
                                    let (tx, rx) = mpsc::channel(10);
                                    let mut response_map = responses_map.lock().await;
                                    if response_map.contains_key(request_id) {
                                        warn!(
                                            ?request_id,
                                            "Start processing marker already exists for this request ID"
                                        );
                                    } else {
                                        response_map
                                            .insert(request_id.to_string(), ResponseMode::Sse(tx));
                                    }
                                }
                                RequestType::Sync => {
                                    let mut response_map = responses_map.lock().await;
                                    if response_map.contains_key(request_id) {
                                        warn!(
                                            ?request_id,
                                            "Start processing marker already exists for this request ID"
                                        );
                                    } else {
                                        response_map.insert(
                                            request_id.to_string(),
                                            ResponseMode::Sync(Vec::new()),
                                        );
                                    }
                                }
                                RequestType::Async => {
                                    let mut response_map = responses_map.lock().await;
                                    if response_map.contains_key(request_id) {
                                        warn!(
                                            ?request_id,
                                            "Start processing marker already exists for this request ID"
                                        );
                                    } else {
                                        response_map.insert(
                                            request_id.to_string(),
                                            ResponseMode::Async(Vec::new()),
                                        );
                                    }
                                }
                            }
                            continue;
                        }

                        if key_suffix == DONE_PROCESSING_MARKER {
                            let final_result_key = format!(
                                "{}.{}.{}",
                                RESPONSE_KEY_PREFIX, pod_replica_id, FINAL_RESULT_KEY_SUFFIX
                            );

                            // Extract the response_mode while holding the lock
                            let response_mode = {
                                let mut response_map = responses_map.lock().await;
                                response_map.get_mut(request_id).cloned()
                            };

                            let Some(response_mode) = response_mode else {
                                continue;
                            };

                            match response_mode {
                                ResponseMode::Sync(responses) => {
                                    let merged_response = merge_bytes_list(&responses);

                                    // Perform the I/O operation outside the lock
                                    kv_store
                                        .put(final_result_key.clone(), merged_response)
                                        .await
                                        .expect("Failed to put final-result key");
                                }
                                ResponseMode::Async(responses) => {
                                    let merged_response = merge_bytes_list(&responses);

                                    // Perform the I/O operation outside the lock
                                    kv_store
                                        .put(final_result_key.clone(), merged_response)
                                        .await
                                        .unwrap();

                                    let mut response_map = responses_map.lock().await;
                                    response_map.remove(request_id);
                                }
                                ResponseMode::Sse(_) => {
                                    // The responses are already being sent to the channel
                                    let mut response_map = responses_map.lock().await;
                                    response_map.remove(request_id);
                                }
                            }
                            continue;
                        }

                        let mut response_map = responses_map.lock().await;
                        if let Some(response_mode) = response_map.get_mut(request_id) {
                            match response_mode {
                                ResponseMode::Sse(tx) => {
                                    if let Err(e) = tx.send(Arc::new(entry.value)).await {
                                        error!(error = ?e, "Failed to send SSE response");
                                    }
                                }
                                ResponseMode::Sync(responses) => {
                                    responses.push(entry.value);
                                }
                                ResponseMode::Async(responses) => {
                                    responses.push(entry.value);
                                }
                            }
                        }
                    }

                    Some(Err(e)) => {
                        error!(error = ?e, "Error from central watcher stream. Re-establishing watch...");
                        watcher = loop {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            match kv_store
                                .watch_from_revision(&watch_pattern, latest_revision + 1)
                                .await
                            {
                                Ok(w) => {
                                    info!(
                                        "Central result store watcher re-established after error."
                                    );
                                    break w;
                                }
                                Err(re_e) => {
                                    error!(error = ?re_e, "Failed to re-establish watch. Trying again after delay.");
                                }
                            }
                        };
                    }

                    None => {
                        error!("Central watcher stream ended unexpectedly. Attempting to restart.");
                        watcher = loop {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            match kv_store
                                .watch_from_revision(&watch_pattern, latest_revision + 1)
                                .await
                            {
                                Ok(w) => {
                                    info!("Central result store  watcher restarted after stream ended.");
                                    break w;
                                }
                                Err(re_e) => {
                                    error!(error = ?re_e, "Failed to restart watch. Trying again after delay.");
                                }
                            }
                        };
                    }
                }
            }
        })
    }
}

#[derive(Clone)]
enum ResponseMode {
    Sse(mpsc::Sender<Arc<Bytes>>),
    Sync(Vec<Bytes>),
    Async(Vec<Bytes>),
}

impl DataStore for JetStreamDataStore {
    /// Retrieve a data from the store. If the datum is not found, `None` is returned.
    async fn retrieve_data(&mut self, id: &str) -> StoreResult<Vec<Vec<u8>>> {
        // Check if the data exists in the in-memory map
        {
            if let Some(ResponseMode::Sync(data) | ResponseMode::Async(data)) = self.responses_map.lock().await.remove(id) {
                return Ok(data.iter().map(|b| b.to_vec()).collect());
            }
        }

        // Fallback to retrieving the data from the store
        let final_result_key = format!(
            "{}.{}.{}",
            RESPONSE_KEY_PREFIX, self.pod_replica_id, FINAL_RESULT_KEY_SUFFIX
        );

        loop {
            let response = self.kv_store.get(&final_result_key).await.map_err(|e| {
                StoreError::StoreRead(format!(
                    "Failed to get final result key from kv store: {e:?}"
                ))
            })?;

            if let Some(result) = response {
                let extracted = extract_bytes_list(&result).map_err(|e| {
                    StoreError::StoreRead(format!("Failed to extract bytes list: {e:?}"))
                })?;
                return Ok(extracted.iter().map(|b| b.to_vec()).collect());
            } else {
                warn!(?id, "No final result key found in kv store, retrying");
                sleep(Duration::from_millis(20)).await;
            }
        }
    }

    /// Stream the response from the store. The response is streamed as it is added to the store.
    async fn stream_data(
        &mut self,
        id: &str,
    ) -> StoreResult<(ReceiverStream<Arc<Bytes>>, JoinHandle<()>)> {
        // the responses in kv bucket are stored in the format rs.{id}.{vertex_name}.{timestamp}
        // so we should watch for all keys that start with rs.{id}
        let watch_key = format!("rs.{}.{id}.*.*", self.pod_replica_id);

        let mut watcher = self
            .kv_store
            .watch_from_revision(&watch_key, 1)
            .await
            .map_err(|e| {
                StoreError::StoreRead(format!("Failed to watch request id in kv store: {e:?}"))
            })?;

        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let response_init_key = format!("rs.{}.{id}.start.processing", self.pod_replica_id);
        let response_done_key = format!("rs.{}.{id}.done.processing", self.pod_replica_id);

        let handle = tokio::spawn(async move {
            while let Some(watch_event) = watcher.next().await {
                let entry = match watch_event {
                    Ok(event) => event,
                    Err(e) => {
                        error!(?e, "Received error from JetStream KV watcher");
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
