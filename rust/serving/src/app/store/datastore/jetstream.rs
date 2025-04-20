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
use async_nats::jetstream::kv::{Store, Watch};
use async_nats::jetstream::Context;
use bytes::{Buf, BufMut, Bytes};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
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
                }
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

                        if let Err(e) =
                            Self::process_entry(&entry, &responses_map, &pod_replica_id, &kv_store)
                                .await
                        {
                            error!(error = ?e, "Failed to process entry");
                        }
                    }
                    Some(Err(e)) => {
                        error!(error = ?e, "Error from watcher stream. Re-establishing...");
                        watcher = match Self::create_watcher(
                            &kv_store,
                            &watch_pattern,
                            latest_revision + 1,
                        )
                        .await
                        {
                            Ok(w) => w,
                            Err(re_e) => {
                                error!(error = ?re_e, "Failed to re-establish watcher. Exiting...");
                                break;
                            }
                        };
                    }
                    None => {
                        error!("Watcher stream ended unexpectedly. Re-establishing...");
                        watcher = match Self::create_watcher(
                            &kv_store,
                            &watch_pattern,
                            latest_revision + 1,
                        )
                        .await
                        {
                            Ok(w) => w,
                            Err(re_e) => {
                                error!(error = ?re_e, "Failed to re-establish watcher. Exiting...");
                                break;
                            }
                        };
                    }
                }
            }
        })
    }

    async fn create_watcher(
        kv_store: &Store,
        watch_pattern: &str,
        start_revision: u64,
    ) -> StoreResult<Watch> {
        loop {
            match kv_store
                .watch_from_revision(watch_pattern, start_revision)
                .await
            {
                Ok(watcher) => {
                    info!(
                        watch_pattern,
                        start_revision, "Watcher created successfully"
                    );
                    return Ok(watcher);
                }
                Err(e) => {
                    error!(error = ?e, "Failed to create watcher. Retrying...");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn process_entry(
        entry: &async_nats::jetstream::kv::Entry,
        responses_map: &Arc<Mutex<HashMap<String, ResponseMode>>>,
        pod_replica_id: &str,
        kv_store: &Store,
    ) -> Result<(), String> {
        let parts: Vec<&str> = entry.key.splitn(5, '.').collect();
        if parts.len() < 5 || parts[0] != RESPONSE_KEY_PREFIX || parts[1] != pod_replica_id {
            return Err(format!("Unexpected key format: {}", entry.key));
        }

        let request_id = parts[2];
        let key_suffix = format!("{}.{}", parts[3], parts[4]);

        if key_suffix == START_PROCESSING_MARKER {
            let request_type = RequestType::try_from(entry.value.clone())
                .map_err(|e| format!("Failed to convert request type: {:?}", e))?;
            
            if request_type == RequestType::Sse {
                return Ok(())
            }
            
            let mut response_map = responses_map.lock().await;
            if response_map.contains_key(request_id) {
                error!(
                    ?request_id,
                    "Start processing marker already exists for this request ID"
                );
                return Err(format!(
                    "Start processing marker already exists for request ID: {}",
                    request_id
                ));
            }
            match request_type {
                RequestType::Sync => {
                    response_map.insert(request_id.to_string(), ResponseMode::Sync(Vec::new()));
                }
                RequestType::Async => {
                    response_map.insert(request_id.to_string(), ResponseMode::Async(Vec::new()));
                }
                _ => {}
            }
        } else if key_suffix == DONE_PROCESSING_MARKER {
            let mut response_map = responses_map.lock().await;
            if let Some(response_mode) = response_map.remove(request_id) {
                if let ResponseMode::Sync(responses) | ResponseMode::Async(responses) =
                    response_mode
                {
                    let merged_response = merge_bytes_list(&responses);
                    let final_result_key = format!(
                        "{}.{}.{}",
                        RESPONSE_KEY_PREFIX, pod_replica_id, FINAL_RESULT_KEY_SUFFIX
                    );
                    kv_store
                        .put(final_result_key, merged_response)
                        .await
                        .map_err(|e| format!("Failed to put final result: {:?}", e))?;
                }
            }
        } else {
            let mut response_map = responses_map.lock().await;
            if let Some(response_mode) = response_map.get_mut(request_id) {
                match response_mode {
                    ResponseMode::Sse(tx) => {
                        tx.send(Arc::new(entry.value.clone()))
                            .await
                            .map_err(|e| format!("Failed to send SSE response: {:?}", e))?;
                    }
                    ResponseMode::Sync(responses) | ResponseMode::Async(responses) => {
                        responses.push(entry.value.clone());
                    }
                }
            }
        }
        Ok(())
    }

    async fn get_data_from_kv_store(&self, id: &str) -> StoreResult<Vec<Vec<u8>>> {
        let final_result_key = format!(
            "{}.{}.{}",
            RESPONSE_KEY_PREFIX, self.pod_replica_id, FINAL_RESULT_KEY_SUFFIX
        );

        loop {
            match self.kv_store.get(&final_result_key).await {
                Ok(Some(result)) => {
                    return extract_bytes_list(&result)
                        .map(|extracted| extracted.iter().map(|b| b.to_vec()).collect())
                        .map_err(|e| {
                            StoreError::StoreRead(format!("Failed to extract bytes list: {e:?}"))
                        });
                }
                Ok(None) => {
                    warn!(?id, "No final result key found in KV store, retrying...");
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(e) => {
                    return Err(StoreError::StoreRead(format!(
                        "Failed to get final result key from KV store: {e:?}"
                    )));
                }
            }
        }
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
        // Fallback to retrieving data from the KV store
        self.get_data_from_kv_store(id).await
    }

    /// Stream the response from the store. The response is streamed as it is added to the store.
    async fn stream_data(&mut self, id: &str) -> StoreResult<ReceiverStream<Arc<Bytes>>> {
        let (tx, rx) = mpsc::channel(10);

        // Update the in-memory map with the new tx
        let mut response_map = self.responses_map.lock().await;
        response_map.insert(id.to_string(), ResponseMode::Sse(tx));

        Ok(ReceiverStream::new(rx))
    }

    async fn ready(&mut self) -> bool {
        true
    }
}
