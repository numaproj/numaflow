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
use async_nats::jetstream::kv::Store;
use async_nats::jetstream::Context;
use bytes::{Buf, BufMut, Bytes};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
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
    _watcher_handle: Arc<JoinHandle<()>>,
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

        // start the central watcher
        let central_watcher_handle = Self::spawn_central_watcher(
            pod_replica_id.clone(),
            kv_store.clone(),
        );
        info!(
            "Central watcher spawned for pod replica ID: {}",
            pod_replica_id
        );
        Ok(Self {
            kv_store,
            pod_replica_id,
            _watcher_handle: Arc::new(central_watcher_handle),
        })
    }

    /// Spawns the central task that watches response keys and manages data collection.
    fn spawn_central_watcher(
        pod_replica_id: String,
        kv_store: Store,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut response_map = HashMap::new();
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
                        error!(error = %e, "Failed to establish initial watch for {}. Retrying...", watch_pattern);
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
                            if response_map.contains_key(request_id) {
                                warn!(
                                    ?request_id,
                                    "Start processing marker already exists for this request ID"
                                );
                            } else {
                                response_map.insert(parts[2].to_string(), Vec::new());
                            }
                            continue;
                        }

                        if key_suffix == DONE_PROCESSING_MARKER {
                            let final_result_key = format!(
                                "{}.{}.{}",
                                RESPONSE_KEY_PREFIX, pod_replica_id, FINAL_RESULT_KEY_SUFFIX
                            );
                            if let Some(responses) = response_map.remove(request_id) {
                                let merged_response = merge_bytes_list(&responses);
                                kv_store
                                    .put(final_result_key.clone(), merged_response)
                                    .await
                                    .unwrap();
                            } else {
                                error!(?request_id, "Not able to write final response");
                            }
                            continue;
                        }

                        if let Some(responses) = response_map.get_mut(request_id) {
                            responses.push(entry.value.clone());
                        } else {
                            error!(?request_id, "Received response for unregistered request ID");
                        }
                    }

                    Some(Err(e)) => {
                        error!(error = %e, "Error from central watcher stream. Re-establishing watch...");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        watcher = loop {
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
                                    error!(error = %re_e, "Failed to re-establish watch. Trying again after delay.");
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                }
                            }
                        };
                    }

                    None => {
                        error!("Central watcher stream ended unexpectedly. Attempting to restart.");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        watcher = loop {
                            match kv_store
                                .watch_from_revision(&watch_pattern, latest_revision + 1)
                                .await
                            {
                                Ok(w) => {
                                    info!("Central result store  watcher restarted after stream ended.");
                                    break w;
                                }
                                Err(re_e) => {
                                    error!(error = %re_e, "Failed to restart watch. Trying again after delay.");
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                }
                            }
                        };
                    }
                }
            }
        })
    }
}

impl DataStore for JetStreamDataStore {
    /// Retrieve a data from the store. If the datum is not found, `None` is returned.
    async fn retrieve_data(&mut self, id: &str) -> StoreResult<Vec<Vec<u8>>> {
        // get the final result from the kv store
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
                    warn!(%id, "No final result key found in kv store, retrying");
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
                        error!(?e, "Received error from Jetstream KV watcher");
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
