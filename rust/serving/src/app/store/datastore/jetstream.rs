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
    pod_hash: String,
    responses_map: Arc<Mutex<HashMap<String, ResponseMode>>>,
}

impl JetStreamDataStore {
    pub(crate) async fn new(
        js_context: Context,
        datum_store_name: &str,
        pod_hash: &str,
    ) -> StoreResult<Self> {
        let kv_store = js_context
            .get_key_value(datum_store_name)
            .await
            .map_err(|e| StoreError::Connection(format!("Failed to get datum kv store: {e:?}")))?;

        let responses_map = Arc::new(Mutex::new(HashMap::new()));
        // start the central watcher
        Self::spawn_central_watcher(
            pod_hash.to_string(),
            kv_store.clone(),
            Arc::clone(&responses_map),
        )
        .await;
        Ok(Self {
            kv_store,
            pod_hash: pod_hash.to_string(),
            responses_map,
        })
    }

    /// Spawns the central task that watches response keys and manages data collection.
    async fn spawn_central_watcher(
        pod_hash: String,
        kv_store: Store,
        responses_map: Arc<Mutex<HashMap<String, ResponseMode>>>,
    ) -> JoinHandle<()> {
        let watch_pattern = format!("{}.{}.*.*.*", RESPONSE_KEY_PREFIX, pod_hash);
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

        tokio::spawn(async move {
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
                            Self::process_entry(&entry, &responses_map, &pod_hash, &kv_store).await
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
        pod_hash: &str,
        kv_store: &Store,
    ) -> Result<(), String> {
        let parts: Vec<&str> = entry.key.splitn(5, '.').collect();
        if parts.len() < 5 || parts[0] != RESPONSE_KEY_PREFIX || parts[1] != pod_hash {
            return Err(format!("Unexpected key format: {}", entry.key));
        }

        let request_id = parts[2];
        let key_suffix = format!("{}.{}", parts[3], parts[4]);

        if key_suffix == START_PROCESSING_MARKER {
            let request_type = RequestType::try_from(entry.value.clone())
                .map_err(|e| format!("Failed to convert request type: {:?}", e))?;

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
                RequestType::Sse => {
                    let (tx, rx) = mpsc::channel(10);
                    response_map.insert(
                        request_id.to_string(),
                        ResponseMode::Sse {
                            tx,
                            rx: Some(ReceiverStream::new(rx)),
                        },
                    );
                }
                RequestType::Sync => {
                    response_map.insert(request_id.to_string(), ResponseMode::Sync(Vec::new()));
                }
                RequestType::Async => {
                    response_map.insert(request_id.to_string(), ResponseMode::Async(Vec::new()));
                }
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
                        RESPONSE_KEY_PREFIX, pod_hash, FINAL_RESULT_KEY_SUFFIX
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
                    ResponseMode::Sse { tx, .. } => {
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
            RESPONSE_KEY_PREFIX, self.pod_hash, FINAL_RESULT_KEY_SUFFIX
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

enum ResponseMode {
    Sse {
        tx: mpsc::Sender<Arc<Bytes>>,
        rx: Option<ReceiverStream<Arc<Bytes>>>,
    },
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
        loop {
            let mut response_map = self.responses_map.lock().await;
            if let Some(ResponseMode::Sse { rx, .. }) = response_map.get_mut(id) {
                if let Some(receiver_stream) = rx.take() {
                    return Ok(receiver_stream);
                }
            }

            sleep(Duration::from_millis(5)).await;
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
    use bytes::Bytes;
    use chrono::Utc;
    use tokio_stream::StreamExt;

    use super::*;
    use crate::app::store::datastore::DataStore;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_retrieve_datum() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let datum_store_name = "test_retrieve_datum_store";
        let pod_hash = "0";

        let _ = context.delete_key_value(datum_store_name).await;

        context
            .create_key_value(Config {
                bucket: datum_store_name.to_string(),
                history: 5,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut store = JetStreamDataStore::new(context.clone(), datum_store_name, pod_hash)
            .await
            .unwrap();

        let id = "test-id";
        let start_key = format!("rs.{pod_hash}.{id}.start.processing");
        let start_payload: Bytes = RequestType::Sync.try_into().unwrap();
        store
            .kv_store
            .put(start_key, start_payload.clone())
            .await
            .unwrap();

        let key = format!("rs.{pod_hash}.{id}.sink.{}", Utc::now().timestamp());
        store
            .kv_store
            .put(key, Bytes::from_static(b"test_payload"))
            .await
            .unwrap();

        let done_key = format!("rs.{pod_hash}.{id}.done.processing");
        store.kv_store.put(done_key, Bytes::new()).await.unwrap();

        let result = store.retrieve_data(id).await.unwrap();
        assert!(result.len() > 0);
        assert_eq!(result[0], b"test_payload");

        // delete store
        context.delete_key_value(datum_store_name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_stream_response() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let datum_store_name = "test_stream_response_store";
        let pod_hash = "0";
        let _ = context.delete_key_value(datum_store_name).await;

        context
            .create_key_value(Config {
                bucket: datum_store_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut store = JetStreamDataStore::new(context.clone(), datum_store_name, pod_hash)
            .await
            .unwrap();

        let id = "test_stream_id";
        let start_key = format!("rs.{pod_hash}.{id}.start.processing");
        let start_payload: Bytes = RequestType::Sse.try_into().unwrap();
        store
            .kv_store
            .put(start_key, start_payload.clone())
            .await
            .unwrap();

        // Simulate a response being added to the store
        let key = format!("rs.{pod_hash}.{id}.0.1234");
        let payload = Bytes::from_static(b"test_payload");
        store.kv_store.put(key, payload.clone()).await.unwrap();

        let mut stream = store.stream_data(id).await.unwrap();

        // Verify that the response is received
        let received_response = stream.next().await.unwrap();
        assert_eq!(received_response, Arc::new(payload));

        // delete store
        context.delete_key_value(datum_store_name).await.unwrap();
    }
}
