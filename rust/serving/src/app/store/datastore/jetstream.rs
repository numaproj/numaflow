//! Stores the response from the processing in a JetStream key-value store. Each response is stored
//! as a new key-value pair in the store, so that no responses are lost. We cannot use the same key
//! and overwrite the value, since the history of the kv store is not per key, but for the entire
//! store. Store is capable of storing multiple results for the same request so it is stored in a
//! concatenated format (length-prefixed format).
//!
//! JetStream Response Entry Format is as follows:
//!  - Response Key - rs.{pod_hash}.{id}.{vertex_name}.{timestamp}
//!  - Response Value - Actual response payload as Bytes

use crate::app::store::datastore::{DataStore, Error as StoreError, Result as StoreResult};
use crate::config::RequestType;
use async_nats::jetstream::Context;
use async_nats::jetstream::kv::{Store, Watch};
use bytes::{Buf, BufMut, Bytes};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const RESPONSE_KEY_PREFIX: &str = "rs";
const FINAL_RESULT_KEY_SUFFIX: &str = "final.result.processed";
const START_PROCESSING_MARKER: &str = "start.processing";
const DONE_PROCESSING_MARKER: &str = "done.processing";

/// Merges a list of Bytes using u64 length prefixing.
fn merge_bytes_list(list: &[Bytes]) -> Bytes {
    let mut accumulator = Vec::new();
    for b in list {
        let len = b.len() as u64;
        accumulator.put_u64_le(len);
        accumulator.put_slice(b);
    }
    Bytes::from(accumulator)
}

/// Extracts a list of Bytes merged with u64 length prefixing.
fn extract_bytes_list(merged: &Bytes) -> Result<Vec<Bytes>, String> {
    let mut extracted = Vec::new();
    let mut cursor = Cursor::new(merged);

    while cursor.has_remaining() {
        // get size/length first
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
        // get the data for the given len
        let len = cursor.get_u64_le();
        if cursor.remaining() < len as usize {
            return Err(format!(
                "Truncated data: declared payload length {} exceeds remaining bytes {}",
                len,
                cursor.remaining()
            ));
        }
        // extract the data
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
    responses_map: Arc<Mutex<HashMap<String, ResponseMode>>>,
    cln_token: CancellationToken,
}

impl JetStreamDataStore {
    pub(crate) async fn new(
        js_context: Context,
        datum_store_name: &'static str,
        pod_hash: &'static str,
        cln_token: CancellationToken,
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
            cln_token.clone(),
        )
        .await;
        Ok(Self {
            kv_store,
            responses_map,
            cln_token,
        })
    }

    /// Spawns the central task that watches response keys and manages data collection. It creates
    /// a tokio task for accumulating responses for a given request id and writing to the KV store.
    async fn spawn_central_watcher(
        pod_hash: String,
        kv_store: Store,
        responses_map: Arc<Mutex<HashMap<String, ResponseMode>>>,
        cln_token: CancellationToken,
    ) -> JoinHandle<()> {
        // watch for all the responses with prefix rs.{pod_hash}.* (ie, this pod)
        let watch_pattern = format!("{RESPONSE_KEY_PREFIX}.{pod_hash}.*.*.*");
        // monotonic revision which is reset when pod is restarted
        let mut latest_revision = 0;

        let mut watcher = Self::create_watcher(
            &kv_store,
            &watch_pattern,
            latest_revision + 1,
            cln_token.clone(),
        )
        .await
        .expect("Failed to create central result watcher");

        tokio::spawn(async move {
            const CONCURRENT_STORE_WRITER: usize = 500;
            let semaphore = Arc::new(Semaphore::new(CONCURRENT_STORE_WRITER));
            loop {
                tokio::select! {
                    // Stop watching if the cancellation token is triggered
                    _ = cln_token.cancelled() => {
                        info!("Cancellation token triggered. Stopping central watcher.");
                        break;
                    }
                    // Process the next entry from the watcher
                    maybe_entry = watcher.next() => {
                        match maybe_entry {
                            Some(Ok(entry)) => {
                                debug!(key = ?entry.key, operation = ?entry.operation, revision = entry.revision, "Response central watcher received entry");
                                latest_revision = entry.revision;
                                if entry.operation != async_nats::jetstream::kv::Operation::Put {
                                    continue;
                                }

                                if let Err(e) = Self::process_entry(&entry, &responses_map, &pod_hash, &kv_store, &semaphore).await {
                                    error!(error = ?e, "Failed to process entry");
                                }
                            }
                            Some(Err(e)) => {
                                error!(error = ?e, "Error from watcher stream. Re-establishing...");
                                watcher = match Self::create_watcher(&kv_store, &watch_pattern, latest_revision + 1, cln_token.clone()).await {
                                    Ok(w) => w,
                                    Err(re_e) => {
                                        error!(error = ?re_e, "Failed to re-establish watcher. Exiting...");
                                        break;
                                    }
                                };
                            }
                            None => {
                                error!("Watcher stream ended unexpectedly. Re-establishing...");
                                watcher = match Self::create_watcher(&kv_store, &watch_pattern, latest_revision + 1, cln_token.clone()).await {
                                    Ok(w) => w,
                                    Err(re_e) => {
                                        error!(error = ?re_e, "Failed to re-establish watcher. Exiting...");
                                        break;
                                    }
                                };
                            }
                        }
                    }
                }
            }
            // this is in exit code, no explicit error handling required
            _ = semaphore
                .acquire_many_owned(CONCURRENT_STORE_WRITER as u32)
                .await
                .expect("should have acquired all tasks");
        })
    }

    /// Creates a new kv watcher for the store with the given watch pattern and revision.
    async fn create_watcher(
        kv_store: &Store,
        watch_pattern: &str,
        start_revision: u64,
        cln_token: CancellationToken,
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
            if cln_token.is_cancelled() {
                info!("Cancellation token triggered, exiting response watcher creation retry loop");
                return Err(StoreError::Connection(
                    "Cancellation token triggered".to_string(),
                ));
            }
        }
    }

    /// process every entry from the response store central watcher.
    async fn process_entry(
        entry: &async_nats::jetstream::kv::Entry,
        responses_map: &Arc<Mutex<HashMap<String, ResponseMode>>>,
        pod_hash: &str,
        kv_store: &Store,
        semaphore: &Arc<Semaphore>,
    ) -> Result<(), String> {
        let parts: Vec<&str> = entry.key.splitn(5, '.').collect();
        let key_prefix = parts[0];
        let request_id = parts[2];
        let key_suffix = format!("{}.{}", parts[3], parts[4]);

        // key format is rs.{pod_hash}.{request_id}.{key_suffix}, anything else is not valid
        if parts.len() != 5 || key_prefix != RESPONSE_KEY_PREFIX || parts[1] != pod_hash {
            return Err(format!("Unexpected key format: {}", entry.key));
        }

        // If it's start of the request, we need to add it to the map for tracking the responses
        // for the given id. The value of start processing marker will be the type of the request
        // based on that we can add the appropriate entry in the map.
        if key_suffix == START_PROCESSING_MARKER {
            let request_type = RequestType::try_from(entry.value.clone())
                .expect("Failed to convert entry value to RequestType");

            let mut response_map = responses_map.lock().await;
            let entry = response_map.entry(request_id.to_string());
            // If the entry is vacant, we can insert the appropriate entry
            // else (if map contains the entry) we need to return an error
            if let Entry::Vacant(vacant_entry) = entry {
                match request_type {
                    // For SSE, create a channel and insert it
                    RequestType::Sse => {
                        let (tx, rx) = mpsc::channel(10);
                        vacant_entry.insert(ResponseMode::Stream {
                            tx,
                            rx: Some(ReceiverStream::new(rx)),
                        });
                    }
                    // For Sync and Async, insert an empty list
                    RequestType::Sync | RequestType::Async => {
                        vacant_entry.insert(ResponseMode::Unary(Vec::new()));
                    }
                }
            } else {
                error!(
                    ?request_id,
                    "Start processing marker already exists for this request ID"
                );
                return Err(format!(
                    "Start processing marker already exists for request ID: {request_id}"
                ));
            }
        } else if key_suffix == DONE_PROCESSING_MARKER {
            // when we get the done processing marker, it means we won't get any more responses for
            // this request id. So we need to merge the responses and put them in the response store, so
            // that it can be retrieved when needed. We only need to store the merged response for
            // sync and async because for sse it will be streaming. It's safe to store for sync request
            // because during failure of sync request it will be converted as an async request.
            let responses = {
                let mut response_map = responses_map.lock().await;
                response_map.remove(request_id)
            };

            if let Some(ResponseMode::Unary(responses)) = responses {
                let permit = Arc::clone(semaphore)
                    .acquire_owned()
                    .await
                    .expect("Semaphore is poisoned");
                // Spawn a task to write the merged response to the KV store
                let kv_store = kv_store.clone();
                let request_id = request_id.to_string();
                tokio::spawn(async move {
                    let _permit = permit;
                    // we use rs.{pod_hash}.{request_id}.final.result.processed as the key for the final result
                    // and the value will be the merged response.
                    let merged_response = merge_bytes_list(&responses);
                    let final_result_key =
                        format!("{RESPONSE_KEY_PREFIX}.{request_id}.{FINAL_RESULT_KEY_SUFFIX}");

                    if let Err(e) = kv_store.put(final_result_key, merged_response).await {
                        error!(error = ?e, "Failed to put final result");
                    }
                });
            }
        } else {
            // we can append the response for the list for sync and async, for sse we can write the
            // response to the response channel
            let mut response_map = responses_map.lock().await;
            if let Some(response_mode) = response_map.get_mut(request_id) {
                match response_mode {
                    ResponseMode::Stream { tx, .. } => {
                        tx.send(Arc::new(entry.value.clone()))
                            .await
                            .map_err(|e| format!("Failed to send SSE response: {e:?}"))?;
                    }
                    ResponseMode::Unary(responses) => {
                        responses.push(entry.value.clone());
                    }
                }
            }
        }
        Ok(())
    }

    /// Retrieves the final result from the KV store for a given request ID using the final key, it
    /// keeps retrying until the final result is available.
    async fn get_data_from_response_store(&self, id: &str) -> StoreResult<Vec<Vec<u8>>> {
        let final_result_key = format!("{RESPONSE_KEY_PREFIX}.{id}.{FINAL_RESULT_KEY_SUFFIX}");

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
                    warn!(%id, "No final result key found in KV store, retrying...");
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

    /// Retrieves the historic response for a given request ID and pod hash. It uses a watcher to
    /// stream the responses from the KV store. The responses are collected until the done processing
    /// marker is received.
    /// This is used during a failover case (pod processing the request is different the one that
    /// originated the request).
    pub(crate) async fn get_historic_response(
        store: Store,
        id: &str,
        pod_hash: &str,
        cln_token: CancellationToken,
    ) -> StoreResult<Vec<Vec<u8>>> {
        let mut latest_revision = 0;
        let watch_pattern = format!("{RESPONSE_KEY_PREFIX}.{pod_hash}.{id}.*.*");
        let mut watcher = Self::create_watcher(
            &store,
            &watch_pattern,
            latest_revision + 1,
            cln_token.clone(),
        )
        .await?;
        let mut responses = vec![];
        loop {
            match watcher.next().await {
                None => {
                    error!(request_id = ?id, "Historical watcher stream ended unexpectedly. Recreating watcher...");
                    watcher = Self::create_watcher(
                        &store,
                        &watch_pattern,
                        latest_revision + 1,
                        cln_token.clone(),
                    )
                    .await?;
                }
                Some(Ok(entry)) => {
                    latest_revision = entry.revision;
                    if entry.key.contains(START_PROCESSING_MARKER) {
                        continue;
                    }

                    if entry.key.contains(DONE_PROCESSING_MARKER) {
                        break;
                    }
                    responses.push(entry.value.to_vec());
                }
                Some(Err(e)) => {
                    error!(request_id = ?id, "Error while receiving response from watcher: {e:?}, Recreating watcher...");
                    watcher = Self::create_watcher(
                        &store,
                        &watch_pattern,
                        latest_revision + 1,
                        cln_token.clone(),
                    )
                    .await?;
                }
            }
        }
        Ok(responses)
    }

    /// Streams the historic responses for a given request ID and pod hash. It uses a watcher to
    /// stream the responses from the KV store. The responses are sent to the provided channel
    /// until the done processing marker is received.
    /// This is like [get_historic_response] except that this is for SSE.
    pub(crate) async fn stream_historic_responses(
        store: Store,
        id: &str,
        pod_hash: &str,
        tx: Sender<Arc<Bytes>>,
        cln_token: CancellationToken,
    ) {
        let mut latest_revision = 0;
        let watch_pattern = format!("{RESPONSE_KEY_PREFIX}.{pod_hash}.{id}.*.*");
        let mut watcher = Self::create_watcher(
            &store,
            &watch_pattern,
            latest_revision + 1,
            cln_token.clone(),
        )
        .await
        .expect("Failed to create watcher");
        loop {
            match watcher.next().await {
                None => {
                    error!(request_id = ?id, "Historical watcher stream ended unexpectedly. Recreating watcher...");
                    watcher = Self::create_watcher(
                        &store,
                        &watch_pattern,
                        latest_revision + 1,
                        cln_token.clone(),
                    )
                    .await
                    .expect("Failed to create watcher");
                }
                Some(Ok(entry)) => {
                    latest_revision = entry.revision;
                    if entry.key.contains(START_PROCESSING_MARKER) {
                        continue;
                    }

                    if entry.key.contains(DONE_PROCESSING_MARKER) {
                        break;
                    }
                    tx.send(Arc::new(entry.value))
                        .await
                        .expect("Failed to send response");
                }
                Some(Err(e)) => {
                    error!(request_id = ?id, "Error while receiving response from watcher: {e:?}, Recreating watcher...");
                    watcher = Self::create_watcher(
                        &store,
                        &watch_pattern,
                        latest_revision + 1,
                        cln_token.clone(),
                    )
                    .await
                    .expect("Failed to create watcher");
                }
            }
        }
    }
}

/// Represents the response mode for the request. For sse it will be streaming but for sync and async
/// it will be unary.
enum ResponseMode {
    Stream {
        tx: Sender<Arc<Bytes>>,
        rx: Option<ReceiverStream<Arc<Bytes>>>,
    },
    Unary(Vec<Bytes>),
}

impl DataStore for JetStreamDataStore {
    /// Retrieve a data from the store. If the datum is not found, `None` is returned.
    async fn retrieve_data(
        &mut self,
        id: &str,
        pod_hash: Option<String>,
    ) -> StoreResult<Vec<Vec<u8>>> {
        // if the pod_hash is same as the current pod hash then we can rely on the central watcher for
        // responses else we will have to create a new watcher for the old pod hash to get all the
        // responses.
        if let Some(pod_hash_value) = pod_hash {
            Self::get_historic_response(
                self.kv_store.clone(),
                id,
                &pod_hash_value,
                self.cln_token.clone(),
            )
            .await
        } else {
            self.get_data_from_response_store(id).await
        }
    }

    /// Stream the response from the store. The response is streamed as it is added to the store.
    async fn stream_data(
        &mut self,
        id: &str,
        failed_pod_hash: Option<String>,
    ) -> StoreResult<ReceiverStream<Arc<Bytes>>> {
        // if the pod_hash is same as the current pod hash then we can rely on the central watcher for
        // responses else we will have to create a new watcher for the old pod hash.
        if let Some(failed_pod_hash) = failed_pod_hash {
            let (tx, rx) = mpsc::channel(10);
            tokio::spawn({
                let kv_store = self.kv_store.clone();
                let request_id = id.to_string();
                let cln_token = self.cln_token.clone();
                async move {
                    Self::stream_historic_responses(
                        kv_store,
                        &request_id,
                        &failed_pod_hash,
                        tx,
                        cln_token,
                    )
                    .await;
                }
            });
            Ok(ReceiverStream::new(rx))
        } else {
            // we need to wait for the start processing marker entry to be processed by the central watcher
            loop {
                {
                    let mut response_map = self.responses_map.lock().await;
                    if let Some(ResponseMode::Stream { rx, .. }) = response_map.get_mut(id)
                        && let Some(receiver_stream) = rx.take()
                    {
                        return Ok(receiver_stream);
                    }
                }
                sleep(Duration::from_millis(5)).await;
            }
        }
    }

    async fn ready(&mut self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::store::datastore::DataStore;
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;
    use bytes::Bytes;
    use chrono::Utc;
    use tokio_stream::StreamExt;

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

        let mut store = JetStreamDataStore::new(
            context.clone(),
            datum_store_name,
            pod_hash,
            CancellationToken::new(),
        )
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

        let result = store.retrieve_data(id, None).await.unwrap();
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

        let mut store = JetStreamDataStore::new(
            context.clone(),
            datum_store_name,
            pod_hash,
            CancellationToken::new(),
        )
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

        let mut stream = store.stream_data(id, None).await.unwrap();

        // Verify that the response is received
        let received_response = stream.next().await.unwrap();
        assert_eq!(received_response, Arc::new(payload));

        // delete store
        context.delete_key_value(datum_store_name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_retrieve_data_with_different_pod_hash() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let datum_store_name = "test_retrieve_data_different_pod_hash_store";
        let current_pod_hash = "abcd";
        let other_pod_hash = "efgh";

        let _ = context.delete_key_value(datum_store_name).await;

        context
            .create_key_value(Config {
                bucket: datum_store_name.to_string(),
                history: 5,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut store = JetStreamDataStore::new(
            context.clone(),
            datum_store_name,
            current_pod_hash,
            CancellationToken::new(),
        )
        .await
        .unwrap();

        let id = "test-unary-id";
        let start_key = format!("rs.{other_pod_hash}.{id}.start.processing");
        let start_payload: Bytes = RequestType::Sync.try_into().unwrap();
        store
            .kv_store
            .put(start_key, start_payload.clone())
            .await
            .unwrap();

        let key = format!("rs.{other_pod_hash}.{id}.sink.1234");
        store
            .kv_store
            .put(key, Bytes::from_static(b"test_payload"))
            .await
            .unwrap();

        let done_key = format!("rs.{other_pod_hash}.{id}.done.processing");
        store.kv_store.put(done_key, Bytes::new()).await.unwrap();

        let result = store
            .retrieve_data(id, Some(other_pod_hash.to_string()))
            .await
            .unwrap();
        assert!(result.len() > 0);
        assert_eq!(result[0], b"test_payload");

        // delete store
        context.delete_key_value(datum_store_name).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_stream_data_with_different_pod_hash() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let datum_store_name = "test_stream_data_different_pod_hash_store";
        let current_pod_hash = "hjck";
        let other_pod_hash = "kclm";

        let _ = context.delete_key_value(datum_store_name).await;

        context
            .create_key_value(Config {
                bucket: datum_store_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut store = JetStreamDataStore::new(
            context.clone(),
            datum_store_name,
            current_pod_hash,
            CancellationToken::new(),
        )
        .await
        .unwrap();

        let id = "test-stream-id";
        let start_key = format!("rs.{other_pod_hash}.{id}.start.processing");
        let start_payload: Bytes = RequestType::Sse.try_into().unwrap();
        store
            .kv_store
            .put(start_key, start_payload.clone())
            .await
            .unwrap();
        let key = format!("rs.{other_pod_hash}.{id}.sink.1234");
        store
            .kv_store
            .put(key, Bytes::from_static(b"test_payload"))
            .await
            .unwrap();

        let done_key = format!("rs.{other_pod_hash}.{id}.done.processing");
        store.kv_store.put(done_key, Bytes::new()).await.unwrap();

        let mut stream = store
            .stream_data(id, Some(other_pod_hash.to_string()))
            .await
            .unwrap();

        let received_response = stream.next().await.unwrap();
        assert_eq!(
            received_response,
            Arc::new(Bytes::from_static(b"test_payload"))
        );

        // delete store
        context.delete_key_value(datum_store_name).await.unwrap();
    }
}
