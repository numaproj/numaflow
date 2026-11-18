//! Tracker is added because when we do data forwarding in [MonoVertex](crate::monovertex::forwarder) or
//! in [Pipeline](crate::pipeline::forwarder), immaterial whether we are in source, UDF, or Sink, we
//! have to track whether the message has completely moved to the next vertex (N+1)th before we can
//! mark that message as done in the Nth vertex. When the [crate::message::AckHandle] is dropped,
//! [crate::pipeline::isb::reader] call [Tracker::delete] to mark that message as done.
//!
//! Items tracked by the tracker and uses [Offset] as the key.
//!   - The oldest Watermark is tracked
//!   - Callbacks for Serving is triggered in the tracker.

use chrono::{DateTime, Utc};
use serving::callback::CallbackHandler;
use serving::{DEFAULT_ID_HEADER, DEFAULT_POD_HASH_KEY};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::Result;
use crate::error::Error;
use crate::message::{Message, Offset};

/// TrackerEntry represents the state of a tracked message.
#[derive(Debug)]
struct TrackerEntry {
    /// Callback info for serving
    serving_callback_info: Option<ServingCallbackInfo>,
    /// Watermark for the message
    watermark: Option<DateTime<Utc>>,
    /// EventTime for the message
    event_time: DateTime<Utc>,
}

/// Identifies one ISB buffer a vertex reads from: the vertex on the other end of the ingress edge
/// and the partition index. Buffers are edge-owned, so a vertex with several ingress edges reads
/// several buffers that share partition indexes, and whose stream sequences all start at 1. An
/// offset is only unique within one of these.
type EdgePartition = (&'static str, u16);

/// TrackerState holds the mutable state of the tracker.
struct TrackerState {
    /// entries organized by (from vertex, partition), each with its own BTreeMap of offsets
    entries: HashMap<EdgePartition, BTreeMap<Offset, TrackerEntry>>,
}

/// Head idle WMB offset for each (from vertex, partition). `None` means that partition of that
/// edge is not idling.
pub(crate) type IdleOffsetMap = HashMap<EdgePartition, Option<i64>>;

impl Drop for TrackerState {
    fn drop(&mut self) {
        let total_entries: usize = self.entries.values().map(|value| value.len()).sum();
        if total_entries > 0 {
            error!(
                total_entries,
                "TrackerState dropped with {} unacknowledged messages still tracked", total_entries
            );
        }
    }
}

#[derive(Debug)]
struct ServingCallbackInfo {
    id: String,
    pod_hash: String,
    from_vertex: String,
    /// at the moment these are just tags.
    responses: Vec<Option<Vec<String>>>,
}

impl TryFrom<&Message> for ServingCallbackInfo {
    type Error = Error;

    fn try_from(message: &Message) -> std::result::Result<Self, Self::Error> {
        let uuid = message
            .headers
            .get(DEFAULT_ID_HEADER)
            .ok_or_else(|| {
                Error::Source(format!(
                    "{DEFAULT_ID_HEADER} is not found in message headers",
                ))
            })?
            .to_owned();

        let pod_hash = message
            .headers
            .get(DEFAULT_POD_HASH_KEY)
            .ok_or_else(|| {
                Error::Source(format!(
                    "{DEFAULT_POD_HASH_KEY} is not found in message headers",
                ))
            })?
            .to_owned();

        let from_vertex = message
            .metadata
            .as_ref()
            .ok_or_else(|| Error::Source("Metadata field is empty in the message".into()))?
            .previous_vertex
            .clone();

        Ok(ServingCallbackInfo {
            id: uuid,
            pod_hash,
            from_vertex,
            responses: vec![None],
        })
    }
}

/// TrackerHandle provides an interface to interact with the Tracker.
/// It allows inserting, updating, deleting, and discarding tracked messages.
#[derive(Clone)]
pub(crate) struct Tracker {
    state: Arc<RwLock<TrackerState>>,
    /// tracks whether each (from vertex, partition) is currently idle (not reading any data)
    /// if it's set to none, it means it is not idle.
    idle_offset_map: Arc<RwLock<IdleOffsetMap>>,
    serving_callback_handler: Option<CallbackHandler>,
    processed_msg_count: Arc<AtomicUsize>,
    /// The ingress edge (named by its from vertex) this handle tracks offsets for. Empty for an
    /// unscoped handle, e.g. a source or a MonoVertex, which only ever see one sequence of offsets.
    /// See [Tracker::for_edge].
    edge: &'static str,
}

impl Tracker {
    /// Creates a new TrackerHandle instance.
    pub(crate) fn new(
        serving_callback_handler: Option<CallbackHandler>,
        cln_token: CancellationToken,
    ) -> Self {
        let processed_msg_count = Arc::new(AtomicUsize::new(0));

        let state = Arc::new(RwLock::new(TrackerState {
            entries: HashMap::new(),
        }));

        let idle_offset_map = Arc::new(RwLock::new(HashMap::new()));
        let tracker = Self {
            state,
            idle_offset_map,
            serving_callback_handler,
            processed_msg_count: Arc::clone(&processed_msg_count),
            edge: "",
        };

        // spawn a task to log the number of processed messages every second, cln_token is used to
        // stop the task when the tracker is dropped.
        tokio::spawn({
            let processed_msg_count = Arc::clone(&processed_msg_count);
            async move {
                Self::log_processed_msg_count(processed_msg_count, cln_token).await;
            }
        });

        tracker
    }

    /// Returns a handle to the same tracker that keys the offsets it inserts, updates, and deletes
    /// by the ingress edge from `from_vertex`. Every reader of a join, and every component that
    /// touches its messages by offset (e.g. the mapper's serving updates), must use the handle for
    /// its own edge: two edges' buffers can hand out the same `(sequence, partition)` offset at the
    /// same time, and an unscoped key would treat the second one as a redelivery of the first.
    ///
    /// State is shared, so watermark and idle tracking still see every edge.
    pub(crate) fn for_edge(&self, from_vertex: &'static str) -> Self {
        Self {
            edge: from_vertex,
            ..self.clone()
        }
    }

    fn key(&self, offset: &Offset) -> EdgePartition {
        (self.edge, offset.partition_idx())
    }

    /// Logs the number of processed messages every second.
    async fn log_processed_msg_count(
        processed_msg_count: Arc<AtomicUsize>,
        cln_token: CancellationToken,
    ) {
        let mut ticker = tokio::time::interval(tokio::time::Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = cln_token.cancelled() => {
                    break;
                }
                _ = ticker.tick() => {
                    let processed = processed_msg_count.swap(0, Ordering::Relaxed);
                    info!(?processed, "Processed messages per second");
                }
            }
        }
    }

    /// This function is called once the message has been successfully processed. This is where
    /// the bookkeeping for a successful message happens, things like,
    /// - call serving callbacks
    /// - watermark progression
    async fn completed_successfully(&self, entry: TrackerEntry) {
        let TrackerEntry {
            serving_callback_info: callback_info,
            ..
        } = entry;

        self.processed_msg_count.fetch_add(1, Ordering::Relaxed);

        let Some(ref callback_handler) = self.serving_callback_handler else {
            return;
        };
        let Some(callback_info) = callback_info else {
            error!("Callback is enabled, but Tracker doesn't contain callback info");
            return;
        };

        let id = callback_info.id.clone();
        let result = callback_handler
            .callback(
                callback_info.id,
                callback_info.pod_hash,
                callback_info.from_vertex,
                callback_info.responses,
            )
            .await;
        if let Err(e) = result {
            error!(?e, id, "Failed to send callback");
        }
    }

    /// Inserts a new message into the Tracker with the given offset.
    ///
    /// Returns `Err(Error::DuplicateInflight)` if the offset is already being tracked for this
    /// edge and partition. Callers use this to detect intra-pod redeliveries and drop them before
    /// they reach downstream components like UDFs or sinks.
    pub(crate) async fn insert(&self, message: &Message) -> Result<()> {
        let offset = message.offset.clone();
        let mut callback_info = None;
        if self.serving_callback_handler.is_some() {
            callback_info = Some(message.try_into()?);
        }

        let key = self.key(&offset);
        let mut state = self.state.write().await;
        let partition_entries = state.entries.entry(key).or_default();
        if partition_entries.contains_key(&offset) {
            return Err(Error::DuplicateInflight(offset.to_string()));
        }
        partition_entries.insert(
            offset,
            TrackerEntry {
                serving_callback_info: callback_info,
                watermark: message.watermark,
                event_time: message.event_time,
            },
        );
        Ok(())
    }

    /// Informs the tracker that a new message has been generated. The tracker should contain
    /// and entry for this message's offset.
    pub(crate) async fn serving_update(
        &self,
        offset: &Offset,
        response_tags: Vec<Option<Arc<[String]>>>,
    ) -> Result<()> {
        if self.serving_callback_handler.is_none() {
            return Ok(());
        }

        let responses: Vec<Option<Vec<String>>> = response_tags
            .into_iter()
            .map(|tags| tags.map(|tags| tags.iter().map(|tag| tag.to_string()).collect()))
            .collect();

        let key = self.key(offset);
        let mut state = self.state.write().await;
        let Some(partition_entries) = state.entries.get_mut(&key) else {
            return Ok(());
        };
        let Some(entry) = partition_entries.get_mut(offset) else {
            return Ok(());
        };

        if let Some(cb) = entry.serving_callback_info.as_mut() {
            cb.responses = responses;
        }
        Ok(())
    }

    /// resets the count and eof status for an offset in the tracker.
    pub(crate) async fn serving_refresh(&self, offset: Offset) -> Result<()> {
        if self.serving_callback_handler.is_none() {
            return Ok(());
        }

        let key = self.key(&offset);
        let mut state = self.state.write().await;
        let Some(partition_entries) = state.entries.get_mut(&key) else {
            return Ok(());
        };
        let Some(mut entry) = partition_entries.remove(&offset) else {
            return Ok(());
        };

        if let Some(serving_info) = &mut entry.serving_callback_info {
            serving_info.responses = vec![];
        }
        partition_entries.insert(offset, entry);
        Ok(())
    }

    pub(crate) async fn serving_append(
        &self,
        offset: Offset,
        message_tags: Option<Arc<[String]>>,
    ) -> Result<()> {
        if self.serving_callback_handler.is_none() {
            return Ok(());
        }

        let response = message_tags.map(|tags| tags.to_vec());
        let key = self.key(&offset);
        let mut state = self.state.write().await;
        let Some(partition_entries) = state.entries.get_mut(&key) else {
            return Ok(());
        };
        let Some(entry) = partition_entries.get_mut(&offset) else {
            return Ok(());
        };

        if let Some(cb) = entry.serving_callback_info.as_mut() {
            cb.responses.push(response);
        }
        Ok(())
    }

    /// Deletes a message from the Tracker with the given offset.
    pub(crate) async fn delete(&self, offset: &Offset) -> Result<()> {
        let key = self.key(offset);
        let mut state = self.state.write().await;
        let Some(partition_entries) = state.entries.get_mut(&key) else {
            return Ok(());
        };
        let Some(entry) = partition_entries.remove(offset) else {
            return Ok(());
        };

        // if count is 0 and is eof we are sure that we can ack the offset.
        // In map-streaming this won't happen because eof is not tied to the message, rather it is
        // tied to channel-close.
        drop(state); // Release the lock before calling completed_successfully
        self.completed_successfully(entry).await;

        Ok(())
    }

    /// Checks if the Tracker is empty. Used for testing to make sure all messages are acknowledged.
    #[cfg(test)]
    pub(crate) async fn is_empty(&self) -> Result<bool> {
        let state = self.state.read().await;
        Ok(state.entries.values().all(|partition| partition.is_empty()))
    }

    /// Returns the lowest watermark among all the tracked offsets.
    pub(crate) async fn lowest_watermark(&self) -> Result<DateTime<Utc>> {
        let state = self.state.read().await;
        // Get the lowest watermark across all partitions
        let watermark = state
            .entries
            .values()
            .filter_map(|partition_entries| {
                partition_entries
                    .first_key_value()
                    .and_then(|(_, entry)| entry.watermark)
            })
            .min();
        Ok(watermark.unwrap_or(DateTime::from_timestamp_millis(-1).unwrap()))
    }

    /// Returns the lowest event_time among all the tracked offsets.
    pub(crate) async fn lowest_event_time(&self) -> Result<DateTime<Utc>> {
        let state = self.state.read().await;
        // Get the lowest event_time across all partitions
        let event_time = state
            .entries
            .values()
            .flat_map(|partition_entries| partition_entries.values())
            .map(|entry| entry.event_time)
            .min();
        Ok(event_time.unwrap_or(DateTime::from_timestamp_millis(-1).unwrap()))
    }

    /// Sets the idle status of the tracker for a partition of the edge coming from `from_vertex`.
    /// Setting idle offset to None means the partition is not idling. This offset is the Head WMB
    /// offset which is used for "optimistic locking" on the idle status and Head WMB. Bear in mind
    /// that the watermark itself is not stored because the Watermark can monotonically increase for
    /// the same Head MWB offset.
    pub(crate) async fn set_idle_offset(
        &self,
        from_vertex: &'static str,
        partition_idx: u16,
        idle_offset: Option<i64>,
    ) -> Result<()> {
        let mut idle_map = self.idle_offset_map.write().await;
        idle_map.insert((from_vertex, partition_idx), idle_offset);
        Ok(())
    }

    /// Gets the idle wmb status of the tracker. It returns a map of (from vertex, partition index)
    /// to the idle offset. This idle offset is compared against the newly fetch Head WMB as part of
    /// the optimistic locking.
    pub(crate) async fn get_idle_offset(&self) -> Result<IdleOffsetMap> {
        let idle_map = self.idle_offset_map.read().await;
        Ok(idle_map.clone())
    }
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;
    use bytes::Bytes;
    use futures::StreamExt;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::time::{Duration, timeout};

    use super::*;
    use crate::message::StringOffset;
    use crate::message::{IntOffset, MessageID, Offset};
    use crate::metadata::Metadata;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn test_message_to_callback_info_conversion() {
        let mut message = Message {
            typ: Default::default(),
            keys: Arc::from([]),
            tags: None,
            value: Bytes::from_static(b"test"),
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: Default::default(),
            watermark: None,
            id: MessageID {
                vertex_name: "in".into(),
                offset: Bytes::from_static(b"0"),
                index: 1,
            },
            ..Default::default()
        };

        let callback_info: super::Result<ServingCallbackInfo> = TryFrom::try_from(&message);
        assert!(callback_info.is_err());

        let headers = [(DEFAULT_ID_HEADER, "1234"), (DEFAULT_POD_HASH_KEY, "abcd")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        message.headers = Arc::new(headers);

        const FROM_VERTEX_NAME: &str = "source-vertex";
        message.metadata = Some(Arc::new(Metadata {
            previous_vertex: FROM_VERTEX_NAME.into(),
            ..Default::default()
        }));

        let callback_info: ServingCallbackInfo = TryFrom::try_from(&message).unwrap();
        assert_eq!(callback_info.id, "1234");
        assert_eq!(callback_info.from_vertex, FROM_VERTEX_NAME);
        assert_eq!(callback_info.responses, vec![None]);
    }

    #[tokio::test]
    async fn test_insert_update_delete() {
        let handle = Tracker::new(None, CancellationToken::new());
        let message = Message {
            typ: Default::default(),
            keys: Arc::from([]),
            tags: None,
            value: Bytes::from_static(b"test"),
            offset: Offset::String(StringOffset::new("offset1".to_string(), 0)),
            event_time: Default::default(),
            watermark: None,
            id: MessageID {
                vertex_name: "in".into(),
                offset: Bytes::from_static(b"offset1"),
                index: 1,
            },
            ..Default::default()
        };

        // Insert a new message
        handle.insert(&message).await.unwrap();

        // Update the message
        handle
            .serving_update(&message.offset, vec![message.tags.clone()])
            .await
            .unwrap();

        handle.delete(&message.offset).await.unwrap();

        assert!(handle.is_empty().await.unwrap(), "Tracker should be empty");
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_tracker_with_callback_handler() -> Result<()> {
        let store_name = "test_tracker_with_callback_handler";
        let client = async_nats::connect("localhost:4222").await?;
        let js_context = jetstream::new(client);

        let _ = js_context.delete_key_value(store_name).await;
        let callback_bucket = js_context
            .create_key_value(Config {
                bucket: store_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await?;

        let callback_handler = CallbackHandler::new(
            "test",
            Arc::new(numaflow_shared::kv::jetstream::JetstreamKVStore::new(
                callback_bucket.clone(),
                store_name,
            )),
            10,
        );

        let handle = Tracker::new(Some(callback_handler), CancellationToken::new());

        let mut headers = HashMap::new();
        headers.insert(DEFAULT_ID_HEADER.to_string(), "1234".to_string());
        headers.insert(DEFAULT_POD_HASH_KEY.to_string(), "abcd".to_string());

        let offset = Offset::String(StringOffset::new("offset1".to_string(), 0));
        let message = Message {
            typ: Default::default(),
            keys: Arc::from([]),
            tags: None,
            value: Bytes::from_static(b"test"),
            offset: offset.clone(),
            event_time: Default::default(),
            watermark: None,
            id: MessageID {
                vertex_name: "in".into(),
                offset: Bytes::from_static(b"offset1"),
                index: 1,
            },
            headers: Arc::new(headers),
            metadata: Some(Arc::new(Metadata {
                previous_vertex: "source-vertex".into(),
                ..Default::default()
            })),
            is_late: false,
            nack_options: None,
        };

        // Insert a new message
        handle.insert(&message).await?;
        handle.delete(&offset).await?;

        assert!(handle.is_empty().await?, "Tracker should be empty");

        // Verify that the callback was written to the KV store
        let result = timeout(Duration::from_secs(1), async {
            loop {
                let mut keys = callback_bucket.keys().await.unwrap();
                if keys.next().await.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        assert!(result.is_ok(), "callback was not written to the KV store");

        // Clean up the KV store
        js_context.delete_key_value(store_name).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_idle_status_tracking() {
        let handle = Tracker::new(None, CancellationToken::new());
        assert_eq!(handle.get_idle_offset().await.unwrap(), HashMap::new());

        handle.set_idle_offset("in", 0, Some(100)).await.unwrap();
        handle.set_idle_offset("in", 1, Some(200)).await.unwrap();
        handle.set_idle_offset("in", 2, None).await.unwrap();

        let idle_offsets = handle.get_idle_offset().await.unwrap();
        assert_eq!(idle_offsets.get(&("in", 0)), Some(&Some(100)));
        assert_eq!(idle_offsets.get(&("in", 1)), Some(&Some(200)));
        assert_eq!(idle_offsets.get(&("in", 2)), Some(&None));

        handle.set_idle_offset("in", 0, None).await.unwrap();
        let idle_offsets = handle.get_idle_offset().await.unwrap();
        assert_eq!(idle_offsets.get(&("in", 0)), Some(&None));
        assert_eq!(idle_offsets.get(&("in", 1)), Some(&Some(200)));
        assert_eq!(idle_offsets.get(&("in", 2)), Some(&None));
    }

    /// Two ingress edges of a join read separate buffers whose sequences both start at 1, so the
    /// same `(sequence, partition)` offset is in flight on both at once. Each must be tracked on
    /// its own, while a real redelivery on one edge is still caught.
    #[tokio::test]
    async fn test_same_offset_tracked_per_edge() {
        let tracker = Tracker::new(None, CancellationToken::new());
        let left = tracker.for_edge("left");
        let right = tracker.for_edge("right");

        let message = |seq: i64, wm_ms: i64| Message {
            offset: Offset::Int(IntOffset::new(seq, 0)),
            watermark: DateTime::from_timestamp_millis(wm_ms),
            ..Default::default()
        };

        left.insert(&message(1, 100)).await.unwrap();
        right
            .insert(&message(1, 100))
            .await
            .expect("same offset from another edge is not a duplicate");
        assert!(matches!(
            left.insert(&message(1, 100)).await,
            Err(Error::DuplicateInflight(_))
        ));

        // Deleting from one edge must not remove the other edge's entry.
        let offset = Offset::Int(IntOffset::new(1, 0));
        left.delete(&offset).await.unwrap();
        assert!(!tracker.is_empty().await.unwrap());
        right.delete(&offset).await.unwrap();
        assert!(tracker.is_empty().await.unwrap());

        // Sequences are not comparable across edges. The left edge's oldest in-flight message has
        // a lower sequence but a later watermark than the right edge's, and the lowest watermark
        // must still come from the right edge.
        left.insert(&message(5, 1_000)).await.unwrap();
        right.insert(&message(7, 300)).await.unwrap();
        assert_eq!(
            tracker.lowest_watermark().await.unwrap(),
            DateTime::from_timestamp_millis(300).unwrap()
        );
    }

    #[tokio::test]
    async fn test_idle_status_tracked_per_edge() {
        let handle = Tracker::new(None, CancellationToken::new());

        // two join edges reading the same partition index must not overwrite each other
        handle.set_idle_offset("left", 0, Some(100)).await.unwrap();
        handle.set_idle_offset("right", 0, None).await.unwrap();

        let idle_offsets = handle.get_idle_offset().await.unwrap();
        assert_eq!(idle_offsets.get(&("left", 0)), Some(&Some(100)));
        assert_eq!(idle_offsets.get(&("right", 0)), Some(&None));

        handle.set_idle_offset("right", 0, Some(7)).await.unwrap();
        let idle_offsets = handle.get_idle_offset().await.unwrap();
        assert_eq!(idle_offsets.get(&("left", 0)), Some(&Some(100)));
        assert_eq!(idle_offsets.get(&("right", 0)), Some(&Some(7)));
    }

    #[tokio::test]
    async fn test_tracker_state_drop_with_unacknowledged_messages() {
        // This test verifies that dropping TrackerState with unacknowledged messages
        // logs an error. We can't easily assert on log output, but we can verify
        // the drop doesn't panic and the logic works correctly.
        let handle = Tracker::new(None, CancellationToken::new());

        let message1 = Message {
            typ: Default::default(),
            keys: Arc::from([]),
            tags: None,
            value: Bytes::from_static(b"test1"),
            offset: Offset::String(StringOffset::new("offset1".to_string(), 0)),
            event_time: Default::default(),
            watermark: None,
            id: MessageID {
                vertex_name: "in".into(),
                offset: Bytes::from_static(b"offset1"),
                index: 1,
            },
            ..Default::default()
        };

        let message2 = Message {
            typ: Default::default(),
            keys: Arc::from([]),
            tags: None,
            value: Bytes::from_static(b"test2"),
            offset: Offset::String(StringOffset::new("offset2".to_string(), 1)),
            event_time: Default::default(),
            watermark: None,
            id: MessageID {
                vertex_name: "in".into(),
                offset: Bytes::from_static(b"offset2"),
                index: 2,
            },
            ..Default::default()
        };

        // Insert messages but don't acknowledge them
        handle.insert(&message1).await.unwrap();
        handle.insert(&message2).await.unwrap();

        // Verify tracker is not empty
        assert!(!handle.is_empty().await.unwrap());

        // When handle is dropped, TrackerState's drop should log an error
        // about 2 unacknowledged messages
        drop(handle);
        // The error log will appear in test output if running with --nocapture
    }
}
