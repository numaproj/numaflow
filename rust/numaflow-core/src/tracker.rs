//! Tracker is added because when we do data forwarding in [MonoVertex](crate::monovertex::forwarder) or
//! in [Pipeline](crate::pipeline::forwarder), immaterial whether we are in source, UDF, or Sink, we
//! have to track whether the message has completely moved to the next vertex (N+1)th before we can
//! mark that message as done in the Nth vertex. We use Tracker to let Read know that it can mark the
//! message as Ack or NAck based on the state of the message. E.g., Ack if successfully written to ISB,
//! NAck otherwise if ISB is failing to accept, and we are in shutdown path.
//! There will be a tracker per input stream reader.
//!
//! Items tracked by the tracker and uses [Offset] as the key.
//!   - Ack or NAck after processing of a message
//!   - The oldest Watermark is tracked
//!   - Callbacks for Serving is triggered in the tracker.

use chrono::{DateTime, Utc};
use serving::callback::CallbackHandler;
use serving::{DEFAULT_ID_HEADER, DEFAULT_POD_HASH_KEY};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Mutex, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::Result;
use crate::error::Error;
use crate::message::{Message, Offset, ReadAck};

/// TrackerEntry represents the state of a tracked message.
#[derive(Debug)]
struct TrackerEntry {
    /// one shot to send the ack back
    ack_send: oneshot::Sender<ReadAck>,
    /// number of messages in flight. the count to reach 0 for the ack to happen.
    /// count++ happens during update and count-- during delete
    count: usize,
    /// end of stream for this offset (not expecting any more streaming data).
    eof: bool,
    /// Callback info for serving
    serving_callback_info: Option<ServingCallbackInfo>,
    /// Watermark for the message
    watermark: Option<DateTime<Utc>>,
}

/// TrackerState holds the mutable state of the tracker.
struct TrackerState {
    /// entries organized by partition, each partition has its own BTreeMap of offsets
    entries: HashMap<u16, BTreeMap<Offset, TrackerEntry>>,
    /// tracks whether the source is currently idle (not reading any data)
    /// if it's set to none, it means the source is not idle.
    idle_offset_map: HashMap<u16, Option<i64>>,
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
pub(crate) struct TrackerHandle {
    state: Arc<Mutex<TrackerState>>,
    serving_callback_handler: Option<CallbackHandler>,
    processed_msg_count: Arc<AtomicUsize>,
    cln_token: CancellationToken,
}

impl Drop for TrackerHandle {
    fn drop(&mut self) {
        // Try to lock the state to check if it's empty
        if let Ok(state) = self.state.try_lock() {
            let total_entries: usize = state
                .entries
                .values()
                .map(|partition| partition.len())
                .sum();
            if total_entries > 0 {
                error!(
                    "Tracker dropped with non-empty entries: {:?}",
                    state.entries
                );
            }
        }
        self.cln_token.cancel();
    }
}

impl TrackerHandle {
    /// Creates a new TrackerHandle instance.
    pub(crate) fn new(serving_callback_handler: Option<CallbackHandler>) -> Self {
        let processed_msg_count = Arc::new(AtomicUsize::new(0));
        let cln_token = CancellationToken::new();

        let state = Arc::new(Mutex::new(TrackerState {
            entries: HashMap::new(),
            idle_offset_map: HashMap::new(),
        }));

        // spawn a task to log the number of processed messages every second, cln_token is used to
        // stop the task when the tracker is dropped.
        tokio::spawn({
            let processed_msg_count = Arc::clone(&processed_msg_count);
            let cln_token = cln_token.clone();
            async move {
                Self::log_processed_msg_count(processed_msg_count, cln_token).await;
            }
        });

        Self {
            state,
            serving_callback_handler,
            processed_msg_count,
            cln_token,
        }
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
                    info!(processed = processed_msg_count.swap(0, Ordering::Relaxed), "Processed messages per second");
                }
            }
        }
    }

    /// This function is called once the message has been successfully processed. This is where
    /// the bookkeeping for a successful message happens, things like,
    /// - ack back
    /// - call serving callbacks
    /// - watermark progression
    async fn completed_successfully(&self, entry: TrackerEntry) {
        let TrackerEntry {
            ack_send,
            serving_callback_info: callback_info,
            ..
        } = entry;

        ack_send.send(ReadAck::Ack).expect("Failed to send ack");
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

    /// Inserts a new message into the Tracker with the given offset and acknowledgment sender.
    pub(crate) async fn insert(
        &self,
        message: &Message,
        ack_send: oneshot::Sender<ReadAck>,
    ) -> Result<()> {
        let offset = message.offset.clone();
        let mut callback_info = None;
        if self.serving_callback_handler.is_some() {
            callback_info = Some(message.try_into()?);
        }

        let partition = offset.partition_idx();
        let mut state = self.state.lock().await;
        let partition_entries = state.entries.entry(partition).or_default();
        partition_entries.insert(
            offset.clone(),
            TrackerEntry {
                ack_send,
                count: 0,
                eof: true,
                serving_callback_info: callback_info,
                watermark: message.watermark,
            },
        );
        Ok(())
    }

    /// Informs the tracker that a new message has been generated. The tracker should contain
    /// and entry for this message's offset.
    pub(crate) async fn update(
        &self,
        offset: Offset,
        response_tags: Vec<Option<Arc<[String]>>>,
    ) -> Result<()> {
        let responses: Vec<Option<Vec<String>>> = response_tags
            .into_iter()
            .map(|tags| tags.map(|tags| tags.iter().map(|tag| tag.to_string()).collect()))
            .collect();

        let partition = offset.partition_idx();
        let mut state = self.state.lock().await;
        let Some(partition_entries) = state.entries.get_mut(&partition) else {
            return Ok(());
        };
        let Some(entry) = partition_entries.get_mut(&offset) else {
            return Ok(());
        };

        entry.count += responses.len();
        if let Some(cb) = entry.serving_callback_info.as_mut() {
            cb.responses = responses;
        }
        Ok(())
    }

    /// resets the count and eof status for an offset in the tracker.
    pub(crate) async fn refresh(&self, offset: Offset) -> Result<()> {
        let partition = offset.partition_idx();
        let mut state = self.state.lock().await;
        let Some(partition_entries) = state.entries.get_mut(&partition) else {
            return Ok(());
        };
        let Some(mut entry) = partition_entries.remove(&offset) else {
            return Ok(());
        };

        entry.count = 0;
        entry.eof = false;

        if let Some(serving_info) = &mut entry.serving_callback_info {
            serving_info.responses = vec![];
        }
        partition_entries.insert(offset, entry);
        Ok(())
    }

    pub(crate) async fn append(
        &self,
        offset: Offset,
        message_tags: Option<Arc<[String]>>,
    ) -> Result<()> {
        let response = message_tags.map(|tags| tags.to_vec());

        let partition = offset.partition_idx();
        let mut state = self.state.lock().await;
        let Some(partition_entries) = state.entries.get_mut(&partition) else {
            return Ok(());
        };
        let Some(entry) = partition_entries.get_mut(&offset) else {
            return Ok(());
        };

        entry.count += 1;
        if let Some(cb) = entry.serving_callback_info.as_mut() {
            cb.responses.push(response);
        }
        Ok(())
    }

    /// Updates the EOF status for an offset in the Tracker
    pub(crate) async fn eof(&self, offset: Offset) -> Result<()> {
        let partition = offset.partition_idx();
        let mut state = self.state.lock().await;

        let Some(partition_entries) = state.entries.get_mut(&partition) else {
            return Ok(());
        };
        let Some(entry) = partition_entries.get_mut(&offset) else {
            return Ok(());
        };

        entry.eof = true;
        // if the count is zero, we can send an ack immediately
        // this is case where map-stream will send eof true after
        // receiving all the messages.
        if entry.count == 0 {
            let entry = partition_entries.remove(&offset).unwrap();
            drop(state); // Release the lock before calling completed_successfully
            self.completed_successfully(entry).await;
        }
        Ok(())
    }

    /// Deletes a message from the Tracker with the given offset.
    pub(crate) async fn delete(&self, offset: Offset) -> Result<()> {
        let partition = offset.partition_idx();
        let mut state = self.state.lock().await;
        let Some(partition_entries) = state.entries.get_mut(&partition) else {
            return Ok(());
        };
        let Some(mut entry) = partition_entries.remove(&offset) else {
            return Ok(());
        };

        if entry.count > 0 {
            entry.count -= 1;
        }

        // if count is 0 and is eof we are sure that we can ack the offset.
        // In map-streaming this won't happen because eof is not tied to the message, rather it is
        // tied to channel-close.
        if entry.count == 0 && entry.eof {
            drop(state); // Release the lock before calling completed_successfully
            self.completed_successfully(entry).await;
        } else {
            // add it back because we removed it
            partition_entries.insert(offset, entry);
        }
        Ok(())
    }

    /// Discards a message from the Tracker with the given offset.
    pub(crate) async fn discard(&self, offset: Offset) -> Result<()> {
        let partition = offset.partition_idx();
        let mut state = self.state.lock().await;
        let Some(partition_entries) = state.entries.get_mut(&partition) else {
            return Ok(());
        };
        let Some(entry) = partition_entries.remove(&offset) else {
            return Ok(());
        };
        entry
            .ack_send
            .send(ReadAck::Nak)
            .expect("Failed to send nak");
        Ok(())
    }

    /// Checks if the Tracker is empty. Used for testing to make sure all messages are acknowledged.
    #[cfg(test)]
    pub(crate) async fn is_empty(&self) -> Result<bool> {
        let state = self.state.lock().await;
        Ok(state.entries.values().all(|partition| partition.is_empty()))
    }

    /// Returns the lowest watermark among all the tracked offsets.
    pub(crate) async fn lowest_watermark(&self) -> Result<DateTime<Utc>> {
        let state = self.state.lock().await;
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

    /// Sets the idle status of the tracker. Setting idle offset to None means the partition is not
    /// idling. This offset is the Head WMB offset which is used for "optimistic locking" on the idle
    /// status and Head WMB. Bear in mind that the watermark itself is not stored because the Watermark
    /// can monotonically increase for the same Head MWB offset.
    pub(crate) async fn set_idle_offset(
        &self,
        partition_idx: u16,
        idle_offset: Option<i64>,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
        state.idle_offset_map.insert(partition_idx, idle_offset);
        Ok(())
    }

    /// Gets the idle wmb status of the tracker. It returns a map of partition index to the idle offset.
    /// This idle offset is compared against the newly fetch Head WMB as part of the optimistic locking.
    pub(crate) async fn get_idle_offset(&self) -> Result<HashMap<u16, Option<i64>>> {
        let state = self.state.lock().await;
        Ok(state.idle_offset_map.clone())
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
    use tokio::sync::oneshot;
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
            headers: Arc::new(HashMap::new()),
            metadata: None,
            is_late: false,
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
        let handle = TrackerHandle::new(None);
        let (ack_send, ack_recv) = oneshot::channel();

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
            headers: Arc::new(HashMap::new()),
            metadata: None,
            is_late: false,
        };

        // Insert a new message
        handle.insert(&message, ack_send).await.unwrap();

        // Update the message
        handle
            .update(message.offset.clone(), vec![message.tags.clone()])
            .await
            .unwrap();
        handle.eof(message.offset.clone()).await.unwrap();

        // Delete the message
        handle.delete(message.offset).await.unwrap();

        // Verify that the message was deleted and ack was received
        let result = timeout(Duration::from_secs(1), ack_recv).await.unwrap();
        assert!(result.is_ok(), "Ack should be received");
        assert_eq!(result.unwrap(), ReadAck::Ack);
        assert!(handle.is_empty().await.unwrap(), "Tracker should be empty");
    }

    #[tokio::test]
    async fn test_update_with_multiple_deletes() {
        let handle = TrackerHandle::new(None);
        let (ack_send, ack_recv) = oneshot::channel();
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
            headers: Arc::new(HashMap::new()),
            metadata: None,
            is_late: false,
        };

        // Insert a new message
        handle.insert(&message, ack_send).await.unwrap();

        let messages: Vec<Message> = std::iter::repeat_n(message.clone(), 3).collect();
        // Update the message with a count of 3
        for message in messages {
            handle
                .update(message.offset.clone(), vec![message.tags.clone()])
                .await
                .unwrap();
        }

        // Delete the message three times
        handle.delete(message.offset.clone()).await.unwrap();
        handle.delete(message.offset.clone()).await.unwrap();
        handle.delete(message.offset.clone()).await.unwrap();

        // Verify that the message was deleted and ack was received after the third delete
        let result = timeout(Duration::from_secs(1), ack_recv).await.unwrap();
        assert!(result.is_ok(), "Ack should be received after three deletes");
        assert_eq!(result.unwrap(), ReadAck::Ack);
        assert!(handle.is_empty().await.unwrap(), "Tracker should be empty");
    }

    #[tokio::test]
    async fn test_discard() {
        let handle = TrackerHandle::new(None);
        let (ack_send, ack_recv) = oneshot::channel();

        let message = Message {
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
            headers: Arc::new(HashMap::new()),
            metadata: None,
            is_late: false,
        };

        // Insert a new message
        handle.insert(&message, ack_send).await.unwrap();

        // Discard the message
        handle.discard(message.offset.clone()).await.unwrap();

        // Verify that the message was discarded and nak was received
        let result = timeout(Duration::from_secs(1), ack_recv).await.unwrap();
        assert!(result.is_ok(), "Nak should be received");
        assert_eq!(result.unwrap(), ReadAck::Nak);
        assert!(handle.is_empty().await.unwrap(), "Tracker should be empty");
    }

    #[tokio::test]
    async fn test_discard_after_update_with_higher_count() {
        let handle = TrackerHandle::new(None);
        let (ack_send, ack_recv) = oneshot::channel();

        let message = Message {
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
            headers: Arc::new(HashMap::new()),
            metadata: None,
            is_late: false,
        };

        // Insert a new message
        handle.insert(&message, ack_send).await.unwrap();

        let messages: Vec<Message> = std::iter::repeat_n(message.clone(), 3).collect();
        for message in messages {
            handle
                .update(message.offset.clone(), vec![message.tags.clone()])
                .await
                .unwrap();
        }

        // Discard the message
        handle.discard(message.offset).await.unwrap();

        // Verify that the message was discarded and nak was received
        let result = timeout(Duration::from_secs(1), ack_recv).await.unwrap();
        assert!(result.is_ok(), "Nak should be received");
        assert_eq!(result.unwrap(), ReadAck::Nak);
        assert!(handle.is_empty().await.unwrap(), "Tracker should be empty");
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_tracker_with_callback_handler() -> Result<()> {
        let store_name = "test_tracker_with_callback_handler";
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let _ = js_context.delete_key_value(store_name).await;
        let callback_bucket = js_context
            .create_key_value(Config {
                bucket: store_name.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let callback_handler =
            CallbackHandler::new("test", js_context.clone(), store_name, 10).await;

        let handle = TrackerHandle::new(Some(callback_handler));
        let (ack_send, ack_recv) = oneshot::channel();

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
        };

        // Insert a new message
        handle.insert(&message, ack_send).await.unwrap();
        handle.eof(offset).await.unwrap();

        // Verify that the message was discarded and Ack was received
        let result = timeout(Duration::from_secs(1), ack_recv).await.unwrap();
        assert!(result.is_ok(), "Ack should be received");
        assert_eq!(result.unwrap(), ReadAck::Ack);
        assert!(handle.is_empty().await.unwrap(), "Tracker should be empty");

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
        js_context.delete_key_value(store_name).await.unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn test_idle_status_tracking() {
        let handle = TrackerHandle::new(None);
        assert_eq!(handle.get_idle_offset().await.unwrap(), HashMap::new());

        handle.set_idle_offset(0, Some(100)).await.unwrap();
        handle.set_idle_offset(1, Some(200)).await.unwrap();
        handle.set_idle_offset(2, None).await.unwrap();

        let idle_offsets = handle.get_idle_offset().await.unwrap();
        assert_eq!(idle_offsets.get(&0), Some(&Some(100)));
        assert_eq!(idle_offsets.get(&1), Some(&Some(200)));
        assert_eq!(idle_offsets.get(&2), Some(&None));

        handle.set_idle_offset(0, None).await.unwrap();
        let idle_offsets = handle.get_idle_offset().await.unwrap();
        assert_eq!(idle_offsets.get(&0), Some(&None));
        assert_eq!(idle_offsets.get(&1), Some(&Some(200)));
        assert_eq!(idle_offsets.get(&2), Some(&None));
    }
}
