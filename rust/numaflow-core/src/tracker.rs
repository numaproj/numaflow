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

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serving::callback::CallbackHandler;
use serving::{DEFAULT_ID_HEADER, DEFAULT_POD_HASH_KEY};
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::Result;
use crate::error::Error;
use crate::message::{Message, Offset, ReadAck};
use crate::watermark::isb::ISBWatermarkHandle;

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
    serving_callback_info: Option<ServingCallbackInfo>,
}

/// ActorMessage represents the messages that can be sent to the Tracker actor.
#[derive(Debug)]
enum ActorMessage {
    Insert {
        offset: Offset,
        ack_send: oneshot::Sender<ReadAck>,
        serving_callback_info: Option<ServingCallbackInfo>,
        watermark: Option<DateTime<Utc>>,
    },
    Update {
        offset: Offset,
        responses: Vec<Option<Vec<String>>>,
    },
    Append {
        offset: Offset,
        response: Option<Vec<String>>,
    },
    Delete {
        offset: Offset,
    },
    Discard {
        offset: Offset,
    },
    #[allow(clippy::upper_case_acronyms)]
    EOF {
        offset: Offset,
    },
    Refresh {
        offset: Offset,
    },
    #[cfg(test)]
    IsEmpty {
        respond_to: oneshot::Sender<bool>,
    },
}

/// Tracker is responsible for managing the state of messages being processed.
/// It keeps track of message offsets and their completeness, and sends acknowledgments.
struct Tracker {
    /// number of entries in the tracker
    entries: HashMap<Offset, TrackerEntry>,
    receiver: mpsc::Receiver<ActorMessage>,
    watermark_handle: Option<ISBWatermarkHandle>,
    serving_callback_handler: Option<CallbackHandler>,
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

impl Drop for Tracker {
    fn drop(&mut self) {
        if !self.entries.is_empty() {
            error!("Tracker dropped with non-empty entries: {:?}", self.entries);
        }
    }
}

impl Tracker {
    /// Creates a new Tracker instance with the given receiver for actor messages.
    fn new(
        receiver: mpsc::Receiver<ActorMessage>,
        watermark_handle: Option<ISBWatermarkHandle>,
        serving_callback_handler: Option<CallbackHandler>,
    ) -> Self {
        Self {
            entries: HashMap::new(),
            receiver,
            watermark_handle,
            serving_callback_handler,
        }
    }

    /// Runs the Tracker, processing incoming actor messages to update the state.
    async fn run(mut self) {
        while let Some(message) = self.receiver.recv().await {
            self.handle_message(message).await;
        }
    }

    /// Handles incoming actor messages to update the state of tracked messages.
    async fn handle_message(&mut self, message: ActorMessage) {
        match message {
            ActorMessage::Insert {
                offset,
                ack_send: respond_to,
                serving_callback_info: callback_info,
                watermark,
            } => {
                self.handle_insert(offset, callback_info, watermark, respond_to)
                    .await;
            }
            ActorMessage::Update { offset, responses } => {
                self.handle_update(offset, responses);
            }
            ActorMessage::EOF { offset } => {
                self.handle_eof(offset).await;
            }
            ActorMessage::Delete { offset } => {
                self.handle_delete(offset).await;
            }
            ActorMessage::Discard { offset } => {
                self.handle_discard(offset).await;
            }
            ActorMessage::Append { offset, response } => {
                self.handle_append(offset, response);
            }
            ActorMessage::Refresh { offset } => {
                self.handle_refresh(offset);
            }
            #[cfg(test)]
            ActorMessage::IsEmpty { respond_to } => {
                let is_empty = self.entries.is_empty();
                let _ = respond_to.send(is_empty);
            }
        }
    }

    /// Inserts a new entry into the tracker with the given offset and ack sender.
    async fn handle_insert(
        &mut self,
        offset: Offset,
        callback_info: Option<ServingCallbackInfo>,
        watermark: Option<DateTime<Utc>>,
        respond_to: oneshot::Sender<ReadAck>,
    ) {
        self.entries.insert(
            offset.clone(),
            TrackerEntry {
                ack_send: respond_to,
                count: 0,
                eof: true,
                serving_callback_info: callback_info,
            },
        );

        if let Some(watermark_handle) = &self.watermark_handle {
            watermark_handle.insert_offset(offset, watermark).await;
        }
    }

    /// Updates an existing entry in the tracker with the number of expected messages for this offset.
    fn handle_update(&mut self, offset: Offset, responses: Vec<Option<Vec<String>>>) {
        let Some(entry) = self.entries.get_mut(&offset) else {
            return;
        };

        entry.count += responses.len();
        if let Some(cb) = entry.serving_callback_info.as_mut() {
            cb.responses = responses;
        }
    }

    /// Appends a response to the serving callback info for the given offset.
    fn handle_append(&mut self, offset: Offset, response: Option<Vec<String>>) {
        let Some(entry) = self.entries.get_mut(&offset) else {
            return;
        };

        entry.count += 1;
        if let Some(cb) = entry.serving_callback_info.as_mut() {
            cb.responses.push(response);
        }
    }

    /// Update whether we have seen the eof (end of stream) for this offset.
    async fn handle_eof(&mut self, offset: Offset) {
        let Some(entry) = self.entries.get_mut(&offset) else {
            return;
        };
        entry.eof = true;
        // if the count is zero, we can send an ack immediately
        // this is case where map-stream will send eof true after
        // receiving all the messages.
        if entry.count == 0 {
            let entry = self.entries.remove(&offset).unwrap();
            self.completed_successfully(offset, entry).await;
        }
    }

    /// Removes an entry from the tracker and sends an acknowledgment if the count is zero
    /// and the entry is marked as EOF.
    async fn handle_delete(&mut self, offset: Offset) {
        let Some(mut entry) = self.entries.remove(&offset) else {
            return;
        };
        if entry.count > 0 {
            entry.count -= 1;
        }

        // if count is 0 and is eof we are sure that we can ack the offset.
        // In map-streaming this won't happen because eof is not tied to the message, rather it is
        // tied to channel-close.
        if entry.count == 0 && entry.eof {
            self.completed_successfully(offset, entry).await;
        } else {
            // add it back because we removed it
            self.entries.insert(offset, entry);
        }
    }

    /// Discards an entry from the tracker and sends a nak.
    async fn handle_discard(&mut self, offset: Offset) {
        let Some(entry) = self.entries.remove(&offset) else {
            return;
        };
        entry
            .ack_send
            .send(ReadAck::Nak)
            .expect("Failed to send nak");
        if let Some(watermark_handle) = &self.watermark_handle {
            watermark_handle.remove_offset(offset).await;
        }
    }

    /// Resets the count and eof status for an offset in the tracker.
    fn handle_refresh(&mut self, offset: Offset) {
        let Some(mut entry) = self.entries.remove(&offset) else {
            return;
        };
        entry.count = 0;
        entry.eof = false;
        if let Some(serving_info) = &mut entry.serving_callback_info {
            serving_info.responses = vec![];
        }
        self.entries.insert(offset, entry);
    }

    /// This function is called once the message has been successfully processed. This is where
    /// the bookkeeping for a successful message happens, things like,
    /// - ack back
    /// - call serving callbacks
    /// - watermark progression
    async fn completed_successfully(&self, offset: Offset, entry: TrackerEntry) {
        let TrackerEntry {
            ack_send,
            serving_callback_info: callback_info,
            ..
        } = entry;

        ack_send.send(ReadAck::Ack).expect("Failed to send ack");

        if let Some(watermark_handle) = &self.watermark_handle {
            watermark_handle.remove_offset(offset).await;
        }

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
}

/// TrackerHandle provides an interface to interact with the Tracker.
/// It allows inserting, updating, deleting, and discarding tracked messages.
#[derive(Clone)]
pub(crate) struct TrackerHandle {
    sender: mpsc::Sender<ActorMessage>,
    enable_callbacks: bool,
}

impl TrackerHandle {
    /// Creates a new TrackerHandle instance and spawns the Tracker.
    pub(crate) fn new(
        watermark_handle: Option<ISBWatermarkHandle>,
        callback_handler: Option<CallbackHandler>,
    ) -> Self {
        let enable_callbacks = callback_handler.is_some();
        let (sender, receiver) = mpsc::channel(1000);
        let tracker = Tracker::new(receiver, watermark_handle, callback_handler);
        tokio::spawn(tracker.run());
        Self {
            sender,
            enable_callbacks,
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
        if self.enable_callbacks {
            callback_info = Some(message.try_into()?);
        }
        let message = ActorMessage::Insert {
            offset,
            ack_send,
            serving_callback_info: callback_info,
            watermark: message.watermark,
        };
        self.sender
            .send(message)
            .await
            .map_err(|e| Error::Tracker(format!("{:?}", e)))?;
        Ok(())
    }

    /// Informs the tracker that a new message has been generated. The tracker should contain
    /// and entry for this message's offset.
    pub(crate) async fn update(
        &self,
        offset: Offset,
        response_tags: Vec<Option<Arc<[String]>>>,
    ) -> Result<()> {
        let responses = response_tags
            .into_iter()
            .map(|tags| tags.map(|tags| tags.iter().map(|tag| tag.to_string()).collect()))
            .collect();
        let message = ActorMessage::Update { offset, responses };
        self.sender
            .send(message)
            .await
            .map_err(|e| Error::Tracker(format!("{:?}", e)))?;
        Ok(())
    }

    /// resets the count and eof status for an offset in the tracker.
    pub(crate) async fn refresh(&self, offset: Offset) -> Result<()> {
        let message = ActorMessage::Refresh { offset };
        self.sender
            .send(message)
            .await
            .map_err(|e| Error::Tracker(format!("{:?}", e)))?;
        Ok(())
    }

    pub(crate) async fn append(
        &self,
        offset: Offset,
        message_tags: Option<Arc<[String]>>,
    ) -> Result<()> {
        let tags = message_tags.map(|tags| tags.to_vec());
        let message = ActorMessage::Append {
            offset,
            response: tags,
        };
        self.sender
            .send(message)
            .await
            .map_err(|e| Error::Tracker(format!("{:?}", e)))?;
        Ok(())
    }

    /// Updates the EOF status for an offset in the Tracker
    pub(crate) async fn eof(&self, offset: Offset) -> Result<()> {
        let message = ActorMessage::EOF { offset };
        self.sender
            .send(message)
            .await
            .map_err(|e| Error::Tracker(format!("{:?}", e)))?;
        Ok(())
    }

    /// Deletes a message from the Tracker with the given offset.
    pub(crate) async fn delete(&self, offset: Offset) -> Result<()> {
        let message = ActorMessage::Delete { offset };
        self.sender
            .send(message)
            .await
            .map_err(|e| Error::Tracker(format!("{:?}", e)))?;
        Ok(())
    }

    /// Discards a message from the Tracker with the given offset.
    pub(crate) async fn discard(&self, offset: Offset) -> Result<()> {
        let message = ActorMessage::Discard { offset };
        self.sender
            .send(message)
            .await
            .map_err(|e| Error::Tracker(format!("{:?}", e)))?;
        Ok(())
    }

    /// Checks if the Tracker is empty. Used for testing to make sure all messages are acknowledged.
    #[cfg(test)]
    pub(crate) async fn is_empty(&self) -> Result<bool> {
        let (respond_to, response) = oneshot::channel();
        let message = ActorMessage::IsEmpty { respond_to };
        self.sender
            .send(message)
            .await
            .map_err(|e| Error::Tracker(format!("{:?}", e)))?;
        response
            .await
            .map_err(|e| Error::Tracker(format!("{:?}", e)))
    }
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;
    use bytes::Bytes;
    use futures::StreamExt;
    use std::sync::Arc;
    use tokio::sync::oneshot;
    use tokio::time::{Duration, timeout};

    use super::*;
    use crate::message::StringOffset;
    use crate::message::{IntOffset, MessageID, Metadata, Offset};

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
            headers: HashMap::new(),
            metadata: None,
        };

        let callback_info: super::Result<ServingCallbackInfo> = TryFrom::try_from(&message);
        assert!(callback_info.is_err());

        let headers = [(DEFAULT_ID_HEADER, "1234"), (DEFAULT_POD_HASH_KEY, "abcd")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        message.headers = headers;

        const FROM_VERTEX_NAME: &str = "source-vertex";
        message.metadata = Some(Metadata {
            previous_vertex: FROM_VERTEX_NAME.into(),
        });

        let callback_info: ServingCallbackInfo = TryFrom::try_from(&message).unwrap();
        assert_eq!(callback_info.id, "1234");
        assert_eq!(callback_info.from_vertex, FROM_VERTEX_NAME);
        assert_eq!(callback_info.responses, vec![None]);
    }

    #[tokio::test]
    async fn test_insert_update_delete() {
        let handle = TrackerHandle::new(None, None);
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
            headers: HashMap::new(),
            metadata: None,
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
        let handle = TrackerHandle::new(None, None);
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
            headers: HashMap::new(),
            metadata: None,
        };

        // Insert a new message
        handle.insert(&message, ack_send).await.unwrap();

        let messages: Vec<Message> = std::iter::repeat(message.clone()).take(3).collect();
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
        let handle = TrackerHandle::new(None, None);
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
            headers: HashMap::new(),
            metadata: None,
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
        let handle = TrackerHandle::new(None, None);
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
            headers: HashMap::new(),
            metadata: None,
        };

        // Insert a new message
        handle.insert(&message, ack_send).await.unwrap();

        let messages: Vec<Message> = std::iter::repeat(message.clone()).take(3).collect();
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
            CallbackHandler::new("test".into(), js_context.clone(), store_name, 10).await;

        let handle = TrackerHandle::new(None, Some(callback_handler));
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
            headers,
            metadata: Some(Metadata {
                previous_vertex: "source-vertex".into(),
            }),
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
                if let Some(_) = keys.next().await {
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
}
