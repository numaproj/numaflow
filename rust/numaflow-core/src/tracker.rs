//! Tracker is added because when we do data forwarding in [MonoVertex](crate::monovertex::forwarder) or
//! in [Pipeline](crate::pipeline::forwarder), immaterial whether we are in source, UDF, or Sink, we
//! have to track whether the message has completely moved to the next vertex (N+1)th before we can
//! mark that message as done in the Nth vertex. We use Tracker to let Read know that it can mark the
//! message as Ack or NAck based on the state of the message. E.g., Ack if successfully written to ISB,
//! NAck otherwise if ISB is failing to accept, and we are in shutdown path.
//! There will be a tracker per input stream reader.
//!
//! In the future Watermark will also be propagated based on this.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use serving::callback::CallbackHandler;
use serving::{DEFAULT_CALLBACK_URL_HEADER_KEY, DEFAULT_ID_HEADER};
use tokio::sync::{mpsc, oneshot};

use crate::error::Error;
use crate::message::{Message, ReadAck};
use crate::Result;

/// TrackerEntry represents the state of a tracked message.
#[derive(Debug)]
struct TrackerEntry {
    ack_send: oneshot::Sender<ReadAck>,
    count: usize,
    eof: bool,
    callback_info: Option<CallbackInfo>,
}

/// ActorMessage represents the messages that can be sent to the Tracker actor.
enum ActorMessage {
    Insert {
        offset: Bytes,
        ack_send: oneshot::Sender<ReadAck>,
        callback_info: Option<CallbackInfo>,
    },
    Update {
        offset: Bytes,
        responses: Option<Vec<String>>,
    },
    UpdateEOF {
        offset: Bytes,
    },
    Delete {
        offset: Bytes,
    },
    Discard {
        offset: Bytes,
    },
    DiscardAll, // New variant for discarding all messages
    #[cfg(test)]
    IsEmpty {
        respond_to: oneshot::Sender<bool>,
    },
}

/// Tracker is responsible for managing the state of messages being processed.
/// It keeps track of message offsets and their completeness, and sends acknowledgments.
struct Tracker {
    entries: HashMap<Bytes, TrackerEntry>,
    receiver: mpsc::Receiver<ActorMessage>,
    callback_handler: Option<CallbackHandler>,
}

#[derive(Debug)]
struct CallbackInfo {
    id: String,
    callback_url: String,
    from_vertex: String,
    responses: Vec<Option<Vec<String>>>,
}

impl TryFrom<&Message> for CallbackInfo {
    type Error = Error;

    fn try_from(message: &Message) -> std::result::Result<Self, Self::Error> {
        let callback_url = message
            .headers
            .get(DEFAULT_CALLBACK_URL_HEADER_KEY)
            .ok_or_else(|| {
                Error::Source(format!(
                "{DEFAULT_CALLBACK_URL_HEADER_KEY} header is not present in the message headers",
            ))
            })?
            .to_owned();
        let uuid = message
            .headers
            .get(DEFAULT_ID_HEADER)
            .ok_or_else(|| {
                Error::Source(format!(
                    "{DEFAULT_ID_HEADER} is not found in message headers",
                ))
            })?
            .to_owned();

        let from_vertex = message
            .metadata
            .as_ref()
            .ok_or_else(|| Error::Source("Metadata field is empty in the message".into()))?
            .previous_vertex
            .clone();

        let mut msg_tags = None;
        if let Some(ref tags) = message.tags {
            if !tags.is_empty() {
                msg_tags = Some(tags.iter().cloned().collect());
            }
        };
        Ok(CallbackInfo {
            id: uuid,
            callback_url,
            from_vertex,
            responses: vec![msg_tags],
        })
    }
}

impl Drop for Tracker {
    fn drop(&mut self) {
        // clear the entries from the map and send nak
        for (_, entry) in self.entries.drain() {
            entry
                .ack_send
                .send(ReadAck::Nak)
                .expect("Failed to send nak");
        }
    }
}

impl Tracker {
    /// Creates a new Tracker instance with the given receiver for actor messages.
    fn new(
        receiver: mpsc::Receiver<ActorMessage>,
        callback_handler: Option<CallbackHandler>,
    ) -> Self {
        Self {
            entries: HashMap::new(),
            receiver,
            callback_handler,
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
                callback_info,
            } => {
                self.handle_insert(offset, callback_info, respond_to);
            }
            ActorMessage::Update { offset, responses } => {
                self.handle_update(offset, responses);
            }
            ActorMessage::UpdateEOF { offset } => {
                self.handle_update_eof(offset).await;
            }
            ActorMessage::Delete { offset } => {
                self.handle_delete(offset).await;
            }
            ActorMessage::Discard { offset } => {
                self.handle_discard(offset);
            }
            ActorMessage::DiscardAll => {
                self.handle_discard_all().await;
            }
            #[cfg(test)]
            ActorMessage::IsEmpty { respond_to } => {
                let is_empty = self.entries.is_empty();
                let _ = respond_to.send(is_empty);
            }
        }
    }

    /// Inserts a new entry into the tracker with the given offset and ack sender.
    fn handle_insert(
        &mut self,
        offset: Bytes,
        callback_info: Option<CallbackInfo>,
        respond_to: oneshot::Sender<ReadAck>,
    ) {
        self.entries.insert(
            offset,
            TrackerEntry {
                ack_send: respond_to,
                count: 0,
                eof: true,
                callback_info,
            },
        );
    }

    /// Updates an existing entry in the tracker with the number of expected messages and EOF status.
    fn handle_update(&mut self, offset: Bytes, responses: Option<Vec<String>>) {
        let Some(entry) = self.entries.get_mut(&offset) else {
            return;
        };

        entry.count += 1;
        if let Some(cb) = entry.callback_info.as_mut() {
            cb.responses.push(responses);
        }
    }

    async fn handle_update_eof(&mut self, offset: Bytes) {
        let Some(entry) = self.entries.get_mut(&offset) else {
            return;
        };
        entry.eof = true;
        // if the count is zero, we can send an ack immediately
        // this is case where map stream will send eof true after
        // receiving all the messages.
        if entry.count == 0 {
            let entry = self.entries.remove(&offset).unwrap();
            self.ack_message(entry).await;
        }
    }

    /// Removes an entry from the tracker and sends an acknowledgment if the count is zero
    /// and the entry is marked as EOF.
    async fn handle_delete(&mut self, offset: Bytes) {
        let Some(mut entry) = self.entries.remove(&offset) else {
            return;
        };
        if entry.count > 0 {
            entry.count -= 1;
        }
        if entry.count == 0 && entry.eof {
            self.ack_message(entry).await;
        } else {
            self.entries.insert(offset, entry);
        }
    }

    /// Discards an entry from the tracker and sends a nak.
    fn handle_discard(&mut self, offset: Bytes) {
        let Some(entry) = self.entries.remove(&offset) else {
            return;
        };
        entry
            .ack_send
            .send(ReadAck::Nak)
            .expect("Failed to send nak");
    }

    /// Discards all entries from the tracker and sends a nak for each.
    async fn handle_discard_all(&mut self) {
        for (_, entry) in self.entries.drain() {
            entry
                .ack_send
                .send(ReadAck::Nak)
                .expect("Failed to send nak");
        }
    }

    async fn ack_message(&self, entry: TrackerEntry) {
        let TrackerEntry {
            ack_send,
            callback_info,
            ..
        } = entry;

        ack_send.send(ReadAck::Ack).expect("Failed to send ack");

        let Some(ref callback_handler) = self.callback_handler else {
            return;
        };
        let Some(callback_info) = callback_info else {
            tracing::error!("Callback is enabled, but Tracker doesn't contain callback info");
            return;
        };

        let id = callback_info.id.clone();
        let result = callback_handler
            .callback(
                callback_info.id,
                callback_info.callback_url,
                callback_info.from_vertex,
                callback_info.responses,
            )
            .await;
        if let Err(e) = result {
            tracing::error!(?e, id, "Failed to send callback");
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
    pub(crate) fn new(callback_handler: Option<CallbackHandler>) -> Self {
        let enable_callbacks = callback_handler.is_some();
        let (sender, receiver) = mpsc::channel(100);
        let tracker = Tracker::new(receiver, callback_handler);
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
        let offset = message.id.offset.clone();
        let mut callback_info = None;
        if self.enable_callbacks {
            callback_info = Some(message.try_into()?);
        }
        let message = ActorMessage::Insert {
            offset,
            ack_send,
            callback_info,
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
        offset: Bytes,
        message_tags: Option<Arc<[String]>>,
    ) -> Result<()> {
        let mut responses: Option<Vec<String>> = None;
        if self.enable_callbacks {
            if let Some(tags) = message_tags {
                if !tags.is_empty() {
                    responses = Some(tags.iter().cloned().collect());
                }
            };
        }
        let message = ActorMessage::Update { offset, responses };
        self.sender
            .send(message)
            .await
            .map_err(|e| Error::Tracker(format!("{:?}", e)))?;
        Ok(())
    }

    /// Updates the EOF status for an offset in the Tracker
    pub(crate) async fn update_eof(&self, offset: Bytes) -> Result<()> {
        let message = ActorMessage::UpdateEOF { offset };
        self.sender
            .send(message)
            .await
            .map_err(|e| Error::Tracker(format!("{:?}", e)))?;
        Ok(())
    }

    /// Deletes a message from the Tracker with the given offset.
    pub(crate) async fn delete(&self, offset: Bytes) -> Result<()> {
        let message = ActorMessage::Delete { offset };
        self.sender
            .send(message)
            .await
            .map_err(|e| Error::Tracker(format!("{:?}", e)))?;
        Ok(())
    }

    /// Discards a message from the Tracker with the given offset.
    pub(crate) async fn discard(&self, offset: Bytes) -> Result<()> {
        let message = ActorMessage::Discard { offset };
        self.sender
            .send(message)
            .await
            .map_err(|e| Error::Tracker(format!("{:?}", e)))?;
        Ok(())
    }

    /// Discards all messages from the Tracker and sends a nak for each.
    pub(crate) async fn discard_all(&self) -> Result<()> {
        let message = ActorMessage::DiscardAll;
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
    use std::sync::Arc;

    use tokio::sync::oneshot;
    use tokio::time::{timeout, Duration};

    use crate::message::MessageID;

    use super::*;

    #[tokio::test]
    async fn test_insert_update_delete() {
        let handle = TrackerHandle::new(None);
        let (ack_send, ack_recv) = oneshot::channel();

        let offset = Bytes::from_static(b"offset1");
        let message = Message {
            keys: Arc::from([]),
            tags: None,
            value: Bytes::from_static(b"test"),
            offset: None,
            event_time: Default::default(),
            id: MessageID {
                vertex_name: "in".into(),
                offset: offset.clone(),
                index: 1,
            },
            headers: HashMap::new(),
            metadata: None,
        };

        // Insert a new message
        handle.insert(&message, ack_send).await.unwrap();

        // Update the message
        handle
            .update(offset.clone(), message.tags.clone())
            .await
            .unwrap();
        handle.update_eof(offset).await.unwrap();

        // Delete the message
        handle.delete("offset1".to_string().into()).await.unwrap();

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

        let offset = Bytes::from_static(b"offset1");
        let message = Message {
            keys: Arc::from([]),
            tags: None,
            value: Bytes::from_static(b"test"),
            offset: None,
            event_time: Default::default(),
            id: MessageID {
                vertex_name: "in".into(),
                offset: offset.clone(),
                index: 1,
            },
            headers: HashMap::new(),
            metadata: None,
        };

        // Insert a new message
        handle.insert(&message, ack_send).await.unwrap();

        let messages: Vec<Message> = std::iter::repeat(message).take(3).collect();
        // Update the message with a count of 3
        for message in messages {
            handle
                .update(offset.clone(), message.tags.clone())
                .await
                .unwrap();
        }

        // Delete the message three times
        handle.delete(offset.clone()).await.unwrap();
        handle.delete(offset.clone()).await.unwrap();
        handle.delete(offset).await.unwrap();

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

        let offset = Bytes::from_static(b"offset1");
        let message = Message {
            keys: Arc::from([]),
            tags: None,
            value: Bytes::from_static(b"test"),
            offset: None,
            event_time: Default::default(),
            id: MessageID {
                vertex_name: "in".into(),
                offset: offset.clone(),
                index: 1,
            },
            headers: HashMap::new(),
            metadata: None,
        };

        // Insert a new message
        handle.insert(&message, ack_send).await.unwrap();

        // Discard the message
        handle.discard(offset).await.unwrap();

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

        let offset = Bytes::from_static(b"offset1");
        let message = Message {
            keys: Arc::from([]),
            tags: None,
            value: Bytes::from_static(b"test"),
            offset: None,
            event_time: Default::default(),
            id: MessageID {
                vertex_name: "in".into(),
                offset: offset.clone(),
                index: 1,
            },
            headers: HashMap::new(),
            metadata: None,
        };

        // Insert a new message
        handle.insert(&message, ack_send).await.unwrap();

        let messages: Vec<Message> = std::iter::repeat(message).take(3).collect();
        for message in messages {
            handle
                .update(offset.clone(), message.tags.clone())
                .await
                .unwrap();
        }

        // Discard the message
        handle.discard(offset).await.unwrap();

        // Verify that the message was discarded and nak was received
        let result = timeout(Duration::from_secs(1), ack_recv).await.unwrap();
        assert!(result.is_ok(), "Nak should be received");
        assert_eq!(result.unwrap(), ReadAck::Nak);
        assert!(handle.is_empty().await.unwrap(), "Tracker should be empty");
    }
}
