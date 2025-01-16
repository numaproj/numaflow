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

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::error::Error;
use crate::message::ReadAck;
use crate::Result;

/// TrackerEntry represents the state of a tracked message.
#[derive(Debug)]
struct TrackerEntry {
    ack_send: oneshot::Sender<ReadAck>,
    count: u32,
    eof: bool,
}

/// ActorMessage represents the messages that can be sent to the Tracker actor.
enum ActorMessage {
    Insert {
        offset: Bytes,
        ack_send: oneshot::Sender<ReadAck>,
    },
    Update {
        offset: Bytes,
        count: u32,
        eof: bool,
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
    fn new(receiver: mpsc::Receiver<ActorMessage>) -> Self {
        Self {
            entries: HashMap::new(),
            receiver,
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
            } => {
                self.handle_insert(offset, respond_to);
            }
            ActorMessage::Update { offset, count, eof } => {
                self.handle_update(offset, count, eof);
            }
            ActorMessage::Delete { offset } => {
                self.handle_delete(offset);
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
    fn handle_insert(&mut self, offset: Bytes, respond_to: oneshot::Sender<ReadAck>) {
        self.entries.insert(
            offset,
            TrackerEntry {
                ack_send: respond_to,
                count: 0,
                eof: true,
            },
        );
    }

    /// Updates an existing entry in the tracker with the number of expected messages and EOF status.
    fn handle_update(&mut self, offset: Bytes, count: u32, eof: bool) {
        if let Some(entry) = self.entries.get_mut(&offset) {
            entry.count += count;
            entry.eof = eof;
            // if the count is zero, we can send an ack immediately
            // this is case where map stream will send eof true after
            // receiving all the messages.
            if entry.count == 0 {
                let entry = self.entries.remove(&offset).unwrap();
                entry
                    .ack_send
                    .send(ReadAck::Ack)
                    .expect("Failed to send ack");
            }
        }
    }

    /// Removes an entry from the tracker and sends an acknowledgment if the count is zero
    /// and the entry is marked as EOF.
    fn handle_delete(&mut self, offset: Bytes) {
        if let Some(mut entry) = self.entries.remove(&offset) {
            if entry.count > 0 {
                entry.count -= 1;
            }
            if entry.count == 0 && entry.eof {
                entry
                    .ack_send
                    .send(ReadAck::Ack)
                    .expect("Failed to send ack");
            } else {
                self.entries.insert(offset, entry);
            }
        }
    }

    /// Discards an entry from the tracker and sends a nak.
    fn handle_discard(&mut self, offset: Bytes) {
        if let Some(entry) = self.entries.remove(&offset) {
            entry
                .ack_send
                .send(ReadAck::Nak)
                .expect("Failed to send nak");
        }
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
}

/// TrackerHandle provides an interface to interact with the Tracker.
/// It allows inserting, updating, deleting, and discarding tracked messages.
#[derive(Clone)]
pub(crate) struct TrackerHandle {
    sender: mpsc::Sender<ActorMessage>,
}

impl TrackerHandle {
    /// Creates a new TrackerHandle instance and spawns the Tracker.
    pub(crate) fn new() -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let tracker = Tracker::new(receiver);
        tokio::spawn(tracker.run());
        Self { sender }
    }

    /// Inserts a new message into the Tracker with the given offset and acknowledgment sender.
    pub(crate) async fn insert(
        &self,
        offset: Bytes,
        ack_send: oneshot::Sender<ReadAck>,
    ) -> Result<()> {
        let message = ActorMessage::Insert { offset, ack_send };
        self.sender
            .send(message)
            .await
            .map_err(|e| Error::Tracker(format!("{:?}", e)))?;
        Ok(())
    }

    /// Updates an existing message in the Tracker with the given offset, count, and EOF status.
    pub(crate) async fn update(&self, offset: Bytes, count: u32, eof: bool) -> Result<()> {
        let message = ActorMessage::Update { offset, count, eof };
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
    use tokio::sync::oneshot;
    use tokio::time::{timeout, Duration};

    use super::*;

    #[tokio::test]
    async fn test_insert_update_delete() {
        let handle = TrackerHandle::new();
        let (ack_send, ack_recv) = oneshot::channel();

        // Insert a new message
        handle
            .insert("offset1".to_string().into(), ack_send)
            .await
            .unwrap();

        // Update the message
        handle
            .update("offset1".to_string().into(), 1, true)
            .await
            .unwrap();

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
        let handle = TrackerHandle::new();
        let (ack_send, ack_recv) = oneshot::channel();

        // Insert a new message
        handle
            .insert("offset1".to_string().into(), ack_send)
            .await
            .unwrap();

        // Update the message with a count of 3
        handle
            .update("offset1".to_string().into(), 3, true)
            .await
            .unwrap();

        // Delete the message three times
        handle.delete("offset1".to_string().into()).await.unwrap();
        handle.delete("offset1".to_string().into()).await.unwrap();
        handle.delete("offset1".to_string().into()).await.unwrap();

        // Verify that the message was deleted and ack was received after the third delete
        let result = timeout(Duration::from_secs(1), ack_recv).await.unwrap();
        assert!(result.is_ok(), "Ack should be received after three deletes");
        assert_eq!(result.unwrap(), ReadAck::Ack);
        assert!(handle.is_empty().await.unwrap(), "Tracker should be empty");
    }

    #[tokio::test]
    async fn test_discard() {
        let handle = TrackerHandle::new();
        let (ack_send, ack_recv) = oneshot::channel();

        // Insert a new message
        handle
            .insert("offset1".to_string().into(), ack_send)
            .await
            .unwrap();

        // Discard the message
        handle.discard("offset1".to_string().into()).await.unwrap();

        // Verify that the message was discarded and nak was received
        let result = timeout(Duration::from_secs(1), ack_recv).await.unwrap();
        assert!(result.is_ok(), "Nak should be received");
        assert_eq!(result.unwrap(), ReadAck::Nak);
        assert!(handle.is_empty().await.unwrap(), "Tracker should be empty");
    }

    #[tokio::test]
    async fn test_discard_after_update_with_higher_count() {
        let handle = TrackerHandle::new();
        let (ack_send, ack_recv) = oneshot::channel();

        // Insert a new message
        handle
            .insert("offset1".to_string().into(), ack_send)
            .await
            .unwrap();

        // Update the message with a count of 3
        handle
            .update("offset1".to_string().into(), 3, false)
            .await
            .unwrap();

        // Discard the message
        handle.discard("offset1".to_string().into()).await.unwrap();

        // Verify that the message was discarded and nak was received
        let result = timeout(Duration::from_secs(1), ack_recv).await.unwrap();
        assert!(result.is_ok(), "Nak should be received");
        assert_eq!(result.unwrap(), ReadAck::Nak);
        assert!(handle.is_empty().await.unwrap(), "Tracker should be empty");
    }
}
