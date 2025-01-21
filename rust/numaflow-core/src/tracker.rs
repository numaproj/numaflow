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
enum ActorMessage {
    Insert {
        offset: Bytes,
        ack_send: oneshot::Sender<ReadAck>,
        serving_callback_info: Option<ServingCallbackInfo>,
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
    /// number of entri
    entries: HashMap<Bytes, TrackerEntry>,
    receiver: mpsc::Receiver<ActorMessage>,
    serving_callback_handler: Option<CallbackHandler>,
}

#[derive(Debug)]
struct ServingCallbackInfo {
    id: String,
    callback_url: String,
    from_vertex: String,
    /// at the moment these are just tags.
    responses: Vec<Option<Vec<String>>>,
}

impl TryFrom<&Message> for ServingCallbackInfo {
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
        Ok(ServingCallbackInfo {
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
        serving_callback_handler: Option<CallbackHandler>,
    ) -> Self {
        Self {
            entries: HashMap::new(),
            receiver,
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
        callback_info: Option<ServingCallbackInfo>,
        respond_to: oneshot::Sender<ReadAck>,
    ) {
        self.entries.insert(
            offset,
            TrackerEntry {
                ack_send: respond_to,
                count: 0,
                eof: true,
                serving_callback_info: callback_info,
            },
        );
    }

    /// Updates an existing entry in the tracker with the number of expected messages for this offset.
    fn handle_update(&mut self, offset: Bytes, responses: Option<Vec<String>>) {
        let Some(entry) = self.entries.get_mut(&offset) else {
            return;
        };

        entry.count += 1;
        if let Some(cb) = entry.serving_callback_info.as_mut() {
            cb.responses.push(responses);
        }
    }

    /// Update whether we have seen the eof (end of stream) for this offset.
    async fn handle_update_eof(&mut self, offset: Bytes) {
        let Some(entry) = self.entries.get_mut(&offset) else {
            return;
        };
        entry.eof = true;
        // if the count is zero, we can send an ack immediately
        // this is case where map-stream will send eof true after
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

        // if count is 0 and is eof we are sure that we can ack the offset.
        // In map-streaming this won't happen because eof is not tied to the message, rather it is
        // tied to channel-close.
        if entry.count == 0 && entry.eof {
            self.ack_message(entry).await;
        } else {
            // add it back because we removed it
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

    /// This function is called once the message has been successfully processed. This is where
    /// the bookkeeping for a successful message happens, things like,
    /// - ack back
    /// - call serving callbacks
    /// - watermark progression
    async fn ack_message(&self, entry: TrackerEntry) {
        let TrackerEntry {
            ack_send,
            serving_callback_info: callback_info,
            ..
        } = entry;

        ack_send.send(ReadAck::Ack).expect("Failed to send ack");

        let Some(ref callback_handler) = self.serving_callback_handler else {
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
            serving_callback_info: callback_info,
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
        let responses: Option<Vec<String>> = match (self.enable_callbacks, message_tags) {
            (true, Some(tags)) => {
                if !tags.is_empty() {
                    Some(tags.iter().cloned().collect::<Vec<String>>())
                } else {
                    None
                }
            }
            _ => None,
        };
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use axum::routing::{get, post};
    use axum::{http::StatusCode, Router};
    use tokio::sync::oneshot;
    use tokio::time::{timeout, Duration};

    use crate::message::{MessageID, Metadata};

    use super::*;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn test_message_to_callback_info_conversion() {
        let offset = Bytes::from_static(b"offset1");
        let mut message = Message {
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

        let callback_info: super::Result<ServingCallbackInfo> = TryFrom::try_from(&message);
        assert!(callback_info.is_err());

        const CALLBACK_URL: &str = "https://localhost/v1/process/callback";
        let headers = [
            (DEFAULT_CALLBACK_URL_HEADER_KEY, CALLBACK_URL),
            (DEFAULT_ID_HEADER, "1234"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        message.headers = headers;

        const FROM_VERTEX_NAME: &str = "source-vetext";
        message.metadata = Some(Metadata {
            previous_vertex: FROM_VERTEX_NAME.into(),
        });

        let callback_info: ServingCallbackInfo = TryFrom::try_from(&message).unwrap();
        assert_eq!(callback_info.id, "1234");
        assert_eq!(callback_info.callback_url, CALLBACK_URL);
        assert_eq!(callback_info.from_vertex, FROM_VERTEX_NAME);
        assert_eq!(callback_info.responses, vec![None]);
    }

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

    #[tokio::test]
    async fn test_tracker_with_callback_handler() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener
            .local_addr()
            .map_err(|e| format!("Failed to bind to 127.0.0.1:0: error={e:?}"))?
            .port();

        let server_addr = format!("127.0.0.1:{port}");
        let callback_url = format!("http://{server_addr}/v1/process/callback");

        let request_count = Arc::new(AtomicUsize::new(0));
        let router = Router::new()
            .route("/livez", get(|| async { StatusCode::OK }))
            .route(
                "/v1/process/callback",
                post({
                    let req_count = Arc::clone(&request_count);
                    || async move {
                        req_count.fetch_add(1, Ordering::Relaxed);
                        StatusCode::OK
                    }
                }),
            );

        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()?;

        // Wait for the server to be ready
        let mut server_ready = false;
        let health_url = format!("http://{server_addr}/livez");
        for _ in 0..10 {
            let Ok(resp) = client.get(&health_url).send().await else {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            };
            if resp.status().is_success() {
                server_ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(server_ready, "Server is not ready");

        let callback_handler = CallbackHandler::new("test".into(), 10);
        let handle = TrackerHandle::new(Some(callback_handler));
        let (ack_send, ack_recv) = oneshot::channel();

        let headers = [
            (DEFAULT_CALLBACK_URL_HEADER_KEY, callback_url),
            (DEFAULT_ID_HEADER, "1234".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

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
            headers,
            metadata: Some(Metadata {
                previous_vertex: "source-vertex".into(),
            }),
        };

        // Insert a new message
        handle.insert(&message, ack_send).await.unwrap();
        handle.update_eof(offset).await.unwrap();

        // Verify that the message was discarded and Ack was received
        let result = timeout(Duration::from_secs(1), ack_recv).await.unwrap();
        assert!(result.is_ok(), "Ack should be received");
        assert_eq!(result.unwrap(), ReadAck::Ack);
        assert!(handle.is_empty().await.unwrap(), "Tracker should be empty");

        // Callback request is made after sending data on ack_send channel.
        let mut received_callback_request = false;
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            received_callback_request = request_count.load(Ordering::Relaxed) == 1;
            if received_callback_request {
                break;
            }
        }
        assert!(received_callback_request, "Expected one callback request");
        server.abort();
        Ok(())
    }
}
