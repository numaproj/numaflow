use crate::error::Error;
use crate::message::ReadAck;
use crate::Result;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
struct TrackerEntry {
    ack_send: oneshot::Sender<ReadAck>,
    count: u32,
    eof: bool,
}

enum ActorMessage {
    Insert {
        offset: String,
        ack_send: oneshot::Sender<ReadAck>,
        completion_notify: oneshot::Sender<Result<()>>,
    },
    Update {
        offset: String,
        count: u32,
        eof: bool,
        completion_notify: oneshot::Sender<Result<()>>,
    },
    Delete {
        offset: String,
        completion_notify: oneshot::Sender<Result<()>>,
    },
}

struct Tracker {
    entries: HashMap<String, TrackerEntry>,
    receiver: mpsc::Receiver<ActorMessage>,
}

impl Tracker {
    fn new(receiver: mpsc::Receiver<ActorMessage>) -> Self {
        Self {
            entries: HashMap::new(),
            receiver,
        }
    }

    async fn run(mut self) {
        while let Some(message) = self.receiver.recv().await {
            self.handle_message(message).await;
        }
    }

    async fn handle_message(&mut self, message: ActorMessage) {
        match message {
            ActorMessage::Insert {
                offset: message_id,
                ack_send: respond_to,
                completion_notify,
            } => {
                self.handle_insert(message_id, respond_to);
                let _ = completion_notify.send(Ok(()));
            }
            ActorMessage::Update {
                offset: message_id,
                count,
                eof,
                completion_notify,
            } => {
                self.handle_update(message_id, count, eof);
                let _ = completion_notify.send(Ok(()));
            }
            ActorMessage::Delete {
                offset: message_id,
                completion_notify,
            } => {
                if let Some(respond_to) = self.handle_delete(message_id) {
                    let _ = respond_to.send(ReadAck::Ack);
                }
                let _ = completion_notify.send(Ok(()));
            }
        }
    }

    fn handle_insert(&mut self, offset: String, respond_to: oneshot::Sender<ReadAck>) {
        self.entries.insert(
            offset,
            TrackerEntry {
                ack_send: respond_to,
                count: 0,
                eof: false,
            },
        );
    }

    fn handle_update(&mut self, offset: String, count: u32, eof: bool) {
        if let Some(entry) = self.entries.get_mut(&offset) {
            entry.count = count;
            entry.eof = eof;
        }
    }

    fn handle_delete(&mut self, offset: String) -> Option<oneshot::Sender<ReadAck>> {
        if let Some(mut entry) = self.entries.remove(&offset) {
            if entry.count > 0 {
                entry.count -= 1;
            }
            if entry.count == 0 || entry.eof {
                return Some(entry.ack_send);
            } else {
                self.entries.insert(offset, entry);
            }
        }
        None
    }
}

#[derive(Clone)]
pub struct TrackerHandle {
    sender: mpsc::Sender<ActorMessage>,
}

impl TrackerHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let tracker = Tracker::new(receiver);
        tokio::spawn(tracker.run());

        Self { sender }
    }

    pub async fn insert(&self, offset: String, ack_send: oneshot::Sender<ReadAck>) -> Result<()> {
        let (completion_notify, completion_recv) = oneshot::channel();
        let message = ActorMessage::Insert {
            offset,
            ack_send,
            completion_notify,
        };
        let _ = self.sender.send(message).await;
        completion_recv
            .await
            .map_err(|e| Error::Tracker(e.to_string()))?
    }

    pub async fn update(&self, offset: String, count: u32, eof: bool) -> Result<()> {
        let (completion_notify, completion_recv) = oneshot::channel();
        let message = ActorMessage::Update {
            offset,
            count,
            eof,
            completion_notify,
        };
        let _ = self.sender.send(message).await;
        completion_recv
            .await
            .map_err(|e| Error::Tracker(e.to_string()))?
    }

    pub async fn delete(&self, offset: String) -> Result<()> {
        let (completion_notify, completion_recv) = oneshot::channel();
        let message = ActorMessage::Delete {
            offset,
            completion_notify,
        };
        let _ = self.sender.send(message).await;
        completion_recv
            .await
            .map_err(|e| Error::Tracker(e.to_string()))?
    }
}
