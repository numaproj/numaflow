use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::app::callback::state::State as CallbackState;
use crate::app::callback::store::redisstore::RedisConnection;
use crate::app::tracker::MessageGraph;
use crate::Settings;
use crate::{Error, Result};

/// [Message] with a oneshot for notifying when the message has been completed processed.
pub(crate) struct MessageWrapper {
    // TODO: this might be more that saving to ISB.
    pub(crate) confirm_save: oneshot::Sender<()>,
    pub(crate) message: Message,
}

/// Serving payload passed on to Numaflow.
#[derive(Debug)]
pub struct Message {
    pub value: Bytes,
    pub id: String,
    pub headers: HashMap<String, String>,
}

enum ActorMessage {
    Read {
        batch_size: usize,
        timeout_at: Instant,
        reply_to: oneshot::Sender<Vec<Message>>,
    },
    Ack {
        offsets: Vec<String>,
        reply_to: oneshot::Sender<()>,
    },
}

struct ServingSourceActor {
    messages: mpsc::Receiver<MessageWrapper>,
    handler_rx: mpsc::Receiver<ActorMessage>,
    tracker: HashMap<String, oneshot::Sender<()>>,
}

impl ServingSourceActor {
    async fn start(
        settings: Arc<Settings>,
        handler_rx: mpsc::Receiver<ActorMessage>,
    ) -> Result<()> {
        let (messages_tx, messages_rx) = mpsc::channel(10000);
        // Create a redis store to store the callbacks and the custom responses
        let redis_store = RedisConnection::new(settings.redis.clone()).await?;
        // Create the message graph from the pipeline spec and the redis store
        let msg_graph = MessageGraph::from_pipeline(&settings.pipeline_spec).map_err(|e| {
            Error::InitError(format!(
                "Creating message graph from pipeline spec: {:?}",
                e
            ))
        })?;
        let callback_state = CallbackState::new(msg_graph, redis_store).await?;

        tokio::spawn(async move {
            let mut serving_actor = ServingSourceActor {
                messages: messages_rx,
                handler_rx,
                tracker: HashMap::new(),
            };
            serving_actor.run().await;
        });
        let app = crate::AppState {
            message: messages_tx,
            settings,
            callback_state,
        };
        tokio::spawn(async move {
            crate::serve(app).await.unwrap();
        });
        Ok(())
    }

    async fn run(&mut self) {
        while let Some(msg) = self.handler_rx.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, actor_msg: ActorMessage) {
        match actor_msg {
            ActorMessage::Read {
                batch_size,
                timeout_at,
                reply_to,
            } => {
                let messages = self.read(batch_size, timeout_at).await;
                let _ = reply_to.send(messages);
            }
            ActorMessage::Ack { offsets, reply_to } => {
                self.ack(offsets).await;
                let _ = reply_to.send(());
            }
        }
    }

    async fn read(&mut self, count: usize, timeout_at: Instant) -> Vec<Message> {
        let mut messages = vec![];
        loop {
            if messages.len() >= count || Instant::now() >= timeout_at {
                break;
            }
            let message = match self.messages.try_recv() {
                Ok(msg) => msg,
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(e) => {
                    tracing::error!(?e, "Receiving messages from the serving channel"); // FIXME:
                    return messages;
                }
            };
            let MessageWrapper {
                confirm_save,
                message,
            } = message;

            self.tracker.insert(message.id.clone(), confirm_save);
            messages.push(message);
        }
        messages
    }

    async fn ack(&mut self, offsets: Vec<String>) {
        for offset in offsets {
            let offset = offset
                .strip_suffix("-0")
                .expect("offset does not end with '-0'"); // FIXME: we hardcode 0 as the partition index when constructing offset
            let confirm_save_tx = self
                .tracker
                .remove(offset)
                .expect("offset was not found in the tracker");
            confirm_save_tx
                .send(())
                .expect("Sending on confirm_save channel");
        }
    }
}

#[derive(Clone)]
pub struct ServingSource {
    batch_size: usize,
    // timeout for each batch read request
    timeout: Duration,
    actor_tx: mpsc::Sender<ActorMessage>,
}

impl ServingSource {
    pub async fn new(
        settings: Arc<Settings>,
        batch_size: usize,
        timeout: Duration,
    ) -> Result<Self> {
        let (actor_tx, actor_rx) = mpsc::channel(1000);
        ServingSourceActor::start(settings, actor_rx).await?;
        Ok(Self {
            batch_size,
            timeout,
            actor_tx,
        })
    }

    pub async fn read_messages(&self) -> Result<Vec<Message>> {
        let start = Instant::now();
        let (tx, rx) = oneshot::channel();
        let actor_msg = ActorMessage::Read {
            reply_to: tx,
            batch_size: self.batch_size,
            timeout_at: Instant::now() + self.timeout,
        };
        let _ = self.actor_tx.send(actor_msg).await;
        let messages = rx.await.map_err(Error::ActorTaskTerminated)?;
        tracing::debug!(
            count = messages.len(),
            requested_count = self.batch_size,
            time_taken_ms = start.elapsed().as_millis(),
            "Got messages from Serving source"
        );
        Ok(messages)
    }

    pub async fn ack_messages(&self, offsets: Vec<String>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let actor_msg = ActorMessage::Ack {
            offsets,
            reply_to: tx,
        };
        let _ = self.actor_tx.send(actor_msg).await;
        rx.await.map_err(Error::ActorTaskTerminated)?;
        Ok(())
    }
}

#[cfg(feature = "redis-tests")]
#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use crate::Settings;

    use super::ServingSource;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
    #[tokio::test]
    async fn test_serving_source() -> Result<()> {
        let settings = Arc::new(Settings::default());
        let serving_source =
            ServingSource::new(Arc::clone(&settings), 10, Duration::from_millis(1)).await?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();

        // Wait for the server
        for _ in 0..10 {
            let resp = client
                .get(format!(
                    "https://localhost:{}/livez",
                    settings.app_listen_port
                ))
                .send()
                .await;
            if resp.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let mut messages = serving_source.read_messages().await.unwrap();
                if messages.is_empty() {
                    // Server has not received any requests yet
                    continue;
                }
                assert_eq!(messages.len(), 1);
                let msg = messages.remove(0);
                serving_source
                    .ack_messages(vec![format!("{}-0", msg.id)])
                    .await
                    .unwrap();
                break;
            }
        });

        let resp = client
            .post(format!(
                "https://localhost:{}/v1/process/async",
                settings.app_listen_port
            ))
            .json("test-payload")
            .send()
            .await?;

        assert!(resp.status().is_success());
        Ok(())
    }
}
