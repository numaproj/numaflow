use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::app::callback::state::State as CallbackState;
use crate::app::callback::store::redisstore::RedisConnection;
use crate::app::tracker::MessageGraph;

mod app;
pub mod config;

pub(crate) mod pipeline;

struct MessageWrapper {
    pub confirm_save: oneshot::Sender<()>,
    message: Message,
}

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

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Initialization error - {0}")]
    InitError(String),

    #[error("Failed to parse configuration - {0}")]
    ParseConfig(String),
    //
    // callback errors
    // TODO: store the ID too?
    #[error("IDNotFound Error - {0}")]
    IDNotFound(&'static str),

    #[error("SubGraphGenerator Error - {0}")]
    // subgraph generator errors
    SubGraphGenerator(String),

    #[error("StoreWrite Error - {0}")]
    // Store write errors
    StoreWrite(String),

    #[error("SubGraphNotFound Error - {0}")]
    // Sub Graph Not Found Error
    SubGraphNotFound(&'static str),

    #[error("SubGraphInvalidInput Error - {0}")]
    // Sub Graph Invalid Input Error
    SubGraphInvalidInput(String),

    #[error("StoreRead Error - {0}")]
    // Store read errors
    StoreRead(String),

    #[error("Metrics Error - {0}")]
    // Metrics errors
    MetricsServer(String),

    #[error("Connection Error - {0}")]
    Connection(String),

    #[error("Failed to receive message from channel. Actor task is terminated: {0:?}")]
    ActorTaskTerminated(oneshot::error::RecvError),

    #[error("{0}")]
    Other(String),
}

type Result<T> = std::result::Result<T, Error>;
pub type Settings = config::Settings;

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
        let app = app::AppState {
            message: messages_tx,
            settings,
            callback_state,
        };
        app::serve(app).await.unwrap();
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
        let (actor_tx, actor_rx) = mpsc::channel(10);
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
