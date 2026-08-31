use std::collections::BTreeMap;
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use pulsar::Authentication;
use pulsar::{Consumer, ConsumerOptions, Pulsar, SubType, TokioExecutor, proto::MessageIdData};
use tokio::time::Instant;
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
    time,
};
use tokio_util::sync::CancellationToken;

use pulsar::consumer::DeadLetterPolicy;
use tokio_stream::StreamExt;
use tracing::{info, warn};

use crate::{Error, PulsarAuth, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct PulsarDeadLetterPolicy {
    pub topic: String,
    pub max_redelivery: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PulsarSourceConfig {
    pub pulsar_server_addr: String,
    pub topic: String,
    pub consumer_name: String,
    pub subscription: String,
    pub max_unack: usize,

    pub dead_letter_policy: Option<PulsarDeadLetterPolicy>,

    pub auth: Option<PulsarAuth>,
}

#[derive(Clone, Default)]
pub struct PulsarSourceState {
    message_ids: Arc<Mutex<BTreeMap<u64, MessageIdData>>>,
}

enum ConsumerActorMessage {
    Read {
        count: usize,
        timeout_at: Instant,
        respond_to: oneshot::Sender<Option<Result<Vec<PulsarMessage>>>>,
    },
    Ack {
        offsets: Vec<u64>,
        respond_to: oneshot::Sender<Result<()>>,
    },
    Nack {
        offsets: Vec<u64>,
        respond_to: oneshot::Sender<Result<()>>,
    },
}

pub struct PulsarMessage {
    pub key: String,
    pub payload: Bytes,
    pub offset: u64,
    pub event_time: DateTime<Utc>,
    pub headers: HashMap<String, String>,
}

struct ConsumerReaderActor {
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    handler_rx: mpsc::Receiver<ConsumerActorMessage>,
    state: PulsarSourceState,
    max_unack: usize,
    topic: String,
    cancel_token: CancellationToken,
}

async fn extract_pending_offsets(
    state: &PulsarSourceState,
    offsets: &[u64],
) -> Result<Vec<(u64, MessageIdData)>> {
    let mut extracted = Vec::with_capacity(offsets.len());
    let mut message_ids = state.message_ids.lock().await;
    for offset in offsets {
        match message_ids.remove(offset) {
            Some(msg_id) => extracted.push((*offset, msg_id)),
            None => {
                for (offset, msg_id) in extracted {
                    message_ids.insert(offset, msg_id);
                }
                return Err(Error::UnknownOffset(*offset));
            }
        }
    }
    Ok(extracted)
}

async fn restore_pending_offsets(
    state: &PulsarSourceState,
    entries: impl IntoIterator<Item = (u64, MessageIdData)>,
) {
    let mut message_ids = state.message_ids.lock().await;
    for (offset, msg_id) in entries {
        message_ids.insert(offset, msg_id);
    }
}

impl ConsumerReaderActor {
    async fn start(
        config: PulsarSourceConfig,
        handler_rx: mpsc::Receiver<ConsumerActorMessage>,
        cancel_token: CancellationToken,
        state: PulsarSourceState,
    ) -> Result<JoinHandle<()>> {
        info!(
            addr = &config.pulsar_server_addr,
            "Pulsar connection details"
        );

        // Rustls doesn't allow accepting self-signed certs: https://github.com/streamnative/pulsar-rs/blob/715411cb365932c379d4b5d0a8fde2ac46c54055/src/connection.rs#L912
        // The `with_allow_insecure_connection()` option has no effect
        let mut pulsar = Pulsar::builder(&config.pulsar_server_addr, TokioExecutor);
        match config.auth {
            Some(PulsarAuth::JWT(token)) => {
                let auth_token = Authentication {
                    name: "token".into(),
                    data: token.into(),
                };
                pulsar = pulsar.with_auth(auth_token);
            }
            Some(PulsarAuth::HTTPBasic { username, password }) => {
                let auth_token = Authentication {
                    name: "basic".into(),
                    data: format!("{username}:{password}").into(),
                };
                pulsar = pulsar.with_auth(auth_token);
            }
            None => info!("No authentication mechanism specified for Pulsar"),
        }

        let pulsar: Pulsar<_> = pulsar
            .build()
            .await
            .map_err(|e| format!("Creating Pulsar client connection: {e:?}"))?;

        let mut builder = pulsar
            .consumer()
            .with_topic(&config.topic)
            .with_consumer_name(&config.consumer_name)
            .with_subscription_type(SubType::Shared)
            .with_subscription(&config.subscription)
            .with_options(ConsumerOptions::default().durable(true));

        if let Some(policy) = &config.dead_letter_policy {
            builder = builder.with_dead_letter_policy(DeadLetterPolicy {
                max_redeliver_count: policy.max_redelivery,
                dead_letter_topic: policy.topic.clone(),
            });
        }

        let consumer = builder
            .build()
            .await
            .map_err(|e| format!("Creating a Pulsar consumer: {e:?}"))?;

        let actor_join = tokio::spawn(async move {
            let mut consumer_actor = ConsumerReaderActor {
                consumer,
                handler_rx,
                state,
                max_unack: config.max_unack,
                topic: config.topic,
                cancel_token,
            };
            consumer_actor.run().await;
        });
        Ok(actor_join)
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => return,
                msg = self.handler_rx.recv() => {
                    let Some(msg) = msg else {
                        return;
                    };
                    self.handle_message(msg).await;
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: ConsumerActorMessage) {
        match msg {
            ConsumerActorMessage::Read {
                count,
                timeout_at,
                respond_to,
            } => {
                let messages = self.get_messages(count, timeout_at).await;
                let _ = respond_to.send(messages);
            }
            ConsumerActorMessage::Ack {
                offsets,
                respond_to,
            } => {
                let status = self.ack_messages(offsets).await;
                let _ = respond_to.send(status);
            }
            ConsumerActorMessage::Nack {
                offsets,
                respond_to,
            } => {
                let status = self.nack_messages(offsets).await;
                let _ = respond_to.send(status);
            }
        }
    }

    async fn get_messages(
        &mut self,
        count: usize,
        timeout_at: Instant,
    ) -> Option<Result<Vec<PulsarMessage>>> {
        if self.cancel_token.is_cancelled() {
            return None;
        }

        let pending = self.state.message_ids.lock().await.len();
        if pending >= self.max_unack {
            return Some(Err(Error::AckPendingExceeded(pending)));
        }
        let mut messages = vec![];
        for _ in 0..count {
            let remaining_time = timeout_at - Instant::now();
            let Ok(msg) = time::timeout(remaining_time, self.consumer.try_next()).await else {
                return Some(Ok(messages));
            };
            let msg = match msg {
                Ok(Some(msg)) => msg,
                Ok(None) => {
                    if messages.is_empty() {
                        return Some(Err(Error::Other(
                            "Pulsar consumer stream closed unexpectedly".into(),
                        )));
                    }
                    break;
                }
                Err(e) => {
                    tracing::error!(?e, "Fetching message from Pulsar");
                    let remaining_time = timeout_at - Instant::now();
                    if remaining_time.as_millis() >= 100 {
                        time::sleep(Duration::from_millis(50)).await; // FIXME: add error metrics. Also, respect the timeout
                        continue;
                    }
                    return Some(Err(Error::Pulsar(e)));
                }
            };
            let offset = msg.message_id().entry_id;
            let event_time = msg
                .metadata()
                .event_time
                .unwrap_or(msg.metadata().publish_time);
            let Some(event_time) = chrono::DateTime::from_timestamp_millis(event_time as i64)
            else {
                // This should never happen
                tracing::error!(
                    event_time = msg.metadata().event_time,
                    publish_time = msg.metadata().publish_time,
                    parsed_event_time = event_time,
                    "Pulsar message contains invalid event_time/publish_time timestamp"
                );
                continue;
                //FIXME: NACK the message
            };

            let mut message_ids = self.state.message_ids.lock().await;
            if message_ids.contains_key(&offset) {
                warn!(
                    offset,
                    "Skipping redelivered Pulsar message that is already being processed"
                );
                continue;
            }
            message_ids.insert(offset, msg.message_id().clone());
            drop(message_ids);

            let headers = msg
                .metadata()
                .properties
                .iter()
                .map(|prop| (prop.key.clone(), prop.value.clone()))
                .collect();

            messages.push(PulsarMessage {
                key: msg.key().unwrap_or_else(|| "".to_string()), // FIXME: This is partition key. Identify the correct option. Also, there is a partition_key_b64_encoded boolean option in Pulsar metadata
                payload: msg.payload.data.into(),
                offset,
                event_time,
                headers,
            });

            // stop reading as soon as we hit max_unack
            if messages.len() >= self.max_unack {
                return Some(Ok(messages));
            }
        }
        Some(Ok(messages))
    }

    // TODO: Identify the longest continuous batch and use cumulative_ack_with_id() to ack them all.
    async fn ack_messages(&mut self, offsets: Vec<u64>) -> Result<()> {
        let extracted = extract_pending_offsets(&self.state, &offsets).await?;

        for (index, (_offset, msg_id)) in extracted.iter().enumerate() {
            if let Err(e) = self.consumer.ack_with_id(&self.topic, msg_id.clone()).await {
                restore_pending_offsets(&self.state, extracted.into_iter().skip(index)).await;
                return Err(Error::Pulsar(e.into()));
            }
        }
        Ok(())
    }

    async fn nack_messages(&mut self, offsets: Vec<u64>) -> Result<()> {
        let extracted = extract_pending_offsets(&self.state, &offsets).await?;

        for (index, (_offset, msg_id)) in extracted.iter().enumerate() {
            if let Err(e) = self
                .consumer
                .nack_with_id(&self.topic, msg_id.clone())
                .await
            {
                restore_pending_offsets(&self.state, extracted.into_iter().skip(index)).await;
                return Err(Error::Pulsar(e.into()));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod state_tests {
    use super::{Error, PulsarSourceState, extract_pending_offsets, restore_pending_offsets};
    use pulsar::proto::MessageIdData;

    fn message_id(entry_id: u64) -> MessageIdData {
        MessageIdData {
            ledger_id: 1,
            entry_id,
            ..Default::default()
        }
    }

    async fn insert_offset(state: &PulsarSourceState, offset: u64) {
        state
            .message_ids
            .lock()
            .await
            .insert(offset, message_id(offset));
    }

    #[tokio::test]
    async fn source_state_preserves_message_ids_across_generations() {
        let state = PulsarSourceState::default();
        let next_generation = state.clone();
        insert_offset(&state, 7).await;

        assert_eq!(
            next_generation
                .message_ids
                .lock()
                .await
                .get(&7)
                .map(|id| id.entry_id),
            Some(7)
        );
    }

    #[tokio::test]
    async fn extract_pending_offsets_restores_on_unknown_offset() {
        let state = PulsarSourceState::default();
        insert_offset(&state, 1).await;
        insert_offset(&state, 3).await;

        let result = extract_pending_offsets(&state, &[1, 2, 3]).await;

        assert!(matches!(result, Err(Error::UnknownOffset(2))));
        let message_ids = state.message_ids.lock().await;
        assert!(message_ids.contains_key(&1));
        assert!(!message_ids.contains_key(&2));
        assert!(message_ids.contains_key(&3));
    }

    #[tokio::test]
    async fn extract_and_restore_pending_offsets_round_trip() {
        let state = PulsarSourceState::default();
        insert_offset(&state, 10).await;
        insert_offset(&state, 11).await;
        insert_offset(&state, 12).await;

        let extracted = extract_pending_offsets(&state, &[10, 11, 12])
            .await
            .expect("all offsets should exist");
        assert!(state.message_ids.lock().await.is_empty());

        restore_pending_offsets(&state, extracted.into_iter().skip(1)).await;

        let message_ids = state.message_ids.lock().await;
        assert!(!message_ids.contains_key(&10));
        assert!(message_ids.contains_key(&11));
        assert!(message_ids.contains_key(&12));
    }
}

#[derive(Clone)]
pub struct PulsarSource {
    inner: Arc<PulsarSourceInner>,
    batch_size: usize,
    /// timeout for each batch read request
    timeout: Duration,
    vertex_replica: u16,
}

struct PulsarSourceInner {
    actor_tx: mpsc::Sender<ConsumerActorMessage>,
    cancel_token: CancellationToken,
    actor_join: Mutex<Option<JoinHandle<()>>>,
}

impl PulsarSource {
    pub async fn new(
        config: PulsarSourceConfig,
        batch_size: usize,
        timeout: Duration,
        vertex_replica: u16,
        cancel_token: CancellationToken,
    ) -> Result<Self> {
        Self::new_with_state(
            config,
            batch_size,
            timeout,
            vertex_replica,
            cancel_token,
            PulsarSourceState::default(),
        )
        .await
    }

    pub async fn new_with_state(
        config: PulsarSourceConfig,
        batch_size: usize,
        timeout: Duration,
        vertex_replica: u16,
        cancel_token: CancellationToken,
        state: PulsarSourceState,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(10);
        let generation_token = cancel_token.child_token();
        let actor_join =
            ConsumerReaderActor::start(config, rx, generation_token.clone(), state).await?;
        Ok(Self {
            inner: Arc::new(PulsarSourceInner {
                actor_tx: tx,
                cancel_token: generation_token,
                actor_join: Mutex::new(Some(actor_join)),
            }),
            batch_size,
            timeout,
            vertex_replica,
        })
    }
}

impl PulsarSource {
    pub async fn read_messages(&self) -> Option<Result<Vec<PulsarMessage>>> {
        let (tx, rx) = oneshot::channel();
        let msg = ConsumerActorMessage::Read {
            count: self.batch_size,
            timeout_at: Instant::now() + self.timeout,
            respond_to: tx,
        };
        let _ = self.inner.actor_tx.send(msg).await;
        rx.await
            .map_err(Error::ActorTaskTerminated)
            .unwrap_or_else(|e| Some(Err(e)))
    }

    pub async fn ack_offsets(&self, offsets: Vec<u64>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .inner
            .actor_tx
            .send(ConsumerActorMessage::Ack {
                offsets,
                respond_to: tx,
            })
            .await;
        rx.await.map_err(Error::ActorTaskTerminated)?
    }

    pub async fn nack_offsets(&self, offsets: Vec<u64>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .inner
            .actor_tx
            .send(ConsumerActorMessage::Nack {
                offsets,
                respond_to: tx,
            })
            .await;
        rx.await.map_err(Error::ActorTaskTerminated)?
    }

    pub async fn pending_count(&self) -> Option<usize> {
        None
    }

    pub fn partitions_vec(&self) -> Vec<u16> {
        vec![self.vertex_replica]
    }

    /// Cancels this consumer generation and waits for its actor to drop the Pulsar consumer.
    pub async fn shutdown(&self) {
        self.inner.cancel_token.cancel();
        if let Some(actor_join) = self.inner.actor_join.lock().await.take()
            && let Err(error) = actor_join.await
        {
            warn!(?error, "Pulsar source actor failed while shutting down");
        }
    }
}
