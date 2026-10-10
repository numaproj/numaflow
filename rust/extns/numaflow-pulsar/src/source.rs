use std::collections::BTreeMap;
use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use pulsar::Authentication;
use pulsar::{Consumer, ConsumerOptions, Pulsar, SubType, TokioExecutor, proto::MessageIdData};
use tokio::time::Instant;
use tokio::{
    sync::{mpsc, oneshot},
    time,
};
use tokio_util::sync::CancellationToken;

use pulsar::consumer::DeadLetterPolicy;
use tokio_stream::StreamExt;
use tracing::{info, warn};

use crate::{Error, PulsarAuth, Result, TlsConfig};

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
    /// TLS configuration, e.g. to trust a custom/self-signed broker CA.
    pub tls: Option<TlsConfig>,
}

enum ConsumerActorMessage {
    Read {
        count: usize,
        timeout_at: Instant,
        respond_to: oneshot::Sender<Option<Result<Vec<PulsarMessage>>>>,
    },
    Ack {
        offsets: Vec<PulsarOffset>,
        respond_to: oneshot::Sender<Result<()>>,
    },
    Nack {
        offsets: Vec<PulsarOffset>,
        respond_to: oneshot::Sender<Result<()>>,
    },
}

pub struct PulsarMessage {
    pub key: String,
    pub payload: Bytes,
    pub offset: PulsarOffset,
    pub event_time: DateTime<Utc>,
    pub headers: HashMap<String, String>,
}

/// Pulsar's canonical message identity; mirrors the Java client's
/// `BatchMessageIdImpl` (`ledgerId:entryId:partitionIndex:batchIndex`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PulsarOffset {
    pub ledger_id: u64,
    pub entry_id: u64,
    pub partition: i32,
    pub batch_index: i32,
}

impl From<&MessageIdData> for PulsarOffset {
    fn from(id: &MessageIdData) -> Self {
        Self {
            ledger_id: id.ledger_id,
            entry_id: id.entry_id,
            partition: id.partition.unwrap_or(-1),
            batch_index: id.batch_index.unwrap_or(-1),
        }
    }
}

impl std::fmt::Display for PulsarOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}",
            self.ledger_id, self.entry_id, self.partition, self.batch_index
        )
    }
}

impl std::str::FromStr for PulsarOffset {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let invalid = || {
            Error::Other(format!(
                "invalid Pulsar offset {s:?}, expected <ledger_id>:<entry_id>:<partition>:<batch_index>"
            ))
        };
        let parts: Vec<&str> = s.split(':').collect();
        let [ledger_id, entry_id, partition, batch_index] = parts.as_slice() else {
            return Err(invalid());
        };
        Ok(Self {
            ledger_id: ledger_id.parse().map_err(|_| invalid())?,
            entry_id: entry_id.parse().map_err(|_| invalid())?,
            partition: partition.parse().map_err(|_| invalid())?,
            batch_index: batch_index.parse().map_err(|_| invalid())?,
        })
    }
}

/// A message pending ack/nack: its physical topic and Pulsar's own id for it.
struct PendingMessage {
    topic: String,
    message_id: MessageIdData,
}

struct ConsumerReaderActor {
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    handler_rx: mpsc::Receiver<ConsumerActorMessage>,
    message_ids: BTreeMap<PulsarOffset, PendingMessage>,
    max_unack: usize,
    cancel_token: CancellationToken,
}

impl ConsumerReaderActor {
    async fn start(
        config: PulsarSourceConfig,
        handler_rx: mpsc::Receiver<ConsumerActorMessage>,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        info!(
            addr = &config.pulsar_server_addr,
            "Pulsar connection details"
        );

        // NOTE: `with_allow_insecure_connection()` has historically had no effect against
        // rustls (see https://github.com/streamnative/pulsar-rs/blob/715411cb365932c379d4b5d0a8fde2ac46c54055/src/connection.rs#L912),
        // so `tls.insecure_skip_verify` below is best-effort. To trust a self-signed/custom
        // CA, prefer `tls.ca_cert`, which uses `with_certificate_chain()` - this adds the
        // provided CA to the client's trust root rather than disabling verification, and is
        // confirmed to work as of the `pulsar` crate version pinned in Cargo.toml.
        let mut pulsar = Pulsar::builder(&config.pulsar_server_addr, TokioExecutor);
        if let Some(tls) = &config.tls {
            if let Some(ca_cert) = &tls.ca_cert {
                pulsar = pulsar.with_certificate_chain(ca_cert.clone());
            }
            if tls.insecure_skip_verify {
                pulsar = pulsar.with_allow_insecure_connection(true);
            }
        }
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

        tokio::spawn(async move {
            let mut consumer_actor = ConsumerReaderActor {
                consumer,
                handler_rx,
                message_ids: BTreeMap::new(),
                max_unack: config.max_unack,
                cancel_token,
            };
            consumer_actor.run().await;
        });
        Ok(())
    }

    async fn run(&mut self) {
        while let Some(msg) = self.handler_rx.recv().await {
            self.handle_message(msg).await;
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

        if self.message_ids.len() >= self.max_unack {
            return Some(Err(Error::AckPendingExceeded(self.message_ids.len())));
        }
        let mut messages = vec![];
        for _ in 0..count {
            let remaining_time = timeout_at - Instant::now();
            let Ok(msg) = time::timeout(remaining_time, self.consumer.try_next()).await else {
                return Some(Ok(messages));
            };
            let msg = match msg {
                Ok(Some(msg)) => msg,
                Ok(None) => break,
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
            let offset = PulsarOffset::from(msg.message_id());
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

            let headers = msg
                .metadata()
                .properties
                .iter()
                .map(|prop| (prop.key.clone(), prop.value.clone()))
                .collect();
            let key = msg.key().unwrap_or_else(|| "".to_string()); // FIXME: This is partition key. Identify the correct option. Also, there is a partition_key_b64_encoded boolean option in Pulsar metadata
            let message_id = msg.message_id().clone();

            // Physical topic (e.g. <topic>-partition-N) that ack/nack must route to.
            self.message_ids.insert(
                offset,
                PendingMessage {
                    topic: msg.topic,
                    message_id,
                },
            );

            messages.push(PulsarMessage {
                key,
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

    /// Acks the given offsets. If an ack fails, the failed offset and the ones after it stay
    /// pending and the error is returned so that the caller can retry the batch. Offsets that are
    /// no longer pending (e.g., acked by an earlier attempt of the same batch) are skipped.
    // TODO: Identify the longest continuous batch and use cumulative_ack_with_id() to ack them all.
    async fn ack_messages(&mut self, offsets: Vec<PulsarOffset>) -> Result<()> {
        for offset in offsets {
            let Some(pending) = self.message_ids.remove(&offset) else {
                warn!(%offset, "Received ACK request for unknown offset");
                continue;
            };

            let Err(e) = self
                .consumer
                .ack_with_id(&pending.topic, pending.message_id.clone())
                .await
            else {
                continue;
            };
            // Insert offset back so that the retry can ack it.
            self.message_ids.insert(offset, pending);
            return Err(Error::Pulsar(e.into()));
        }
        Ok(())
    }

    /// Nacks the given offsets, with the same partial failure semantics as [Self::ack_messages].
    async fn nack_messages(&mut self, offsets: Vec<PulsarOffset>) -> Result<()> {
        for offset in offsets {
            let Some(pending) = self.message_ids.remove(&offset) else {
                warn!(%offset, "Received NACK request for unknown offset");
                continue;
            };

            let Err(e) = self
                .consumer
                .nack_with_id(&pending.topic, pending.message_id.clone())
                .await
            else {
                continue;
            };
            // Insert offset back so that the retry can nack it.
            self.message_ids.insert(offset, pending);
            return Err(Error::Pulsar(e.into()));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct PulsarSource {
    batch_size: usize,
    /// timeout for each batch read request
    timeout: Duration,
    actor_tx: mpsc::Sender<ConsumerActorMessage>,
    vertex_replica: u16,
}

impl PulsarSource {
    pub async fn new(
        config: PulsarSourceConfig,
        batch_size: usize,
        timeout: Duration,
        vertex_replica: u16,
        cancel_token: CancellationToken,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(10);
        ConsumerReaderActor::start(config, rx, cancel_token).await?;
        Ok(Self {
            actor_tx: tx,
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
        let _ = self.actor_tx.send(msg).await;
        rx.await
            .map_err(Error::ActorTaskTerminated)
            .unwrap_or_else(|e| Some(Err(e)))
    }

    pub async fn ack_offsets(&self, offsets: Vec<PulsarOffset>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .actor_tx
            .send(ConsumerActorMessage::Ack {
                offsets,
                respond_to: tx,
            })
            .await;
        rx.await.map_err(Error::ActorTaskTerminated)?
    }

    pub async fn nack_offsets(&self, offsets: Vec<PulsarOffset>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self
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
}
