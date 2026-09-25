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
use tracing::info;

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
        offsets: Vec<String>,
        respond_to: oneshot::Sender<Result<()>>,
    },
    Nack {
        offsets: Vec<String>,
        respond_to: oneshot::Sender<Result<()>>,
    },
}

pub struct PulsarMessage {
    pub key: String,
    pub payload: Bytes,
    pub offset: String,
    pub event_time: DateTime<Utc>,
    pub headers: HashMap<String, String>,
}

struct PendingMessage {
    topic: String,
    message_id: MessageIdData,
}

struct ConsumerReaderActor {
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    handler_rx: mpsc::Receiver<ConsumerActorMessage>,
    pending_messages: BTreeMap<String, PendingMessage>,
    max_unack: usize,
    cancel_token: CancellationToken,
}

// A Pulsar entry ID is only unique within a ledger and partition. Keep the actual topic in the
// opaque offset as well: partitioned-topic consumers need it to route acknowledgements to the
// correct internal consumer.
fn message_offset(topic: &str, message_id: &MessageIdData) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        topic,
        message_id.partition.unwrap_or(-1),
        message_id.ledger_id,
        message_id.entry_id,
        message_id.batch_index.unwrap_or(-1),
    )
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
                pending_messages: BTreeMap::new(),
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

        if self.pending_messages.len() >= self.max_unack {
            return Some(Err(Error::AckPendingExceeded(self.pending_messages.len())));
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
            let offset = message_offset(&msg.topic, msg.message_id());
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

            self.pending_messages.insert(
                offset.clone(),
                PendingMessage {
                    topic: msg.topic.clone(),
                    message_id: msg.message_id().clone(),
                },
            );
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
    async fn ack_messages(&mut self, offsets: Vec<String>) -> Result<()> {
        for offset in offsets {
            let pending_message = self.pending_messages.remove(&offset);

            let Some(pending_message) = pending_message else {
                return Err(Error::UnknownOffset(offset));
            };

            let Err(e) = self
                .consumer
                .ack_with_id(&pending_message.topic, pending_message.message_id.clone())
                .await
            else {
                continue;
            };
            // Insert offset back
            self.pending_messages.insert(offset, pending_message);
            return Err(Error::Pulsar(e.into()));
        }
        Ok(())
    }

    async fn nack_messages(&mut self, offsets: Vec<String>) -> Result<()> {
        for offset in offsets {
            let pending_message = self.pending_messages.remove(&offset);

            let Some(pending_message) = pending_message else {
                return Err(Error::UnknownOffset(offset));
            };

            let Err(e) = self
                .consumer
                .nack_with_id(&pending_message.topic, pending_message.message_id.clone())
                .await
            else {
                continue;
            };
            // Insert offset back
            self.pending_messages.insert(offset, pending_message);
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

    pub async fn ack_offsets(&self, offsets: Vec<String>) -> Result<()> {
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

    pub async fn nack_offsets(&self, offsets: Vec<String>) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn message_id(
        partition: i32,
        ledger_id: u64,
        entry_id: u64,
        batch_index: i32,
    ) -> MessageIdData {
        MessageIdData {
            partition: Some(partition),
            ledger_id,
            entry_id,
            batch_index: Some(batch_index),
            ..Default::default()
        }
    }

    #[test]
    fn message_offset_is_stable_and_unique() {
        let topic = "persistent://public/default/events-partition-0";
        let id = message_id(0, 10, 20, 0);

        assert_eq!(
            message_offset(topic, &id),
            "persistent://public/default/events-partition-0:0:10:20:0"
        );
        assert_eq!(message_offset(topic, &id), message_offset(topic, &id));

        assert_ne!(
            message_offset(topic, &id),
            message_offset("persistent://public/default/other-events-partition-0", &id,)
        );
        assert_ne!(
            message_offset(topic, &id),
            message_offset(topic, &message_id(1, 10, 20, 0))
        );
        assert_ne!(
            message_offset(topic, &id),
            message_offset(topic, &message_id(0, 11, 20, 0))
        );
        assert_ne!(
            message_offset(topic, &id),
            message_offset(topic, &message_id(0, 10, 21, 0))
        );
        assert_ne!(
            message_offset(topic, &id),
            message_offset(topic, &message_id(0, 10, 20, 1))
        );

        let id_without_optional_fields = MessageIdData {
            ledger_id: 10,
            entry_id: 20,
            ..Default::default()
        };
        assert_eq!(
            message_offset(topic, &id_without_optional_fields),
            "persistent://public/default/events-partition-0:-1:10:20:-1"
        );
    }
}
