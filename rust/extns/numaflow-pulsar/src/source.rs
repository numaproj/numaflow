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

use numaflow_shared::redacted_url::RedactedUrl;
use pulsar::consumer::DeadLetterPolicy;
use secrecy::ExposeSecret;
use tokio_stream::StreamExt;
use tracing::{info, warn};

use crate::{Error, PulsarAuth, Result, TlsConfig};

#[derive(Debug, Clone, PartialEq)]
pub struct PulsarDeadLetterPolicy {
    pub topic: String,
    pub max_redelivery: usize,
}

#[derive(Debug, Clone)]
pub struct PulsarSourceConfig {
    pub pulsar_server_addr: RedactedUrl,
    pub topic: String,
    pub consumer_name: String,
    pub subscription: String,
    pub max_unack: usize,
    pub dead_letter_policy: Option<PulsarDeadLetterPolicy>,
    pub auth: Option<PulsarAuth>,
    /// TLS configuration for connecting to a Pulsar broker.
    ///
    /// Only server-authentication (one-way TLS) is supported: `ca_cert` lets the
    /// client trust a custom/self-signed broker CA via
    /// `PulsarBuilder::with_certificate_chain`. Mutual TLS (presenting a client
    /// certificate) is intentionally not supported here because the underlying
    /// `pulsar` crate's `PulsarBuilder` does not currently expose a way to set a
    /// client certificate/key for the broker connection - see
    /// https://docs.rs/pulsar/latest/pulsar/struct.PulsarBuilder.html.
    ///
    /// `ca_cert`: PEM-encoded custom CA certificate chain used to authenticate the Pulsar
    /// broker's TLS certificate. Required when the broker presents a
    /// certificate that isn't trusted by the process's default trust store
    /// (e.g. a self-signed/internal CA).
    ///
    /// `insecure_skip_verify`: When true, skip TLS server verification entirely. Prefer setting
    /// `ca_cert` instead; this is only for testing against untrusted setups.
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
    Pending {
        respond_to: oneshot::Sender<Option<usize>>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct EntryKey {
    ledger_id: u64,
    entry_id: u64,
    partition: i32,
}

impl From<&PulsarOffset> for EntryKey {
    fn from(offset: &PulsarOffset) -> Self {
        Self {
            ledger_id: offset.ledger_id,
            entry_id: offset.entry_id,
            partition: offset.partition,
        }
    }
}

struct BatchEntry {
    settled: Vec<bool>,
    remaining: usize,
    nacked: bool,
}

impl BatchEntry {
    fn is_settled(&self, offset: &PulsarOffset) -> Option<bool> {
        self.settled.get(offset.batch_index as usize).copied()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Settlement {
    Ack,
    Nack,
}

// pulsar-rs acks the whole batch on any ack, so we ack a batch only after all its messages are done.
#[derive(Default)]
struct PendingMessages {
    messages: BTreeMap<PulsarOffset, PendingMessage>,
    batches: HashMap<EntryKey, BatchEntry>,
}

impl PendingMessages {
    fn len(&self) -> usize {
        self.messages.len()
    }

    fn insert(&mut self, offset: PulsarOffset, message: PendingMessage, batch_size: Option<i32>) {
        if offset.batch_index >= 0
            && let Some(size) = batch_size.filter(|&size| size > 1)
        {
            self.batches
                .entry(EntryKey::from(&offset))
                .or_insert(BatchEntry {
                    settled: vec![false; size as usize],
                    remaining: size as usize,
                    nacked: false,
                });
        }
        self.messages.insert(offset, message);
    }

    fn plan(
        &self,
        offset: &PulsarOffset,
        settlement: Settlement,
    ) -> Result<Option<(&PendingMessage, Settlement)>> {
        let Some(message) = self.messages.get(offset) else {
            return Err(Error::UnknownOffset(*offset));
        };
        let Some(batch) = self.batches.get(&EntryKey::from(offset)) else {
            return Ok(Some((message, settlement)));
        };
        match batch.is_settled(offset) {
            None => return Ok(Some((message, settlement))),
            Some(true) => return Ok(None),
            Some(false) if batch.remaining > 1 => return Ok(None),
            Some(false) => {}
        }
        let settlement = if batch.nacked {
            Settlement::Nack
        } else {
            settlement
        };
        Ok(Some((message, settlement)))
    }

    fn settle(&mut self, offset: &PulsarOffset, settlement: Settlement) {
        if self.messages.remove(offset).is_none() {
            return;
        }
        let key = EntryKey::from(offset);
        let Some(batch) = self.batches.get_mut(&key) else {
            return;
        };
        let Some(settled) = batch.settled.get_mut(offset.batch_index as usize) else {
            return;
        };
        if *settled {
            return;
        }
        *settled = true;
        batch.remaining -= 1;
        batch.nacked |= settlement == Settlement::Nack;
        if batch.remaining == 0 {
            self.batches.remove(&key);
        }
    }
}

struct ConsumerReaderActor {
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    handler_rx: mpsc::Receiver<ConsumerActorMessage>,
    pending: PendingMessages,
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
            addr = ?config.pulsar_server_addr,
            "Pulsar connection details"
        );

        // NOTE: `with_allow_insecure_connection()` has historically had no effect against
        // rustls (see https://github.com/streamnative/pulsar-rs/blob/715411cb365932c379d4b5d0a8fde2ac46c54055/src/connection.rs#L912),
        // so `tls.insecure_skip_verify` below is best-effort. To trust a self-signed/custom
        // CA, prefer `tls.ca_cert`, which uses `with_certificate_chain()` - this adds the
        // provided CA to the client's trust root rather than disabling verification, and is
        // confirmed to work as of the `pulsar` crate version pinned in Cargo.toml.
        let mut pulsar = Pulsar::builder(
            config.pulsar_server_addr.expose_url().as_str(),
            TokioExecutor,
        );
        if let Some(tls) = &config.tls {
            if let Some(ca_cert) = &tls.ca_cert {
                pulsar = pulsar.with_certificate_chain(ca_cert.expose_secret().as_bytes().to_vec());
            }
            if tls.insecure_skip_verify {
                pulsar = pulsar.with_allow_insecure_connection(true);
            }
        }
        match config.auth {
            Some(PulsarAuth::JWT(token)) => {
                let auth_token = Authentication {
                    name: "token".into(),
                    data: token.expose_secret().into(),
                };
                pulsar = pulsar.with_auth(auth_token);
            }
            Some(PulsarAuth::HTTPBasic { username, password }) => {
                let auth_token = Authentication {
                    name: "basic".into(),
                    data: format!("{username}:{}", password.expose_secret()).into(),
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
                pending: PendingMessages::default(),
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
                let status = self.settle_messages(offsets, Settlement::Ack).await;
                let _ = respond_to.send(status);
            }
            ConsumerActorMessage::Nack {
                offsets,
                respond_to,
            } => {
                let status = self.settle_messages(offsets, Settlement::Nack).await;
                let _ = respond_to.send(status);
            }
            ConsumerActorMessage::Pending { respond_to } => {
                let _ = respond_to.send(self.pending_count().await);
            }
        }
    }

    async fn pending_count(&mut self) -> Option<usize> {
        match self.consumer.get_stats().await {
            Ok(stats) => stats
                .iter()
                .map(|s| s.msg_backlog.map(|n| n as usize))
                .sum(),
            Err(e) => {
                warn!(?e, "Fetching Pulsar consumer stats");
                None
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

        if self.pending.len() >= self.max_unack {
            return Some(Err(Error::AckPendingExceeded(self.pending.len())));
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
                    if !messages.is_empty() {
                        return Some(Ok(messages));
                    }
                    return Some(Err(Error::Pulsar(e)));
                }
            };
            let offset = PulsarOffset::from(msg.message_id());
            let publish_time = msg.metadata().publish_time;
            let event_time = msg
                .metadata()
                .event_time
                .and_then(|t| chrono::DateTime::from_timestamp_millis(t as i64))
                .or_else(|| chrono::DateTime::from_timestamp_millis(publish_time as i64))
                .unwrap_or_else(|| {
                    warn!(
                        event_time = msg.metadata().event_time,
                        publish_time, "Pulsar message has no valid timestamp, using current time"
                    );
                    Utc::now()
                });

            let headers = msg
                .metadata()
                .properties
                .iter()
                .map(|prop| (prop.key.clone(), prop.value.clone()))
                .collect();
            let key = msg.key().unwrap_or_else(|| "".to_string()); // FIXME: This is partition key. Identify the correct option. Also, there is a partition_key_b64_encoded boolean option in Pulsar metadata
            let message_id = msg.message_id().clone();

            // Physical topic (e.g. <topic>-partition-N) that ack/nack must route to.
            self.pending.insert(
                offset,
                PendingMessage {
                    topic: msg.topic,
                    message_id,
                },
                msg.message_id.batch_size,
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

    // TODO: Identify the longest continuous batch and use cumulative_ack_with_id() to ack them all.
    async fn settle_messages(
        &mut self,
        offsets: Vec<PulsarOffset>,
        settlement: Settlement,
    ) -> Result<()> {
        let verb = match settlement {
            Settlement::Ack => "ack",
            Settlement::Nack => "nack",
        };
        let total = offsets.len();
        let mut failed = Vec::new();
        for offset in offsets {
            let Ok(planned) = self.pending.plan(&offset, settlement) else {
                warn!(%offset, "Received {verb} request for unknown offset");
                continue;
            };
            if let Some((message, action)) = planned {
                let topic = &message.topic;
                let message_id = message.message_id.clone();
                let result = match action {
                    Settlement::Ack => self.consumer.ack_with_id(topic, message_id).await,
                    Settlement::Nack => self.consumer.nack_with_id(topic, message_id).await,
                };
                if let Err(e) = result {
                    failed.push(format!("{offset}: {e}"));
                    continue;
                }
            }
            self.pending.settle(&offset, settlement);
        }
        if failed.is_empty() {
            return Ok(());
        }
        Err(Error::Other(format!(
            "Failed to {verb} {} of {total} offsets: {}",
            failed.len(),
            failed.join("; ")
        )))
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
        let (tx, rx) = oneshot::channel();
        let _ = self
            .actor_tx
            .send(ConsumerActorMessage::Pending { respond_to: tx })
            .await;
        rx.await.ok().flatten()
    }

    pub fn partitions_vec(&self) -> Vec<u16> {
        vec![self.vertex_replica]
    }
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use super::*;

    fn offset(entry_id: u64, batch_index: i32) -> PulsarOffset {
        PulsarOffset {
            ledger_id: 7,
            entry_id,
            partition: -1,
            batch_index,
        }
    }

    fn message(offset: &PulsarOffset) -> PendingMessage {
        PendingMessage {
            topic: "persistent://public/default/topic".into(),
            message_id: MessageIdData {
                ledger_id: offset.ledger_id,
                entry_id: offset.entry_id,
                batch_index: (offset.batch_index >= 0).then_some(offset.batch_index),
                ..Default::default()
            },
        }
    }

    fn insert_batch(pending: &mut PendingMessages, entry_id: u64, size: i32) -> Vec<PulsarOffset> {
        (0..size)
            .map(|i| {
                let offset = offset(entry_id, i);
                pending.insert(offset, message(&offset), Some(size));
                offset
            })
            .collect()
    }

    fn planned(
        pending: &PendingMessages,
        offset: &PulsarOffset,
        s: Settlement,
    ) -> Option<Settlement> {
        pending
            .plan(offset, s)
            .expect("offset is pending")
            .map(|(_, action)| action)
    }

    #[test]
    fn unbatched_message_is_sent_immediately() {
        let mut pending = PendingMessages::default();
        let offset = offset(1, -1);
        pending.insert(offset, message(&offset), None);

        assert_eq!(
            planned(&pending, &offset, Settlement::Ack),
            Some(Settlement::Ack)
        );
        assert_eq!(
            planned(&pending, &offset, Settlement::Nack),
            Some(Settlement::Nack)
        );

        pending.settle(&offset, Settlement::Ack);
        assert_eq!(pending.len(), 0);
        assert!(pending.batches.is_empty());
    }

    #[test]
    fn batch_of_one_is_sent_immediately() {
        let mut pending = PendingMessages::default();
        let offsets = insert_batch(&mut pending, 1, 1);

        assert_eq!(
            planned(&pending, &offsets[0], Settlement::Ack),
            Some(Settlement::Ack)
        );
        assert!(pending.batches.is_empty());
    }

    #[test]
    fn batch_is_acked_once_every_message_is_acked() {
        let mut pending = PendingMessages::default();
        let offsets = insert_batch(&mut pending, 1, 3);

        for offset in [&offsets[2], &offsets[0]] {
            assert_eq!(planned(&pending, offset, Settlement::Ack), None);
            pending.settle(offset, Settlement::Ack);
        }
        assert_eq!(pending.len(), 1);

        assert_eq!(
            planned(&pending, &offsets[1], Settlement::Ack),
            Some(Settlement::Ack)
        );
        pending.settle(&offsets[1], Settlement::Ack);
        assert_eq!(pending.len(), 0);
        assert!(pending.batches.is_empty());
    }

    #[test]
    fn batch_is_nacked_if_any_message_is_nacked() {
        let mut pending = PendingMessages::default();
        let offsets = insert_batch(&mut pending, 1, 3);

        assert_eq!(planned(&pending, &offsets[0], Settlement::Nack), None);
        pending.settle(&offsets[0], Settlement::Nack);
        assert_eq!(planned(&pending, &offsets[1], Settlement::Ack), None);
        pending.settle(&offsets[1], Settlement::Ack);

        assert_eq!(
            planned(&pending, &offsets[2], Settlement::Ack),
            Some(Settlement::Nack)
        );
        pending.settle(&offsets[2], Settlement::Ack);
        assert!(pending.batches.is_empty());
    }

    #[test]
    fn batches_in_different_entries_are_independent() {
        let mut pending = PendingMessages::default();
        let first = insert_batch(&mut pending, 1, 2);
        let second = insert_batch(&mut pending, 2, 2);

        pending.settle(&first[0], Settlement::Ack);
        assert_eq!(planned(&pending, &second[0], Settlement::Ack), None);
        assert_eq!(
            planned(&pending, &first[1], Settlement::Ack),
            Some(Settlement::Ack)
        );
    }

    #[test]
    fn plan_does_not_change_state() {
        let mut pending = PendingMessages::default();
        let offsets = insert_batch(&mut pending, 1, 2);
        pending.settle(&offsets[0], Settlement::Ack);

        assert_eq!(
            planned(&pending, &offsets[1], Settlement::Ack),
            Some(Settlement::Ack)
        );
        assert_eq!(
            planned(&pending, &offsets[1], Settlement::Ack),
            Some(Settlement::Ack)
        );
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn redelivered_copy_does_not_reset_batch() {
        let mut pending = PendingMessages::default();
        let offsets = insert_batch(&mut pending, 1, 2);

        pending.insert(offsets[0], message(&offsets[0]), Some(2));
        pending.settle(&offsets[0], Settlement::Ack);

        assert_eq!(
            planned(&pending, &offsets[1], Settlement::Ack),
            Some(Settlement::Ack)
        );
    }

    #[test]
    fn redelivered_copy_of_settled_message_is_not_counted_twice() {
        let mut pending = PendingMessages::default();
        let offsets = insert_batch(&mut pending, 1, 4);
        pending.settle(&offsets[0], Settlement::Ack);

        pending.insert(offsets[0], message(&offsets[0]), Some(4));
        for offset in [&offsets[1], &offsets[2], &offsets[0]] {
            assert_eq!(planned(&pending, offset, Settlement::Ack), None);
            pending.settle(offset, Settlement::Ack);
        }
        assert_eq!(pending.len(), 1);

        assert_eq!(
            planned(&pending, &offsets[3], Settlement::Ack),
            Some(Settlement::Ack)
        );
        pending.settle(&offsets[3], Settlement::Ack);
        assert!(pending.batches.is_empty());
    }

    #[test]
    fn unknown_offset_is_an_error() {
        let pending = PendingMessages::default();
        let result = pending.plan(&offset(1, 0), Settlement::Ack);
        assert!(matches!(result, Err(Error::UnknownOffset(_))));
    }

    #[cfg(feature = "pulsar-tests")]
    #[tokio::test]
    async fn test_retry_skips_already_settled_offsets() {
        let id = uuid::Uuid::new_v4().simple();
        let topic = format!("persistent://public/default/pulsar_source_{id}");
        let source = PulsarSource::new(
            PulsarSourceConfig {
                pulsar_server_addr: numaflow_shared::redacted_url::RedactedUrl::parse(
                    "pulsar://localhost:6650",
                )
                .unwrap(),
                topic: topic.clone(),
                consumer_name: "test".into(),
                subscription: format!("source_subscription_{id}"),
                max_unack: 100,
                dead_letter_policy: None,
                auth: None,
                tls: None,
            },
            10,
            Duration::from_millis(500),
            0,
            CancellationToken::new(),
        )
        .await
        .unwrap();

        let client = Pulsar::builder("pulsar://localhost:6650", TokioExecutor)
            .build()
            .await
            .unwrap();
        let mut producer = client.producer().with_topic(&topic).build().await.unwrap();
        for i in 0..4 {
            producer
                .send_non_blocking(format!("message {i}"))
                .await
                .unwrap()
                .await
                .unwrap();
        }

        let mut offsets = Vec::new();
        for _ in 0..20 {
            if offsets.len() == 4 {
                break;
            }
            let batch = source.read_messages().await.unwrap().unwrap();
            offsets.extend(batch.into_iter().map(|m| m.offset));
        }
        let [a, b, c, d] = offsets.as_slice() else {
            panic!("expected 4 offsets, got {}", offsets.len());
        };

        source.ack_offsets(vec![*a]).await.unwrap();
        source.ack_offsets(vec![*a, *b]).await.unwrap();
        source.nack_offsets(vec![*c]).await.unwrap();
        source.nack_offsets(vec![*c, *d]).await.unwrap();
    }

    #[cfg(feature = "pulsar-tests")]
    #[tokio::test]
    async fn test_invalid_event_time_does_not_block_batch() {
        let id = uuid::Uuid::new_v4().simple();
        let topic = format!("persistent://public/default/pulsar_source_{id}");
        let config = PulsarSourceConfig {
            pulsar_server_addr: numaflow_shared::redacted_url::RedactedUrl::parse(
                "pulsar://localhost:6650",
            )
            .unwrap(),
            topic: topic.clone(),
            consumer_name: "test".into(),
            subscription: format!("source_subscription_{id}"),
            max_unack: 100,
            dead_letter_policy: None,
            auth: None,
            tls: None,
        };
        let new_source = || {
            PulsarSource::new(
                config.clone(),
                10,
                Duration::from_millis(500),
                0,
                CancellationToken::new(),
            )
        };
        let source = new_source().await.unwrap();

        let client = Pulsar::builder("pulsar://localhost:6650", TokioExecutor)
            .build()
            .await
            .unwrap();
        let mut producer = client
            .producer()
            .with_topic(&topic)
            .with_options(pulsar::producer::ProducerOptions {
                batch_size: Some(3),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let mut receipts = Vec::new();
        for i in 0..3u64 {
            let event_time = if i == 1 {
                9_000_000_000_000_000
            } else {
                1_700_000_000_000
            };
            let msg = pulsar::producer::Message {
                payload: format!("message {i}").into_bytes(),
                event_time: Some(event_time),
                ..Default::default()
            };
            receipts.push(producer.send_non_blocking(msg).await.unwrap());
        }
        for receipt in receipts {
            receipt.await.unwrap();
        }

        let mut read = Vec::new();
        for _ in 0..6 {
            read.extend(source.read_messages().await.unwrap().unwrap());
        }
        assert_eq!(read.len(), 3);
        assert_eq!(
            read.iter()
                .map(|m| m.offset.batch_index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );

        source
            .ack_offsets(read.iter().map(|m| m.offset).collect())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(source);
        tokio::time::sleep(Duration::from_secs(2)).await;

        let source = new_source().await.unwrap();
        let mut redelivered = Vec::new();
        for _ in 0..6 {
            redelivered.extend(source.read_messages().await.unwrap().unwrap());
        }
        assert_eq!(redelivered.len(), 0);
    }
}
