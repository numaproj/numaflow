use std::collections::{BTreeMap, BTreeSet};
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
use pulsar::error::ConsumerError;
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

/// Whether an error from the consumer's message stream means the consumer has given up its
/// subscription. The client resubscribes on its own when the broker closes the consumer; if
/// that fails, it puts the connection or lookup error on the stream once and then delivers
/// nothing more, while still looking alive. A per-message error (for example a payload it
/// cannot decode) leaves the subscription in place.
fn subscription_abandoned(e: &pulsar::Error) -> bool {
    matches!(
        e,
        pulsar::Error::Connection(_) | pulsar::Error::ServiceDiscovery(_)
    )
}

/// What an ack or nack finds for an offset.
enum Pending {
    Live(PendingMessage),
    /// Read from a consumer that has since been replaced. The broker redelivers the message
    /// to the new consumer, so its ack or nack is dropped.
    Stale,
    Unknown,
}

/// Messages read but not yet acked or nacked.
#[derive(Default)]
struct PendingMessages {
    live: BTreeMap<PulsarOffset, PendingMessage>,
    stale: BTreeSet<PulsarOffset>,
}

impl PendingMessages {
    fn len(&self) -> usize {
        self.live.len()
    }

    fn insert(&mut self, offset: PulsarOffset, message: PendingMessage) {
        self.live.insert(offset, message);
    }

    fn take(&mut self, offset: PulsarOffset) -> Pending {
        if let Some(message) = self.live.remove(&offset) {
            return Pending::Live(message);
        }
        if self.stale.remove(&offset) {
            return Pending::Stale;
        }
        Pending::Unknown
    }

    /// Marks every live message stale, when the consumer that read them is replaced.
    fn retire_all(&mut self) {
        self.stale
            .extend(std::mem::take(&mut self.live).into_keys());
    }
}

/// How long to wait after a failed consumer rebuild before trying again. Acks are retried
/// every 100 ms, so without it each retry would look the topic up on the broker again.
const REBUILD_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Spaces out consumer rebuilds while they keep failing.
#[derive(Default)]
struct RebuildBackoff {
    last_failure: Option<Instant>,
}

impl RebuildBackoff {
    fn ready(&self, now: Instant) -> bool {
        self.last_failure
            .is_none_or(|failed_at| now.duration_since(failed_at) >= REBUILD_RETRY_INTERVAL)
    }

    fn failed(&mut self, now: Instant) {
        self.last_failure = Some(now);
    }

    fn succeeded(&mut self) {
        self.last_failure = None;
    }
}

struct ConsumerReaderActor {
    // Kept to build a new consumer when the current one stops.
    pulsar: Pulsar<TokioExecutor>,
    config: PulsarSourceConfig,
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    handler_rx: mpsc::Receiver<ConsumerActorMessage>,
    pending: PendingMessages,
    rebuild: RebuildBackoff,
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
        match &config.auth {
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

        let consumer = Self::build_consumer(&pulsar, &config).await?;

        tokio::spawn(async move {
            let mut consumer_actor = ConsumerReaderActor {
                pulsar,
                max_unack: config.max_unack,
                config,
                consumer,
                handler_rx,
                pending: PendingMessages::default(),
                rebuild: RebuildBackoff::default(),
                cancel_token,
            };
            consumer_actor.run().await;
        });
        Ok(())
    }

    async fn build_consumer(
        pulsar: &Pulsar<TokioExecutor>,
        config: &PulsarSourceConfig,
    ) -> Result<Consumer<Vec<u8>, TokioExecutor>> {
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

        Ok(builder
            .build()
            .await
            .map_err(|e| format!("Creating a Pulsar consumer: {e:?}"))?)
    }

    /// Replaces a consumer whose task has stopped. Without this the source would retry its
    /// acks forever and never read again. The broker redelivers the old consumer's unacked
    /// messages to the new one, so they are marked stale.
    async fn replace_consumer(&mut self) -> Result<()> {
        if !self.rebuild.ready(Instant::now()) {
            return Err(Error::Other(
                "Pulsar consumer stopped, waiting to create a new one".to_string(),
            ));
        }
        warn!(
            pending = self.pending.len(),
            "Pulsar consumer stopped, creating a new one; its unacked messages will be redelivered"
        );
        match Self::build_consumer(&self.pulsar, &self.config).await {
            Ok(consumer) => {
                self.consumer = consumer;
                self.pending.retire_all();
                self.rebuild.succeeded();
                Ok(())
            }
            Err(e) => {
                self.rebuild.failed(Instant::now());
                Err(e)
            }
        }
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
                // The consumer's stream ends only when its task has stopped.
                Ok(None) => {
                    if let Err(e) = self.replace_consumer().await {
                        return Some(Err(e));
                    }
                    break;
                }
                Err(e) => {
                    tracing::error!(?e, "Fetching message from Pulsar");
                    if subscription_abandoned(&e) {
                        if let Err(e) = self.replace_consumer().await {
                            return Some(Err(e));
                        }
                        break;
                    }
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
            self.pending.insert(
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

    // TODO: Identify the longest continuous batch and use cumulative_ack_with_id() to ack them all.
    async fn ack_messages(&mut self, offsets: Vec<PulsarOffset>) -> Result<()> {
        self.settle(offsets, true).await
    }

    async fn nack_messages(&mut self, offsets: Vec<PulsarOffset>) -> Result<()> {
        self.settle(offsets, false).await
    }

    async fn settle(&mut self, offsets: Vec<PulsarOffset>, ack: bool) -> Result<()> {
        let action = if ack { "ack" } else { "nack" };
        let total = offsets.len();
        let mut failed = Vec::new();
        for offset in offsets {
            let pending = match self.pending.take(offset) {
                Pending::Live(pending) => pending,
                Pending::Stale => continue,
                Pending::Unknown => {
                    warn!(%offset, "Received {action} request for unknown offset");
                    continue;
                }
            };
            let result = if ack {
                self.consumer
                    .ack_with_id(&pending.topic, pending.message_id.clone())
                    .await
            } else {
                self.consumer
                    .nack_with_id(&pending.topic, pending.message_id.clone())
                    .await
            };
            match result {
                Ok(()) => {}
                Err(ConsumerError::Closed) => {
                    self.pending.insert(offset, pending);
                    self.replace_consumer().await?;
                    // Stale now along with the rest, so this one is dropped too.
                    self.pending.take(offset);
                }
                Err(e) => {
                    self.pending.insert(offset, pending);
                    failed.push(format!("{offset}: {e}"));
                }
            }
        }
        if failed.is_empty() {
            return Ok(());
        }
        Err(Error::Other(format!(
            "Failed to {action} {} of {total} offsets: {}",
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
mod tests {
    use super::*;

    // Needs a Pulsar broker on localhost:6650.
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

    fn offset(entry_id: u64) -> PulsarOffset {
        PulsarOffset {
            ledger_id: 1,
            entry_id,
            partition: -1,
            batch_index: -1,
        }
    }

    fn message() -> PendingMessage {
        PendingMessage {
            topic: "persistent://public/default/test".to_string(),
            message_id: MessageIdData::default(),
        }
    }

    #[test]
    fn only_resubscribe_failures_abandon_the_subscription() {
        use pulsar::error::{ConnectionError, ServiceDiscoveryError};

        assert!(subscription_abandoned(&pulsar::Error::Connection(
            ConnectionError::Disconnected
        )));
        assert!(subscription_abandoned(&pulsar::Error::ServiceDiscovery(
            ServiceDiscoveryError::NotFound
        )));
        assert!(!subscription_abandoned(&pulsar::Error::Consumer(
            ConsumerError::MissingPayload("no payload".to_string())
        )));
    }

    #[test]
    fn rebuilds_wait_after_a_failure_until_the_interval_passes() {
        let mut backoff = RebuildBackoff::default();
        let start = Instant::now();
        assert!(backoff.ready(start));

        backoff.failed(start);
        assert!(!backoff.ready(start + Duration::from_millis(100)));
        assert!(backoff.ready(start + REBUILD_RETRY_INTERVAL));

        backoff.failed(start + REBUILD_RETRY_INTERVAL);
        backoff.succeeded();
        assert!(backoff.ready(start + REBUILD_RETRY_INTERVAL));
    }

    #[test]
    fn take_returns_a_live_message_once() {
        let mut pending = PendingMessages::default();
        pending.insert(offset(1), message());

        assert!(matches!(pending.take(offset(1)), Pending::Live(_)));
        assert!(matches!(pending.take(offset(1)), Pending::Unknown));
        assert!(matches!(pending.take(offset(2)), Pending::Unknown));
    }

    #[test]
    fn retired_messages_are_dropped_once_and_free_the_unack_budget() {
        let mut pending = PendingMessages::default();
        pending.insert(offset(1), message());
        pending.insert(offset(2), message());

        pending.retire_all();
        assert_eq!(pending.len(), 0);

        // A message read by the new consumer is live alongside the stale ones.
        pending.insert(offset(3), message());
        assert_eq!(pending.len(), 1);

        assert!(matches!(pending.take(offset(1)), Pending::Stale));
        assert!(matches!(pending.take(offset(1)), Pending::Unknown));
        assert!(matches!(pending.take(offset(2)), Pending::Stale));
        assert!(matches!(pending.take(offset(3)), Pending::Live(_)));
    }
}
