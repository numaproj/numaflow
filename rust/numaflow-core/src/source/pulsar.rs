use std::sync::Arc;
use std::time::Duration;

use numaflow_pulsar::source::{PulsarMessage, PulsarOffset, PulsarSource, PulsarSourceConfig};

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::error::Error;
use crate::message::{Message, MessageID, NackOffset, Offset, StringOffset};
use crate::metadata::Metadata;
use crate::source;

impl TryFrom<PulsarMessage> for Message {
    type Error = Error;

    fn try_from(message: PulsarMessage) -> crate::Result<Self> {
        let offset = Offset::String(StringOffset::new(
            message.offset.to_string(),
            *get_vertex_replica(),
        ));

        Ok(Message {
            typ: Default::default(),
            keys: Arc::from(vec![message.key]),
            tags: None,
            value: message.payload,
            offset: offset.clone(),
            event_time: message.event_time,
            watermark: None,
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: offset.to_string().into(),
                index: 0,
            },
            headers: Arc::new(message.headers),
            // Set default metadata so that metadata is always present.
            metadata: Some(Arc::new(Metadata::default())),
            is_late: false,
            nack_options: None,
        })
    }
}

impl From<numaflow_pulsar::Error> for Error {
    fn from(value: numaflow_pulsar::Error) -> Self {
        match value {
            numaflow_pulsar::Error::Pulsar(e) => Error::Source(e.to_string()),
            numaflow_pulsar::Error::UnknownOffset(_) => Error::Source(value.to_string()),
            numaflow_pulsar::Error::AckPendingExceeded(pending) => {
                Error::AckPendingExceeded(pending)
            }
            numaflow_pulsar::Error::ActorTaskTerminated(_) => {
                Error::ActorPatternRecv(value.to_string())
            }
            numaflow_pulsar::Error::Other(e) => Error::Source(e),
        }
    }
}

pub(crate) async fn new_pulsar_source(
    cfg: PulsarSourceConfig,
    batch_size: usize,
    timeout: Duration,
    vertex_replica: u16,
    cancel_token: tokio_util::sync::CancellationToken,
) -> crate::Result<PulsarSource> {
    Ok(PulsarSource::new(cfg, batch_size, timeout, vertex_replica, cancel_token).await?)
}

impl source::SourceReader for PulsarSource {
    fn name(&self) -> &'static str {
        "Pulsar"
    }

    async fn read(&mut self) -> Option<crate::Result<Vec<Message>>> {
        match self.read_messages().await {
            Some(Ok(messages)) => {
                let result: crate::Result<Vec<Message>> =
                    messages.into_iter().map(|msg| msg.try_into()).collect();
                Some(result)
            }
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }

    async fn partitions(&mut self) -> crate::error::Result<source::SourcePartitions> {
        let partitions = self.partitions_vec();
        // For Pulsar with shared subscriptions, there's no partition assignment like Kafka.
        // Each vertex replica is treated as a separate "partition" for watermark purposes.
        // total_partitions is None because we don't know the total replica count at runtime.
        Ok(source::SourcePartitions::new(partitions, None))
    }
}

/// Parses a Pulsar `Offset::String` back into a `PulsarOffset`.
fn to_pulsar_offset(offset: &Offset) -> crate::Result<PulsarOffset> {
    let Offset::String(string_offset) = offset else {
        return Err(Error::Source(format!(
            "Expected Offset::String type for Pulsar. offset={offset:?}"
        )));
    };
    let text = String::from_utf8_lossy(&string_offset.offset);
    text.parse()
        .map_err(|e| Error::Source(format!("Invalid Pulsar offset. offset={text}, error={e:?}")))
}

impl source::SourceAcker for PulsarSource {
    async fn ack(&mut self, offsets: Vec<Offset>) -> crate::error::Result<()> {
        let mut pulsar_offsets = Vec::with_capacity(offsets.len());
        for offset in &offsets {
            pulsar_offsets.push(to_pulsar_offset(offset)?);
        }
        self.ack_offsets(pulsar_offsets).await.map_err(Into::into)
    }

    async fn nack(&mut self, offsets: Vec<NackOffset>) -> crate::error::Result<()> {
        let mut pulsar_offsets = Vec::with_capacity(offsets.len());
        let mut logged = false;

        for offset in &offsets {
            if !logged && offset.option.is_some() {
                tracing::error!(
                    "Pulsar does not support per-message nack options; ignoring supplied options."
                );
                logged = true;
            }

            pulsar_offsets.push(to_pulsar_offset(&offset.offset)?);
        }

        self.nack_offsets(pulsar_offsets).await.map_err(Into::into)
    }
}

impl source::LagReader for PulsarSource {
    async fn pending(&mut self) -> crate::error::Result<Option<usize>> {
        Ok(self.pending_count().await)
    }
}

#[cfg(feature = "pulsar-tests")]
#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::str::FromStr;

    use numaflow_pulsar::source::PulsarOffset;
    use pulsar::{Pulsar, TokioExecutor, producer, proto};
    use source::{LagReader, SourceAcker, SourceReader};
    use tokio::time::Instant;

    use super::*;
    use crate::shared::test_utils::server::get_rand_str;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    const PULSAR_ADDR: &str = "pulsar://localhost:6650";
    const ADMIN_ADDR: &str = "http://localhost:8080";

    fn full_topic(local: &str) -> String {
        format!("persistent://public/default/{local}")
    }

    /// Builds a `PulsarSource` for the given topic/subscription, with no DLQ/auth/tls.
    async fn new_test_source(
        topic: &str,
        subscription: &str,
        batch_size: usize,
        timeout: Duration,
    ) -> PulsarSource {
        let cfg = PulsarSourceConfig {
            pulsar_server_addr: PULSAR_ADDR.into(),
            topic: topic.into(),
            consumer_name: "test".into(),
            subscription: subscription.into(),
            max_unack: 100,
            dead_letter_policy: None,
            auth: None,
            tls: None,
        };
        new_pulsar_source(
            cfg,
            batch_size,
            timeout,
            0,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("failed to create test Pulsar source")
    }

    /// Creates a partitioned topic via admin REST. Asserts a 2xx response.
    async fn create_partitioned_topic(local: &str, partitions: u32) {
        let resp = reqwest::Client::new()
            .put(format!(
                "{ADMIN_ADDR}/admin/v2/persistent/public/default/{local}/partitions"
            ))
            .json(&partitions)
            .send()
            .await
            .expect("admin create-partitioned-topic request failed");
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            panic!("create partitioned topic failed: {status} {body}");
        }
    }

    /// Creates a non-partitioned persistent topic via admin REST. Asserts a 2xx response.
    async fn create_non_partitioned_topic(local: &str) {
        let resp = reqwest::Client::new()
            .put(format!(
                "{ADMIN_ADDR}/admin/v2/persistent/public/default/{local}"
            ))
            .send()
            .await
            .expect("admin create-topic request failed");
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            panic!("create topic failed: {status} {body}");
        }
    }

    /// Creates a durable subscription via admin REST, with no consumer attached.
    async fn create_subscription(local: &str, subscription: &str) {
        let resp = reqwest::Client::new()
            .put(format!(
                "{ADMIN_ADDR}/admin/v2/persistent/public/default/{local}/subscription/{subscription}"
            ))
            .send()
            .await
            .expect("admin create-subscription request failed");
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            panic!("create subscription failed: {status} {body}");
        }
    }

    /// Unloads the topic via admin REST, forcing a ledger rollover on the next write.
    async fn unload_topic(local: &str) {
        let resp = reqwest::Client::new()
            .put(format!(
                "{ADMIN_ADDR}/admin/v2/persistent/public/default/{local}/unload"
            ))
            .send()
            .await
            .expect("admin unload request failed");
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            panic!("unload topic failed: {status} {body}");
        }
    }

    /// Sends `payloads` to `topic` and awaits every receipt; `batch` sends them as one Pulsar batch.
    /// Producer creation is retried (bounded) since it can race a topic reload after an unload.
    async fn produce(topic: &str, payloads: Vec<String>, batch: bool) {
        let client = Pulsar::builder(PULSAR_ADDR, TokioExecutor)
            .build()
            .await
            .expect("failed to build Pulsar client");

        let mut builder = client.producer().with_topic(topic);
        if batch {
            builder = builder.with_options(producer::ProducerOptions {
                batch_size: Some(payloads.len() as u32),
                ..Default::default()
            });
        }

        let mut producer = None;
        let mut last_err = None;
        for _ in 0..30 {
            match builder.clone().build().await {
                Ok(p) => {
                    producer = Some(p);
                    break;
                }
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        let mut producer =
            producer.unwrap_or_else(|| panic!("failed to build Pulsar producer: {last_err:?}"));

        let mut receipts = Vec::with_capacity(payloads.len());
        for payload in payloads {
            receipts.push(
                producer
                    .send_non_blocking(payload)
                    .await
                    .expect("sending message to Pulsar"),
            );
        }
        if batch {
            producer.send_batch().await.expect("flushing Pulsar batch");
        }
        for receipt in receipts {
            receipt.await.expect("awaiting Pulsar send receipt");
        }
    }

    /// Parses a `Message`'s `Offset::String` into `PulsarOffset`. Panics otherwise.
    fn parse_offset(offset: &Offset) -> PulsarOffset {
        let Offset::String(string_offset) = offset else {
            panic!("expected Offset::String, got {offset:?}");
        };
        let text = String::from_utf8_lossy(&string_offset.offset);
        PulsarOffset::from_str(&text).unwrap_or_else(|e| panic!("parsing offset {text:?}: {e}"))
    }

    /// Reads until `want` messages have arrived or `deadline` elapses.
    async fn read_until(
        source: &mut PulsarSource,
        want: usize,
        deadline: Duration,
    ) -> Vec<Message> {
        let start = Instant::now();
        let mut messages = Vec::new();
        while messages.len() < want && start.elapsed() < deadline {
            match source.read().await {
                Some(Ok(batch)) => messages.extend(batch),
                Some(Err(e)) => panic!("reading Pulsar messages: {e:?}"),
                None => break,
            }
        }
        messages
    }

    /// Polls admin stats every 200ms until the subscription's backlog is 0, or `deadline` elapses.
    async fn wait_for_backlog_zero(
        local: &str,
        partitioned: bool,
        subscription: &str,
        deadline: Duration,
    ) {
        let client = reqwest::Client::new();
        let path = if partitioned {
            format!("{ADMIN_ADDR}/admin/v2/persistent/public/default/{local}/partitioned-stats")
        } else {
            format!("{ADMIN_ADDR}/admin/v2/persistent/public/default/{local}/stats")
        };
        let pointer = format!("/subscriptions/{subscription}/msgBacklog");

        let start = Instant::now();
        let mut last_backlog = None;
        let mut last_body = String::new();
        while start.elapsed() < deadline {
            let resp = client
                .get(&path)
                .send()
                .await
                .expect("admin stats request failed");
            let status = resp.status();
            let text = resp.text().await.expect("reading stats response body");
            assert!(status.is_success(), "admin stats failed: {status} {text}");
            let body: serde_json::Value = serde_json::from_str(&text).expect("parsing stats JSON");

            match body.pointer(&pointer).and_then(|v| v.as_u64()) {
                Some(0) => return,
                Some(n) => last_backlog = Some(n),
                None => last_body = body.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!(
            "subscription {subscription} backlog did not reach 0; last backlog={last_backlog:?}, last body when pointer missing={last_body:?}"
        );
    }

    #[tokio::test]
    async fn test_pulsar_source() -> Result<()> {
        let topic = full_topic(&format!("r1-{}", get_rand_str()));
        let subscription = format!("sub-{}", get_rand_str());

        let cfg = PulsarSourceConfig {
            pulsar_server_addr: PULSAR_ADDR.into(),
            topic: topic.clone(),
            consumer_name: "test".into(),
            subscription,
            max_unack: 100,
            dead_letter_policy: None,
            auth: None,
            tls: None,
        };
        let mut pulsar = new_pulsar_source(
            cfg,
            10,
            Duration::from_millis(200),
            0,
            tokio_util::sync::CancellationToken::new(),
        )
        .await?;
        assert_eq!(pulsar.name(), "Pulsar");

        // Read should return before the timeout
        let msgs = tokio::time::timeout(Duration::from_millis(400), pulsar.read_messages()).await;
        assert!(msgs.is_ok());

        assert!(pulsar.pending().await.unwrap().is_none());

        let pulsar_producer: Pulsar<_> = Pulsar::builder(PULSAR_ADDR, TokioExecutor)
            .build()
            .await
            .unwrap();
        let mut pulsar_producer = pulsar_producer
            .producer()
            .with_topic(&topic)
            .with_name("my producer")
            .with_options(producer::ProducerOptions {
                schema: Some(proto::Schema {
                    r#type: proto::schema::Type::String as i32,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .build()
            .await
            .unwrap();

        let data: Vec<String> = (0..10).map(|i| format!("test_data_{i}")).collect();
        let send_futures = pulsar_producer
            .send_all(data)
            .await
            .map_err(|e| format!("Sending messages to Pulsar: {e:?}"))?;
        for fut in send_futures {
            fut.await?;
        }

        let messages = pulsar.read().await.unwrap()?;
        assert_eq!(messages.len(), 10);

        for message in &messages {
            assert!(
                matches!(message.offset, Offset::String(_)),
                "expected Offset::String, got {:?}",
                message.offset
            );
        }

        let offsets: Vec<Offset> = messages.into_iter().map(|m| m.offset).collect();

        pulsar.ack(offsets).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_pulsar_source_single_partition() {
        let local = format!("r2-{}", get_rand_str());
        let topic = full_topic(&local);
        let subscription = format!("sub-{}", get_rand_str());

        create_partitioned_topic(&local, 1).await;

        // The source must exist before producing: PulsarSource subscribes at Latest.
        let mut source =
            new_test_source(&topic, &subscription, 10, Duration::from_millis(500)).await;

        produce(
            &format!("{topic}-partition-0"),
            vec!["a".into(), "b".into(), "c".into()],
            false,
        )
        .await;

        let messages = read_until(&mut source, 3, Duration::from_secs(15)).await;
        assert_eq!(messages.len(), 3);

        let offsets: Vec<Offset> = messages.into_iter().map(|m| m.offset).collect();
        source.ack(offsets).await.expect("ack failed");

        wait_for_backlog_zero(&local, true, &subscription, Duration::from_secs(15)).await;
    }

    #[tokio::test]
    async fn test_pulsar_source_partitioned_ack() {
        let local = format!("i1-{}", get_rand_str());
        let topic = full_topic(&local);
        let subscription = format!("sub-{}", get_rand_str());

        create_partitioned_topic(&local, 3).await;

        let mut source =
            new_test_source(&topic, &subscription, 10, Duration::from_millis(500)).await;

        for i in 0..3 {
            produce(
                &format!("{topic}-partition-{i}"),
                vec![format!("{i}-a"), format!("{i}-b")],
                false,
            )
            .await;
        }

        let messages = read_until(&mut source, 6, Duration::from_secs(15)).await;
        assert_eq!(messages.len(), 6);

        let offsets: Vec<Offset> = messages.iter().map(|m| m.offset.clone()).collect();
        let ack_result = source.ack(offsets.clone()).await;
        assert!(ack_result.is_ok(), "ack failed: {ack_result:?}");

        let distinct_offsets: HashSet<&Offset> = offsets.iter().collect();
        assert_eq!(distinct_offsets.len(), 6);
        let distinct_ids: HashSet<_> = messages.iter().map(|m| &m.id.offset).collect();
        assert_eq!(distinct_ids.len(), 6);

        let partitions: HashSet<i32> = offsets
            .iter()
            .map(parse_offset)
            .map(|o| o.partition)
            .collect();
        assert_eq!(partitions, HashSet::from([0, 1, 2]));

        wait_for_backlog_zero(&local, true, &subscription, Duration::from_secs(15)).await;
    }

    #[tokio::test]
    async fn test_pulsar_source_partitioned_nack_redelivery() {
        let local = format!("i2-{}", get_rand_str());
        let subscription = format!("sub-{}", get_rand_str());

        create_partitioned_topic(&local, 2).await;

        // Short topic name: partition topics stay short, so ack/nack must route to them verbatim.
        let mut source =
            new_test_source(&local, &subscription, 10, Duration::from_millis(500)).await;

        for i in 0..2 {
            produce(
                &format!("{local}-partition-{i}"),
                vec![format!("n{i}")],
                false,
            )
            .await;
        }

        let first = read_until(&mut source, 2, Duration::from_secs(15)).await;
        assert_eq!(first.len(), 2);

        let first_offsets: Vec<Offset> = first.iter().map(|m| m.offset.clone()).collect();
        let nack_offsets = first_offsets
            .iter()
            .cloned()
            .map(|offset| NackOffset {
                offset,
                option: None,
            })
            .collect();
        let nack_result = source.nack(nack_offsets).await;
        assert!(nack_result.is_ok(), "nack failed: {nack_result:?}");

        let redelivered = read_until(&mut source, 2, Duration::from_secs(15)).await;
        assert_eq!(redelivered.len(), 2);

        let first_set: HashSet<Offset> = first_offsets.into_iter().collect();
        let redelivered_set: HashSet<Offset> =
            redelivered.iter().map(|m| m.offset.clone()).collect();
        assert_eq!(redelivered_set, first_set);

        let ack_offsets: Vec<Offset> = redelivered.into_iter().map(|m| m.offset).collect();
        source.ack(ack_offsets).await.expect("ack failed");

        wait_for_backlog_zero(&local, true, &subscription, Duration::from_secs(15)).await;
    }

    #[tokio::test]
    async fn test_pulsar_source_batched_producer() {
        let local = format!("i3-{}", get_rand_str());
        let topic = full_topic(&local);
        let subscription = format!("sub-{}", get_rand_str());

        let mut source =
            new_test_source(&topic, &subscription, 10, Duration::from_millis(500)).await;

        produce(&topic, (0..5).map(|i| format!("batch-{i}")).collect(), true).await;

        let messages = read_until(&mut source, 5, Duration::from_secs(15)).await;
        assert_eq!(messages.len(), 5);

        let offsets: Vec<Offset> = messages.iter().map(|m| m.offset.clone()).collect();
        let ack_result = source.ack(offsets.clone()).await;
        assert!(ack_result.is_ok(), "ack failed: {ack_result:?}");

        let distinct_offsets: HashSet<&Offset> = offsets.iter().collect();
        assert_eq!(distinct_offsets.len(), 5);
        let distinct_ids: HashSet<_> = messages.iter().map(|m| &m.id.offset).collect();
        assert_eq!(distinct_ids.len(), 5);

        let parsed: Vec<PulsarOffset> = offsets.iter().map(parse_offset).collect();
        let ledger_entries: HashSet<(u64, u64)> =
            parsed.iter().map(|o| (o.ledger_id, o.entry_id)).collect();
        assert_eq!(
            ledger_entries.len(),
            1,
            "batch members should share (ledger_id, entry_id): {parsed:?}"
        );
        let batch_indices: HashSet<i32> = parsed.iter().map(|o| o.batch_index).collect();
        assert_eq!(batch_indices, HashSet::from([0, 1, 2, 3, 4]));
    }

    #[tokio::test]
    async fn test_pulsar_source_ledger_rollover() {
        let local = format!("i4-{}", get_rand_str());
        let topic = full_topic(&local);
        let subscription = format!("sub-{}", get_rand_str());

        create_non_partitioned_topic(&local).await;
        create_subscription(&local, &subscription).await;

        produce(&topic, vec!["l0".into(), "l1".into()], false).await;

        unload_topic(&local).await;

        produce(&topic, vec!["l2".into(), "l3".into()], false).await;

        let mut source =
            new_test_source(&topic, &subscription, 10, Duration::from_millis(500)).await;

        let messages = read_until(&mut source, 4, Duration::from_secs(15)).await;
        assert_eq!(messages.len(), 4);

        let offsets: Vec<Offset> = messages.iter().map(|m| m.offset.clone()).collect();
        let distinct_offsets: HashSet<&Offset> = offsets.iter().collect();
        assert_eq!(distinct_offsets.len(), 4);

        let distinct_ids: HashSet<_> = messages.iter().map(|m| &m.id.offset).collect();
        assert_eq!(distinct_ids.len(), 4);

        let ledgers: HashSet<u64> = offsets
            .iter()
            .map(parse_offset)
            .map(|o| o.ledger_id)
            .collect();
        assert!(
            ledgers.len() >= 2,
            "expected >= 2 distinct ledger_ids, got {ledgers:?}"
        );

        let ack_result = source.ack(offsets).await;
        assert!(ack_result.is_ok(), "ack failed: {ack_result:?}");

        wait_for_backlog_zero(&local, false, &subscription, Duration::from_secs(15)).await;
    }
}
