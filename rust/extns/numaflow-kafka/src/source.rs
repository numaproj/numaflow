use std::sync::Arc;
use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use rdkafka::Offset;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use tracing::{error, info, warn};

use crate::{Error, KafkaSaslAuth, Result, TlsConfig};

#[derive(Debug, Clone, PartialEq)]
pub struct KafkaSourceConfig {
    /// The list of Kafka brokers to connect to.
    pub brokers: Vec<String>,
    /// The Kafka topics to consume messages from.
    pub topics: Vec<String>,
    /// The consumer group to use for the Kafka consumer.
    pub consumer_group: String,
    /// The authentication mechanism to use for the Kafka consumer.
    pub auth: Option<KafkaSaslAuth>,
    /// The TLS configuration for the Kafka consumer.
    pub tls: Option<TlsConfig>,
    /// Any supported kafka client configuration options from
    /// https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html
    pub kafka_raw_config: HashMap<String, String>,
}

/// Message represents a message received from Kafka which can be converted to Numaflow Message.
#[derive(Debug)]
pub struct KafkaMessage {
    /// The topic name.
    pub topic: String,
    /// The user payload.
    pub value: Bytes,
    /// Key of the message
    pub key: Option<String>,
    /// The partition number.
    pub partition: i32,
    /// The offset of the message.
    pub offset: i64,
    /// The headers of the message.
    pub headers: HashMap<String, String>,
    /// The timestamp of the message in milliseconds since epoch.
    /// None if timestamp is not available.
    pub timestamp: Option<i64>,
}

// A context can be used to change the behavior of consumers by adding callbacks
// that will be executed by librdkafka.
struct KafkaContext;

impl ClientContext for KafkaContext {}

impl ConsumerContext for KafkaContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

/// Represents a Kafka offset for a specific topic.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KafkaOffset {
    /// The topic name
    pub topic: String,
    /// The partition id within a topic
    pub partition: i32,
    /// The offset of the message within a partition
    pub offset: i64,
}

enum KafkaActorMessage {
    Read {
        respond_to: oneshot::Sender<Option<Result<Vec<KafkaMessage>>>>,
    },
    Ack {
        offsets: Vec<KafkaOffset>,
        respond_to: oneshot::Sender<Result<()>>,
    },
    Pending {
        respond_to: oneshot::Sender<Result<Option<usize>>>,
    },
    PartitionsInfo {
        respond_to: oneshot::Sender<Result<Vec<i32>>>,
    },
}

type NumaflowConsumer = StreamConsumer<KafkaContext>;

struct KafkaActor {
    consumer: Arc<NumaflowConsumer>,
    read_timeout: Duration,
    batch_size: usize,
    topics: Vec<String>,
    handler_rx: mpsc::Receiver<KafkaActorMessage>,
    cancel_token: CancellationToken,
}

impl KafkaActor {
    async fn start(
        config: KafkaSourceConfig,
        batch_size: usize,
        read_timeout: Duration,
        handler_rx: mpsc::Receiver<KafkaActorMessage>,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        let mut client_config = ClientConfig::new();
        // https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html
        client_config
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("auto.offset.reset", "earliest");
        if !config.kafka_raw_config.is_empty() {
            info!(
                "Applying user-specified kafka config: {}",
                config
                    .kafka_raw_config
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<String>>()
                    .join(", ")
            );
            for (key, value) in config.kafka_raw_config {
                client_config.set(key, value);
            }
        }
        client_config
            .set("group.id", &config.consumer_group)
            .set("bootstrap.servers", config.brokers.join(","))
            .set("enable.auto.commit", "false")
            .set_log_level(RDKafkaLogLevel::Warning);

        crate::update_auth_config(&mut client_config, config.tls, config.auth);

        let context = KafkaContext;
        let consumer: Arc<NumaflowConsumer> =
            Arc::new(client_config.create_with_context(context).map_err(|err| {
                Error::Connection {
                    server: config.brokers.join(","),
                    error: err.to_string(),
                }
            })?);

        // NOTE: Subscribing to a non-existent topic will not return an error
        // The error happens only when we try to read from the topic
        // 2025-05-09T02:45:42.239784Z ERROR rdkafka::client: librdkafka: Global error: UnknownTopicOrPartition (Broker: Unknown topic or partition): Subscribed topic not available: test-topic: Broker: Unknown topic or partition
        // Currently, the pending returns Ok(Some(0)) if the topic is not found. When the topic is created,
        // the consumer starts to pull messages without the need for a restart.
        let topics: Vec<&str> = config.topics.iter().map(|s| s.as_str()).collect();
        consumer
            .subscribe(&topics)
            .map_err(|err| Error::Kafka(format!("Failed to subscribe to topic: {err}")))?;

        // The consumer.subscribe() will not fail even if the credentials are invalid.
        // To ensure creds/certificates are valid, we make a call to pending_messages() before starting the actor.
        let mut actor = KafkaActor {
            consumer,
            read_timeout,
            batch_size,
            topics: config.topics,
            handler_rx,
            cancel_token,
        };

        actor
            .pending_messages()
            .await
            .map_err(|err| Error::Kafka(format!("Failed to get pending messages: {err:?}")))?;

        tokio::spawn(async move {
            tracing::info!("Starting Kafka consumer...");
            // This actor terminates when sender end of the handler_rx is closed
            actor.run().await;
        });

        Ok(())
    }

    // run method will only return when the sender end of the handler_rx is closed.
    async fn run(mut self) {
        while let Some(msg) = self.handler_rx.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: KafkaActorMessage) {
        match msg {
            KafkaActorMessage::Read { respond_to } => {
                let messages = self.read_messages().await;
                respond_to
                    .send(messages)
                    .inspect_err(|e| {
                        error!(?e, "Failed to send messages from Kafka actor to main task");
                    })
                    .expect("Failed to send messages from Kafka actor to main task");
            }
            KafkaActorMessage::Ack {
                offsets,
                respond_to,
            } => {
                let status = self.ack_messages(offsets).await;
                respond_to
                    .send(status)
                    .inspect_err(|e| {
                        error!(
                            ?e,
                            "Failed to send ack messages from Kafka actor to main task"
                        );
                    })
                    .expect("Failed to send ack messages from Kafka actor to main task");
            }
            KafkaActorMessage::Pending { respond_to } => {
                let pending = self.pending_messages().await;
                respond_to
                    .send(pending)
                    .inspect_err(|e| {
                        error!(
                            ?e,
                            "Failed to send pending messages from Kafka actor to main task"
                        );
                    })
                    .expect("Failed to send pending messages from Kafka actor to main task");
            }
            KafkaActorMessage::PartitionsInfo { respond_to } => {
                let partitions = self.partitions_info().await;
                respond_to
                    .send(partitions)
                    .inspect_err(|e| {
                        error!(
                            ?e,
                            "Failed to send partition count from Kafka actor to main task"
                        );
                    })
                    .expect("Failed to send partition count from Kafka actor to main task");
            }
        }
    }

    async fn read_messages(&mut self) -> Option<Result<Vec<KafkaMessage>>> {
        if self.cancel_token.is_cancelled() {
            return None;
        }

        let mut messages: Vec<KafkaMessage> = vec![];
        let timeout = tokio::time::timeout(self.read_timeout, std::future::pending::<()>());
        tokio::pin!(timeout);

        // Return error if the number of continuous failures exceeds MAX_FAILURE_COUNT
        // A successful read will reset the failure count
        const MAX_FAILURE_COUNT: usize = 10;
        let mut continuous_failure_count = 0;
        loop {
            if messages.len() >= self.batch_size {
                break;
            }
            tokio::select! {
                biased;

                _ = &mut timeout => {
                    break;
                }

                message = self.consumer.recv() => {
                    let message = match message {
                        Ok(msg) => {
                            continuous_failure_count = 0;
                            msg
                        }
                        Err(e) => {
                            // TODO: Check the error when topic doesn't exist
                            continuous_failure_count += 1;
                            if continuous_failure_count > MAX_FAILURE_COUNT {
                                return Some(Err(Error::Kafka(format!(
                                    "Failed to read messages after {MAX_FAILURE_COUNT} retries: {e:?}"
                                ))));
                            }
                            error!(?e, "Failed to read messages, will retry after 100 milliseconds");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    };

                    let mut headers = match message.headers() {
                        Some(headers) => headers
                            .iter()
                            .map(|header| {
                                (
                                    header.key.to_string(),
                                    String::from_utf8_lossy(header.value.unwrap_or_default()).to_string(),
                                )
                            })
                            .collect(),
                        None => HashMap::new(),
                    };
                    headers.insert(crate::KAFKA_TOPIC_HEADER_KEY.to_string(), message.topic().to_string());

                    let value = match message.payload() {
                        Some(payload) => Bytes::copy_from_slice(payload),
                        // The rdkafka doc says that the payload can be None if there is no payload.
                        None => Bytes::new(),
                    };

                    let timestamp = message.timestamp().to_millis();

                    let message = KafkaMessage {
                        topic: message.topic().to_string(),
                        value,
                        key: message.key().map(|k| String::from_utf8_lossy(k).to_string()),
                        partition: message.partition(),
                        offset: message.offset(),
                        headers,
                        timestamp,
                    };

                    messages.push(message);
                }
            }
        }
        tracing::debug!(msg_count = messages.len(), "Read messages from Kafka");
        Some(Ok(messages))
    }

    async fn ack_messages(&mut self, offsets: Vec<KafkaOffset>) -> Result<()> {
        use std::collections::HashMap;
        // topic -> partition -> offset
        let mut topic_partition_offsets: HashMap<String, HashMap<i32, i64>> = HashMap::new();

        // For each topic and partition, keep only the highest offset
        for kafka_offset in offsets {
            topic_partition_offsets
                .entry(kafka_offset.topic.clone())
                .or_default()
                .entry(kafka_offset.partition)
                .and_modify(|current| {
                    if kafka_offset.offset > *current {
                        *current = kafka_offset.offset;
                    }
                })
                .or_insert(kafka_offset.offset);
        }

        let mut ack_tasks = vec![];
        for (topic, partition_offsets) in topic_partition_offsets {
            let mut tpl = TopicPartitionList::new();
            for (partition, offset) in partition_offsets {
                // Commit offset+1 (as per Kafka semantics)
                // In Kafka, the offset represents the position of the next message to be read.
                // When we commit offset N, it means the next message to be read will be at offset N.
                // Since we've already processed the message at the current offset, we need to commit
                // offset+1 to indicate we want to read the next message in the partition.
                tpl.add_partition_offset(&topic, partition, Offset::Offset(offset + 1))
                    .map_err(|e| {
                        Error::Kafka(format!(
                            "Failed to add partition offset for acknowledging messages: {e}",
                        ))
                    })?;
            }
            // commit internally calls [rd_kafka_offset_store](https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html#ab96539928328f14c3c9177ea0c896c87)
            // This may be a blocking call, so we spawn a new task to run it.
            let consumer = Arc::clone(&self.consumer);
            let task = tokio::task::spawn_blocking(move || {
                consumer
                    .commit(&tpl, CommitMode::Sync)
                    .map_err(|e| Error::Kafka(format!("Failed to commit offsets: {e}")))
            });
            ack_tasks.push(task);
        }
        for task in ack_tasks {
            task.await.map_err(|e| {
                Error::Kafka(format!("Waiting for spawned ack tasks to complete: {e:?}"))
            })??;
        }
        Ok(())
    }

    /// Returns the total number of pending messages across all topics and partitions.
    /// The total pending is calculated as the sum of (high watermark - committed offset)
    /// for each partition across all topics.
    async fn pending_messages(&mut self) -> Result<Option<usize>> {
        let timeout = Duration::from_secs(5);
        let mut handles = Vec::new();
        for topic in &self.topics {
            let consumer = Arc::clone(&self.consumer);
            let topic = topic.clone();

            // fetch_metadata internally calls [rd_kafka_metadata](https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html#a84bba4a4b13fdb515f1a22d6fd4f7344)
            // This may be a blocking call, so we spawn a new task to run it.
            handles.push(tokio::task::spawn_blocking(move || {
                let metadata = consumer
                    .fetch_metadata(Some(&topic), timeout)
                    .map_err(|e| Error::Kafka(format!("Failed to fetch metadata: {e}")))?;
                let Some(topic_metadata) = metadata.topics().first() else {
                    warn!(topic = topic, "No topic metadata found");
                    return Ok(0);
                };
                let mut topic_pending = 0;
                for partition in 0..topic_metadata.partitions().len() {
                    let mut tpl = TopicPartitionList::new();
                    tpl.add_partition(&topic, partition as i32);
                    let committed = consumer.committed_offsets(tpl, timeout).map_err(|e| {
                        Error::Kafka(format!("Failed to get committed offsets: {e}"))
                    })?;
                    let (low, high) = consumer
                        .fetch_watermarks(&topic, partition as i32, timeout)
                        .map_err(|e| Error::Kafka(format!("Failed to fetch watermarks: {e}")))?;
                    let committed_offset = match committed.elements_for_topic(&topic).first() {
                        Some(element) => match element.offset() {
                            Offset::Offset(offset) => offset,
                            _ => low,
                        },
                        None => low,
                    };
                    topic_pending += (high - committed_offset) as usize;
                }
                Ok(topic_pending)
            }));
        }
        let mut total_pending = 0;
        for handle in handles {
            match handle.await {
                Ok(Ok(count)) => total_pending += count,
                Ok(Err(e)) => {
                    error!(?e, "Error fetching pending messages");
                    return Err(e);
                }
                Err(e) => {
                    error!(?e, "Tokio task join error fetching pending messages");
                    return Err(Error::Other(format!("Tokio task join error: {e}")));
                }
            }
        }
        Ok(Some(total_pending))
    }

    /// Returns the partition IDs of the topic with the maximum number of partitions.
    /// Since we support specifying multiple topics, this method returns the Vec of partition IDs
    /// from the topic that has the most partitions among all configured topics.
    async fn partitions_info(&mut self) -> Result<Vec<i32>> {
        let timeout = Duration::from_secs(5);
        let mut handles = Vec::new();
        for topic in &self.topics {
            let consumer = Arc::clone(&self.consumer);
            let topic = topic.clone();

            // fetch_metadata internally calls [rd_kafka_metadata](https://docs.confluent.io/platform/current/clients/librdkafka/html/rdkafka_8h.html#a84bba4a4b13fdb515f1a22d6fd4f7344)
            // This may be a blocking call, so we spawn a new task to run it.
            handles.push(tokio::task::spawn_blocking(move || {
                let metadata = consumer
                    .fetch_metadata(Some(&topic), timeout)
                    .map_err(|e| Error::Kafka(format!("Failed to fetch metadata: {e}")))?;
                let Some(topic_metadata) = metadata.topics().first() else {
                    warn!(topic = topic, "No topic metadata found");
                    return Ok(Vec::new());
                };
                let partitions: Vec<i32> =
                    topic_metadata.partitions().iter().map(|p| p.id()).collect();
                Ok(partitions)
            }));
        }
        let mut max_partitions = 0;
        let mut result: Vec<i32> = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(Ok(partitions)) => {
                    if partitions.len() > max_partitions {
                        max_partitions = partitions.len();
                        result = partitions;
                    }
                }
                Ok(Err(e)) => {
                    error!(?e, "Error fetching partitions info");
                    return Err(e);
                }
                Err(e) => {
                    error!(?e, "Tokio task join error fetching partitions info");
                    return Err(Error::Other(format!("Tokio task join error: {e}")));
                }
            }
        }
        Ok(result)
    }
}

#[derive(Clone)]
pub struct KafkaSource {
    actor_tx: mpsc::Sender<KafkaActorMessage>,
}

impl KafkaSource {
    pub async fn connect(
        config: KafkaSourceConfig,
        batch_size: usize,
        read_timeout: Duration,
        cancel_token: CancellationToken,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(10);
        KafkaActor::start(config, batch_size, read_timeout, rx, cancel_token).await?;
        Ok(Self { actor_tx: tx })
    }

    pub async fn read_messages(&self) -> Option<Result<Vec<KafkaMessage>>> {
        let (tx, rx) = oneshot::channel();
        let msg = KafkaActorMessage::Read { respond_to: tx };
        let _ = self.actor_tx.send(msg).await;
        rx.await
            .unwrap_or_else(|_| Some(Err(Error::Other("Actor task terminated".into()))))
    }

    pub async fn ack_messages(&self, offsets: Vec<KafkaOffset>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let msg = KafkaActorMessage::Ack {
            offsets,
            respond_to: tx,
        };
        let _ = self.actor_tx.send(msg).await;
        rx.await
            .map_err(|_| Error::Other("Actor task terminated".into()))?
    }

    pub async fn pending_messages(&self) -> Result<Option<usize>> {
        let (tx, rx) = oneshot::channel();
        let msg = KafkaActorMessage::Pending { respond_to: tx };
        let _ = self.actor_tx.send(msg).await;
        rx.await
            .map_err(|_| Error::Other("Actor task terminated".into()))?
    }

    pub async fn partitions_info(&self) -> Result<Vec<i32>> {
        let (tx, rx) = oneshot::channel();
        let msg = KafkaActorMessage::PartitionsInfo { respond_to: tx };
        let _ = self.actor_tx.send(msg).await;
        rx.await
            .map_err(|_| Error::Other("Actor task terminated".into()))?
    }
}

/// Expose methods so that numaflow-core crate doesn't have to depend on rdkafka.
#[cfg(feature = "kafka-tests-utils")]
pub mod test_utils {
    use super::*;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use std::time::Duration;

    pub async fn setup_test_topic() -> (FutureProducer, String) {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .create()
            .expect("Failed to create producer");

        let topic_name = format!(
            "kafka_source_test_topic_{}",
            uuid::Uuid::new_v4().to_string().replace("-", "")
        );

        // Create topic if it doesn't exist
        let admin_client = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .create::<rdkafka::admin::AdminClient<_>>()
            .expect("Failed to create admin client");

        let topic_config = rdkafka::admin::NewTopic::new(
            topic_name.as_str(),
            1,
            rdkafka::admin::TopicReplication::Fixed(1),
        );
        let _ = admin_client
            .create_topics(&[topic_config], &rdkafka::admin::AdminOptions::new())
            .await
            .expect("Failed to create topic");

        (producer, topic_name)
    }

    pub async fn produce_test_messages(producer: &FutureProducer, topic: &str, count: usize) {
        for i in 0..count {
            let payload = format!("message {}", i);
            let key = format!("key {}", i);
            let record = FutureRecord::to(topic).payload(&payload).key(&key);
            producer
                .send(record, Duration::from_secs(5))
                .await
                .expect("Failed to send message");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::message::{Header, OwnedHeaders};
    use rdkafka::producer::FutureRecord;
    use tokio::time::Instant;

    #[cfg(all(feature = "kafka-tests", feature = "kafka-tests-utils"))]
    #[tokio::test]
    async fn test_kafka_source() {
        let (producer, topic_name) = test_utils::setup_test_topic().await;

        // Produce 100 messages
        test_utils::produce_test_messages(&producer, &topic_name, 100).await;

        let config = KafkaSourceConfig {
            brokers: vec!["localhost:9092".to_string()],
            topics: vec![topic_name.clone()],
            consumer_group: "test_consumer_group".to_string(),
            auth: None,
            tls: None,
            kafka_raw_config: HashMap::from([
                ("connections.max.idle.ms".to_string(), "540000".to_string()), // 9 minutes, default value
            ]),
        };

        let read_timeout = Duration::from_secs(5);
        let source = KafkaSource::connect(config, 30, read_timeout, CancellationToken::new())
            .await
            .expect("Failed to connect to Kafka");

        let pending = source
            .pending_messages()
            .await
            .expect("Failed to get pending messages");
        assert_eq!(
            pending,
            Some(100),
            "Pending messages should include unacknowledged messages"
        );

        // Read messages
        let messages = source
            .read_messages()
            .await
            .expect("Failed to read messages")
            .unwrap();

        assert_eq!(messages.len(), 30);
        let pending = source
            .pending_messages()
            .await
            .expect("Failed to get pending messages");
        assert_eq!(
            pending,
            Some(100),
            "Pending messages should include unacknowledged messages"
        );

        // Ack messages
        let offsets: Vec<KafkaOffset> = messages
            .iter()
            .map(|msg| KafkaOffset {
                topic: topic_name.clone(),
                partition: msg.partition,
                offset: msg.offset,
            })
            .collect();
        source
            .ack_messages(offsets)
            .await
            .expect("Failed to ack messages");

        tokio::time::sleep(Duration::from_millis(5)).await;
        let pending = source
            .pending_messages()
            .await
            .expect("Failed to get pending messages");
        assert_eq!(
            pending,
            Some(70),
            "Pending messages should be 70 after acking 30 messages"
        );

        // Read remaining messages
        let messages = source
            .read_messages()
            .await
            .expect("Failed to read messages")
            .unwrap();

        assert_eq!(messages.len(), 30);

        // Ack remaining messages
        let offsets: Vec<KafkaOffset> = messages
            .iter()
            .map(|msg| KafkaOffset {
                topic: topic_name.clone(),
                partition: msg.partition,
                offset: msg.offset,
            })
            .collect();
        source
            .ack_messages(offsets)
            .await
            .expect("Failed to ack messages");

        // Check pending messages
        tokio::time::sleep(Duration::from_millis(5)).await;
        let pending = source
            .pending_messages()
            .await
            .expect("Failed to get pending messages");
        assert_eq!(
            pending,
            Some(40),
            "Pending messages should be 40 after acking another 30 messages"
        );

        // Read remaining messages
        let messages = source
            .read_messages()
            .await
            .expect("Failed to read messages")
            .unwrap();
        assert_eq!(messages.len(), 30);

        // Ack remaining messages
        let offsets: Vec<KafkaOffset> = messages
            .iter()
            .map(|msg| KafkaOffset {
                topic: topic_name.clone(),
                partition: msg.partition,
                offset: msg.offset,
            })
            .collect();
        source
            .ack_messages(offsets)
            .await
            .expect("Failed to ack messages");

        // Check pending messages
        tokio::time::sleep(Duration::from_millis(5)).await;
        let pending = source
            .pending_messages()
            .await
            .expect("Failed to get pending messages");
        assert_eq!(
            pending,
            Some(10),
            "Pending messages should be 10 after acking another 30 messages"
        );

        // Read remaining messages
        let messages = source
            .read_messages()
            .await
            .expect("Failed to read messages")
            .unwrap();
        assert_eq!(messages.len(), 10);

        // Ack remaining messages
        let offsets: Vec<KafkaOffset> = messages
            .iter()
            .map(|msg| KafkaOffset {
                topic: topic_name.clone(),
                partition: msg.partition,
                offset: msg.offset,
            })
            .collect();
        source
            .ack_messages(offsets)
            .await
            .expect("Failed to ack messages");

        // Check pending messages
        tokio::time::sleep(Duration::from_millis(5)).await;
        let pending = source
            .pending_messages()
            .await
            .expect("Failed to get pending messages");
        assert_eq!(
            pending,
            Some(0),
            "Pending messages should be 0 after acking all messages"
        );

        // Ensure read operation returns after the read timeout
        let start = Instant::now();
        let messages = source
            .read_messages()
            .await
            .expect("Failed to read messages")
            .unwrap();

        let elapsed = start.elapsed();
        assert!(
            elapsed < read_timeout + Duration::from_millis(100),
            "Read operation should return in 1 second"
        );
        assert!(
            messages.is_empty(),
            "No messages should be returned after all messages are acked"
        );
    }

    #[cfg(all(feature = "kafka-tests", feature = "kafka-tests-utils"))]
    #[tokio::test]
    async fn test_kafka_source_with_headers() {
        let (producer, topic_name) = test_utils::setup_test_topic().await;

        // Produce a message with headers
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "header1",
                value: Some("value1"),
            })
            .insert(Header {
                key: "header2",
                value: Some("value2"),
            });

        let record = FutureRecord::to(&topic_name)
            .payload("test message")
            .key("test key")
            .headers(headers);

        producer
            .send(record, Duration::from_secs(5))
            .await
            .expect("Failed to send message");

        let config = KafkaSourceConfig {
            brokers: vec!["localhost:9092".to_string()],
            topics: vec![topic_name.clone()],
            consumer_group: "test_consumer_group_headers".to_string(),
            auth: None,
            tls: None,
            kafka_raw_config: HashMap::new(),
        };

        let source =
            KafkaSource::connect(config, 1, Duration::from_secs(5), CancellationToken::new())
                .await
                .expect("Failed to connect to Kafka");

        let messages = source
            .read_messages()
            .await
            .expect("Failed to read messages")
            .unwrap();

        assert_eq!(messages.len(), 1);

        let message = &messages[0];
        assert_eq!(message.headers.get("header1"), Some(&"value1".to_string()));
        assert_eq!(message.headers.get("header2"), Some(&"value2".to_string()));
        // Verify that timestamp is present (should be Some since Kafka sets timestamps)
        assert!(message.timestamp.is_some());
    }
}
