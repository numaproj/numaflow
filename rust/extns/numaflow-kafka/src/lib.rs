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
use tracing::{error, info, warn};

const KAFKA_TOPIC_HEADER_KEY: &str = "X-NF-Kafka-TopicName";

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Connecting to Kafka {server} - {error}")]
    Connection { server: String, error: String },

    #[error("Kafka - {0}")]
    Kafka(String),

    #[error("{0}")]
    Other(String),
}

/// Represents the authentication method used to connect to Kafka.
#[derive(Debug, Clone, PartialEq)]
pub enum KafkaAuth {
    Plain {
        username: String,
        password: String,
    },
    ScramSha256 {
        username: String,
        password: String,
    },
    ScramSha512 {
        username: String,
        password: String,
    },
    Gssapi {
        service_name: String,
        realm: String,
        username: String,
        password: Option<String>,
        keytab: Option<String>,
        kerberos_config: Option<String>,
        auth_type: String,
    },
    Oauth {
        client_id: String,
        client_secret: String,
        token_endpoint: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct TlsConfig {
    pub insecure_skip_verify: bool,
    pub ca_cert: Option<String>,
    pub client_auth: Option<TlsClientAuthCerts>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TlsClientAuthCerts {
    pub client_cert: String,
    pub client_cert_private_key: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KafkaSourceConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    pub consumer_group: String,
    pub auth: Option<KafkaAuth>,
    pub tls: Option<TlsConfig>,
}

/// Message represents a message received from Kafka which can be converted to Numaflow Message.
#[derive(Debug)]
pub struct KafkaMessage {
    /// The user payload.
    pub value: Bytes,
    /// The partition number.
    pub partition: i32,
    /// The offset of the message.
    pub offset: i64,
    /// The headers of the message.
    pub headers: HashMap<String, String>,
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

type NumaflowConsumer = StreamConsumer<KafkaContext>;

/// Represents a Kafka offset for a specific topic.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KafkaOffset {
    /// The partition id within a topic
    pub partition: i32,
    /// The offset of the message within a partition
    pub offset: i64,
}

enum KafkaActorMessage {
    Read {
        respond_to: oneshot::Sender<Result<Vec<KafkaMessage>>>,
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

struct KafkaActor {
    consumer: NumaflowConsumer,
    read_timeout: Duration,
    batch_size: usize,
    topic: String,
    handler_rx: mpsc::Receiver<KafkaActorMessage>,
}

impl KafkaActor {
    async fn start(
        config: KafkaSourceConfig,
        batch_size: usize,
        read_timeout: Duration,
        handler_rx: mpsc::Receiver<KafkaActorMessage>,
    ) -> Result<()> {
        let mut client_config = ClientConfig::new();
        // https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html
        client_config
            .set("group.id", &config.consumer_group)
            .set("bootstrap.servers", config.brokers.join(","))
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest") // TODO: Make this configurable
            .set_log_level(RDKafkaLogLevel::Warning);

        let has_tls = config.tls.is_some();
        if let Some(tls_config) = config.tls.clone() {
            client_config.set("security.protocol", "SSL");
            if tls_config.insecure_skip_verify {
                warn!(
                    "'insecureSkipVerify' is set to true, certificate validation will not be performed when connecting to Kafka server"
                );
                client_config.set("enable.ssl.certificate.verification", "false");
            }
            if let Some(ca_cert) = tls_config.ca_cert {
                client_config.set("ssl.ca.pem", ca_cert);
            }
            if let Some(client_auth) = tls_config.client_auth {
                client_config
                    .set("ssl.certificate.pem", client_auth.client_cert)
                    .set("ssl.key.pem", client_auth.client_cert_private_key);
            }
        }

        if let Some(auth) = config.auth {
            client_config.set(
                "security.protocol",
                if has_tls {
                    "SASL_SSL"
                } else {
                    "SASL_PLAINTEXT"
                },
            );
            match auth {
                KafkaAuth::Plain { username, password } => {
                    client_config
                        .set("sasl.mechanisms", "PLAIN")
                        .set("sasl.username", username)
                        .set("sasl.password", password);
                }
                KafkaAuth::ScramSha256 { username, password } => {
                    client_config
                        .set("sasl.mechanisms", "SCRAM-SHA-256")
                        .set("sasl.username", username)
                        .set("sasl.password", password);
                }
                KafkaAuth::ScramSha512 { username, password } => {
                    client_config
                        .set("sasl.mechanisms", "SCRAM-SHA-512")
                        .set("sasl.username", username)
                        .set("sasl.password", password);
                }
                KafkaAuth::Gssapi {
                    service_name,
                    realm: _,
                    username,
                    password: _,
                    keytab,
                    kerberos_config,
                    auth_type: _,
                } => {
                    client_config.set("sasl.mechanisms", "GSSAPI");
                    client_config.set("sasl.kerberos.service.name", service_name);
                    client_config.set("sasl.kerberos.principal", username);
                    if let Some(keytab) = keytab {
                        client_config.set("sasl.kerberos.keytab", keytab);
                    }
                    if let Some(kerberos_config) = kerberos_config {
                        client_config.set("sasl.kerberos.kinit.cmd", kerberos_config);
                    }
                }
                KafkaAuth::Oauth {
                    client_id,
                    client_secret,
                    token_endpoint,
                } => {
                    client_config.set("sasl.mechanisms", "OAUTHBEARER");
                    client_config.set("sasl.oauthbearer.client.id", client_id);
                    client_config.set("sasl.oauthbearer.client.secret", client_secret);
                    client_config.set("sasl.oauthbearer.token.endpoint.url", token_endpoint);
                }
            }
        }

        let context = KafkaContext;
        let consumer: NumaflowConsumer =
            client_config
                .create_with_context(context)
                .map_err(|err| Error::Connection {
                    server: config.brokers.join(","),
                    error: err.to_string(),
                })?;

        // NOTE: Subscribing to a non-existent topic will not return an error
        // The error happens only when we try to read from the topic
        // 2025-05-09T02:45:42.239784Z ERROR rdkafka::client: librdkafka: Global error: UnknownTopicOrPartition (Broker: Unknown topic or partition): Subscribed topic not available: test-topic: Broker: Unknown topic or partition
        // Currently, the pending returns Ok(Some(0)) if the topic is not found. When the topic is created,
        // the consumer starts to pull messages without the need for a restart.
        consumer
            .subscribe(&[&config.topic])
            .map_err(|err| Error::Kafka(format!("Failed to subscribe to topic: {}", err)))?;

        // The consumer.subscribe() will not fail even if the credentials are invalid.
        // To ensure creds/certificates are valid, we make a call to pending_messages() before starting the actor.
        let mut actor = KafkaActor {
            consumer,
            read_timeout,
            batch_size,
            topic: config.topic,
            handler_rx,
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

    async fn read_messages(&mut self) -> Result<Vec<KafkaMessage>> {
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
                                return Err(Error::Kafka(format!(
                                    "Failed to read messages after {} retries: {e:?}",
                                    MAX_FAILURE_COUNT
                                )));
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
                    headers.insert(KAFKA_TOPIC_HEADER_KEY.to_string(), message.topic().to_string());

                    let value = match message.payload() {
                        Some(payload) => Bytes::copy_from_slice(payload),
                        // The rdkafka doc says that the payload can be None if there is no payload.
                        None => Bytes::new(),
                    };

                    let message = KafkaMessage {
                        value,
                        partition: message.partition(),
                        offset: message.offset(),
                        headers,
                    };

                    messages.push(message);
                }
            }
        }
        tracing::debug!(msg_count = messages.len(), "Read messages from Kafka");
        Ok(messages)
    }

    async fn ack_messages(&mut self, offsets: Vec<KafkaOffset>) -> Result<()> {
        use std::collections::HashMap;
        let mut partition_offsets: HashMap<i32, i64> = HashMap::new();

        // For each partition, keep only the highest offset
        for kafka_offset in offsets {
            partition_offsets
                .entry(kafka_offset.partition)
                .and_modify(|current| {
                    if kafka_offset.offset > *current {
                        *current = kafka_offset.offset;
                    }
                })
                .or_insert(kafka_offset.offset);
        }

        let mut tpl = TopicPartitionList::new();
        for (partition, offset) in partition_offsets {
            // Commit offset+1 (as per Kafka semantics)
            // In Kafka, the offset represents the position of the next message to be read.
            // When we commit offset N, it means the next message to be read will be at offset N.
            // Since we've already processed the message at the current offset, we need to commit
            // offset+1 to indicate we want to read the next message in the partition.
            tpl.add_partition_offset(&self.topic, partition, Offset::Offset(offset + 1))
                .map_err(|e| {
                    Error::Kafka(format!(
                        "Failed to add partition offset for acknowledging messages: {}",
                        e
                    ))
                })?;
        }

        // Wait for confirmation from the Kafka broker that the offsets have been committed.
        self.consumer
            .commit(&tpl, CommitMode::Sync)
            .map_err(|e| Error::Kafka(format!("Failed to commit offsets: {}", e)))?;

        Ok(())
    }

    async fn pending_messages(&mut self) -> Result<Option<usize>> {
        let mut total_pending = 0;
        let timeout = Duration::from_secs(5);

        let metadata = self
            .consumer
            .fetch_metadata(Some(&self.topic), timeout)
            .map_err(|e| Error::Kafka(format!("Failed to fetch metadata: {}", e)))?;

        let Some(topic_metadata) = metadata.topics().first() else {
            warn!(topic = self.topic, "No topic metadata found");
            return Ok(Some(0));
        };

        for partition in 0..topic_metadata.partitions().len() {
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition(&self.topic, partition as i32);
            let committed = self
                .consumer
                .committed_offsets(tpl, timeout)
                .map_err(|e| Error::Kafka(format!("Failed to get committed offsets: {}", e)))?;

            let (low, high) = self
                .consumer
                .fetch_watermarks(&self.topic, partition as i32, timeout)
                .map_err(|e| Error::Kafka(format!("Failed to fetch watermarks: {}", e)))?;

            let committed_offset = match committed.elements_for_topic(&self.topic).first() {
                Some(element) => match element.offset() {
                    Offset::Offset(offset) => offset,
                    _ => low,
                },
                None => low,
            };

            total_pending += (high - committed_offset) as usize;
        }

        Ok(Some(total_pending))
    }

    async fn partitions_info(&mut self) -> Result<Vec<i32>> {
        let timeout = Duration::from_secs(5);
        let metadata = self
            .consumer
            .fetch_metadata(Some(&self.topic), timeout)
            .map_err(|e| Error::Kafka(format!("Failed to fetch metadata: {}", e)))?;

        let Some(topic_metadata) = metadata.topics().first() else {
            warn!(topic = self.topic, "No topic metadata found");
            return Err(Error::Kafka("No topic metadata found".to_string()));
        };
        Ok(topic_metadata.partitions().iter().map(|p| p.id()).collect())
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
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(10);
        KafkaActor::start(config, batch_size, read_timeout, rx).await?;
        Ok(Self { actor_tx: tx })
    }

    pub async fn read_messages(&self) -> Result<Vec<KafkaMessage>> {
        let (tx, rx) = oneshot::channel();
        let msg = KafkaActorMessage::Read { respond_to: tx };
        let _ = self.actor_tx.send(msg).await;
        rx.await
            .map_err(|_| Error::Other("Actor task terminated".into()))?
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

#[cfg(feature = "kafka-tests")]
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

    #[cfg(feature = "kafka-tests")]
    #[tokio::test]
    async fn test_kafka_source() {
        let (producer, topic_name) = test_utils::setup_test_topic().await;

        // Produce 100 messages
        test_utils::produce_test_messages(&producer, &topic_name, 100).await;

        let config = KafkaSourceConfig {
            brokers: vec!["localhost:9092".to_string()],
            topic: topic_name.clone(),
            consumer_group: "test_consumer_group".to_string(),
            auth: None,
            tls: None,
        };

        let read_timeout = Duration::from_secs(5);
        let source = KafkaSource::connect(config, 30, read_timeout)
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
            .expect("Failed to read messages");
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
            .expect("Failed to read messages");
        assert_eq!(messages.len(), 30);

        // Ack remaining messages
        let offsets: Vec<KafkaOffset> = messages
            .iter()
            .map(|msg| KafkaOffset {
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
            .expect("Failed to read messages");
        assert_eq!(messages.len(), 30);

        // Ack remaining messages
        let offsets: Vec<KafkaOffset> = messages
            .iter()
            .map(|msg| KafkaOffset {
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
            .expect("Failed to read messages");
        assert_eq!(messages.len(), 10);

        // Ack remaining messages
        let offsets: Vec<KafkaOffset> = messages
            .iter()
            .map(|msg| KafkaOffset {
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
            .expect("Failed to read messages");
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

    #[cfg(feature = "kafka-tests")]
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
            topic: topic_name,
            consumer_group: "test_consumer_group_headers".to_string(),
            auth: None,
            tls: None,
        };

        let source = KafkaSource::connect(config, 1, Duration::from_secs(5))
            .await
            .expect("Failed to connect to Kafka");

        let messages = source
            .read_messages()
            .await
            .expect("Failed to read messages");
        assert_eq!(messages.len(), 1);

        let message = &messages[0];
        assert_eq!(message.headers.get("header1"), Some(&"value1".to_string()));
        assert_eq!(message.headers.get("header2"), Some(&"value2".to_string()));
    }
}
