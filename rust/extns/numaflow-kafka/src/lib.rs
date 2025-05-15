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
    Sasl {
        mechanism: String,
        username: String,
        password: String,
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

type LoggingConsumer = StreamConsumer<KafkaContext>;

enum KafkaActorMessage {
    Read {
        respond_to: oneshot::Sender<Result<Vec<KafkaMessage>>>,
    },
    Ack {
        offsets: Vec<(i32, i64)>,
        respond_to: oneshot::Sender<Result<()>>,
    },
    Pending {
        respond_to: oneshot::Sender<Result<Option<usize>>>,
    },
}

struct KafkaActor {
    consumer: LoggingConsumer,
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
            .set_log_level(RDKafkaLogLevel::Debug);

        if let Some(auth) = config.auth {
            match auth {
                KafkaAuth::Sasl {
                    mechanism,
                    username,
                    password,
                } => {
                    let supported_mechanisms = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"];
                    if !supported_mechanisms.contains(&mechanism.as_str()) {
                        return Err(Error::Kafka(format!(
                            "Unsupported SASL mechanism: {}. Currently supported mechanisms: {}",
                            mechanism,
                            supported_mechanisms.join(", ")
                        )));
                    }
                    client_config.set("security.protocol", "SASL_PLAINTEXT");
                    if config.tls.is_some() {
                        client_config.set("security.protocol", "SASL_SSL");
                    }
                    client_config
                        .set("sasl.mechanisms", mechanism)
                        .set("sasl.username", username)
                        .set("sasl.password", password);
                }
            }
        }

        if let Some(tls_config) = config.tls {
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

        let context = KafkaContext;
        let consumer: LoggingConsumer =
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
            actor.run().await;
        });

        Ok(())
    }

    async fn run(&mut self) {
        while let Some(msg) = self.handler_rx.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: KafkaActorMessage) {
        match msg {
            KafkaActorMessage::Read { respond_to } => {
                let messages = self.read_messages().await;
                let _ = respond_to.send(messages);
            }
            KafkaActorMessage::Ack {
                offsets,
                respond_to,
            } => {
                let status = self.ack_messages(offsets).await;
                let _ = respond_to.send(status);
            }
            KafkaActorMessage::Pending { respond_to } => {
                let pending = self.pending_messages().await;
                let _ = respond_to.send(pending);
            }
        }
    }

    async fn read_messages(&mut self) -> Result<Vec<KafkaMessage>> {
        let mut messages: Vec<KafkaMessage> = vec![];
        let timeout = tokio::time::timeout(self.read_timeout, std::future::pending::<()>());
        tokio::pin!(timeout);
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
                        Ok(msg) => msg,
                        Err(e) => {
                            error!(?e, "Failed to read messages, will retry after 1 second");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };

                    let headers = match message.headers() {
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

                    let value = match message.payload() {
                        Some(payload) => Bytes::copy_from_slice(payload),
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

    async fn ack_messages(&mut self, offsets: Vec<(i32, i64)>) -> Result<()> {
        let mut tpl = TopicPartitionList::new();
        for (partition, offset) in offsets {
            tpl.add_partition_offset(&self.topic, partition, Offset::Offset(offset + 1))
                .map_err(|e| Error::Kafka(format!("Failed to add partition offset: {}", e)))?;
        }

        self.consumer
            .commit(&tpl, CommitMode::Async)
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

    pub async fn ack_messages(&self, offsets: Vec<(i32, i64)>) -> Result<()> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::ClientConfig;
    use rdkafka::message::{Header, OwnedHeaders};
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use tokio::time::Instant;

    async fn setup_kafka() -> (FutureProducer, String) {
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

    #[cfg(feature = "kafka-tests")]
    #[tokio::test]
    async fn test_kafka_source() {
        let (producer, topic_name) = setup_kafka().await;

        // Produce 100 messages
        for i in 0..100 {
            let payload = format!("message {}", i);
            let key = format!("key {}", i);
            let record = FutureRecord::to(&topic_name).payload(&payload).key(&key);
            producer
                .send(record, Duration::from_secs(5))
                .await
                .expect("Failed to send message");
        }

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
        let offsets: Vec<(i32, i64)> = messages
            .iter()
            .map(|msg| (msg.partition, msg.offset))
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
        let offsets: Vec<(i32, i64)> = messages
            .iter()
            .map(|msg| (msg.partition, msg.offset))
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
        let offsets: Vec<(i32, i64)> = messages
            .iter()
            .map(|msg| (msg.partition, msg.offset))
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
        let offsets: Vec<(i32, i64)> = messages
            .iter()
            .map(|msg| (msg.partition, msg.offset))
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
        let (producer, topic_name) = setup_kafka().await;

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
