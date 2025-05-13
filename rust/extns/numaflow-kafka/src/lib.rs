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
use tracing::{info, warn};

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
            .set_log_level(RDKafkaLogLevel::Debug);

        if let Some(auth) = config.auth {
            match auth {
                KafkaAuth::Sasl {
                    mechanism,
                    username,
                    password,
                } => {
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

        // FIXME: Subscribing to a non-existent topic will not return an error
        // Library shows error at a later point:
        // 2025-05-09T02:45:42.239784Z ERROR rdkafka::client: librdkafka: Global error: UnknownTopicOrPartition (Broker: Unknown topic or partition): Subscribed topic not available: test-topic: Broker: Unknown topic or partition
        // 2025-05-09T02:45:42.239847Z  WARN numaflow_kafka: Kafka error: Message consumption error: UnknownTopicOrPartition (Broker: Unknown topic or partition)
        consumer
            .subscribe(&[&config.topic])
            .map_err(|err| Error::Kafka(format!("Failed to subscribe to topic: {}", err)))?;

        /*
        FIXME: Authentication failure also comes at a later point
        2025-05-12T05:06:01.214282Z ERROR librdkafka: librdkafka: FAIL [thrd:ssl://kafka:29092/bootstrap]: ssl://kafka:29092/bootstrap: Receive failed: ssl/record/rec_layer_s3.c:911:ssl3_read_bytes error:0A000412:SSL routines::ssl/tls alert bad certificate: SSL alert number 42 (after 0ms in state APIVERSION_QUERY)
        2025-05-12T05:06:01.214310Z ERROR rdkafka::client: librdkafka: Global error: BrokerTransportFailure (Local: Broker transport failure): ssl://kafka:29092/bootstrap: Receive failed: ssl/record/rec_layer_s3.c:911:ssl3_read_bytes error:0A000412:SSL routines::ssl/tls alert bad certificate: SSL alert number 42 (after 0ms in state APIVERSION_QUERY)
        2025-05-12T05:06:01.214319Z  WARN numaflow_kafka: Kafka error: Message consumption error: BrokerTransportFailure (Local: Broker transport failure)
        */

        tokio::spawn(async move {
            let mut actor = KafkaActor {
                consumer,
                read_timeout,
                batch_size,
                topic: config.topic,
                handler_rx,
            };
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
                            warn!("Kafka error: {}", e);
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

        let metadata = self
            .consumer
            .fetch_metadata(Some(&self.topic), Duration::from_secs(1))
            .map_err(|e| Error::Kafka(format!("Failed to fetch metadata: {}", e)))?;

        let topic_metadata = metadata
            .topics()
            .first()
            .ok_or_else(|| Error::Kafka("No topic metadata found".to_string()))?;

        for partition in 0..topic_metadata.partitions().len() {
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition(&self.topic, partition as i32);
            let committed = self
                .consumer
                .committed_offsets(tpl, Duration::from_secs(1))
                .map_err(|e| Error::Kafka(format!("Failed to get committed offsets: {}", e)))?;

            let (low, high) = self
                .consumer
                .fetch_watermarks(&self.topic, partition as i32, Duration::from_secs(1))
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
