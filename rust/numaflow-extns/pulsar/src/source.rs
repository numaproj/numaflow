use bytes::Bytes;
use chrono::{DateTime, Utc};
use prost::Message as ProstMessage;
use pulsar::{proto::MessageIdData, Consumer, ConsumerOptions, Pulsar, SubType, TokioExecutor};
use std::collections::BTreeMap;
use std::{collections::HashMap, time::Duration};
use tokio::time::Instant;
use tokio::{
    sync::{mpsc, oneshot},
    time,
};
use tonic::codegen::tokio_stream::StreamExt;

pub struct Message {
    /// keys of the message
    pub keys: Vec<String>,
    /// actual payload of the message
    pub value: Bytes,
    /// offset of the message, it is optional because offset is only
    /// available when we read the message, and we don't persist the
    /// offset in the ISB.
    pub offset: Bytes,
    /// event time of the message
    pub event_time: DateTime<Utc>,
    /// id of the message
    pub id: MessageID,
    /// headers of the message
    pub headers: HashMap<String, String>,
}

pub struct MessageID {
    pub vertex_name: String,
    pub offset: String,
    pub index: i32,
}

pub struct PulsarSourceConfig {
    pub pulsar_server_addr: String,
    pub topic: String,
    pub consumer_name: String,
    pub subscription: String,
    pub max_unack: usize,
}

enum ConsumerActorMessage {
    Read {
        count: usize,
        timeout_at: Instant,
        respond_to: oneshot::Sender<Vec<PulsarMessage>>,
    },
    Ack {
        offsets: Vec<u64>,
        respond_to: oneshot::Sender<()>,
    },
}

pub struct PulsarMessage {
    pub payload: Bytes,
    pub offset: u64,
    pub event_time: DateTime<Utc>,
    pub headers: HashMap<String, String>
}

struct ConsumerReaderActor {
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    handler_rx: mpsc::Receiver<ConsumerActorMessage>,
    message_ids: BTreeMap<u64, MessageIdData>,
    max_unack: usize,
    topic: String,
}

impl ConsumerReaderActor {
    async fn start(config: PulsarSourceConfig, handler_rx: mpsc::Receiver<ConsumerActorMessage>) {
        tracing::info!(
            addr = &config.pulsar_server_addr,
            "Pulsar connection details"
        );
        let pulsar: Pulsar<_> = Pulsar::builder(&config.pulsar_server_addr, TokioExecutor)
            .build()
            .await
            .unwrap();

        let consumer: Consumer<Vec<u8>, TokioExecutor> = pulsar
            .consumer()
            .with_topic(&config.topic)
            .with_consumer_name(&config.consumer_name)
            .with_subscription_type(SubType::Shared)
            .with_subscription(&config.subscription)
            .with_options(ConsumerOptions::default().durable(true))
            .build()
            .await
            .unwrap();

        tokio::spawn(async move {
            let mut consumer_actor = ConsumerReaderActor {
                consumer,
                handler_rx,
                message_ids: BTreeMap::new(),
                max_unack: config.max_unack,
                topic: config.topic,
            };
            consumer_actor.run().await;
        });
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
                self.ack_messages(offsets).await;
                let _ = respond_to.send(());
            }
        }
    }

    async fn get_messages(&mut self, count: usize, timeout_at: Instant) -> Vec<PulsarMessage> {
        let mut messages = vec![];
        for _ in 0..count {
            let remaining_time = timeout_at - Instant::now();
            let Ok(msg) = time::timeout(remaining_time, self.consumer.try_next()).await else {
                return messages;
            };
            let msg = match msg {
                Ok(Some(msg)) => msg,
                Ok(None) => break,
                Err(e) => {
                    tracing::error!(?e, "Fetching message from Pulsar");
                    time::sleep(Duration::from_millis(500)).await; // FIXME: add error metrics. Also, respect the timeout
                    continue;
                }
            };
            let offset = msg.message_id().entry_id;
            let event_time = msg
                .metadata()
                .event_time
                .unwrap_or(msg.metadata().publish_time);
            let Some(event_time) = chrono::DateTime::from_timestamp_millis(event_time as i64)
            else {
                tracing::error!(
                    event_time = msg.metadata().event_time,
                    publish_time = msg.metadata().publish_time,
                    parsed_event_time = event_time,
                    "Pulsar message contains invalid event_time/publish_time timestamp"
                );
                continue;
                //FIXME: NACK the message
            };
            self.message_ids.insert(offset, msg.message_id().clone());
            messages.push(PulsarMessage {
                payload: msg.payload.data.into(),
                offset,
                event_time,
                headers: HashMap::new() // FIXME:
            });

            // stop reading as soon as we hit max_unack
            if messages.len() >= self.max_unack {
                return messages; // FIXME: we should return error here or log it
            }
        }
        messages
    }

    async fn ack_messages(&mut self, offsets: Vec<u64>) {
        for offset in offsets {
            let msg_id = self.message_ids.remove(&offset);

            let msg_id = match msg_id {
                None =>{todo!()},
                Some(msg_id) => msg_id,
            };
            self.consumer.ack_with_id(&self.topic, msg_id).await.unwrap(); // FIXME: error handling
        }
    }
}

#[derive(Clone)]
pub struct PulsarSource {
    batch_size: usize,
    /// timeout for each batch read request
    timeout: Duration,
    actor_tx: mpsc::Sender<ConsumerActorMessage>,
}

impl PulsarSource {
    pub async fn new(config: PulsarSourceConfig, batch_size: usize, timeout: Duration) -> Self {
        let (tx, rx) = mpsc::channel(10);
        ConsumerReaderActor::start(config, rx).await;
        Self {
            actor_tx: tx,
            batch_size,
            timeout,
        }
    }
}

impl PulsarSource {
    pub async fn read(&self) -> Vec<PulsarMessage> {
        let start = Instant::now();
        let (tx, rx) = oneshot::channel();
        let msg = ConsumerActorMessage::Read {
            count: self.batch_size,
            timeout_at: Instant::now() + self.timeout,
            respond_to: tx,
        };
        let _ = self.actor_tx.send(msg).await;
        let messages = rx.await.expect("Actor task has been killed"); //FIXME:
        tracing::debug!(
            count = messages.len(),
            requested_count = self.batch_size,
            time_taken_ms = start.elapsed().as_millis(),
            "Got messages from pulsar"
        );
        messages
    }

    pub async fn ack(&self, offsets: Vec<u64>) {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .actor_tx
            .send(ConsumerActorMessage::Ack {
                offsets,
                respond_to: tx,
            })
            .await;
        rx.await.expect("Actor task has been killed"); //FIXME:
    }

    pub async fn pending(&self) -> Option<usize> {
        None
    }

    pub fn partitions(&self) -> Vec<u16> {
        todo!()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PulsarOffset {
    message_id_data: Vec<u8>,
    topic_name: String,
}

impl PulsarOffset {
    fn message_id_data(
        &self,
    ) -> Result<MessageIdData, Box<dyn std::error::Error + Send + Sync + 'static>> {
        MessageIdData::decode(self.message_id_data.as_slice()).map_err(Into::into)
    }

    fn serialize(&self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        bincode::serialize(self).map_err(Into::into)
    }
}

impl TryFrom<Bytes> for PulsarOffset {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn try_from(offset: Bytes) -> Result<Self, Self::Error> {
        let pulsar_offset: PulsarOffset = bincode::deserialize(offset.as_ref())?;
        Ok(pulsar_offset)
    }
}

impl From<pulsar::consumer::message::Message<Vec<u8>>> for Message {
    fn from(msg: pulsar::consumer::message::Message<Vec<u8>>) -> Self {
        let partition_id = msg.message_id().partition();
        // We need topic name along with message id data (comes from Pulsar message) to ack the message.
        // The message id data is serialized using prost.
        // Then the wrapper struct PulsarOffset that includes topic name is serialized using bincode.
        let pulsar_offset = PulsarOffset {
            message_id_data: msg.message_id().encode_to_vec(),
            topic_name: msg.topic.clone(),
        };

        let offset: Vec<u8> = pulsar_offset.serialize().unwrap_or_else(|e| {
            tracing::error!(?e, "Serializing Pulsar offset");
            vec![]
        });

        let offset_id = msg.message_id().entry_id.to_string();
        let metadata = msg.payload.metadata;
        let event_time = metadata.event_time.unwrap_or(metadata.publish_time);
        let event_time = chrono::DateTime::from_timestamp_millis(event_time as i64)
            .expect("Invalid event_time/publish_time timestamp");
        let payload = msg.payload.data;
        Message {
            event_time,
            id: MessageID {
                vertex_name: msg.topic.clone(), // FIXME:
                offset: offset_id,
                index: partition_id,
            },
            headers: HashMap::new(),
            keys: vec![],
            offset: offset.into(),
            value: payload.into(),
        }
    }
}
