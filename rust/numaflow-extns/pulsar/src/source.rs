use std::collections::BTreeMap;
use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use pulsar::{proto::MessageIdData, Consumer, ConsumerOptions, Pulsar, SubType, TokioExecutor};
use tokio::time::Instant;
use tokio::{
    sync::{mpsc, oneshot},
    time,
};
use tonic::codegen::tokio_stream::StreamExt;

use crate::{Error, Result};

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
        respond_to: oneshot::Sender<Result<Vec<PulsarMessage>>>,
    },
    Ack {
        offsets: Vec<u64>,
        respond_to: oneshot::Sender<Result<()>>,
    },
}

pub struct PulsarMessage {
    pub key: String,
    pub payload: Bytes,
    pub offset: u64,
    pub event_time: DateTime<Utc>,
    pub headers: HashMap<String, String>,
}

struct ConsumerReaderActor {
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    handler_rx: mpsc::Receiver<ConsumerActorMessage>,
    message_ids: BTreeMap<u64, MessageIdData>,
    max_unack: usize,
    topic: String,
    prev_messages: Vec<PulsarMessage>,
}

impl ConsumerReaderActor {
    async fn start(
        config: PulsarSourceConfig,
        handler_rx: mpsc::Receiver<ConsumerActorMessage>,
    ) -> Result<()> {
        tracing::info!(
            addr = &config.pulsar_server_addr,
            "Pulsar connection details"
        );
        let pulsar: Pulsar<_> = Pulsar::builder(&config.pulsar_server_addr, TokioExecutor)
            .build()
            .await
            .map_err(|e| format!("Creating Pulsar client connection: {e:?}"))?;

        let consumer: Consumer<Vec<u8>, TokioExecutor> = pulsar
            .consumer()
            .with_topic(&config.topic)
            .with_consumer_name(&config.consumer_name)
            .with_subscription_type(SubType::Shared)
            .with_subscription(&config.subscription)
            .with_options(ConsumerOptions::default().durable(true))
            .build()
            .await
            .map_err(|e| format!("Creating a Pulsar consumer: {e:?}"))?;

        tokio::spawn(async move {
            let mut consumer_actor = ConsumerReaderActor {
                consumer,
                handler_rx,
                message_ids: BTreeMap::new(),
                max_unack: config.max_unack,
                topic: config.topic,
                prev_messages: vec![],
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
        }
    }

    async fn get_messages(
        &mut self,
        count: usize,
        timeout_at: Instant,
    ) -> Result<Vec<PulsarMessage>> {
        if self.message_ids.len() >= self.max_unack {
            return Err(Error::AckPendingExceeded(self.message_ids.len()));
        }
        let mut messages = vec![];
        for _ in 0..count {
            let remaining_time = timeout_at - Instant::now();
            let Ok(msg) = time::timeout(remaining_time, self.consumer.try_next()).await else {
                return Ok(messages);
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
                    return Err(Error::Pulsar(e));
                }
            };
            let offset = msg.message_id().entry_id;
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

            self.message_ids.insert(offset, msg.message_id().clone());
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
                return Ok(messages);
            }
        }
        Ok(messages)
    }

    // TODO: Identify the longest continuous batch and use cumulative_ack_with_id() to ack them all.
    async fn ack_messages(&mut self, offsets: Vec<u64>) -> Result<()> {
        for offset in offsets {
            let msg_id = self.message_ids.remove(&offset);

            let Some(msg_id) = msg_id else {
                return Err(Error::UnknownOffset(offset));
            };

            let Err(e) = self.consumer.ack_with_id(&self.topic, msg_id.clone()).await else {
                continue;
            };
            // Insert offset back
            self.message_ids.insert(offset, msg_id);
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
}

impl PulsarSource {
    pub async fn new(
        config: PulsarSourceConfig,
        batch_size: usize,
        timeout: Duration,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(10);
        ConsumerReaderActor::start(config, rx).await?;
        Ok(Self {
            actor_tx: tx,
            batch_size,
            timeout,
        })
    }
}

impl PulsarSource {
    pub async fn read(&self) -> Result<Vec<PulsarMessage>> {
        let start = Instant::now();
        let (tx, rx) = oneshot::channel();
        let msg = ConsumerActorMessage::Read {
            count: self.batch_size,
            timeout_at: Instant::now() + self.timeout,
            respond_to: tx,
        };
        let _ = self.actor_tx.send(msg).await;
        let messages = rx.await.map_err(Error::ActorTaskTerminated)??;
        tracing::debug!(
            count = messages.len(),
            requested_count = self.batch_size,
            time_taken_ms = start.elapsed().as_millis(),
            "Got messages from pulsar"
        );
        Ok(messages)
    }

    pub async fn ack(&self, offsets: Vec<u64>) -> Result<()> {
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

    pub async fn pending(&self) -> Option<usize> {
        None
    }

    pub fn partitions(&self) -> Vec<u16> {
        todo!()
    }
}
