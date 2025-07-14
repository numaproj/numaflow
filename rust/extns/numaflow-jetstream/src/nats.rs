use crate::{Error, NatsAuth, Result, TlsConfig, tls};
use async_nats::ConnectOptions;
use bytes::Bytes;
use chrono::DateTime;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub struct NatsSourceConfig {
    pub addr: String,
    pub subject: String,
    pub queue: String,
    pub auth: Option<NatsAuth>,
    pub tls: Option<TlsConfig>,
}

/// NatsMessage represents a Numaflow Message which can be converted from Nats message
#[derive(Debug)]
pub struct NatsMessage {
    pub value: Bytes,
    pub id: String,
    pub event_time: DateTime<chrono::Utc>,
}

impl TryFrom<async_nats::Message> for NatsMessage {
    type Error = Error;
    fn try_from(msg: async_nats::Message) -> Result<Self> {
        Ok(NatsMessage {
            value: Bytes::from(msg.payload),
            id: Uuid::new_v4().to_string(),
            event_time: chrono::Utc::now(),
        })
    }
}

enum NatsActorMessage {
    Read {
        respond_to: oneshot::Sender<Result<Vec<NatsMessage>>>,
    },
}

struct NatsActor {
    sub: async_nats::Subscriber,
    read_timeout: Duration,
    batch_size: usize,
    handler_rx: mpsc::Receiver<NatsActorMessage>,
}

impl NatsActor {
    async fn start(
        config: NatsSourceConfig,
        batch_size: usize,
        read_timeout: Duration,
        handler_rx: mpsc::Receiver<NatsActorMessage>,
    ) -> Result<()> {
        let mut conn_opts = ConnectOptions::new()
            .max_reconnects(None) // unlimited reconnects
            .reconnect_delay_callback(|attempts| {
                std::time::Duration::from_millis(std::cmp::min((attempts * 10) as u64, 1000))
            })
            .ping_interval(Duration::from_secs(3))
            .retry_on_initial_connect();
        if let Some(auth) = config.auth {
            conn_opts = match auth {
                NatsAuth::Basic { username, password } => {
                    conn_opts.user_and_password(username, password)
                }
                NatsAuth::NKey(nkey) => conn_opts.nkey(nkey),
                NatsAuth::Token(token) => conn_opts.token(token),
            };
        }
        if let Some(tls_config) = config.tls {
            conn_opts = tls::configure_tls(conn_opts, tls_config)?;
        }
        let client = async_nats::connect_with_options(&config.addr, conn_opts)
            .await
            .map_err(|err| Error::Connection {
                server: config.addr.to_string(),
                error: err.to_string(),
            })?;

        // Subscribe to the subject/queue
        let sub = client
            .queue_subscribe(config.subject.clone(), config.queue.clone())
            .await
            .map_err(|err| Error::Subscription {
                subject: config.subject.clone(),
                queue: config.queue.clone(),
                error: err.to_string(),
            })?;

        tokio::spawn(async move {
            let mut actor = NatsActor {
                sub,
                read_timeout,
                batch_size,
                handler_rx,
            };
            tracing::info!(subject=?config.subject, queue=?config.queue, "Starting NATS source actor...");
            actor.run().await;
        });

        Ok(())
    }

    async fn run(&mut self) {
        while let Some(msg) = self.handler_rx.recv().await {
            self.handle_message(msg).await;
        }
    }

    /// Reads messages from the NATS subscriber, up to batch_size or until timeout
    async fn read_messages(&mut self) -> Result<Vec<NatsMessage>> {
        let mut messages: Vec<NatsMessage> = Vec::with_capacity(self.batch_size);
        let timeout = tokio::time::timeout(self.read_timeout, std::future::pending::<()>());
        tokio::pin!(timeout);
        loop {
            if messages.len() >= self.batch_size {
                break;
            }
            tokio::select! {
                biased;

                _ = &mut timeout => {
                    tracing::debug!(msg_count = messages.len(), "Timed out waiting for NATS messages");
                    break;
                }
                maybe_msg = self.sub.next() => {
                    let Some(msg) = maybe_msg else {
                        break;
                    };
                    let nats_msg = NatsMessage::try_from(msg)?;
                    messages.push(nats_msg);
                }
            }
        }
        tracing::debug!(msg_count = messages.len(), "Read messages from NATS");
        Ok(messages)
    }

    async fn handle_message(&mut self, msg: NatsActorMessage) {
        match msg {
            NatsActorMessage::Read { respond_to } => {
                let messages = self.read_messages().await;
                let _ = respond_to.send(messages);
            }
        }
    }
}

#[derive(Clone)]
pub struct NatsSource {
    actor_tx: mpsc::Sender<NatsActorMessage>,
}

impl NatsSource {
    pub async fn connect(
        config: NatsSourceConfig,
        batch_size: usize,
        read_timeout: Duration,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(10);
        NatsActor::start(config, batch_size, read_timeout, rx).await?;
        Ok(Self { actor_tx: tx })
    }

    pub async fn read_messages(&self) -> Result<Vec<NatsMessage>> {
        let (tx, rx) = oneshot::channel();
        let msg = NatsActorMessage::Read { respond_to: tx };
        let _ = self.actor_tx.send(msg).await;
        rx.await
            .map_err(|_| Error::Other("Actor task terminated".into()))?
    }
}
