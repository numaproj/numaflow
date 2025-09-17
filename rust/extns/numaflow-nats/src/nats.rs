use std::time::Duration;

use async_nats::ConnectOptions;
use bytes::Bytes;
use chrono::DateTime;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use tokio_stream::StreamExt;
use tracing::debug;
use uuid::Uuid;

use crate::{Error, NatsAuth, Result, TlsConfig, tls};

#[derive(Debug, Clone, PartialEq)]
pub struct NatsSourceConfig {
    pub addr: String,
    pub subject: String,
    pub queue: String,
    pub auth: Option<NatsAuth>,
    pub tls: Option<TlsConfig>,
}

/// NatsMessage represents a Numaflow Message which can be converted from a Nats message
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
            value: msg.payload,
            id: Uuid::new_v4().to_string(),
            event_time: chrono::Utc::now(),
        })
    }
}

/// NatsActorMessage represents a message sent to the NatsActor
enum NatsActorMessage {
    Read {
        respond_to: oneshot::Sender<Option<Result<Vec<NatsMessage>>>>,
    },
}

/// NatsActor represents a NATS subscriber actor
struct NatsActor {
    sub: async_nats::Subscriber,
    read_timeout: Duration,
    batch_size: usize,
    handler_rx: mpsc::Receiver<NatsActorMessage>,
    cancel_token: CancellationToken,
}

impl NatsActor {
    async fn start(
        config: NatsSourceConfig,
        batch_size: usize,
        read_timeout: Duration,
        handler_rx: mpsc::Receiver<NatsActorMessage>,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        let mut conn_opts = ConnectOptions::new()
            .max_reconnects(None) // unlimited reconnects
            .reconnect_delay_callback(|attempts| {
                std::time::Duration::from_millis(std::cmp::min((attempts * 10) as u64, 1000))
            })
            .ping_interval(Duration::from_secs(3))
            .retry_on_initial_connect(); // run in background
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

        // Wait for the subscription to be ready
        let sub = client
            .queue_subscribe(config.subject.clone(), config.queue.clone())
            .await
            .map_err(|err| Error::Subscription {
                subject: config.subject.clone(),
                queue: config.queue.clone(),
                error: err.to_string(),
            })?;

        // Spawn the NATS actor
        tokio::spawn(async move {
            let mut actor = NatsActor {
                sub,
                read_timeout,
                batch_size,
                handler_rx,
                cancel_token,
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
    async fn read_messages(&mut self) -> Option<Result<Vec<NatsMessage>>> {
        if self.cancel_token.is_cancelled() {
            return None;
        }
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
                    let nats_msg = match NatsMessage::try_from(msg) {
                        Ok(msg) => msg,
                        Err(e) => return Some(Err(e)),
                    };
                    messages.push(nats_msg);
                }
            }
        }
        debug!(msg_count = messages.len(), "Read messages from NATS");
        Some(Ok(messages))
    }

    /// Handles messages sent to the NatsActor
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
        cancel_token: CancellationToken,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(10);
        NatsActor::start(config, batch_size, read_timeout, rx, cancel_token).await?;
        Ok(Self { actor_tx: tx })
    }

    pub async fn read_messages(&self) -> Option<Result<Vec<NatsMessage>>> {
        let (tx, rx) = oneshot::channel();
        let msg = NatsActorMessage::Read { respond_to: tx };
        let _ = self.actor_tx.send(msg).await;
        rx.await
            .unwrap_or_else(|_| Some(Err(Error::Other("Actor task terminated".into()))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_nats_source() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let subject = "nats_source_test_subject";
        let queue = "nats_source_test_queue";
        let msg_count = 5;

        let config = NatsSourceConfig {
            addr: "localhost".to_string(),
            subject: subject.to_string(),
            queue: queue.to_string(),
            auth: None,
            tls: None,
        };

        // Connect to the NATS server with batch size and read timeout
        let read_timeout = Duration::from_secs(1);
        let source = NatsSource::connect(config, 2, read_timeout, CancellationToken::new())
            .await
            .unwrap();
        // Wait for NATS Actor to start and subscribe to the subject with queue
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Publish messages
        let client = async_nats::connect("localhost").await.unwrap();
        for i in 0..msg_count {
            client
                .publish(subject, format!("message {}", i).into())
                .await
                .unwrap();
        }

        // Read the first batch
        // Read Messages loop will break when batch size is reached in this case
        let messages = source.read_messages().await.unwrap().unwrap();
        assert_eq!(messages.len(), 2);

        // Read the second batch
        // Read Messages loop will break when batch size is reached in this case
        let messages = source.read_messages().await.unwrap().unwrap();
        assert_eq!(messages.len(), 2);

        // Read the third batch
        // Read Messages loop will break when timeout is reached in this case
        // as batch size is 2 and remaining messages are 1
        let messages = source.read_messages().await.unwrap().unwrap();
        assert_eq!(messages.len(), 1);

        // Should be empty after all messages are read
        let messages = source.read_messages().await.unwrap().unwrap();
        assert!(
            messages.is_empty(),
            "No messages should be returned after all messages are read"
        );
    }
}
