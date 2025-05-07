use aws_config::meta::region::RegionProviderChain;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::Error::ActorTaskTerminated;
use crate::{Error, Result};

pub const SQS_DEFAULT_REGION: &str = "us-west-2";

#[derive(Clone, Debug, PartialEq)]
pub struct SqsSinkConfig {
    pub region: String,
    pub queue_name: String,
    pub queue_owner_aws_account_id: String,
}

impl SqsSinkConfig {
    #[allow(clippy::result_large_err)]
    pub fn validate(&self) -> Result<()> {
        // Validate required fields
        if self.region.is_empty() {
            return Err(Error::InvalidConfig("region is required".to_string()));
        }

        if self.queue_name.is_empty() {
            return Err(Error::InvalidConfig("queue name is required".to_string()));
        }
        
        if self.queue_owner_aws_account_id.is_empty() {
            return Err(Error::InvalidConfig(
                "queue owner AWS account ID is required".to_string(),
            ));
        }
        
        Ok(())
    }
}

/// Creates and configures an SQS client for sink based on the provided configuration.
pub async fn create_sqs_client(config: Option<SqsSinkConfig>) -> Result<Client> {
    tracing::info!(
        "Creating SQS sink client for queue {queue_name} in region {region}",
        region = config.as_ref().map(|c| c.region.clone()).unwrap_or_default(),
        queue_name = config.as_ref().map(|c| c.queue_name.clone()).unwrap_or_default()
    );

    crate::create_sqs_client(config.map(crate::SqsConfig::Sink)).await
}

enum SqsSinkActorMessage {
    SendMessageBatch {
        respond_to: oneshot::Sender<Result<Vec<SqsSinkResponse>>>,
        messages: Vec<SqsSinkMessage>,
    },
}

pub struct SqsSinkMessage {
    pub id: String,
    pub message_body: Bytes,
}

struct SqsSinkActor {
    handler_rx: mpsc::Receiver<SqsSinkActorMessage>,
    client: Client,
    queue_url: String,
}

impl SqsSinkActor {
    pub fn new(
        handler_rx: mpsc::Receiver<SqsSinkActorMessage>,
        client: Client,
        queue_url: String,
    ) -> Self {
        Self {
            handler_rx,
            client,
            queue_url,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.handler_rx.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&self, msg: SqsSinkActorMessage) {
        match msg {
            SqsSinkActorMessage::SendMessageBatch {
                respond_to,
                messages,
            } => {
                let sink_responses = self.sink_message_batch(messages).await;
                respond_to
                    .send(sink_responses)
                    .expect("failed to send response from SqsSinkActorMessage::SendMessageBatch");
            }
        }
    }

    async fn sink_message_batch(
        &self,
        messages: Vec<SqsSinkMessage>,
    ) -> Result<Vec<SqsSinkResponse>> {
        let mut entries = Vec::with_capacity(messages.len());
        let mut sink_message_responses = Vec::with_capacity(messages.len());
        for message in messages {
            let entry = SendMessageBatchRequestEntry::builder()
                .id(message.id)
                .message_body(String::from_utf8_lossy(&message.message_body).to_string())
                .build();

            match entry {
                Ok(entry) => {
                    tracing::debug!("SQS message entry created: {:?}", entry);
                    entries.push(entry);
                }
                Err(err) => {
                    tracing::error!("Failed to create SQS message entry: {:?}", err);
                    return Err(Error::Sqs(err.into()));
                }
            }
        }

        let send_message_batch_output = self
            .client
            .send_message_batch()
            .queue_url(self.queue_url.clone())
            .set_entries(Some(entries))
            .send()
            .await;

        match send_message_batch_output {
            Ok(output) => {
                for succeeded in output.successful {
                    let id = succeeded.id;
                    let status = Ok(());
                    sink_message_responses.push(SqsSinkResponse {
                        id,
                        status,
                        code: None,
                        sender_fault: None,
                    });
                }

                for failed in output.failed {
                    let id = failed.id;
                    let status = Err(Error::Other(failed.message.unwrap_or_default().to_string()));
                    sink_message_responses.push(SqsSinkResponse {
                        id,
                        status,
                        code: Some(failed.code),
                        sender_fault: Some(failed.sender_fault),
                    });
                }

                Ok(sink_message_responses)
            }
            Err(err) => {
                tracing::error!("Failed to send messages: {:?}", err);
                Err(Error::Sqs(err.into()))
            }
        }
    }
}

#[derive(Clone)]
pub struct SqsSink {
    actor_tx: mpsc::Sender<SqsSinkActorMessage>,
}

#[derive(Clone)]
pub struct SqsSinkBuilder {
    config: SqsSinkConfig,
    client: Option<Client>,
}
#[derive(Debug)]
pub struct SqsSinkResponse {
    pub id: String,
    pub status: Result<()>,
    pub code: Option<String>,
    pub sender_fault: Option<bool>,
}

impl Default for SqsSinkBuilder {
    fn default() -> Self {
        Self::new(SqsSinkConfig {
            region: SQS_DEFAULT_REGION.to_string(),
            queue_name: "".to_string(),
            queue_owner_aws_account_id: "".to_string(),
        })
    }
}

impl SqsSinkBuilder {
    pub fn new(config: SqsSinkConfig) -> Self {
        Self {
            config,
            client: None,
        }
    }

    pub fn config(mut self, config: SqsSinkConfig) -> Self {
        self.config = config;
        self
    }

    pub fn client(mut self, client: Client) -> Self {
        self.client = Some(client);
        self
    }

    pub async fn build(self) -> Result<SqsSink> {
        let sqs_client = match self.client {
            Some(client) => client,
            None => create_sqs_client(Some(self.config.clone())).await?,
        };

        let queue_name = self.config.queue_name.clone();
        let queue_owner_aws_account_id = self.config.queue_owner_aws_account_id.clone();

        let get_queue_url_output = sqs_client
            .get_queue_url()
            .queue_name(queue_name)
            .queue_owner_aws_account_id(queue_owner_aws_account_id)
            .send()
            .await
            .map_err(|err| Error::Sqs(err.into()))?;

        let queue_url = get_queue_url_output
            .queue_url
            .ok_or_else(|| Error::Other("Queue URL not found".to_string()))?;

        tracing::info!(queue_url = queue_url.clone(), "Queue URL found");

        let (handler_tx, handler_rx) = mpsc::channel(10);
        tokio::spawn(async move {
            let mut actor = SqsSinkActor::new(handler_rx, sqs_client, queue_url);
            actor.run().await;
        });

        Ok(SqsSink {
            actor_tx: handler_tx,
        })
    }
}

impl SqsSink {
    pub async fn sink_messages(
        &self,
        messages: Vec<SqsSinkMessage>,
    ) -> Result<Vec<SqsSinkResponse>> {
        let (tx, rx) = oneshot::channel();
        let msg = SqsSinkActorMessage::SendMessageBatch {
            respond_to: tx,
            messages,
        };
        if (self.actor_tx.send(msg).await).is_err() {
            tracing::error!("Failed to send message to SqsSinkActor");
        }
        rx.await.map_err(ActorTaskTerminated)?
    }
}
