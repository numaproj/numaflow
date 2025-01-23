use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_sqs::config::Region;
use aws_sdk_sqs::types::{MessageSystemAttributeName, QueueAttributeName};
use aws_sdk_sqs::{Client};
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};

use crate::{Error, Result};

#[derive(serde::Deserialize, Clone, PartialEq)]
pub struct SQSSourceConfig {
    pub region: String,
    pub queue_name: String,
}

enum SQSActorMessage {
    Receive {
        respond_to: oneshot::Sender<Result<Vec<SQSMessage>>>,
        queue_name: String,
        count: i32,
        timeout_at: Instant,
    },
    Delete {
        respond_to: oneshot::Sender<Result<()>>,
        queue_name: String,
        offsets: Vec<String>,
    },
    GetQueueAttributes {
        respond_to: oneshot::Sender<Result<Option<usize>>>,
        queue_name: String,
        attribute_name: QueueAttributeName,
    },
}

#[derive(Debug)]
pub struct SQSMessage {
    pub key: String,
    pub payload: Bytes,
    pub offset: String,
    pub event_time: DateTime<Utc>,
    pub attributes: Option<HashMap<String, String>>,
    pub headers: HashMap<String, String>,
}

struct SQSActor {
    handler_rx: mpsc::Receiver<SQSActorMessage>,
    client: Client,
    queue_url: String,
}

impl SQSActor {
    async fn start(
        handler_rx: mpsc::Receiver<SQSActorMessage>,
        client: Option<Client>,
        config: Option<SQSSourceConfig>,
    ) -> Result<()> {
        let sqs_client = match client {
            Some(client) => client,
            None => Self::get_sqs_client(config).await,
        };

        // spawn actor
        tokio::spawn(async move {
            let mut actor = SQSActor {
                handler_rx,
                client: sqs_client,
                queue_url: "".to_string(),
            };
            actor.run().await;
        });

        Ok(())
    }

    async fn get_sqs_client(config: Option<SQSSourceConfig>) -> Client {
        let region = match config {
            Some(config) => config.region.clone(),
            None => "us-west-2".to_string(),
        };

        tracing::info!(region = region.clone(), "Creating SQS client in region");

        // read aws config
        let region_provider = RegionProviderChain::first_try(Region::new(region.clone()))
            .or_default_provider()
            .or_else(Region::new("us-west-2"));

        let shared_config = aws_config::defaults(BehaviorVersion::v2024_03_28())
            .region(region_provider)
            .load()
            .await;

        // create sqs client
        Client::new(&shared_config)
    }

    async fn run(&mut self) {
        while let Some(msg) = self.handler_rx.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn get_queue_url(&mut self, queue_name: String) -> Result<()> {
        tracing::debug!(queue_name = queue_name.clone(), "Getting queue URL");
        if !self.queue_url.is_empty() && self.queue_url.contains(queue_name.as_str()) {
            return Ok(());
        }

        let get_queue_url_output = self
            .client
            .get_queue_url()
            .queue_name(queue_name.clone())
            .send()
            .await;

        match get_queue_url_output {
            Ok(result) => {
                self.queue_url = result.queue_url.unwrap_or_default();
                tracing::debug!(queue_url = self.queue_url.clone(), "Got queue URL");
                Ok(())
            }
            Err(err) => {
                tracing::error!(?err, queue_name, "Error getting queue URL");
                Err(Error::SQS(err.into()))
            }
        }
    }

    async fn handle_message(&mut self, msg: SQSActorMessage) {
        match msg {
            SQSActorMessage::Receive {
                respond_to,
                queue_name,
                count,
                timeout_at,
            } => {
                let messages = self.get_messages(queue_name, count, timeout_at).await;
                let _ = respond_to.send(messages);
            }
            SQSActorMessage::Delete {
                respond_to,
                queue_name,
                offsets,
            } => {
                let status = self.delete_messages(queue_name, offsets).await;
                let _ = respond_to.send(status);
            }
            SQSActorMessage::GetQueueAttributes {
                respond_to,
                queue_name,
                attribute_name,
            } => match attribute_name {
                QueueAttributeName::ApproximateNumberOfMessages => {
                    let status = self.get_pending_messages(queue_name).await;
                    let _ = respond_to.send(status);
                }
                _ => {
                    tracing::error!("Unsupported attribute name");
                    let _ = respond_to
                        .send(Err(Error::Other("Unsupported attribute name".to_string())));
                }
            },
        }
    }

    async fn get_messages(
        &mut self,
        queue_name: String,
        count: i32,
        timeout_at: Instant,
    ) -> Result<Vec<SQSMessage>> {
        let get_queue_url_response = self.get_queue_url(queue_name.clone()).await;

        if let Err(err) = get_queue_url_response {
            tracing::error!(
                ?err,
                queue_name = queue_name.clone(),
                "Failed to get queue url"
            );
            return Err(err);
        }

        let remaining_time = timeout_at - Instant::now();

        let sdk_response = self
            .client
            .receive_message()
            .queue_url(self.queue_url.clone())
            .max_number_of_messages(count)
            .message_attribute_names("All")
            .wait_time_seconds(remaining_time.as_secs() as i32)
            .send()
            .await;

        let receive_message_output = match sdk_response {
            Ok(output) => output,
            Err(err) => {
                tracing::error!(
                    ?err,
                    queue_url = self.queue_url,
                    "Failed to receive messages from SQS"
                );
                return Err(Error::SQS(err.into()));
            }
        };

        let messages = receive_message_output
            .messages
            .unwrap_or_default()
            .iter()
            .map(|msg| {
                let key = msg.message_id.clone().unwrap_or_default();
                let payload = Bytes::from(msg.body.clone().unwrap_or_default());
                let offset = msg.receipt_handle.clone().unwrap_or_default();
                let event_time = msg
                    .attributes
                    .as_ref()
                    .and_then(|attrs| attrs.get(&MessageSystemAttributeName::SentTimestamp))
                    .and_then(|timestamp| timestamp.parse::<i64>().ok())
                    .map(|timestamp| Utc.timestamp_millis_opt(timestamp).single())
                    .flatten()
                    .unwrap_or_else(Utc::now);
                let attributes = msg.message_attributes.as_ref().map(|attrs| {
                    attrs
                        .iter()
                        .map(|(k, v)| (k.clone(), v.string_value.clone().unwrap_or_default()))
                        .collect()
                });
                let headers = msg
                    .attributes
                    .as_ref()
                    .map(|attrs| {
                        attrs
                            .iter()
                            .map(|(k, v)| (k.to_string().clone(), v.to_string().clone()))
                            .collect()
                    })
                    .unwrap_or_default();
                SQSMessage {
                    key,
                    payload,
                    offset,
                    event_time,
                    attributes,
                    headers,
                }
            })
            .collect();

        Ok(messages)
    }

    async fn delete_messages(&mut self, queue_name: String, offsets: Vec<String>) -> Result<()> {
        let get_queue_url_response = self.get_queue_url(queue_name.clone()).await;

        if let Err(err) = get_queue_url_response {
            tracing::error!(
                ?err,
                queue_name = queue_name.clone(),
                "Failed to get queue url"
            );
            return Err(err);
        }

        for offset in offsets {
            if let Err(err) = self
                .client
                .delete_message()
                .queue_url(self.queue_url.clone())
                .receipt_handle(offset)
                .send()
                .await
            {
                tracing::error!(
                    ?err,
                    "{} {}",
                    self.queue_url.clone(),
                    "Error while deleting message from SQS"
                );
                return Err(Error::SQS(err.into()));
            }
        }

        Ok(())
    }

    async fn get_pending_messages(&mut self, queue_name: String) -> Result<Option<usize>> {
        let get_queue_url_response = self.get_queue_url(queue_name.clone()).await;

        if let Err(err) = get_queue_url_response {
            tracing::error!(
                ?err,
                queue_name = queue_name.clone(),
                "Failed to get queue url"
            );
            return Err(err);
        }

        let sdk_response = self
            .client
            .get_queue_attributes()
            .queue_url(self.queue_url.clone())
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await;

        let get_queue_attributes_output = match sdk_response {
            Ok(output) => output,
            Err(err) => {
                tracing::error!(
                    ?err,
                    queue_url = self.queue_url,
                    "Failed to get queue attributes from SQS"
                );
                return Err(Error::SQS(err.into()));
            }
        };

        let attributes = match get_queue_attributes_output.attributes {
            Some(attributes) => attributes,
            None => return Ok(None),
        };

        let value = match attributes.get(&QueueAttributeName::ApproximateNumberOfMessages) {
            Some(value) => value,
            None => return Ok(None),
        };

        let approx_pending_messages_count = match value.parse::<usize>() {
            Ok(count) => count,
            Err(err) => {
                tracing::error!(?err, "Failed to parse ApproximateNumberOfMessages");
                return Err(Error::Other(
                    "Failed to parse ApproximateNumberOfMessages".to_string(),
                ));
            }
        };
        Ok(Some(approx_pending_messages_count))
    }
}

#[derive(Clone)]
pub struct SQSSource {
    batch_size: usize,
    /// timeout for each batch read request
    timeout: Duration,
    actor_tx: mpsc::Sender<SQSActorMessage>,
    queue_name: String,
}

impl SQSSource {
    pub async fn new(
        config: SQSSourceConfig,
        batch_size: usize,
        timeout: Duration,
        client: Option<Client>,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(10);
        SQSActor::start(rx, client, Some(config.clone())).await?;
        Ok(Self {
            actor_tx: tx,
            queue_name: config.queue_name.clone(),
            batch_size,
            timeout,
        })
    }
}

impl SQSSource {
    pub async fn read_messages(&self) -> Result<Vec<SQSMessage>> {
        tracing::debug!("Reading messages from SQS");
        let start = Instant::now();
        let (tx, rx) = oneshot::channel();

        let msg = SQSActorMessage::Receive {
            respond_to: tx,
            queue_name: self.queue_name.clone(),
            count: self.batch_size as i32,
            timeout_at: Instant::now() + self.timeout,
        };

        let _ = self.actor_tx.send(msg).await;
        let messages = rx.await.map_err(Error::ActorTaskTerminated)??;
        tracing::info!(
            count = messages.len(),
            requested_count = self.batch_size,
            time_taken_ms = start.elapsed().as_millis(),
            "Got messages from sqs"
        );
        Ok(messages)
    }

    pub async fn ack_offsets(&self, offsets: Vec<String>) -> Result<()> {
        tracing::debug!(offsets = ?offsets, "Acknowledging offsets");
        let (tx, rx) = oneshot::channel();
        let msg = SQSActorMessage::Delete {
            offsets,
            queue_name: self.queue_name.clone(),
            respond_to: tx,
        };
        let _ = self.actor_tx.send(msg).await;
        rx.await.map_err(Error::ActorTaskTerminated)?
    }

    pub async fn pending_count(&self) -> Option<usize> {
        tracing::debug!("Getting pending count");
        let (tx, rx) = oneshot::channel();
        let msg = SQSActorMessage::GetQueueAttributes {
            queue_name: self.queue_name.clone(),
            respond_to: tx,
            attribute_name: QueueAttributeName::ApproximateNumberOfMessages,
        };
        let _ = self.actor_tx.send(msg).await;

        let actor_result = rx.await.map_err(Error::ActorTaskTerminated);

        match actor_result {
            Ok(Ok(Some(count))) => Some(count),
            _ => None,
        }
    }

    pub fn partitions(&self) ->  Vec<u16> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;

    #[tokio::test]
    async fn test_sqssourcehandle_read() {
        // Test case 1: Successful message retrieval
        // Verifies that we can successfully:
        // - Get the queue URL
        // - Receive multiple messages
        // - Parse and forward messages correctly
        {
            let queue_url = get_queue_url_request_response();
            let message = get_messages_request_response();

            let replay_client = StaticReplayClient::new(vec![queue_url, message]);
            let config = get_test_config(replay_client.clone());
            let client = aws_sdk_sqs::Client::from_conf(config);

            let source = SQSSource::new(
                SQSSourceConfig {
                    region: "us-west-2".to_string(),
                    queue_name: "test-q".to_string(),
                },
                2,
                Duration::from_secs(0),
                Some(client),
            ).await.unwrap();

            // Read messages from the source
            let messages = source.read_messages().await.unwrap();

            // Assert we got the expected number of messages
            assert_eq!(messages.len(), 2, "Should receive exactly 2 messages");

            // Verify first message
            let msg1 = &messages[0];
            assert_eq!(msg1.key, "219f8380-5770-4cc2-8c3e-5c715e145f5e");
            assert_eq!(msg1.payload, "This is a test message");
            assert_eq!(
                msg1.offset,
                "AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q"
            );

            // Verify second message
            let msg2 = &messages[1];
            assert_eq!(msg2.key, "219f8380-5770-4cc2-8c3e-5c715e145f5e");
            assert_eq!(msg2.payload, "This is a second test message");
            assert_eq!(
                msg2.offset,
                "AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q"
            );
        }
    }

    fn get_test_config(replay_client: StaticReplayClient) -> aws_sdk_sqs::Config {
        aws_sdk_sqs::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_sqs_test_credentials())
            .region(aws_sdk_sqs::config::Region::new("us-west-2"))
            .http_client(replay_client.clone())
            .build()
    }

    fn make_sqs_test_credentials() -> aws_sdk_sqs::config::Credentials {
        aws_sdk_sqs::config::Credentials::new(
            "ATESTCLIENT",
            "astestsecretkey",
            Some("atestsessiontoken".to_string()),
            None,
            "",
        )
    }

    fn get_messages_request_response() -> ReplayEvent {
        ReplayEvent::new(
            http::Request::builder()
                .method("POST")
                .uri(http::uri::Uri::from_static("https://sqs.us-west-2.amazonaws.com/"))
                .header("Content-Type", "application/x-amz-json-1.0")
                .body(SdkBody::from(r#"{"QueueUrl": "https://sqs.us-west-2.amazonaws.com/926113353675/test-q", "MaxNumberOfMessages": 2, "MessageAttributeNames": ["All"], "WaitTimeSeconds":0}"#))
                .unwrap(),
            http::Response::builder()
                .status(http::StatusCode::from_u16(200).unwrap())
                .header("Content-Type", "application/x-amz-json-1.0")
                .body(SdkBody::from(r#"{
    "Messages": [
        {
            "Attributes": {
                "SenderId": "AIDASSYFHUBOBT7F4XT75",
                "ApproximateFirstReceiveTimestamp": "1677112433437",
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1677112427387"
            },
            "Body": "This is a test message",
            "MD5OfBody": "fafb00f5732ab283681e124bf8747ed1",
            "MessageId": "219f8380-5770-4cc2-8c3e-5c715e145f5e",
            "ReceiptHandle": "AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q"
        },
        {
            "Attributes": {
                "SenderId": "AIDASSYFHUBOBT7F4XT75",
                "ApproximateFirstReceiveTimestamp": "1677112433437",
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1677112427387"
            },
            "Body": "This is a second test message",
            "MD5OfBody": "fafb00f5732ab283681e124bf8747ed1",
            "MessageId": "219f8380-5770-4cc2-8c3e-5c715e145f5e",
            "ReceiptHandle": "AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q"
        }
    ]
}"#))
                .unwrap(),
        )
    }

    fn get_queue_url_request_response() -> ReplayEvent {
        ReplayEvent::new(
            http::Request::builder()
                .method("POST")
                .uri(http::uri::Uri::from_static(
                    "https://sqs.us-west-2.amazonaws.com/",
                ))
                .header("Content-Type", "application/x-amz-json-1.0")
                .body(SdkBody::from(r#"{"QueueName": "test-q"}"#))
                .unwrap(),
            http::Response::builder()
                .status(http::StatusCode::from_u16(200).unwrap())
                .header("Content-Type", "application/x-amz-json-1.0")
                .body(SdkBody::from(
                    r#"{
                "QueueUrl": "https://sqs.us-west-2.amazonaws.com/926113353675/test-q"
            }"#,
                ))
                .unwrap(),
        )
    }
}
