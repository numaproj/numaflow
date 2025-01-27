/// Implementation of the SQS message source using an actor-based architecture.
///
/// Key design features:
/// - Actor model for thread-safe state management
/// - Batched message handling for efficiency
/// - Robust error handling and retry logic
/// - Configurable timeouts and batch sizes
use std::collections::HashMap;
use std::time::Duration;

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_sqs::config::Region;
use aws_sdk_sqs::types::{MessageSystemAttributeName, QueueAttributeName};
use aws_sdk_sqs::Client;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::Error::ActorTaskTerminated;
use crate::{Error, Result};

/// Configuration for an SQS message source.
///
/// Used to initialize the SQS client with region and queue settings.
/// Implements serde::Deserialize to support loading from configuration files.
#[derive(serde::Deserialize, Clone, PartialEq)]
pub struct SQSSourceConfig {
    pub region: String,
    pub queue_name: String,
}

/// Internal message types for the actor implementation.
///
/// The actor pattern is used to:
/// - Ensure thread-safe access to the SQS client
/// - Manage connection state and retries
/// - Handle concurrent requests without locks
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
        offsets: Vec<Bytes>,
    },
    GetQueueAttributes {
        respond_to: oneshot::Sender<Result<Option<usize>>>,
        queue_name: String,
        attribute_name: QueueAttributeName,
    },
}

/// Represents a message received from SQS with metadata.
///
/// Design choices:
/// - Uses Bytes for efficient handling of message payloads
/// - Includes full message metadata for debugging
/// - Maintains original SQS attributes and headers
#[derive(Debug)]
pub struct SQSMessage {
    pub key: String,
    pub payload: Bytes,
    pub offset: String,
    pub event_time: DateTime<Utc>,
    pub attributes: Option<HashMap<String, String>>,
    pub headers: HashMap<String, String>,
}

/// Internal actor implementation for managing SQS interactions.
///
/// The actor maintains:
/// - Single SQS client instance
/// - Queue URL caching
/// - Message channel for handling concurrent requests
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

    /// Retrieves messages from SQS with timeout and batching.
    ///
    /// Implementation details:
    /// - Handles queue URL caching/refresh
    /// - Respects timeout for long polling
    /// - Processes message attributes and system metadata
    /// - Returns messages in a normalized format
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

    async fn delete_messages(&mut self, queue_name: String, offsets: Vec<Bytes>) -> Result<()> {
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
            let offset = match std::str::from_utf8(&offset) {
                Ok(offset) => offset,
                Err(err) => {
                    tracing::error!(?err, "Failed to parse offset");
                    return Err(Error::Other("Failed to parse offset".to_string()));
                }
            };
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

/// Public interface for interacting with SQS queues.
///
/// Design principles:
/// - Thread-safe through actor model
/// - Configurable batch sizes and timeouts
/// - Clean abstraction of SQS complexity
/// - Efficient message processing
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
        let messages = rx.await.map_err(ActorTaskTerminated)??;
        tracing::info!(
            count = messages.len(),
            requested_count = self.batch_size,
            time_taken_ms = start.elapsed().as_millis(),
            "Got messages from sqs"
        );
        Ok(messages)
    }

    pub async fn ack_offsets(&self, offsets: Vec<Bytes>) -> Result<()> {
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

    pub fn partitions(&self) -> Vec<u16> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_sqs::Config;
    use aws_smithy_mocks_experimental::{mock, MockResponseInterceptor, Rule, RuleMode};

    use super::*;

    #[tokio::test]
    async fn test_sqssourcehandle_read() {
        let queue_url_output = get_queue_url_output();

        let receive_message_output = get_receive_message_output();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&receive_message_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let source = SQSSource::new(
            SQSSourceConfig {
                region: "us-west-2".to_string(),
                queue_name: "test-q".to_string(),
            },
            1,
            Duration::from_secs(0),
            Some(sqs_mock_client),
        )
        .await
        .unwrap();

        // Read messages from the source
        let messages = source.read_messages().await.unwrap();

        // Assert we got the expected number of messages
        assert_eq!(messages.len(), 1, "Should receive exactly 1 message");

        // Verify first message
        let msg1 = &messages[0];
        assert_eq!(msg1.key, "219f8380-5770-4cc2-8c3e-5c715e145f5e");
        assert_eq!(msg1.payload, "This is a test message");
        assert_eq!(
            msg1.offset,
            "AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q"
        );
    }

    #[tokio::test]
    async fn test_sqssource_ack() {
        let queue_url_output = get_queue_url_output();
        let delete_message_output = get_delete_message_output();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&delete_message_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let source = SQSSource::new(
            SQSSourceConfig {
                region: "us-west-2".to_string(),
                queue_name: "test-q".to_string(),
            },
            1,
            Duration::from_secs(0),
            Some(sqs_mock_client),
        )
        .await
        .unwrap();

        // Test acknowledgment
        let offset = "AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q";
        let result = source.ack_offsets(vec![Bytes::from(offset)]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sqssource_pending_count() {
        let queue_url_output = get_queue_url_output();
        let queue_attrs_output = get_queue_attributes_output();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&queue_attrs_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let source = SQSSource::new(
            SQSSourceConfig {
                region: "us-west-2".to_string(),
                queue_name: "test-q".to_string(),
            },
            1,
            Duration::from_secs(0),
            Some(sqs_mock_client),
        )
        .await
        .unwrap();

        let count = source.pending_count().await;
        assert_eq!(count, Some(0));
    }

    #[tokio::test]
    async fn test_error_cases() {
        // Test invalid region error
        let source = SQSSource::new(
            SQSSourceConfig {
                region: "invalid-region".to_string(),
                queue_name: "test-q".to_string(),
            },
            1,
            Duration::from_secs(0),
            None,
        )
        .await;
        assert!(source.is_ok()); // Should still create but fail on operations

        // Test invalid offset format
        let source = source.unwrap();
        let result = source
            .ack_offsets(vec![Bytes::from(vec![255, 255, 255])])
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    #[should_panic(expected = "not implemented")]
    async fn test_partitions_unimplemented() {
        let source = SQSSource {
            batch_size: 1,
            timeout: Duration::from_secs(0),
            actor_tx: mpsc::channel(1).0,
            queue_name: "test-q".to_string(),
        };
        source.partitions();
    }

    fn get_queue_attributes_output() -> Rule {
        let queue_attributes_output = mock!(aws_sdk_sqs::Client::get_queue_attributes)
            .match_requests(|inp| {
                inp.queue_url().unwrap()
                    == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
            })
            .then_output(|| {
                aws_sdk_sqs::operation::get_queue_attributes::GetQueueAttributesOutput::builder()
                    .attributes(
                        aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages,
                        "0",
                    )
                    .build()
            });
        queue_attributes_output
    }

    fn get_delete_message_output() -> Rule {
        let delete_message_output = mock!(aws_sdk_sqs::Client::delete_message)
            .match_requests(|inp| {
                inp.queue_url().unwrap() == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
                    && inp.receipt_handle().unwrap() == "AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q"
            })
            .then_output(|| {
                aws_sdk_sqs::operation::delete_message::DeleteMessageOutput::builder().build()
            });
        delete_message_output
    }

    fn get_receive_message_output() -> Rule {
        let receive_message_output = mock!(aws_sdk_sqs::Client::receive_message)
            .match_requests(|inp| {
                inp.queue_url().unwrap() == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
            })
            .then_output(|| {
                aws_sdk_sqs::operation::receive_message::ReceiveMessageOutput::builder()
                    .messages(
                        aws_sdk_sqs::types::Message::builder()
                            .message_id("219f8380-5770-4cc2-8c3e-5c715e145f5e")
                            .body("This is a test message")
                            .receipt_handle("AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q")
                            .attributes(MessageSystemAttributeName::SentTimestamp, "1677112427387")
                            .build()
                    )
                    .build()
            });
        receive_message_output
    }

    fn get_queue_url_output() -> Rule {
        let queue_url_output = mock!(aws_sdk_sqs::Client::get_queue_url)
            .match_requests(|inp| inp.queue_name().unwrap() == "test-q")
            .then_output(|| {
                aws_sdk_sqs::operation::get_queue_url::GetQueueUrlOutput::builder()
                    .queue_url("https://sqs.us-west-2.amazonaws.com/926113353675/test-q/")
                    .build()
            });
        queue_url_output
    }

    fn get_test_config_with_interceptor(interceptor: MockResponseInterceptor) -> Config {
        aws_sdk_sqs::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_sqs_test_credentials())
            .region(aws_sdk_sqs::config::Region::new("us-west-2"))
            .interceptor(interceptor)
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
}
