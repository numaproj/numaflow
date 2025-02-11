//! Implementation of the SQS message source using an actor-based architecture.
//!
//! Key design features:
//! - Actor model for thread-safe state management
//! - Batched message handling for efficiency
//! - Robust error handling and retry logic
//! - Configurable timeouts and batch sizes
use std::collections::HashMap;
use std::time::Duration;

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_sqs::config::Region;
use aws_sdk_sqs::types::{MessageSystemAttributeName, QueueAttributeName};
use aws_sdk_sqs::Client;
use aws_smithy_types::timeout::TimeoutConfig;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::Error::ActorTaskTerminated;
use crate::{Error, Result};

pub const SQS_DEFAULT_REGION: &str = "us-west-2";

/// Configuration for an SQS message source.
///
/// Used to initialize the SQS client with region and queue settings.
/// Implements serde::Deserialize to support loading from configuration files.
/// TODO: add support for all sqs configs and different ways to authenticate
#[derive(serde::Deserialize, Clone, PartialEq)]
pub struct SqsSourceConfig {
    pub region: String,
    pub queue_name: String,
}

/// Internal message types for the actor implementation.
///
/// The actor pattern is used to:
/// - Ensure thread-safe access to the SQS client
/// - Manage connection state and retries
/// - Handle concurrent requests without locks
enum SqsActorMessage {
    Receive {
        respond_to: oneshot::Sender<Result<Vec<SqsMessage>>>,
        count: i32,
        timeout_at: Instant,
    },
    Delete {
        respond_to: oneshot::Sender<Result<()>>,
        offsets: Vec<Bytes>,
    },
    GetPending {
        respond_to: oneshot::Sender<Result<Option<usize>>>,
    },
}

/// Represents a message received from SQS with metadata.
///
/// Design choices:
/// - Uses Bytes for efficient handling of message payloads
/// - Includes full message metadata for debugging
/// - Maintains original SQS attributes and headers
#[derive(Debug)]
pub struct SqsMessage {
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
/// - Message channel for handling concurrent requests
struct SqsActor {
    handler_rx: mpsc::Receiver<SqsActorMessage>,
    client: Client,
    queue_url: String,
}

impl SqsActor {
    fn new(handler_rx: mpsc::Receiver<SqsActorMessage>, client: Client, queue_url: String) -> Self {
        Self {
            handler_rx,
            client,
            queue_url,
        }
    }

    async fn create_sqs_client(config: Option<SqsSourceConfig>) -> Client {
        let region = match config {
            Some(config) => config.region.clone(),
            None => SQS_DEFAULT_REGION.to_string(),
        };

        tracing::info!(region = region.clone(), "Creating SQS client in region");

        // read aws config
        let region_provider = RegionProviderChain::first_try(Region::new(region.clone()))
            .or_default_provider()
            .or_else(Region::new(SQS_DEFAULT_REGION));

        let shared_config = aws_config::defaults(BehaviorVersion::v2024_03_28())
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_attempt_timeout(Duration::from_secs(10))
                    .build(),
            )
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

    async fn handle_message(&mut self, msg: SqsActorMessage) {
        match msg {
            SqsActorMessage::Receive {
                respond_to,
                count,
                timeout_at,
            } => {
                let messages = self.get_messages(count, timeout_at).await;
                respond_to
                    .send(messages)
                    .expect("failed to send response from SqsActorMessage::Receive");
            }
            SqsActorMessage::Delete {
                respond_to,
                offsets,
            } => {
                let status = self.delete_messages(offsets).await;
                respond_to
                    .send(status)
                    .expect("failed to send response from SqsActorMessage::Delete");
            }
            SqsActorMessage::GetPending { respond_to } => {
                let status = self.get_pending_messages().await;
                respond_to
                    .send(status)
                    .expect("failed to send response from SqsActorMessage::GetPending");
            }
        }
    }

    /// Retrieves messages from SQS with timeout and batching.
    ///
    /// Implementation details:
    /// - Respects timeout for long polling
    /// - Processes message attributes and system metadata
    /// - Returns messages in a normalized format
    async fn get_messages(&mut self, count: i32, timeout_at: Instant) -> Result<Vec<SqsMessage>> {
        let remaining_time = timeout_at - Instant::now();

        // default to one second if remaining time is less than one second
        // as sqs sdk requires wait_time_seconds to be at least 1
        // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-short-long-polling-differences
        // TODO: find a better way to handle user input timeout. should allow the users
        // to choose long/short polling. For now, we default to 1 second (long polling).
        let remaining_time = if remaining_time.as_millis() < 1000 {
            Duration::from_secs(1)
        } else {
            remaining_time
        };

        let sdk_response = self
            .client
            .receive_message()
            .queue_url(self.queue_url.clone())
            .max_number_of_messages(count)
            .message_attribute_names("All")
            .message_system_attribute_names(MessageSystemAttributeName::All)
            .wait_time_seconds(remaining_time.as_secs() as i32)
            .send()
            .await;

        let receive_message_output = match sdk_response {
            Ok(output) => output,
            Err(err) => {
                tracing::error!(
                    ?err,
                    queue_url = self.queue_url,
                    "failed to receive messages from SQS"
                );
                return Err(Error::Sqs(err.into()));
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

                // event_time is set to match the SentTimestamp attribute if available
                // see: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html#API_ReceiveMessage_RequestSyntax
                let event_time = msg
                    .attributes
                    .as_ref()
                    .and_then(|attrs| attrs.get(&MessageSystemAttributeName::SentTimestamp))
                    .and_then(|timestamp| timestamp.parse::<i64>().ok())
                    .and_then(|timestamp| Utc.timestamp_millis_opt(timestamp).single())
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

                SqsMessage {
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

    // delete message from SQS, serves as Numaflow source ack.
    async fn delete_messages(&mut self, offsets: Vec<Bytes>) -> Result<()> {
        for offset in offsets {
            let offset = match std::str::from_utf8(&offset) {
                Ok(offset) => offset,
                Err(err) => {
                    tracing::error!(?err, "failed to parse offset");
                    return Err(Error::Other("failed to parse offset".to_string()));
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
                return Err(Error::Sqs(err.into()));
            }
        }
        Ok(())
    }

    // get the pending message count from SQS using the ApproximateNumberOfMessages attribute
    // Note: The ApproximateNumberOfMessages metrics may not achieve consistency until at least
    // 1 minute after the producers stop sending messages.
    // This period is required for the queue metadata to reach eventual consistency.
    async fn get_pending_messages(&mut self) -> Result<Option<usize>> {
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
                    "failed to get queue attributes from SQS"
                );
                return Err(Error::Sqs(err.into()));
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
                tracing::error!(?err, "failed to parse ApproximateNumberOfMessages");
                return Err(Error::Other(
                    "failed to parse ApproximateNumberOfMessages".to_string(),
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
pub struct SqsSource {
    batch_size: usize,
    /// timeout for each batch read request
    timeout: Duration,
    actor_tx: mpsc::Sender<SqsActorMessage>,
}

/// Builder for creating an `SqsSource`.
///
/// This builder allows for configuring the SQS source with various parameters
/// such as region, queue name, batch size, timeout, and an optional SQS client.
#[derive(Clone)]
pub struct SqsSourceBuilder {
    config: SqsSourceConfig,
    batch_size: usize,
    timeout: Duration,
    client: Option<Client>,
}

impl Default for SqsSourceBuilder {
    fn default() -> Self {
        Self::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION.to_string(),
            queue_name: "".to_string(),
        })
    }
}

impl SqsSourceBuilder {
    pub fn new(config: SqsSourceConfig) -> Self {
        Self {
            config,
            batch_size: 1,
            timeout: Duration::from_secs(1),
            client: None,
        }
    }
    pub fn config(mut self, config: SqsSourceConfig) -> Self {
        self.config = config;
        self
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn client(mut self, client: Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Builds an `SqsSource` instance with the provided configuration.
    ///
    /// This method consumes `self` and initializes the SQS client, retrieves the queue URL,
    /// and spawns an actor to handle SQS interactions. It returns a `Result` containing
    /// the constructed `SqsSource` or an error if the initialization fails.
    ///
    /// # Returns
    /// - `Ok(SqsSource)` if the source is successfully built.
    /// - `Err(Error)` if there is an error during the initialization process.
    pub async fn build(self) -> Result<SqsSource> {
        let sqs_client = match self.client {
            Some(client) => client,
            None => SqsActor::create_sqs_client(Some(self.config.clone())).await,
        };

        let queue_name = self.config.queue_name.clone();

        let get_queue_url_output = sqs_client
            .get_queue_url()
            .queue_name(queue_name)
            .send()
            .await
            .map_err(|err| Error::Sqs(err.into()))?;

        let queue_url = get_queue_url_output
            .queue_url
            .ok_or_else(|| Error::Other("Queue URL not found".to_string()))?;

        tracing::info!(queue_url = queue_url.clone(), "Queue URL found");

        let (handler_tx, handler_rx) = mpsc::channel(10);
        tokio::spawn(async move {
            let mut actor = SqsActor::new(handler_rx, sqs_client, queue_url);
            actor.run().await;
        });

        Ok(SqsSource {
            batch_size: self.batch_size,
            timeout: self.timeout,
            actor_tx: handler_tx,
        })
    }
}

impl SqsSource {
    // read messages from SQS, corresponding sqs sdk method is receive_message
    pub async fn read_messages(&self) -> Result<Vec<SqsMessage>> {
        tracing::debug!("Reading messages from SQS");
        let start = Instant::now();
        let (tx, rx) = oneshot::channel();

        let msg = SqsActorMessage::Receive {
            respond_to: tx,
            count: self.batch_size as i32,
            timeout_at: start + self.timeout,
        };

        let _ = self.actor_tx.send(msg).await;
        let messages = rx.await.map_err(ActorTaskTerminated)??;
        tracing::debug!(
            count = messages.len(),
            requested_count = self.batch_size,
            time_taken_ms = start.elapsed().as_millis(),
            "Got messages from sqs"
        );
        Ok(messages)
    }

    // acknowledge the offsets of the messages read from SQS
    // corresponding sqs sdk method is delete_message
    pub async fn ack_offsets(&self, offsets: Vec<Bytes>) -> Result<()> {
        tracing::debug!(offsets = ?offsets, "Acknowledging offsets");
        let (tx, rx) = oneshot::channel();
        let msg = SqsActorMessage::Delete {
            offsets,
            respond_to: tx,
        };
        let _ = self.actor_tx.send(msg).await;
        rx.await.map_err(Error::ActorTaskTerminated)?
    }

    // get the pending message count from SQS
    // corresponding sqs sdk method is get_queue_attributes
    // with the attribute name ApproximateNumberOfMessages
    pub async fn pending_count(&self) -> Option<usize> {
        let (tx, rx) = oneshot::channel();
        let msg = SqsActorMessage::GetPending { respond_to: tx };
        let _ = self.actor_tx.send(msg).await;

        let actor_result = rx.await.map_err(Error::ActorTaskTerminated);

        match actor_result {
            Ok(Ok(Some(count))) => {
                tracing::debug!(pending_count = count, "Pending message count retrieved");
                Some(count)
            }
            _ => None,
        }
    }

    /// Returns the partitions for the SQS source.
    ///
    /// This method is currently unimplemented in this module.
    /// Note: It is implemented in the core to return the current vertex replica.
    /// See `numaflow-core/src/source/sqs.rs` for the implementation.
    pub fn partitions(&self) -> Vec<u16> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_sqs::Config;
    use aws_smithy_mocks_experimental::{mock, MockResponseInterceptor, Rule, RuleMode};
    use aws_smithy_types::error::ErrorMetadata;

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

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION.to_string(),
            queue_name: "test-q".to_string(),
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build()
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

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION.to_string(),
            queue_name: "test-q".to_string(),
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build()
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

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION.to_string(),
            queue_name: "test-q".to_string(),
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build()
        .await
        .unwrap();

        let count = source.pending_count().await;
        assert_eq!(count, Some(0));
    }

    #[tokio::test]
    async fn test_error_cases() {
        // Test invalid region error
        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&get_queue_url_output_err());

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION.to_string(),
            queue_name: "test-q".to_string(),
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build()
        .await;
        assert!(source.is_err());
    }

    #[tokio::test]
    #[should_panic(expected = "not implemented")]
    async fn test_partitions_unimplemented() {
        let source = SqsSource {
            batch_size: 1,
            timeout: Duration::from_secs(0),
            actor_tx: mpsc::channel(1).0,
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

    fn get_queue_url_output_err() -> Rule {
        mock!(aws_sdk_sqs::Client::get_queue_url).then_error(|| {
            aws_sdk_sqs::operation::get_queue_url::GetQueueUrlError::generic(
                ErrorMetadata::builder().code("InvalidAddress").build(),
            )
        })
    }

    fn get_test_config_with_interceptor(interceptor: MockResponseInterceptor) -> Config {
        aws_sdk_sqs::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(make_sqs_test_credentials())
            .region(aws_sdk_sqs::config::Region::new(SQS_DEFAULT_REGION))
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
