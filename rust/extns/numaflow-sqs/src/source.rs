//! Implementation of the SQS message source using an actor-based architecture.
//!
//! Key design features:
//! - Actor model for thread-safe state management
//! - Batched message handling for efficiency
//! - Robust error handling and retry logic
//! - Configurable timeouts and batch sizes

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::{
    DeleteMessageBatchRequestEntry, MessageSystemAttributeName, QueueAttributeName,
};
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::Error::ActorTaskTerminated;
use crate::{AssumeRoleConfig, Error, SqsConfig, SqsSourceError};

pub const SQS_DEFAULT_REGION: &str = "us-west-2";

pub type Result<T> = std::result::Result<T, SqsSourceError>;

/// Configuration for an SQS message source.
///
/// Used to initialize the SQS client with region and queue settings.
#[derive(Debug, Clone, PartialEq)]
pub struct SqsSourceConfig {
    // Required fields
    pub region: &'static str,
    pub queue_name: &'static str,
    pub queue_owner_aws_account_id: &'static str,

    // Optional fields
    pub visibility_timeout: Option<i32>,
    pub max_number_of_messages: Option<i32>,
    pub wait_time_seconds: Option<i32>,
    pub endpoint_url: Option<String>,
    pub attribute_names: Vec<String>,
    pub message_attribute_names: Vec<String>,
    pub assume_role_config: Option<AssumeRoleConfig>,
}

/// Internal message types for the actor implementation.
///
/// The actor pattern is used to:
/// - Ensure thread-safe access to the SQS client
/// - Manage connection state and retries
/// - Handle concurrent requests without locks
enum SQSActorMessage {
    Receive {
        respond_to: oneshot::Sender<Option<Result<Vec<SqsMessage>>>>,
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
    handler_rx: mpsc::Receiver<SQSActorMessage>,
    client: Client,
    queue_url: String,
    config: SqsSourceConfig,
    cancel_token: CancellationToken,
}

impl SqsActor {
    fn new(
        handler_rx: mpsc::Receiver<SQSActorMessage>,
        client: Client,
        queue_url: String,
        config: SqsSourceConfig,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            handler_rx,
            client,
            queue_url,
            config,
            cancel_token,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.handler_rx.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: SQSActorMessage) {
        match msg {
            SQSActorMessage::Receive {
                respond_to,
                count,
                timeout_at,
            } => {
                let messages = self.get_messages(count, timeout_at).await;
                respond_to
                    .send(messages)
                    .expect("failed to send response from SqsActorMessage::Receive");
            }
            SQSActorMessage::Delete {
                respond_to,
                offsets,
            } => {
                let status = self.delete_messages(offsets).await;
                respond_to
                    .send(status)
                    .expect("failed to send response from SqsActorMessage::Delete");
            }
            SQSActorMessage::GetPending { respond_to } => {
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
    async fn get_messages(
        &mut self,
        count: i32,
        timeout_at: Instant,
    ) -> Option<Result<Vec<SqsMessage>>> {
        if self.cancel_token.is_cancelled() {
            return None;
        }

        let remaining_time = timeout_at - Instant::now();

        // default to one second if remaining time is less than one second
        // as sqs sdk requires wait_time_seconds to be at least 1
        // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-short-long-polling-differences
        // TODO: find a better way to handle user input timeout. should allow the users
        // to choose long/short polling. For now, we default to 1 second (long polling).
        let wait_time = if remaining_time.as_millis() < 1000 {
            1
        } else {
            (remaining_time.as_secs() as i32).min(20) // SQS max wait time is 20 seconds
        };

        // Use configured max messages if provided, otherwise use the requested count
        let max_messages = self.config.max_number_of_messages.unwrap_or(count).min(10); // SQS max batch size is 10

        let mut receive_message_builder = self
            .client
            .receive_message()
            .queue_url(&self.queue_url)
            .max_number_of_messages(max_messages)
            .wait_time_seconds(wait_time);

        // Apply visibility timeout if configured
        if let Some(visibility_timeout) = self.config.visibility_timeout {
            receive_message_builder =
                receive_message_builder.visibility_timeout(visibility_timeout);
        }

        // Apply attribute names if configured
        if !self.config.attribute_names.is_empty() {
            for attr in &self.config.attribute_names {
                let attr_name = MessageSystemAttributeName::from_str(attr);
                match attr_name {
                    Ok(attr_name) => {
                        receive_message_builder =
                            receive_message_builder.message_system_attribute_names(attr_name);
                    }
                    Err(err) => {
                        tracing::error!(?err, "failed to parse attribute name");
                    }
                }
            }
        } else {
            receive_message_builder = receive_message_builder
                .message_system_attribute_names(MessageSystemAttributeName::All);
        }

        // Apply message attribute names if configured
        if !self.config.message_attribute_names.is_empty() {
            for attr in &self.config.message_attribute_names {
                receive_message_builder = receive_message_builder.message_attribute_names(attr);
            }
        } else {
            receive_message_builder = receive_message_builder.message_attribute_names("All");
        }

        let sdk_response = receive_message_builder.send().await;

        let receive_message_output = match sdk_response {
            Ok(output) => output,
            Err(err) => {
                tracing::error!(
                    ?err,
                    queue_url = self.queue_url,
                    "failed to receive messages from SQS"
                );
                return Some(Err(SqsSourceError::from(Error::Sqs(err.into()))));
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
                            .map(|(k, v)| (k.to_string(), v.to_string()))
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

        Some(Ok(messages))
    }

    /// deletes batch of messages from SQS, serves as Numaflow source ack.
    async fn delete_messages(&mut self, offsets: Vec<Bytes>) -> Result<()> {
        let mut batch_builder = self
            .client
            .delete_message_batch()
            .queue_url(&self.queue_url);
        for (id, offset) in offsets.iter().enumerate() {
            let offset = match std::str::from_utf8(offset) {
                Ok(offset) => offset,
                Err(err) => {
                    error!(?err, ?offset, "failed to parse offset");
                    return Err(SqsSourceError::from(Error::Other(
                        "failed to parse offset".to_string(),
                    )));
                }
            };
            // id is just used to track request to response (it should be unique in the batch, index
            // is used as id (since it will be unique per batch).
            // receipt handle(message offset that needs to be deleted) and id are mandatory fields in
            // the batch builder.
            batch_builder = batch_builder.entries(
                DeleteMessageBatchRequestEntry::builder()
                    .receipt_handle(offset)
                    .id(id.to_string())
                    .build()
                    .map_err(|err| {
                        error!(?err, "Failed to build DeleteMessageBatchRequestEntry",);
                        Error::Sqs(err.into())
                    })?,
            );
        }

        if let Err(e) = batch_builder.send().await {
            error!(
                ?e,
                queue_url = self.queue_url,
                "Failed to delete messages from SQS"
            );
            return Err(SqsSourceError::from(Error::Sqs(e.into())));
        }
        Ok(())
    }

    /// get the pending message count from SQS using the ApproximateNumberOfMessages attribute
    /// Note: The ApproximateNumberOfMessages metrics may not achieve consistency until at least
    /// 1 minute after the producers stop sending messages.
    /// This period is required for the queue metadata to reach eventual consistency.
    async fn get_pending_messages(&mut self) -> Result<Option<usize>> {
        let sdk_response = self
            .client
            .get_queue_attributes()
            .queue_url(self.queue_url.clone())
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await;

        let get_queue_attributes_output = match sdk_response {
            Ok(output) => output,
            Err(err) => {
                tracing::error!(
                    ?err,
                    queue_url = ?self.queue_url,
                    "failed to get queue attributes from SQS"
                );
                return Err(SqsSourceError::from(Error::Sqs(err.into())));
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
                return Err(SqsSourceError::from(Error::Other(
                    "failed to parse ApproximateNumberOfMessages".to_string(),
                )));
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
    actor_tx: mpsc::Sender<SQSActorMessage>,
    vertex_replica: u16,
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
    vertex_replica: u16,
}

impl Default for SqsSourceBuilder {
    fn default() -> Self {
        Self::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_name: "",
            queue_owner_aws_account_id: "",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: Vec::new(),
            message_attribute_names: Vec::new(),
            assume_role_config: None,
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
            vertex_replica: 0,
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

    pub fn vertex_replica(mut self, vertex_replica: u16) -> Self {
        self.vertex_replica = vertex_replica;
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
    pub async fn build(self, cancel_token: CancellationToken) -> Result<SqsSource> {
        let sqs_client = match self.client {
            Some(client) => client,
            None => crate::create_sqs_client(SqsConfig::Source(self.config.clone())).await?,
        };

        let queue_name = self.config.queue_name;
        let queue_owner_aws_account_id = self.config.queue_owner_aws_account_id;

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
            let mut actor =
                SqsActor::new(handler_rx, sqs_client, queue_url, self.config, cancel_token);
            actor.run().await;
        });

        Ok(SqsSource {
            batch_size: self.batch_size,
            timeout: self.timeout,
            actor_tx: handler_tx,
            vertex_replica: self.vertex_replica,
        })
    }
}

impl SqsSource {
    /// read messages from SQS, corresponding sqs sdk method is receive_message
    pub async fn read_messages(&self) -> Option<Result<Vec<SqsMessage>>> {
        tracing::debug!("Reading messages from SQS");
        let start = Instant::now();
        let (tx, rx) = oneshot::channel();

        let msg = SQSActorMessage::Receive {
            respond_to: tx,
            count: self.batch_size as i32,
            timeout_at: start + self.timeout,
        };

        let _ = self.actor_tx.send(msg).await;
        rx.await
            .map_err(ActorTaskTerminated)
            .unwrap_or_else(|e| Some(Err(SqsSourceError::Error(e))))
    }

    /// acknowledge the offsets of the messages read from SQS
    /// corresponding sqs sdk method is delete_message
    pub async fn ack_offsets(&self, offsets: Vec<Bytes>) -> Result<()> {
        tracing::debug!(offsets = ?offsets, "Acknowledging offsets");
        let (tx, rx) = oneshot::channel();
        let msg = SQSActorMessage::Delete {
            offsets,
            respond_to: tx,
        };
        self.actor_tx.send(msg).await.expect("rx was dropped");
        rx.await.map_err(Error::ActorTaskTerminated)?
    }

    /// get the pending message count from SQS
    /// corresponding sqs sdk method is get_queue_attributes
    /// with the attribute name ApproximateNumberOfMessages
    pub async fn pending_count(&self) -> Option<usize> {
        let (tx, rx) = oneshot::channel();
        let msg = SQSActorMessage::GetPending { respond_to: tx };
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
        vec![self.vertex_replica]
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_sqs::Config;
    use aws_sdk_sqs::types::MessageAttributeValue;
    use aws_smithy_mocks::{MockResponseInterceptor, Rule, RuleMode, mock};
    use aws_smithy_types::error::ErrorMetadata;
    use test_log::test;

    use super::*;

    #[tokio::test]
    async fn test_client_creation_with_defaults() {
        let config = SqsSourceConfig {
            region: "us-west-2",
            queue_name: "test-queue",
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        };

        let result = crate::create_sqs_client(SqsConfig::Source(config.clone())).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_client_creation_with_custom_endpoint() {
        let mut config = SqsSourceConfig {
            region: "us-west-2",
            queue_name: "test-queue",
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: Some(30),
            max_number_of_messages: Some(5),
            wait_time_seconds: Some(10),
            endpoint_url: Some("http://localhost:4566".to_string()),
            attribute_names: vec!["All".to_string()],
            message_attribute_names: vec!["All".to_string()],
            assume_role_config: None,
        };

        let result = crate::create_sqs_client(SqsConfig::Source(config.clone())).await;
        assert!(result.is_ok());

        // Test with invalid endpoint
        config.endpoint_url = Some("invalid-url".to_string());
        let result = crate::create_sqs_client(SqsConfig::Source(config)).await;
        assert!(result.is_ok()); // The URL is validated when making requests, not during client creation
    }

    #[test(tokio::test)]
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
            region: SQS_DEFAULT_REGION,
            queue_name: "test-q",
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: Some(300),
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![MessageSystemAttributeName::SentTimestamp.to_string()],
            message_attribute_names: vec![MessageSystemAttributeName::AwsTraceHeader.to_string()],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        // Read messages from the source
        let messages = source.read_messages().await.unwrap().unwrap();

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
        assert_eq!(msg1.attributes.clone().unwrap().len(), 1);

        // test another config

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&receive_message_output);
        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));
        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_name: "test-q",
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: Some(300),
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        // Read messages from the source
        let messages = source.read_messages().await.unwrap().unwrap();

        // Assert we got the expected number of messages
        assert_eq!(messages.len(), 1, "Should receive exactly 1 message");
    }

    #[test(tokio::test)]
    async fn test_sqssourcehandle_read_error() {
        let queue_url_output = get_queue_url_output();

        let receive_message_output = get_receive_message_error();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&receive_message_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_name: "test-q",
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: Some(300),
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![MessageSystemAttributeName::SentTimestamp.to_string()],
            message_attribute_names: vec![MessageSystemAttributeName::AwsTraceHeader.to_string()],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        // Read messages from the source
        let messages = source.read_messages().await;

        match messages {
            Some(Ok(_)) => panic!("Expected an error, but got a successful response"),
            Some(Err(err)) => {
                assert_eq!(
                    err.to_string(),
                    "SQS Source Error: Failed with SQS error - unhandled error (InvalidAddress)"
                );
            }
            None => panic!("Expected an error, but got None"),
        }
    }

    #[test(tokio::test)]
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
            region: SQS_DEFAULT_REGION,
            queue_name: "test-q",
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        // Test acknowledgment
        let offset = "AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q";
        let result = source.ack_offsets(vec![Bytes::from(offset)]).await;
        assert!(result.is_ok());
    }

    #[test(tokio::test)]
    async fn test_sqssource_ack_error() {
        let queue_url_output = get_queue_url_output();
        let delete_message_output = get_delete_message_error();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&delete_message_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_name: "test-q",
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        // Test acknowledgment
        let offset = "AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q";
        let result = source.ack_offsets(vec![Bytes::from(offset)]).await;
        assert!(result.is_err());
    }

    #[test(tokio::test)]
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
            region: SQS_DEFAULT_REGION,
            queue_name: "test-q",
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        let count = source.pending_count().await;
        assert_eq!(count, Some(0));
    }

    #[test(tokio::test)]
    async fn test_sqssource_pending_count_error() {
        let queue_url_output = get_queue_url_output();
        let queue_attrs_output = get_queue_attributes_error();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&queue_attrs_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_name: "test-q",
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        let count = source.pending_count().await;
        assert_eq!(count, None);
    }

    #[test(tokio::test)]
    async fn test_error_cases() {
        // Test invalid region error
        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&get_queue_url_output_err());

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_name: "test-q",
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(0))
        .client(sqs_mock_client)
        .build(CancellationToken::new())
        .await;
        assert!(source.is_err());
    }

    #[tokio::test]
    async fn test_partitions_unimplemented() {
        let source = SqsSource {
            batch_size: 1,
            timeout: Duration::from_secs(0),
            actor_tx: mpsc::channel(1).0,
            vertex_replica: 1,
        };
        assert_eq!(source.partitions(), vec![1]);
    }

    #[tokio::test]
    async fn test_sqs_source_builder() {
        // test default
        let builder = SqsSourceBuilder::default();
        assert_eq!(builder.batch_size, 1);
        assert_eq!(builder.timeout, Duration::from_secs(1));

        // test with vertex replica
        let builder = SqsSourceBuilder::default().vertex_replica(2);
        assert_eq!(builder.vertex_replica, 2);

        // test with custom config
        let config = SqsSourceConfig {
            region: "us-east-2",
            queue_name: "test-queue-custom",
            queue_owner_aws_account_id: "123456789012",
            visibility_timeout: Some(300),
            max_number_of_messages: Some(2000),
            wait_time_seconds: Some(10),
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        };
        let builder = SqsSourceBuilder::default().config(config);
        assert_eq!(builder.config.region, "us-east-2");
        assert_eq!(builder.config.queue_name, "test-queue-custom");
        assert_eq!(builder.config.queue_owner_aws_account_id, "123456789012");
        assert_eq!(builder.config.visibility_timeout, Some(300));
        assert_eq!(builder.config.max_number_of_messages, Some(2000));
        assert_eq!(builder.config.wait_time_seconds, Some(10));
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

    fn get_queue_attributes_error() -> Rule {
        let queue_attributes_output = mock!(aws_sdk_sqs::Client::get_queue_attributes)
            .match_requests(|inp| {
                inp.queue_url().unwrap()
                    == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
            })
            .then_error(|| {
                aws_sdk_sqs::operation::get_queue_attributes::GetQueueAttributesError::generic(
                    ErrorMetadata::builder()
                        .code("QueueDoesNotExist")
                        .message("The specified queue does not exist for this wsdl version.")
                        .build(),
                )
            });
        queue_attributes_output
    }

    fn get_delete_message_output() -> Rule {
        let delete_message_output = mock!(aws_sdk_sqs::Client::delete_message_batch)
            .match_requests(|inp| {
                inp.queue_url().unwrap()
                    == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
                    && inp.entries.clone().unwrap().len() == 1
            })
            .then_output(|| {
                aws_sdk_sqs::operation::delete_message_batch::DeleteMessageBatchOutput::builder()
                    .successful(
                        aws_sdk_sqs::types::DeleteMessageBatchResultEntry::builder()
                            .id("1")
                            .build()
                            .unwrap(),
                    )
                    .failed(
                        aws_sdk_sqs::types::BatchResultErrorEntry::builder()
                            .id("") // Empty string ID (minimal valid value)
                            .code("") // Empty string code (minimal valid value)
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap()
            });
        delete_message_output
    }

    fn get_delete_message_error() -> Rule {
        let delete_message_output = mock!(aws_sdk_sqs::Client::delete_message)
            .match_requests(|inp| {
                inp.queue_url().unwrap() == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
                    && inp.receipt_handle().unwrap() == "AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q"
            })
            .then_error(|| {
                aws_sdk_sqs::operation::delete_message::DeleteMessageError::generic(
                    ErrorMetadata::builder().code("ReceiptHandleIsInvalid").build(),
                )
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
                            .message_attributes(
                                "AwsTraceHeader",
                                MessageAttributeValue::builder()
                                    .set_data_type(Some("String".to_string()))
                                    .set_string_value(Some("Root=1-5e4f8a2c-0b2d3e4f8a2c0b2d3e4f8a2c".to_string()))
                                    .build().unwrap()
                            )
                            .build()
                    )
                    .build()
            });
        receive_message_output
    }

    fn get_receive_message_error() -> Rule {
        let receive_message_output = mock!(aws_sdk_sqs::Client::receive_message)
            .match_requests(|inp| {
                inp.queue_url().unwrap()
                    == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
            })
            .then_error(|| {
                aws_sdk_sqs::operation::receive_message::ReceiveMessageError::generic(
                    ErrorMetadata::builder().code("InvalidAddress").build(),
                )
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
            .behavior_version(crate::aws_behavior_version())
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
