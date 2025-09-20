/// Module for handling AWS SQS sink operations, allowing messages to be sent to SQS queues.
///
/// This module provides functionality to configure and use AWS SQS as a sink for messaging.
use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
use bytes::Bytes;

use crate::{AssumeRoleConfig, Error, SqsConfig, SqsSinkError};

pub const SQS_DEFAULT_REGION: &str = "us-west-2";

pub type Result<T> = std::result::Result<T, SqsSinkError>;

/// Configuration for the AWS SQS sink.
#[derive(Clone, Debug, PartialEq)]
pub struct SqsSinkConfig {
    /// AWS region where the SQS queue is located
    pub region: &'static str,
    /// Name of the SQS queue
    pub queue_name: &'static str,
    /// AWS account ID of the queue owner
    pub queue_owner_aws_account_id: &'static str,
    /// Assume role configuration for AWS credentials
    pub assume_role_config: Option<AssumeRoleConfig>,
}

/// Message to be sent to SQS.
pub struct SqsSinkMessage {
    /// Unique identifier for the message
    pub id: String,
    /// Message body content as bytes
    pub message_body: Bytes,
}

/// Main SQS sink client that handles sending messages to SQS.
#[derive(Clone)]
pub struct SqsSink {
    client: Client,
    queue_url: &'static str,
}

/// Builder for creating and configuring an SQS sink.
#[derive(Clone)]
pub struct SqsSinkBuilder {
    config: SqsSinkConfig,
    client: Option<Client>,
}
/// Response from sending a message to SQS.
#[derive(Debug)]
pub struct SqsSinkResponse {
    /// ID of the message that was sent
    pub id: String,
    /// Status of the send operation
    pub status: Result<()>,
    /// Error code if any
    pub code: Option<String>,
    /// Indicates if the error was caused by the sender
    pub sender_fault: Option<bool>,
}

impl Default for SqsSinkBuilder {
    fn default() -> Self {
        Self::new(SqsSinkConfig {
            region: SQS_DEFAULT_REGION,
            queue_name: "",
            queue_owner_aws_account_id: "",
            assume_role_config: None,
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
            None => crate::create_sqs_client(SqsConfig::Sink(self.config.clone())).await?,
        };

        let queue_name = self.config.queue_name;
        let queue_owner_aws_account_id = self.config.queue_owner_aws_account_id;

        let get_queue_url_output = sqs_client
            .clone()
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

        Ok(SqsSink {
            client: sqs_client.clone(),
            queue_url: Box::leak(queue_url.clone().to_string().into_boxed_str()),
        })
    }
}

impl SqsSink {
    /// Sends a batch of messages to the SQS queue.
    ///
    /// Returns responses for each message, including success or failure status.
    pub async fn sink_messages(
        &self,
        messages: Vec<SqsSinkMessage>,
    ) -> Result<Vec<SqsSinkResponse>> {
        let mut entries = Vec::with_capacity(messages.len());
        let mut id_correlation = std::collections::HashMap::with_capacity(messages.len());

        for (index, message) in messages.into_iter().enumerate() {
            let sqs_batch_id = format!("msg_{}", index);
            id_correlation.insert(sqs_batch_id.clone(), message.id);

            let entry = SendMessageBatchRequestEntry::builder()
                .id(sqs_batch_id)
                .message_body(String::from_utf8_lossy(&message.message_body).to_string())
                .build()
                .map_err(|e| {
                    SqsSinkError::from(Error::Other(format!("Failed to build entry: {}", e)))
                })?;

            entries.push(entry);
        }

        // on error, we will cascade the error to numaflow core which will initiate a shutdown.
        let output = self
            .client
            .send_message_batch()
            .queue_url(self.queue_url)
            .set_entries(Some(entries))
            .send()
            .await
            .map_err(|e| SqsSinkError::from(Error::Sqs(e.into())))?;

        let mut responses = Vec::new();

        // Process successful messages
        for succeeded in output.successful {
            let original_id = id_correlation
                .remove(&succeeded.id)
                .expect("AWS returned unknown batch ID - this should never happen");

            responses.push(SqsSinkResponse {
                id: original_id,
                status: Ok(()),
                code: None,
                sender_fault: None,
            });
        }

        // Process failed messages
        for failed in output.failed {
            let original_id = id_correlation
                .remove(&failed.id)
                .expect("AWS returned unknown batch ID - this should never happen");

            responses.push(SqsSinkResponse {
                id: original_id,
                status: Err(SqsSinkError::from(Error::Other(
                    failed.message.unwrap_or_default(),
                ))),
                code: Some(failed.code),
                sender_fault: Some(failed.sender_fault),
            });
        }

        Ok(responses)
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_sqs::types::BatchResultErrorEntry;
    use aws_sdk_sqs::{Client, Config};
    use aws_smithy_mocks::{MockResponseInterceptor, Rule, RuleMode, mock};
    use aws_smithy_types::error::ErrorMetadata;
    use bytes::Bytes;
    use test_log::test;

    use crate::sink::{SqsSinkBuilder, SqsSinkConfig, SqsSinkMessage};
    use crate::source::SQS_DEFAULT_REGION;
    use crate::{Error, SqsConfig, SqsSinkError};

    #[test(tokio::test)]
    async fn test_client_creation_with_defaults() {
        let config = SqsSinkConfig {
            region: "us-west-2",
            queue_name: "test-queue",
            queue_owner_aws_account_id: "123456789012",
            assume_role_config: None,
        };

        let result = crate::create_sqs_client(SqsConfig::Sink(config.clone())).await;
        assert!(result.is_ok());
    }

    #[test(tokio::test)]
    async fn test_sqs_sink_builder() {
        // test default
        let builder = SqsSinkBuilder::default();
        assert_eq!(builder.config.region, SQS_DEFAULT_REGION);
        assert_eq!(builder.config.queue_name, "");

        let queue_url_output = get_queue_url_output();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let config = SqsSinkConfig {
            region: SQS_DEFAULT_REGION,
            queue_name: "test-q",
            queue_owner_aws_account_id: "123456789012",
            assume_role_config: None,
        };

        let sink = SqsSinkBuilder::new(config.clone())
            .client(sqs_mock_client)
            .build()
            .await;
        assert!(sink.is_ok());

        let sink = sink.unwrap();
        assert_eq!(
            sink.queue_url,
            "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
        );
    }

    #[test(tokio::test)]
    async fn test_sqs_sink_send_messages() {
        let queue_url_output = get_queue_url_output();
        let send_message_output = get_send_message_output();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&send_message_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let config = SqsSinkConfig {
            region: SQS_DEFAULT_REGION,
            queue_name: "test-q",
            queue_owner_aws_account_id: "123456789012",
            assume_role_config: None,
        };

        let sink = SqsSinkBuilder::new(config.clone())
            .client(sqs_mock_client)
            .build()
            .await;
        assert!(sink.is_ok());

        let sink = sink.unwrap();
        let messages = vec![SqsSinkMessage {
            id: "1".to_string(),
            message_body: Bytes::from("test message"),
        }];

        let result = sink.sink_messages(messages).await;
        assert!(result.is_ok());

        let responses = result.unwrap();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].id, "1");
        assert!(responses[0].status.is_ok());
        assert_eq!(responses[0].code, None);
        assert_eq!(responses[0].sender_fault, None);
    }

    #[test(tokio::test)]
    async fn test_sqs_sink_send_messages_with_failed() {
        let queue_url_output = get_queue_url_output();
        let send_message_output = get_send_message_output_with_failed();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&send_message_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let config = SqsSinkConfig {
            region: SQS_DEFAULT_REGION,
            queue_name: "test-q",
            queue_owner_aws_account_id: "123456789012",
            assume_role_config: None,
        };

        let sink = SqsSinkBuilder::new(config.clone())
            .client(sqs_mock_client)
            .build()
            .await;
        assert!(sink.is_ok());

        let sink = sink.unwrap();
        let messages = vec![
            SqsSinkMessage {
                id: "1".to_string(),
                message_body: Bytes::from("test message 1"),
            },
            SqsSinkMessage {
                id: "2".to_string(),
                message_body: Bytes::from("test message 2"),
            },
        ];

        let result = sink.sink_messages(messages).await;
        assert!(result.is_ok());

        let responses = result.unwrap();
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0].id, "1");
        assert!(responses[0].status.is_ok());
        assert_eq!(responses[0].code, None);
        assert_eq!(responses[0].sender_fault, None);
        assert_eq!(responses[1].id, "2");
        assert!(responses[1].status.is_err());
        assert_eq!(responses[1].code, Some("InvalidParameterValue".to_string()));
        assert_eq!(responses[1].sender_fault, Some(true));
    }
    #[test(tokio::test)]
    async fn test_sqs_sink_send_messages_all_fail() {
        let queue_url_output = get_queue_url_output();
        let send_message_output = get_send_message_output_all_fail();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&send_message_output);

        let sqs_mock_client =
            Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let config = SqsSinkConfig {
            region: SQS_DEFAULT_REGION,
            queue_name: "test-q",
            queue_owner_aws_account_id: "123456789012",
            assume_role_config: None,
        };

        let sink = SqsSinkBuilder::new(config.clone())
            .client(sqs_mock_client)
            .build()
            .await;
        assert!(sink.is_ok());

        let sink = sink.unwrap();
        let messages = vec![SqsSinkMessage {
            id: "1".to_string(),
            message_body: Bytes::from("test message"),
        }];

        let result = sink.sink_messages(messages).await;
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "SQS Sink Error: Failed with SQS error - unhandled error (InvalidParameterValue)"
        );
        assert!(matches!(error, SqsSinkError::Error(Error::Sqs(_))));
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

    fn get_send_message_output() -> Rule {
        let successful = aws_sdk_sqs::types::SendMessageBatchResultEntry::builder()
            .id("msg_0")
            .message_id("msg-id-1")
            .md5_of_message_body("f11a425906289abf8cce1733622834c8")
            .build()
            .unwrap();

        let send_message_output = mock!(aws_sdk_sqs::Client::send_message_batch)
            .match_requests(|inp| {
                inp.queue_url().unwrap()
                    == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
            })
            .then_output(move || {
                // Create a vector of successful entries
                let successful_entries = vec![successful.clone()];

                // Create an empty vector for failed entries
                let failed_entries: Vec<BatchResultErrorEntry> = Vec::new();

                aws_sdk_sqs::operation::send_message_batch::SendMessageBatchOutput::builder()
                    .set_successful(Some(successful_entries))
                    .set_failed(Some(failed_entries))
                    .build()
                    .unwrap()
            });
        send_message_output
    }

    fn get_send_message_output_with_failed() -> Rule {
        let successful = aws_sdk_sqs::types::SendMessageBatchResultEntry::builder()
            .id("msg_0")
            .message_id("msg-id-1")
            .md5_of_message_body("84769b6348524b3317694d80c0ac6df9")
            .build()
            .unwrap();

        let failed = aws_sdk_sqs::types::BatchResultErrorEntry::builder()
            .id("msg_1")
            .code("InvalidParameterValue")
            .message("The message is too large for the queue.")
            .sender_fault(true)
            .build()
            .unwrap();

        let send_message_output = mock!(aws_sdk_sqs::Client::send_message_batch)
            .match_requests(|inp| {
                inp.queue_url().unwrap()
                    == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
            })
            .then_output(move || {
                // Create a vector of successful entries
                let successful_entries = vec![successful.clone()];

                // Create an empty vector for failed entries
                let failed_entries: Vec<BatchResultErrorEntry> = vec![failed.clone()];

                aws_sdk_sqs::operation::send_message_batch::SendMessageBatchOutput::builder()
                    .set_successful(Some(successful_entries))
                    .set_failed(Some(failed_entries))
                    .build()
                    .unwrap()
            });
        send_message_output
    }

    fn get_send_message_output_all_fail() -> Rule {
        let send_message_output = mock!(aws_sdk_sqs::Client::send_message_batch)
            .match_requests(|inp| {
                inp.queue_url().unwrap()
                    == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
            })
            .then_error(|| {
                aws_sdk_sqs::operation::send_message_batch::SendMessageBatchError::generic(
                    ErrorMetadata::builder()
                        .message("The message is too large for the queue.")
                        .code("InvalidParameterValue")
                        .build(),
                )
            });
        send_message_output
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
