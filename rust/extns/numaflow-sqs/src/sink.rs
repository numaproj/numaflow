/// Module for handling AWS SQS sink operations, allowing messages to be sent to SQS queues.
///
/// This module provides functionality to configure and use AWS SQS as a sink for messaging.
use std::collections::HashMap;

use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
use bytes::Bytes;

use crate::{
    AssumeRoleConfig, Error, HEADER_DELAY_SECONDS, HEADER_MESSAGE_DEDUPLICATION_ID,
    HEADER_MESSAGE_GROUP_ID, SqsConfig, SqsSinkError, extract_aws_error,
};

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
    /// Headers for the message
    pub headers: HashMap<String, String>,
}

/// Input for converting a sink message to a batch request entry.
struct BatchEntryInput {
    index: usize,
    message: SqsSinkMessage,
}

/// Converts BatchEntryInput into (entry, original_id) tuple.
/// The batch_id can be retrieved from entry.id().
impl TryFrom<BatchEntryInput> for SendMessageBatchRequestEntry {
    type Error = SqsSinkError;

    fn try_from(input: BatchEntryInput) -> std::result::Result<Self, Self::Error> {
        let batch_id = format!("msg_{}", input.index);

        let mut entry = SendMessageBatchRequestEntry::builder()
            .id(&batch_id)
            .message_body(String::from_utf8_lossy(&input.message.message_body).to_string());

        if let Some(delay) = input.message.headers.get(HEADER_DELAY_SECONDS) {
            match delay.parse::<i32>() {
                Ok(delay_val) if delay_val >= 0 => {
                    entry = entry.delay_seconds(delay_val);
                }
                Ok(delay_val) => {
                    tracing::warn!(
                        delay_seconds = delay_val,
                        "Invalid DelaySeconds: must be non-negative, ignoring"
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        delay_seconds = %delay,
                        "Invalid DelaySeconds: failed to parse as integer, ignoring"
                    );
                }
            }
        }

        if let Some(group_id) = input.message.headers.get(HEADER_MESSAGE_GROUP_ID) {
            entry = entry.message_group_id(group_id);
        }

        if let Some(dedup_id) = input.message.headers.get(HEADER_MESSAGE_DEDUPLICATION_ID) {
            entry = entry.message_deduplication_id(dedup_id);
        }

        let entry = entry.build().map_err(|e| {
            SqsSinkError::from(Error::Other(format!("Failed to build entry: {}", e)))
        })?;

        Ok(entry)
    }
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

impl SqsSinkResponse {
    fn success(id: String) -> Self {
        Self {
            id,
            status: Ok(()),
            code: None,
            sender_fault: None,
        }
    }

    fn failure(id: String, message: String, code: String, sender_fault: bool) -> Self {
        Self {
            id,
            status: Err(SqsSinkError::from(Error::Other(message))),
            code: Some(code),
            sender_fault: Some(sender_fault),
        }
    }
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
            .map_err(|err| Error::Sqs(extract_aws_error(&err)))?;

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

/// Maximum number of messages per SQS SendMessageBatch request.
/// AWS SQS has a hard limit of 10 messages per batch.
const SQS_MAX_BATCH_SIZE: usize = 10;

impl SqsSink {
    /// Sends a batch of messages to the SQS queue.
    ///
    /// Returns responses for each message, including success or failure status.
    /// Messages are automatically chunked into batches of 10 (SQS limit).
    pub async fn sink_messages(
        &self,
        messages: Vec<SqsSinkMessage>,
    ) -> Result<Vec<SqsSinkResponse>> {
        let mut all_responses = Vec::with_capacity(messages.len());

        // Chunk messages into batches of SQS_MAX_BATCH_SIZE (10) to comply with SQS limits
        for chunk in messages.chunks(SQS_MAX_BATCH_SIZE) {
            let mut entries = Vec::with_capacity(chunk.len());
            let mut id_correlation = HashMap::with_capacity(chunk.len());

            for (index, message) in chunk.iter().enumerate() {
                let original_id = message.id.clone();
                let entry: SendMessageBatchRequestEntry = BatchEntryInput {
                    index,
                    message: SqsSinkMessage {
                        id: message.id.clone(),
                        message_body: message.message_body.clone(),
                        headers: message.headers.clone(),
                    },
                }
                .try_into()?;

                id_correlation.insert(entry.id().to_string(), original_id);
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
                .map_err(|e| SqsSinkError::from(Error::Sqs(extract_aws_error(&e))))?;

            let mut responses: Vec<_> = output
                .successful
                .into_iter()
                .map(|s| {
                    SqsSinkResponse::success(
                        id_correlation
                            .remove(&s.id)
                            .expect("AWS returned unknown batch ID - this should never happen"),
                    )
                })
                .collect();

            responses.extend(output.failed.into_iter().map(|f| {
                SqsSinkResponse::failure(
                    id_correlation
                        .remove(&f.id)
                        .expect("AWS returned unknown batch ID - this should never happen"),
                    f.message.unwrap_or_default(),
                    f.code,
                    f.sender_fault,
                )
            }));

            all_responses.extend(responses);
        }

        Ok(all_responses)
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

    use aws_sdk_sqs::types::SendMessageBatchRequestEntry;

    use crate::sink::{BatchEntryInput, SqsSinkBuilder, SqsSinkConfig, SqsSinkMessage};
    use crate::source::SQS_DEFAULT_REGION;
    use crate::{
        Error, HEADER_DELAY_SECONDS, HEADER_MESSAGE_DEDUPLICATION_ID, HEADER_MESSAGE_GROUP_ID,
        SqsConfig, SqsSinkError,
    };

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
            headers: Default::default(),
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
                headers: Default::default(),
            },
            SqsSinkMessage {
                id: "2".to_string(),
                message_body: Bytes::from("test message 2"),
                headers: Default::default(),
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
            headers: Default::default(),
        }];

        let result = sink.sink_messages(messages).await;
        assert!(result.is_err());

        let error = result.unwrap_err();
        // Error now includes both code and message from AWS
        assert!(error.to_string().contains("InvalidParameterValue"));
        assert!(matches!(error, SqsSinkError::Error(Error::Sqs(_))));
    }

    #[test(tokio::test)]
    async fn test_sqs_sink_send_messages_with_headers() {
        let queue_url_output = get_queue_url_output();

        // Custom rule to verify that the headers were correctly applied to the request
        let send_message_output = mock!(aws_sdk_sqs::Client::send_message_batch)
            .match_requests(|inp| {
                let entries = inp.entries();
                if entries.len() != 1 {
                    return false;
                }
                let entry = &entries[0];

                // Verify all our headers were mapped correctly
                entry.delay_seconds() == Some(10)
                    && entry.message_group_id() == Some("group-1")
                    && entry.message_deduplication_id() == Some("dedup-1")
            })
            .then_output(|| {
                let successful = aws_sdk_sqs::types::SendMessageBatchResultEntry::builder()
                    .id("msg_0")
                    .message_id("msg-id-1")
                    .md5_of_message_body("dummy")
                    .build()
                    .unwrap();

                aws_sdk_sqs::operation::send_message_batch::SendMessageBatchOutput::builder()
                    .set_successful(Some(vec![successful]))
                    .set_failed(Some(vec![]))
                    .build()
                    .unwrap()
            });

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

        let sink = SqsSinkBuilder::new(config)
            .client(sqs_mock_client)
            .build()
            .await
            .unwrap();

        let mut headers = std::collections::HashMap::new();
        headers.insert(HEADER_DELAY_SECONDS.to_string(), "10".to_string());
        headers.insert(HEADER_MESSAGE_GROUP_ID.to_string(), "group-1".to_string());
        headers.insert(
            HEADER_MESSAGE_DEDUPLICATION_ID.to_string(),
            "dedup-1".to_string(),
        );

        let messages = vec![SqsSinkMessage {
            id: "1".to_string(),
            message_body: Bytes::from("test message"),
            headers,
        }];

        let result = sink.sink_messages(messages).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_batch_entry_conversion_basic() {
        let message = SqsSinkMessage {
            id: "original-id-123".to_string(),
            message_body: Bytes::from("test message body"),
            headers: Default::default(),
        };

        let input = BatchEntryInput { index: 0, message };
        let entry: SendMessageBatchRequestEntry = input.try_into().unwrap();

        assert_eq!(entry.id(), "msg_0");
        assert_eq!(entry.message_body(), "test message body");
        assert_eq!(entry.delay_seconds(), None);
        assert_eq!(entry.message_group_id(), None);
        assert_eq!(entry.message_deduplication_id(), None);
    }

    #[test]
    fn test_batch_entry_conversion_with_valid_delay_seconds() {
        let mut headers = std::collections::HashMap::new();
        headers.insert(HEADER_DELAY_SECONDS.to_string(), "30".to_string());

        let message = SqsSinkMessage {
            id: "test-id".to_string(),
            message_body: Bytes::from("body"),
            headers,
        };

        let input = BatchEntryInput { index: 0, message };
        let entry: SendMessageBatchRequestEntry = input.try_into().unwrap();

        assert_eq!(entry.delay_seconds(), Some(30));
    }

    #[test]
    fn test_batch_entry_conversion_with_negative_delay_seconds_ignored() {
        let mut headers = std::collections::HashMap::new();
        headers.insert(HEADER_DELAY_SECONDS.to_string(), "-5".to_string());

        let message = SqsSinkMessage {
            id: "test-id".to_string(),
            message_body: Bytes::from("body"),
            headers,
        };

        let input = BatchEntryInput { index: 0, message };
        let entry: SendMessageBatchRequestEntry = input.try_into().unwrap();

        // Negative delay is ignored (warning logged)
        assert_eq!(entry.delay_seconds(), None);
    }

    #[test]
    fn test_batch_entry_conversion_with_invalid_delay_seconds_ignored() {
        let mut headers = std::collections::HashMap::new();
        headers.insert(HEADER_DELAY_SECONDS.to_string(), "not-a-number".to_string());

        let message = SqsSinkMessage {
            id: "test-id".to_string(),
            message_body: Bytes::from("body"),
            headers,
        };

        let input = BatchEntryInput { index: 0, message };
        let entry: SendMessageBatchRequestEntry = input.try_into().unwrap();

        // Invalid delay is ignored (warning logged)
        assert_eq!(entry.delay_seconds(), None);
    }

    #[test]
    fn test_batch_entry_conversion_with_message_group_id() {
        let mut headers = std::collections::HashMap::new();
        headers.insert(HEADER_MESSAGE_GROUP_ID.to_string(), "my-group".to_string());

        let message = SqsSinkMessage {
            id: "test-id".to_string(),
            message_body: Bytes::from("body"),
            headers,
        };

        let input = BatchEntryInput { index: 0, message };
        let entry: SendMessageBatchRequestEntry = input.try_into().unwrap();

        assert_eq!(entry.message_group_id(), Some("my-group"));
    }

    #[test]
    fn test_batch_entry_conversion_with_deduplication_id() {
        let mut headers = std::collections::HashMap::new();
        headers.insert(
            HEADER_MESSAGE_DEDUPLICATION_ID.to_string(),
            "dedup-123".to_string(),
        );

        let message = SqsSinkMessage {
            id: "test-id".to_string(),
            message_body: Bytes::from("body"),
            headers,
        };

        let input = BatchEntryInput { index: 0, message };
        let entry: SendMessageBatchRequestEntry = input.try_into().unwrap();

        assert_eq!(entry.message_deduplication_id(), Some("dedup-123"));
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
