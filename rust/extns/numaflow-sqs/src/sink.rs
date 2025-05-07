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
        region = config
            .as_ref()
            .map(|c| c.region.clone())
            .unwrap_or_default(),
        queue_name = config
            .as_ref()
            .map(|c| c.queue_name.clone())
            .unwrap_or_default()
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

#[cfg(test)]
mod tests {
    use crate::Error;
    use crate::sink::{SqsSinkBuilder, SqsSinkConfig, SqsSinkMessage, create_sqs_client};
    use crate::source::SQS_DEFAULT_REGION;
    use aws_config::BehaviorVersion;
    use aws_sdk_sqs::types::BatchResultErrorEntry;
    use aws_sdk_sqs::{Client, Config};
    use aws_smithy_mocks_experimental::{MockResponseInterceptor, Rule, RuleMode, mock};
    use aws_smithy_types::error::ErrorMetadata;
    use bytes::Bytes;
    use test_log::test;

    #[test(tokio::test)]
    async fn test_client_creation_with_defaults() {
        let config = SqsSinkConfig {
            region: "us-west-2".to_string(),
            queue_name: "test-queue".to_string(),
            queue_owner_aws_account_id: "123456789012".to_string(),
        };

        let result = create_sqs_client(Some(config)).await;
        assert!(result.is_ok());
    }

    #[test(tokio::test)]
    async fn test_client_creation_validation_failures() {
        // Test missing config
        let result = create_sqs_client(None).await;
        assert!(matches!(result, Err(Error::InvalidConfig(_))));

        // Test empty region
        let config = SqsSinkConfig {
            region: "".to_string(),
            queue_name: "test-queue".to_string(),
            queue_owner_aws_account_id: "123456789012".to_string(),
        };
        let result = create_sqs_client(Some(config)).await;
        assert!(matches!(result, Err(Error::InvalidConfig(_))));

        // Test empty queue name
        let config = SqsSinkConfig {
            region: "us-west-2".to_string(),
            queue_name: "".to_string(),
            queue_owner_aws_account_id: "123456789012".to_string(),
        };
        let result = create_sqs_client(Some(config)).await;
        assert!(matches!(result, Err(Error::InvalidConfig(_))));

        // Test empty queue owner AWS account ID
        let config = SqsSinkConfig {
            region: "us-west-2".to_string(),
            queue_name: "test-queue".to_string(),
            queue_owner_aws_account_id: "".to_string(),
        };
        let result = create_sqs_client(Some(config)).await;
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
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
            region: SQS_DEFAULT_REGION.to_string(),
            queue_name: "test-q".to_string(),
            queue_owner_aws_account_id: "123456789012".to_string(),
        };

        let sink = SqsSinkBuilder::new(config.clone())
            .client(sqs_mock_client)
            .build()
            .await;
        assert!(sink.is_ok());

        let sink = sink.unwrap();
        assert_eq!(sink.actor_tx.capacity(), 10);
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
            region: SQS_DEFAULT_REGION.to_string(),
            queue_name: "test-q".to_string(),
            queue_owner_aws_account_id: "123456789012".to_string(),
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
            region: SQS_DEFAULT_REGION.to_string(),
            queue_name: "test-q".to_string(),
            queue_owner_aws_account_id: "123456789012".to_string(),
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
            region: SQS_DEFAULT_REGION.to_string(),
            queue_name: "test-q".to_string(),
            queue_owner_aws_account_id: "123456789012".to_string(),
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
            "Failed with SQS error - unhandled error (InvalidParameterValue)"
        );
        assert!(matches!(error, Error::Sqs(_)));
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
            .id("1")
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
            .id("1")
            .message_id("msg-id-1")
            .md5_of_message_body("84769b6348524b3317694d80c0ac6df9")
            .build()
            .unwrap();

        let failed = aws_sdk_sqs::types::BatchResultErrorEntry::builder()
            .id("2")
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
            .behavior_version(BehaviorVersion::v2025_01_17())
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
