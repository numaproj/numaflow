use std::sync::Arc;
use std::time::Duration;

use numaflow_sqs::source::{SqsMessage, SqsSource, SqsSourceBuilder, SqsSourceConfig};

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::error::Error;
use crate::message::{Message, MessageID, Offset, StringOffset};
use crate::source;

impl TryFrom<SqsMessage> for Message {
    type Error = Error;

    fn try_from(message: SqsMessage) -> crate::Result<Self> {
        let offset = Offset::String(StringOffset::new(message.offset, *get_vertex_replica()));

        Ok(Message {
            typ: Default::default(),
            keys: Arc::from(vec![message.key]),
            tags: None,
            value: message.payload,
            offset: offset.clone(),
            event_time: message.event_time,
            watermark: Some(message.event_time),
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: offset.to_string().into(),
                index: 0,
            },
            headers: Arc::new(message.headers),
            // Set default metadata so that metadata is always present.
            metadata: Some(Arc::new(crate::metadata::Metadata::default())),
            is_late: false,
            ack_handle: None,
        })
    }
}

impl From<numaflow_sqs::SqsSourceError> for Error {
    fn from(value: numaflow_sqs::SqsSourceError) -> Self {
        match value {
            numaflow_sqs::SqsSourceError::Error(numaflow_sqs::Error::Sqs(e)) => {
                Error::Source(e.to_string())
            }
            numaflow_sqs::SqsSourceError::Error(numaflow_sqs::Error::Sts(e)) => {
                Error::Source(e.to_string())
            }
            numaflow_sqs::SqsSourceError::Error(numaflow_sqs::Error::ActorTaskTerminated(_)) => {
                Error::ActorPatternRecv(value.to_string())
            }
            numaflow_sqs::SqsSourceError::Error(numaflow_sqs::Error::InvalidConfig(e)) => {
                Error::Source(e)
            }
            numaflow_sqs::SqsSourceError::Error(numaflow_sqs::Error::Other(e)) => Error::Source(e),
        }
    }
}

pub(crate) async fn new_sqs_source(
    cfg: SqsSourceConfig,
    batch_size: usize,
    timeout: Duration,
    vertex_replica: u16,
    cancel_token: tokio_util::sync::CancellationToken,
) -> crate::Result<SqsSource> {
    Ok(SqsSourceBuilder::new(cfg)
        .batch_size(batch_size)
        .timeout(timeout)
        .vertex_replica(vertex_replica)
        .build(cancel_token)
        .await?)
}

impl source::SourceReader for SqsSource {
    fn name(&self) -> &'static str {
        "SQS"
    }

    async fn read(&mut self) -> Option<crate::Result<Vec<Message>>> {
        match self.read_messages().await {
            Some(Ok(messages)) => {
                let result: crate::Result<Vec<Message>> =
                    messages.into_iter().map(|msg| msg.try_into()).collect();
                Some(result)
            }
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }

    // if source doesn't support partitions, we should return the vec![vertex_replica]
    async fn partitions(&mut self) -> crate::Result<Vec<u16>> {
        Ok(vec![*get_vertex_replica()])
    }
}

impl source::SourceAcker for SqsSource {
    async fn ack(&mut self, offsets: Vec<Offset>) -> crate::error::Result<()> {
        let mut sqs_offsets = Vec::with_capacity(offsets.len());
        for offset in offsets {
            let Offset::String(string_offset) = offset else {
                return Err(Error::Source(format!(
                    "Expected Offset::String type for SQS. offset={offset:?}"
                )));
            };
            sqs_offsets.push(string_offset.offset);
        }
        self.ack_offsets(sqs_offsets).await.map_err(Into::into)
    }

    async fn nack(&mut self, _offsets: Vec<Offset>) -> crate::error::Result<()> {
        // SQS doesn't support nack - no-op
        Ok(())
    }
}

impl source::LagReader for SqsSource {
    async fn pending(&mut self) -> crate::error::Result<Option<usize>> {
        Ok(self.pending_count().await)
    }
}

#[cfg(feature = "sqs-tests")]
#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use aws_sdk_sqs::Config;
    use aws_sdk_sqs::config::BehaviorVersion;
    use aws_sdk_sqs::types::MessageSystemAttributeName;
    use aws_smithy_mocks::{MockResponseInterceptor, Rule, RuleMode, mock};
    use bytes::Bytes;
    use chrono::Utc;
    use numaflow_sqs::source::{SQS_DEFAULT_REGION, SqsSourceBuilder};
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::source::{Source, SourceType};

    #[tokio::test]
    async fn test_sqs_message_conversion() {
        let ts = Utc::now();
        let mut headers = HashMap::new();
        headers.insert("foo".to_string(), "bar".to_string());

        let sqs_message = SqsMessage {
            key: "key".to_string(),
            payload: Bytes::from("value".to_string()),
            offset: "offset".to_string(),
            event_time: ts,
            attributes: None,
            headers: headers.clone(),
        };

        let message: Message = sqs_message.try_into().unwrap();

        assert_eq!(message.keys.len(), 1);
        assert_eq!(message.keys[0], "key");
        assert_eq!(message.value, "value");
        assert_eq!(
            message.offset,
            Offset::String(StringOffset::new("offset".to_string(), 0)),
        );
        assert_eq!(message.event_time, ts);
        assert_eq!(*message.headers, headers);
    }

    #[tokio::test]
    async fn test_sqs_source_e2e() {
        let queue_url_output = get_queue_url_output();

        let receive_message_output = get_receive_message_output();

        let delete_message_output = get_delete_message_output();

        let queue_attributes_output = get_queue_attributes_output();

        let sqs_operation_mocks = MockResponseInterceptor::new()
            .rule_mode(RuleMode::MatchAny)
            .with_rule(&queue_url_output)
            .with_rule(&receive_message_output)
            .with_rule(&delete_message_output)
            .with_rule(&queue_attributes_output);

        let sqs_client =
            aws_sdk_sqs::Client::from_conf(get_test_config_with_interceptor(sqs_operation_mocks));

        let sqs_source = SqsSourceBuilder::new(SqsSourceConfig {
            region: SQS_DEFAULT_REGION,
            queue_name: "test-q",
            queue_owner_aws_account_id: "12345678912",
            visibility_timeout: None,
            max_number_of_messages: None,
            wait_time_seconds: None,
            endpoint_url: None,
            attribute_names: vec![],
            message_attribute_names: vec![],
            assume_role_config: None,
        })
        .batch_size(1)
        .timeout(Duration::from_secs(1))
        .client(sqs_client)
        .build(CancellationToken::new())
        .await
        .unwrap();

        // create SQS source with test client
        use crate::tracker::Tracker;
        let tracker = Tracker::new(None, CancellationToken::new());
        let cln_token = CancellationToken::new();

        let source: Source<crate::typ::WithoutRateLimiter> = Source::new(
            1,
            SourceType::Sqs(sqs_source),
            tracker.clone(),
            true,
            None,
            None,
            None,
        )
        .await;

        // create sink writer
        use crate::sinker::sink::{SinkClientType, SinkWriterBuilder};
        let sink_writer =
            SinkWriterBuilder::new(10, Duration::from_millis(100), SinkClientType::Log)
                .build()
                .await
                .unwrap();

        // create the forwarder with the source and sink writer
        let forwarder =
            crate::monovertex::forwarder::Forwarder::new(source.clone(), None, sink_writer);

        let cancel_token = cln_token.clone();
        let _forwarder_handle: JoinHandle<crate::error::Result<()>> = tokio::spawn(async move {
            forwarder.start(cancel_token).await?;
            Ok(())
        });

        tokio::time::sleep(Duration::from_millis(1000)).await;

        // wait for one sec to check if the pending becomes zero, because all the messages
        // should be read and acked; if it doesn't, then fail the test
        let tokio_result = tokio::time::timeout(Duration::from_secs(5), async move {
            loop {
                let pending = source.pending().await.unwrap();
                if pending == Some(0) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        assert!(
            tokio_result.is_ok(),
            "Timeout occurred before pending became zero"
        );

        tracing::info!("queue url output calls: {}", queue_url_output.num_calls());
        tracing::info!(
            "receive message output calls: {}",
            receive_message_output.num_calls()
        );
        tracing::info!(
            "delete message output calls: {}",
            delete_message_output.num_calls()
        );
        tracing::info!(
            "queue attributes output calls: {}",
            queue_attributes_output.num_calls()
        );
    }

    fn get_queue_attributes_output() -> Rule {
        mock!(aws_sdk_sqs::Client::get_queue_attributes)
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
            })
    }

    fn get_delete_message_output() -> Rule {
        mock!(aws_sdk_sqs::Client::delete_message_batch)
            .match_requests(|inp| {
                inp.queue_url().unwrap()
                    == "https://sqs.us-west-2.amazonaws.com/926113353675/test-q/"
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
            })
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
        mock!(aws_sdk_sqs::Client::get_queue_url)
            .match_requests(|inp| inp.queue_name().unwrap() == "test-q")
            .then_output(|| {
                aws_sdk_sqs::operation::get_queue_url::GetQueueUrlOutput::builder()
                    .queue_url("https://sqs.us-west-2.amazonaws.com/926113353675/test-q/")
                    .build()
            })
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
