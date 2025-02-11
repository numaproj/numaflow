use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use numaflow_sqs::source::{
    sent_timestamp_message_system_attribute_name, SqsMessage, SqsSource, SqsSourceBuilder,
    SqsSourceConfig,
};

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::error::Error;
use crate::message::{Message, MessageID, Offset, StringOffset};
use crate::source;

impl TryFrom<SqsMessage> for Message {
    type Error = Error;

    fn try_from(message: SqsMessage) -> crate::Result<Self> {
        let offset = Offset::String(StringOffset::new(message.offset, *get_vertex_replica()));

        // find the SentTimestamp header and convert it to a chrono::DateTime<Utc>
        let sent_timestamp = match message
            .headers
            .get(sent_timestamp_message_system_attribute_name())
        {
            Some(sent_timestamp_str) => sent_timestamp_str
                .parse::<i64>()
                .ok()
                .and_then(DateTime::from_timestamp_millis),
            None => None,
        };

        Ok(Message {
            keys: Arc::from(vec![message.key]),
            tags: None,
            value: message.payload,
            offset: offset.clone(),
            event_time: message.event_time,
            watermark: sent_timestamp,
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: offset.to_string().into(),
                index: 0,
            },
            headers: message.headers,
            metadata: None,
        })
    }
}

impl From<numaflow_sqs::Error> for Error {
    fn from(value: numaflow_sqs::Error) -> Self {
        match value {
            numaflow_sqs::Error::Sqs(e) => Error::Source(e.to_string()),
            numaflow_sqs::Error::ActorTaskTerminated(_) => {
                Error::ActorPatternRecv(value.to_string())
            }
            numaflow_sqs::Error::Other(e) => Error::Source(e),
        }
    }
}

pub(crate) async fn new_sqs_source(
    cfg: SqsSourceConfig,
    batch_size: usize,
    timeout: Duration,
) -> crate::Result<SqsSource> {
    Ok(SqsSourceBuilder::new()
        .config(cfg)
        .batch_size(batch_size)
        .timeout(timeout)
        .build()
        .await?)
}

impl source::SourceReader for SqsSource {
    fn name(&self) -> &'static str {
        "Sqs"
    }

    async fn read(&mut self) -> crate::Result<Vec<Message>> {
        self.read_messages()
            .await?
            .into_iter()
            .map(|msg| msg.try_into())
            .collect()
    }

    // if source doesn't support partitions, we should return the vec![vertex_replica]
    fn partitions(&self) -> Vec<u16> {
        vec![*get_vertex_replica()]
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

    use aws_sdk_sqs::config::BehaviorVersion;
    use aws_sdk_sqs::types::MessageSystemAttributeName;
    use aws_sdk_sqs::Config;
    use aws_smithy_mocks_experimental::{mock, MockResponseInterceptor, Rule, RuleMode};
    use bytes::Bytes;
    use chrono::Utc;
    use numaflow_sqs::source::{SqsSourceBuilder, SQS_DEFAULT_REGION};
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
        assert_eq!(message.headers, headers);
    }

    #[tokio::test]
    async fn test_sqs_e2e() {
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

        let sqs_source = SqsSourceBuilder::new()
            .config(SqsSourceConfig {
                region: SQS_DEFAULT_REGION.to_string(),
                queue_name: "test-q".to_string(),
            })
            .batch_size(1)
            .timeout(Duration::from_secs(1))
            .client(sqs_client)
            .build()
            .await
            .unwrap();

        // create SQS source with test client
        use crate::tracker::TrackerHandle;
        let tracker_handle = TrackerHandle::new(None, None);
        let source = Source::new(
            1,
            SourceType::Sqs(sqs_source),
            tracker_handle.clone(),
            true,
            None,
            None,
        );

        // create sink writer
        use crate::sink::{SinkClientType, SinkWriterBuilder};
        let sink_writer = SinkWriterBuilder::new(
            10,
            Duration::from_millis(100),
            SinkClientType::Log,
            tracker_handle.clone(),
        )
        .build()
        .await
        .unwrap();

        // create the forwarder with the source and sink writer
        let cln_token = CancellationToken::new();
        let forwarder = crate::monovertex::forwarder::Forwarder::new(
            source.clone(),
            sink_writer,
            cln_token.clone(),
        );

        let _forwarder_handle: JoinHandle<crate::error::Result<()>> = tokio::spawn(async move {
            forwarder.start().await?;
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
