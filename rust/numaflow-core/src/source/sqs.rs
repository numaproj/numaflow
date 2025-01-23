use std::sync::Arc;
use std::time::Duration;

use numaflow_sqs::source::{SQSMessage, SQSSource, SQSSourceConfig};

use crate::config::get_vertex_name;
use crate::error::Error;
use crate::message::{Message, MessageID, Offset, StringOffset};
use crate::source;

impl TryFrom<SQSMessage> for Message {
    type Error = Error;

    fn try_from(message: SQSMessage) -> crate::Result<Self> {
        let offset = Offset::String(StringOffset::new(message.offset, 0));
        Ok(Message {
            keys: Arc::from(vec![message.key]),
            tags: None,
            value: message.payload,
            offset: Some(offset.clone()),
            event_time: message.event_time,
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: offset.to_string().into(),
                index: 0,
            },
            headers: message.headers,
        })
    }
}

impl From<numaflow_sqs::Error> for Error {
    fn from(value: numaflow_sqs::Error) -> Self {
        match value {
            numaflow_sqs::Error::SQS(e) => Error::Source(e.to_string()),
            numaflow_sqs::Error::UnknownOffset(_) => Error::Source(value.to_string()),
            numaflow_sqs::Error::ActorTaskTerminated(_) => {
                Error::ActorPatternRecv(value.to_string())
            }
            numaflow_sqs::Error::Other(e) => Error::Source(e),
        }
    }
}

pub(crate) async fn new_sqs_source(
    cfg: SQSSourceConfig,
    batch_size: usize,
    timeout: Duration,
) -> crate::Result<SQSSource> {
    Ok(SQSSource::new(cfg, batch_size, timeout, None).await?)
}

impl source::SourceReader for SQSSource {
    fn name(&self) -> &'static str {
        "SQS"
    }

    async fn read(&mut self) -> crate::Result<Vec<Message>> {
        self.read_messages()
            .await?
            .into_iter()
            .map(|msg| msg.try_into())
            .collect()
    }

    fn partitions(&self) -> Vec<u16> {
        Self::partitions(self)
    }
}

impl source::SourceAcker for SQSSource {
    async fn ack(&mut self, offsets: Vec<Offset>) -> crate::error::Result<()> {
        let mut sqs_offsets = Vec::with_capacity(offsets.len());
        for offset in offsets {
            let Offset::String(string_offset) = offset else {
                return Err(Error::Source(format!(
                    "Expected Offset::String type for SQS. offset={offset:?}"
                )));
            };
        }
        self.ack_offsets(sqs_offsets).await.map_err(Into::into)
    }
}

impl source::LagReader for SQSSource {
    async fn pending(&mut self) -> crate::error::Result<Option<usize>> {
        Ok(self.pending_count().await)
    }
}

#[cfg(feature = "sqs-tests")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{Source, SourceType};
    use bytes::Bytes;
    use chrono::Utc;
    use std::collections::HashMap;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_sqs_message_conversion() {
        let ts = Utc::now();
        let mut headers = HashMap::new();
        headers.insert("foo".to_string(), "bar".to_string());

        let sqs_message = SQSMessage {
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
            Some(Offset::String(StringOffset::new("offset".to_string(), 0)))
        );
        assert_eq!(message.event_time, ts);
        assert_eq!(message.headers, headers);
    }

    use aws_sdk_sqs::Config;
    use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use tokio::task::JoinHandle;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    #[tokio::test]
    async fn test_sqs_e2e() {

        tracing_subscriber::registry()
            .with(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info".into()),
            )
            .with(tracing_subscriber::fmt::layer().with_ansi(false))
            .init();

        // Setup SQS static replay client for testing purposes
        let queue_url = get_queue_url_request_response();
        let message = get_messages_request_response();
        let delete_message = get_delete_message_request_response();
        let queue_attributes = get_queue_attributes_request_response();

        let replay_client = StaticReplayClient::new(vec![
            queue_url,
            message,
            delete_message,
            queue_attributes,
        ]);
        let config = get_test_config(replay_client.clone());
        let client = aws_sdk_sqs::Client::from_conf(config);

        let sqs_source = SQSSource::new(
            SQSSourceConfig {
                region: "us-west-1".to_string(),
                queue_name: "test-q".to_string(),
            },
            10,
            Duration::from_secs(1),
            Some(client),
        )
        .await
        .unwrap();

        // create SQS source with test client
        use crate::tracker::TrackerHandle;
        let tracker_handle = TrackerHandle::new();
        let source = Source::new(1, SourceType::SQS(sqs_source), tracker_handle.clone(), true);

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
        let forwarder = crate::monovertex::forwarder::ForwarderBuilder::new(
            source.clone(),
            sink_writer,
            cln_token.clone(),
        )
        .build();

        let forwarder_handle: JoinHandle<crate::error::Result<()>> = tokio::spawn(async move {
            forwarder.start().await?;
            Ok(())
        });

        tokio::time::sleep(Duration::from_millis(1000)).await;

        // wait for one sec to check if the pending becomes zero, because all the messages
        // should be read and acked; if it doesn't, then fail the test
        let tokio_result = tokio::time::timeout(Duration::from_secs(1), async move {
            loop {
                let pending = source.pending().await.unwrap();
                // println!("Pending: {:?}", pending);
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

    fn get_delete_message_request_response() -> ReplayEvent {
        ReplayEvent::new(
            http::Request::builder()
                .method("POST")
                .uri(http::uri::Uri::from_static("https://sqs.us-west-2.amazonaws.com/"))
                .header("Content-Type", "application/x-amz-json-1.0")
                .body(SdkBody::from(r#"{"QueueUrl": "https://sqs.us-west-2.amazonaws.com/926113353675/test-q", "ReceiptHandle": "test_receipt_handle"}"#))
                .unwrap(),
            http::Response::builder()
                .status(http::StatusCode::from_u16(200).unwrap())
                .header("Content-Type", "application/x-amz-json-1.0")
                .body(SdkBody::from(r#"{}"#))
                .unwrap(),
        )
    }

    fn get_queue_attributes_request_response() -> ReplayEvent {
        ReplayEvent::new(
            http::Request::builder()
                .method("POST")
                .uri(http::uri::Uri::from_static("https://sqs.us-west-2.amazonaws.com/"))
                .header("Content-Type", "application/x-amz-json-1.0")
                .body(SdkBody::from(r#"{"QueueUrl": "https://sqs.us-west-2.amazonaws.com/926113353675/test-q", "AttributeNames": ["ApproximateNumberOfMessages"]}"#))
                .unwrap(),
            http::Response::builder()
                .status(http::StatusCode::from_u16(200).unwrap())
                .header("Content-Type", "application/x-amz-json-1.0")
                .body(SdkBody::from(r#"{
    "Attributes": {
        "ApproximateNumberOfMessages": "0"
    }
}"#))
                .unwrap(),
        )
    }

    fn get_test_config(replay_client: StaticReplayClient) -> Config {
        use aws_config::BehaviorVersion;

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
}
