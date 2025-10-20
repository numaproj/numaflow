use numaflow_nats::jetstream::{
    JetstreamSource, JetstreamSourceConfig, Message as JetstreamMessage,
};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::message::{IntOffset, MessageID, Offset};
use crate::metadata::Metadata;
use crate::source::SourceReader;
use crate::{Error, Result, message::Message};

use super::SourceAcker;

impl From<JetstreamMessage> for Message {
    fn from(message: JetstreamMessage) -> Self {
        let offset = Offset::Int(IntOffset::new(
            message.stream_sequence as i64,
            *get_vertex_replica(),
        ));

        Message {
            typ: Default::default(),
            keys: Arc::from(vec![]),
            tags: None,
            value: message.value,
            offset: offset.clone(),
            event_time: message.published_timestamp,
            watermark: None,
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: offset.to_string().into(),
                index: 0,
            },
            headers: Arc::new(message.headers),
            // Set default metadata so that metadata is always present.
            metadata: Some(Arc::new(Metadata::default())),
            is_late: false,
            ack_handle: None,
        }
    }
}

impl From<numaflow_nats::Error> for Error {
    fn from(value: numaflow_nats::Error) -> Self {
        Self::Source(format!("Jetstream source: {value:?}"))
    }
}

pub(crate) async fn new_jetstream_source(
    cfg: JetstreamSourceConfig,
    batch_size: usize,
    timeout: Duration,
    cancel_token: CancellationToken,
) -> Result<JetstreamSource> {
    Ok(JetstreamSource::connect(cfg, batch_size, timeout, cancel_token).await?)
}

impl SourceReader for JetstreamSource {
    fn name(&self) -> &'static str {
        "Jetstream"
    }

    async fn read(&mut self) -> Option<Result<Vec<Message>>> {
        match self.read_messages().await {
            Ok(messages) => Some(Ok(messages.into_iter().map(Message::from).collect())),
            Err(e) => Some(Err(e.into())),
        }
    }

    async fn partitions(&mut self) -> Result<Vec<u16>> {
        Ok(vec![*get_vertex_replica()])
    }
}

impl SourceAcker for JetstreamSource {
    async fn ack(&mut self, offsets: Vec<Offset>) -> Result<()> {
        let mut jetstream_offsets = Vec::with_capacity(offsets.len());
        for offset in offsets {
            let Offset::Int(seq_num) = offset else {
                return Err(Error::Source(format!(
                    "Expected integer offset for Jetstream source. Got: {offset:?}"
                )));
            };
            jetstream_offsets.push(seq_num.offset as u64);
        }
        self.ack_messages(jetstream_offsets).await?;
        Ok(())
    }

    async fn nack(&mut self, offsets: Vec<Offset>) -> Result<()> {
        let mut jetstream_offsets = Vec::with_capacity(offsets.len());
        for offset in offsets {
            let Offset::Int(seq_num) = offset else {
                return Err(Error::Source(format!(
                    "Expected integer offset for Jetstream source. Got: {offset:?}"
                )));
            };
            jetstream_offsets.push(seq_num.offset as u64);
        }
        self.nack_messages(jetstream_offsets).await?;
        Ok(())
    }
}

impl super::LagReader for JetstreamSource {
    async fn pending(&mut self) -> Result<Option<usize>> {
        Ok(self.pending_messages().await?)
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use bytes::Bytes;
    use numaflow_nats::jetstream::ConsumerDeliverPolicy;
    use numaflow_nats::jetstream::Message as JetstreamMessage;

    use super::*;

    #[tokio::test]
    async fn test_try_from_jetstream_message_success() {
        let test_timestamp = chrono::DateTime::parse_from_rfc3339("2023-01-01T12:30:45.123456789Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        let jetstream_message = JetstreamMessage {
            value: Bytes::from("test_value"),
            stream_sequence: 42,
            headers: {
                let mut headers = HashMap::new();
                headers.insert("key".to_string(), "value".to_string());
                headers
            },
            published_timestamp: test_timestamp,
        };

        let message: Message = jetstream_message.into();

        assert_eq!(message.value, Bytes::from("test_value"));
        assert_eq!(message.offset.to_string(), "42-0");
        assert_eq!(message.headers.get("key"), Some(&"value".to_string()));
        assert_eq!(message.metadata.unwrap().previous_vertex, "");

        // Verify that the published timestamp is correctly used as event_time
        assert_eq!(message.event_time, test_timestamp);
        assert_eq!(message.event_time.timestamp(), 1672576245);
        assert_eq!(message.event_time.timestamp_subsec_nanos(), 123456789);
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_source_reader_acker_lagreader() {
        use crate::reader::LagReader;

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // Setup Jetstream context and stream
        let client = async_nats::connect("localhost").await.unwrap();
        let js = async_nats::jetstream::new(client);

        let stream_name = "test_stream_js_source_numa_core";
        let _ = js.delete_stream(stream_name).await;
        let stream = js
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: stream_name.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        let consumer = format!("{}_consumer", stream_name);
        stream
            .get_or_create_consumer(
                &consumer,
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(consumer.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        // Publish messages to the stream
        for i in 0..50 {
            js.publish(stream_name.to_string(), format!("message {}", i).into())
                .await
                .unwrap();
        }

        // Configure JetstreamSource
        let config = numaflow_nats::jetstream::JetstreamSourceConfig {
            addr: "localhost".to_string(),
            stream: stream_name.to_string(),
            consumer,
            filter_subjects: vec![],
            deliver_policy: ConsumerDeliverPolicy::ALL,
            auth: None,
            tls: None,
        };

        let read_timeout = Duration::from_secs(1);
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let mut source: JetstreamSource =
            super::new_jetstream_source(config, 20, read_timeout, cancel_token)
                .await
                .unwrap();

        assert_eq!(source.partitions().await.unwrap(), vec![0]);

        // Test SourceReader::read
        let messages = source.read().await.unwrap().unwrap();
        assert_eq!(messages.len(), 20, "Should read 20 messages in a batch");
        assert_eq!(messages[0].value, Bytes::from("message 0"));
        assert_eq!(messages[19].value, Bytes::from("message 19"));

        // Verify that event_time is set to the published timestamp, not default
        for message in &messages {
            assert_ne!(
                message.event_time.timestamp(),
                0,
                "Event time should not be default value, should be set to published timestamp"
            );
        }

        // Test SourceAcker::ack
        let offsets: Vec<Offset> = messages.iter().map(|msg| msg.offset.clone()).collect();
        source.ack(offsets).await.unwrap();
        // When we query pending message count from Nats server immediately after acking a batch of
        // messages, Nats intermittently returns wrong value.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Test LagReader::pending
        let pending = source.pending().await.unwrap();
        assert_eq!(
            pending,
            Some(30),
            "Pending messages should be 30 after acking 20 messages"
        );

        // Read and ack remaining messages
        let messages = source.read().await.unwrap().unwrap();
        assert_eq!(messages.len(), 20, "Should read another 20 messages");
        let offsets: Vec<Offset> = messages.iter().map(|msg| msg.offset.clone()).collect();
        source.ack(offsets).await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        let pending = source.pending().await.unwrap();
        assert_eq!(
            pending,
            Some(10),
            "Pending messages should be 10 after acking another 20 messages"
        );

        let messages = source.read().await.unwrap().unwrap();
        assert_eq!(messages.len(), 10, "Should read the last 10 messages");
        let offsets: Vec<Offset> = messages.iter().map(|msg| msg.offset.clone()).collect();
        source.ack(offsets).await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        let pending = source.pending().await.unwrap();
        assert_eq!(
            pending,
            Some(0),
            "Pending messages should be 0 after acking all messages"
        );
    }
}
