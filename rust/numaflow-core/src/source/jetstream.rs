use std::sync::Arc;
use std::time::Duration;

use numaflow_jetstream::{JetstreamSource, JetstreamSourceConfig};

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::message::{IntOffset, MessageID, Metadata, Offset};
use crate::source::SourceReader;
use crate::{message::Message, Error, Result};

use super::SourceAcker;

impl From<numaflow_jetstream::Message> for Message {
    fn from(message: numaflow_jetstream::Message) -> Self {
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
            event_time: Default::default(),
            watermark: None,
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: offset.to_string().into(),
                index: 0,
            },
            headers: message.headers,
            metadata: Some(Metadata {
                previous_vertex: get_vertex_name().to_string(),
            }),
        }
    }
}

impl From<numaflow_jetstream::Error> for crate::Error {
    fn from(value: numaflow_jetstream::Error) -> Self {
        Self::Source(format!("Jetstream source: {value:?}"))
    }
}

pub(crate) async fn new_jetstream_source(
    cfg: JetstreamSourceConfig,
    batch_size: usize,
    timeout: Duration,
) -> crate::Result<JetstreamSource> {
    Ok(JetstreamSource::connect(cfg, batch_size, timeout).await?)
}

impl SourceReader for JetstreamSource {
    fn name(&self) -> &'static str {
        "Jetstream"
    }

    async fn read(&mut self) -> Result<Vec<Message>> {
        Ok(self
            .read_messages()
            .await?
            .into_iter()
            .map(Message::from)
            .collect())
    }

    async fn partitions(&mut self) -> Result<Vec<u16>> {
        Ok(vec![*get_vertex_replica()])
    }

    async fn is_ready(&mut self) -> bool {
        true
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
}

impl super::LagReader for JetstreamSource {
    async fn pending(&mut self) -> crate::error::Result<Option<usize>> {
        Ok(self.pending_messages().await?)
    }
}

#[cfg(test)]
mod tests {
    use crate::reader::LagReader;

    use super::*;
    use bytes::Bytes;
    use numaflow_jetstream::Message as JetstreamMessage;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_try_from_jetstream_message_success() {
        let jetstream_message = JetstreamMessage {
            value: Bytes::from("test_value"),
            stream_sequence: 42,
            headers: {
                let mut headers = HashMap::new();
                headers.insert("key".to_string(), "value".to_string());
                headers
            },
        };

        let message: Message = jetstream_message.into();

        assert_eq!(message.value, Bytes::from("test_value"));
        assert_eq!(message.offset.to_string(), "42-0");
        assert_eq!(message.headers.get("key"), Some(&"value".to_string()));
        assert_eq!(message.metadata.unwrap().previous_vertex, get_vertex_name());
    }

    #[tokio::test]
    async fn test_jetstream_source_reader_acker_lagreader() {
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

        stream
            .get_or_create_consumer(
                stream_name,
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(stream_name.to_string()),
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
        let config = numaflow_jetstream::JetstreamSourceConfig {
            addr: "localhost".to_string(),
            stream: stream_name.to_string(),
            consumer: stream_name.to_string(),
            auth: None,
            tls: None,
        };

        let read_timeout = Duration::from_secs(1);
        let mut source = super::new_jetstream_source(config, 20, read_timeout)
            .await
            .unwrap();

        assert!(source.is_ready().await);
        assert_eq!(source.partitions().await.unwrap(), vec![0]);

        // Test SourceReader::read
        let messages = source.read().await.unwrap();
        assert_eq!(messages.len(), 20, "Should read 20 messages in a batch");
        assert_eq!(messages[0].value, Bytes::from("message 0"));
        assert_eq!(messages[19].value, Bytes::from("message 19"));

        // Test SourceAcker::ack
        let offsets: Vec<Offset> = messages.iter().map(|msg| msg.offset.clone()).collect();
        source.ack(offsets).await.unwrap();

        // Test LagReader::pending
        let pending = source.pending().await.unwrap();
        assert_eq!(
            pending,
            Some(30),
            "Pending messages should be 30 after acking 20 messages"
        );

        // Read and ack remaining messages
        let messages = source.read().await.unwrap();
        assert_eq!(messages.len(), 20, "Should read another 20 messages");
        let offsets: Vec<Offset> = messages.iter().map(|msg| msg.offset.clone()).collect();
        source.ack(offsets).await.unwrap();

        let pending = source.pending().await.unwrap();
        assert_eq!(
            pending,
            Some(10),
            "Pending messages should be 10 after acking another 20 messages"
        );

        let messages = source.read().await.unwrap();
        assert_eq!(messages.len(), 10, "Should read the last 10 messages");
        let offsets: Vec<Offset> = messages.iter().map(|msg| msg.offset.clone()).collect();
        source.ack(offsets).await.unwrap();

        let pending = source.pending().await.unwrap();
        assert_eq!(
            pending,
            Some(0),
            "Pending messages should be 0 after acking all messages"
        );
    }
}
