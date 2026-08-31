use numaflow_nats::jetstream::{
    JetstreamSource, JetstreamSourceConfig, JetstreamSourceState, Message as JetstreamMessage,
};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::error::{SourceFailureAction, SourceFailureImpact};
use crate::message::{IntOffset, MessageID, NackOffset, Offset};
use crate::metadata::Metadata;
use crate::source::SourceReader;
use crate::source::builtin::{BuiltinSourceBackend, ConnectFactory, SourceBackend};
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
            nack_options: None,
        }
    }
}

fn nats_error_to_source_redrive(source_name: &'static str, value: numaflow_nats::Error) -> Error {
    let (action, code) = match &value {
        numaflow_nats::Error::Connection { .. } => {
            (SourceFailureAction::StayDegraded, "nats_connection_config")
        }
        numaflow_nats::Error::Subscription { .. } => (
            SourceFailureAction::StayDegraded,
            "nats_subscription_config",
        ),
        numaflow_nats::Error::Jetstream(_) => (SourceFailureAction::Recreate, "jetstream"),
        numaflow_nats::Error::Nats(_) => (SourceFailureAction::Recreate, "nats"),
        numaflow_nats::Error::Other(_) => (SourceFailureAction::Recreate, "nats_internal"),
    };
    Error::SourceRedrive {
        source_name,
        operation: "backend",
        action,
        impact: SourceFailureImpact::Outage,
        code,
        message: value.to_string(),
    }
}

impl From<numaflow_nats::Error> for Error {
    fn from(value: numaflow_nats::Error) -> Self {
        nats_error_to_source_redrive("NATS", value)
    }
}

#[cfg(test)]
pub(crate) async fn new_jetstream_source(
    cfg: JetstreamSourceConfig,
    batch_size: usize,
    timeout: Duration,
    cancel_token: CancellationToken,
) -> Result<JetstreamSource> {
    JetstreamSource::connect(cfg, batch_size, timeout, cancel_token)
        .await
        .map_err(|e| nats_error_to_source_redrive("Jetstream", e))
}

pub(crate) fn new_jetstream_source_factory(
    config: JetstreamSourceConfig,
    batch_size: usize,
    timeout: Duration,
    cancel_token: CancellationToken,
) -> ConnectFactory {
    let state = JetstreamSourceState::default();
    ConnectFactory::new("Jetstream", move || {
        let config = config.clone();
        let cancel_token = cancel_token.clone();
        let state = state.clone();
        async move {
            let source = JetstreamSource::connect_with_state(
                config,
                batch_size,
                timeout,
                cancel_token,
                state.clone(),
            )
            .await
            .map_err(|e| nats_error_to_source_redrive("Jetstream", e))?;
            let source_to_retire = source.clone();
            Ok(Box::new(SourceBackend::with_retire(source, move || {
                let state = state.clone();
                let source = source_to_retire.clone();
                async move {
                    // Stop the actor before draining shared WIP state so the retiring generation
                    // cannot insert another stale tracker after the drain.
                    source.shutdown().await;
                    state.retire_in_progress_generation().await;
                }
            })) as Box<dyn BuiltinSourceBackend>)
        }
    })
}

impl SourceReader for JetstreamSource {
    fn name(&self) -> &'static str {
        "Jetstream"
    }

    async fn read(&mut self) -> Option<Result<Vec<Message>>> {
        match self.read_messages().await {
            Ok(messages) => Some(Ok(messages.into_iter().map(Message::from).collect())),
            Err(e) => Some(Err(nats_error_to_source_redrive("Jetstream", e))),
        }
    }

    async fn partitions(&mut self) -> Result<super::SourcePartitions> {
        Ok(super::SourcePartitions::new(
            vec![*get_vertex_replica()],
            None,
        ))
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
        self.ack_messages(jetstream_offsets)
            .await
            .map_err(|e| nats_error_to_source_redrive("Jetstream", e))?;
        Ok(())
    }

    async fn nack(&mut self, offsets: Vec<NackOffset>) -> Result<()> {
        let mut jetstream_offsets = Vec::with_capacity(offsets.len());
        for offset in offsets {
            let Offset::Int(seq_num) = offset.offset else {
                return Err(Error::Source(format!(
                    "Expected integer offset for Jetstream source. Got: {:?}",
                    offset.offset
                )));
            };
            jetstream_offsets.push(seq_num.offset as u64);
        }
        self.nack_messages(jetstream_offsets)
            .await
            .map_err(|e| nats_error_to_source_redrive("Jetstream", e))?;
        Ok(())
    }
}

impl super::LagReader for JetstreamSource {
    async fn pending(&mut self) -> Result<Option<usize>> {
        self.pending_messages()
            .await
            .map_err(|e| nats_error_to_source_redrive("Jetstream", e))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use numaflow_nats::jetstream::ConsumerDeliverPolicy;
    use numaflow_nats::jetstream::Message as JetstreamMessage;

    use super::*;

    #[test]
    fn nats_connection_configuration_stays_degraded() {
        let error: Error = numaflow_nats::Error::Connection {
            server: "nats://localhost".into(),
            error: "authorization violation".into(),
        }
        .into();
        assert!(matches!(
            error,
            Error::SourceRedrive {
                source_name: "NATS",
                action: SourceFailureAction::StayDegraded,
                impact: SourceFailureImpact::Outage,
                code: "nats_connection_config",
                ..
            }
        ));
    }

    #[test]
    fn jetstream_errors_use_jetstream_source_name() {
        let error = nats_error_to_source_redrive(
            "Jetstream",
            numaflow_nats::Error::Connection {
                server: "nats://localhost".into(),
                error: "authorization violation".into(),
            },
        );
        assert!(matches!(
            error,
            Error::SourceRedrive {
                source_name: "Jetstream",
                action: SourceFailureAction::StayDegraded,
                code: "nats_connection_config",
                ..
            }
        ));
    }

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

        let partitions = source.partitions().await.unwrap();
        assert_eq!(partitions.active_partitions, vec![0]);
        assert!(partitions.total_partitions.is_none());

        // Test SourceReader::read
        let messages = source.read().await.unwrap().unwrap();
        assert_eq!(messages.len(), 20, "Should read 20 messages in a batch");
        assert_eq!(
            messages
                .first()
                .expect("Expected at least one message")
                .value,
            Bytes::from("message 0")
        );
        assert_eq!(
            messages
                .last()
                .expect("Expected at least one message")
                .value,
            Bytes::from("message 19")
        );

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

    #[tokio::test]
    async fn jetstream_source_factory_build_fails_with_unreachable_server() {
        use crate::source::builtin::BuiltinSourceFactory;

        let factory = new_jetstream_source_factory(
            JetstreamSourceConfig {
                addr: "nats://127.0.0.1:1".into(),
                stream: "test-stream".into(),
                consumer: "test-consumer".into(),
                deliver_policy: ConsumerDeliverPolicy::NEW,
                filter_subjects: vec![],
                auth: None,
                tls: None,
            },
            1,
            Duration::from_millis(100),
            CancellationToken::new(),
        );
        assert!(factory.build().await.is_err());
    }
}
