use std::sync::Arc;
use std::time::Duration;

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::message::Message;
use crate::message::{MessageID, Offset, StringOffset};
use crate::metadata::Metadata;
use crate::source::SourceReader;
use numaflow_nats::nats::{NatsMessage, NatsSource, NatsSourceConfig};

use super::SourceAcker;

pub(crate) async fn new_nats_source(
    config: NatsSourceConfig,
    batch_size: usize,
    read_timeout: Duration,
    cancel_token: tokio_util::sync::CancellationToken,
) -> crate::Result<NatsSource> {
    Ok(NatsSource::connect(config, batch_size, read_timeout, cancel_token).await?)
}

impl From<NatsMessage> for Message {
    fn from(message: NatsMessage) -> Self {
        let offset = Offset::String(StringOffset::new(message.id.clone(), *get_vertex_replica()));
        Message {
            typ: Default::default(),
            keys: Arc::from(vec![]),
            tags: None,
            value: message.value,
            offset: offset.clone(),
            event_time: message.event_time,
            watermark: None,
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: offset.to_string().into(),
                index: 0,
            },
            headers: Default::default(),
            // Set default metadata so that metadata is always present.
            metadata: Some(Arc::new(Metadata::default())),
            is_late: false,
            ack_handle: None,
        }
    }
}

impl SourceReader for NatsSource {
    fn name(&self) -> &'static str {
        "NATS"
    }

    async fn read(&mut self) -> Option<crate::Result<Vec<Message>>> {
        match self.read_messages().await {
            Some(Ok(messages)) => Some(Ok(messages.into_iter().map(Message::from).collect())),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }

    async fn partitions(&mut self) -> crate::Result<Vec<u16>> {
        Ok(vec![*get_vertex_replica()])
    }
}

impl SourceAcker for NatsSource {
    async fn ack(&mut self, _offsets: Vec<Offset>) -> crate::Result<()> {
        // NATS ack is a no-op
        Ok(())
    }

    async fn nack(&mut self, _offsets: Vec<Offset>) -> crate::Result<()> {
        // NATS nack is a no-op (plain NATS doesn't support nack)
        Ok(())
    }
}

impl super::LagReader for NatsSource {
    async fn pending(&mut self) -> crate::error::Result<Option<usize>> {
        // NATS pending is always None
        Ok(None)
    }
}

#[cfg(test)]
mod tests {

    use crate::reader::LagReader;

    use super::*;
    use bytes::Bytes;
    use numaflow_nats::nats::NatsMessage;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_try_from_nats_message_success() {
        let test_timestamp = chrono::DateTime::parse_from_rfc3339("2023-01-01T12:30:45.123456789Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        let nats_message = NatsMessage {
            value: Bytes::from("test_value"),
            id: "msg-id-123".to_string(),
            event_time: test_timestamp,
        };

        let message: Message = nats_message.into();

        assert_eq!(message.value, Bytes::from("test_value"));
        assert_eq!(message.offset.to_string(), "msg-id-123-0");
        assert_eq!(message.metadata.unwrap().previous_vertex, "");
        assert_eq!(message.event_time, test_timestamp);
        assert_eq!(message.event_time.timestamp(), 1672576245);
        assert_eq!(message.event_time.timestamp_subsec_nanos(), 123456789);
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_new_nats_source_returns_source() {
        let config = NatsSourceConfig {
            addr: "localhost".to_string(),
            subject: "dummy".to_string(),
            queue: "dummy".to_string(),
            auth: None,
            tls: None,
        };
        let batch_size = 1;
        let read_timeout = Duration::from_millis(10);
        let result =
            super::new_nats_source(config, batch_size, read_timeout, CancellationToken::new())
                .await;
        assert!(result.is_ok());
        let source = result.unwrap();
        let name = source.name();
        assert_eq!(name, "NATS");
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_nats_source_read_empty_returns_empty_vec() {
        // It simply checks that read() returns an empty Vec when there are no messages.
        // We already have checked read_messages functionality in the test_nats_source test.
        let config = NatsSourceConfig {
            addr: "localhost".to_string(),
            subject: "test_nats_source_read_empty".to_string(),
            queue: "test_nats_source_read_empty".to_string(),
            auth: None,
            tls: None,
        };
        let batch_size = 2;
        let read_timeout = Duration::from_millis(50);

        // Connect to the NATS server
        let mut source =
            NatsSource::connect(config, batch_size, read_timeout, CancellationToken::new())
                .await
                .unwrap();
        let messages = source.read().await.unwrap().unwrap();
        assert!(
            messages.is_empty(),
            "Expected empty Vec when no messages are published"
        );
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_nats_source_pending_returns_none() {
        // A dummy NatsSource
        let config = NatsSourceConfig {
            addr: "localhost".to_string(),
            subject: "dummy".to_string(),
            queue: "dummy".to_string(),
            auth: None,
            tls: None,
        };

        // Connect to the NATS server
        let mut source = NatsSource::connect(
            config,
            1,
            std::time::Duration::from_millis(10),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let pending = source.pending().await.unwrap();
        assert_eq!(pending, None);
    }

    #[tokio::test]
    async fn test_nats_source_ack_is_noop() {
        // A dummy NatsSource
        let config = NatsSourceConfig {
            addr: "localhost".to_string(),
            subject: "dummy".to_string(),
            queue: "dummy".to_string(),
            auth: None,
            tls: None,
        };

        // Connect to the NATS server
        let mut source = NatsSource::connect(
            config,
            1,
            std::time::Duration::from_millis(10),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        // Ack should succeed and do nothing
        let result = source.ack(vec![]).await;
        assert!(result.is_ok());
    }
}
