use std::sync::Arc;
use std::time::Duration;

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::message::Message;
use crate::message::{MessageID, Metadata, Offset, StringOffset};
use crate::source::SourceReader;
use numaflow_nats::nats::{NatsMessage, NatsSource, NatsSourceConfig};

use super::SourceAcker;

pub(crate) async fn new_nats_source(
    config: NatsSourceConfig,
    batch_size: usize,
    read_timeout: Duration,
) -> crate::Result<NatsSource> {
    Ok(NatsSource::connect(config, batch_size, read_timeout).await?)
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
            metadata: Some(Metadata {
                previous_vertex: get_vertex_name().to_string(),
                sys_metadata: Default::default(),
                user_metadata: Default::default(),
            }),
            is_late: false,
        }
    }
}

impl SourceReader for NatsSource {
    fn name(&self) -> &'static str {
        "NATS"
    }

    async fn read(&mut self) -> crate::Result<Vec<Message>> {
        Ok(self
            .read_messages()
            .await?
            .into_iter()
            .map(Message::from)
            .collect())
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
}

impl super::LagReader for NatsSource {
    async fn pending(&mut self) -> crate::error::Result<Option<usize>> {
        // NATS pending is always None
        Ok(None)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use bytes::Bytes;
    use numaflow_nats::nats::NatsMessage;

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
        assert_eq!(message.offset.to_string(), format!("msg-id-123-0"));
        assert_eq!(message.metadata.unwrap().previous_vertex, get_vertex_name());
        assert_eq!(message.event_time, test_timestamp);
        assert_eq!(message.event_time.timestamp(), 1672576245);
        assert_eq!(message.event_time.timestamp_subsec_nanos(), 123456789);
    }
}
