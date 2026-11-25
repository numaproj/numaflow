//! Numaflow-core wiring for the ROS 2 source.
//!
//! This module implements the three source traits:
//! - [`SourceReader`] — reads a batch of messages from the ROS 2 topic.
//! - [`SourceAcker`] — ack/nack are no-ops (ROS 2 pub/sub has no acknowledgement).
//! - [`LagReader`]   — pending is `None` (DDS does not expose an offset concept).

use std::sync::Arc;

use numaflow_ros2::source::{Ros2Message, Ros2Source, Ros2SourceConfig};
use tokio_util::sync::CancellationToken;

use crate::config::{get_vertex_name, get_vertex_replica};
use crate::error::Error;
use crate::message::{Message, MessageID, NackOffset, Offset, StringOffset};
use crate::metadata::Metadata;
use crate::source::{LagReader, SourceAcker, SourcePartitions, SourceReader};

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

pub(crate) async fn new_ros2_source(
    config: Ros2SourceConfig,
    cancel_token: CancellationToken,
) -> crate::Result<Ros2Source> {
    Ok(Ros2Source::connect(config, cancel_token).await?)
}

// ---------------------------------------------------------------------------
// Message conversion
// ---------------------------------------------------------------------------

impl From<Ros2Message> for Message {
    fn from(msg: Ros2Message) -> Self {
        let offset = Offset::String(StringOffset::new(msg.offset.clone(), *get_vertex_replica()));
        Message {
            typ: Default::default(),
            keys: Arc::from(vec![]),
            tags: None,
            value: msg.payload,
            offset: offset.clone(),
            event_time: msg.event_time,
            watermark: None,
            id: MessageID {
                vertex_name: get_vertex_name().to_string().into(),
                offset: offset.to_string().into(),
                index: 0,
            },
            headers: Default::default(),
            metadata: Some(Arc::new(Metadata::default())),
            is_late: false,
            nack_options: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

impl From<numaflow_ros2::Error> for Error {
    fn from(e: numaflow_ros2::Error) -> Self {
        match e {
            numaflow_ros2::Error::NonRetryable(msg) => Error::NonRetryable(msg),
            other => Error::Source(other.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// Trait impls
// ---------------------------------------------------------------------------

impl SourceReader for Ros2Source {
    fn name(&self) -> &'static str {
        "ROS2"
    }

    async fn read(&mut self) -> Option<crate::Result<Vec<Message>>> {
        match self.read_messages().await {
            Some(Ok(msgs)) => Some(Ok(msgs.into_iter().map(Message::from).collect())),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }

    async fn partitions(&mut self) -> crate::Result<SourcePartitions> {
        // ROS 2 pub/sub has no partition concept; expose one logical partition
        // per vertex replica, mirroring the NATS source.
        Ok(SourcePartitions::new(vec![*get_vertex_replica()], None))
    }
}

impl SourceAcker for Ros2Source {
    async fn ack(&mut self, _offsets: Vec<Offset>) -> crate::Result<()> {
        // ROS 2 pub/sub does not support acknowledgement.
        Ok(())
    }

    async fn nack(&mut self, _offsets: Vec<NackOffset>) -> crate::Result<()> {
        // ROS 2 pub/sub does not support nack.
        Ok(())
    }
}

impl LagReader for Ros2Source {
    async fn pending(&mut self) -> crate::Result<Option<usize>> {
        // DDS does not expose an offset / pending-message count.
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;
    use numaflow_ros2::source::Ros2Message;

    #[test]
    fn test_ros2_message_to_numaflow_message() {
        let ros_msg = Ros2Message {
            payload: Bytes::from("hello ros2"),
            event_time: Utc::now(),
            offset: "/robot/telemetry:42".to_string(),
        };

        let msg = Message::from(ros_msg);
        assert_eq!(msg.value, Bytes::from("hello ros2"));
        assert!(msg.offset.to_string().contains("42"));
        assert!(msg.metadata.is_some());
    }
}
