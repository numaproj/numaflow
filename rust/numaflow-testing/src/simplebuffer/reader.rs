//! Simple buffer reader implementation.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use super::buffer::{BufferState, Message, MessageState, Offset};
use super::error::{Result, SimpleBufferError};
use super::error_injector::ErrorInjector;

/// Simple buffer reader.
#[derive(Debug, Clone)]
pub struct SimpleReader {
    pub(super) state: Arc<RwLock<BufferState>>,
    pub(super) name: &'static str,
    #[allow(dead_code)]
    pub(super) partition_idx: u16,
    pub(super) error_injector: Arc<ErrorInjector>,
    pub(super) wip_ack_interval: Duration,
}

impl SimpleReader {
    /// Fetches a batch of messages from the buffer.
    ///
    /// This is a blocking call that waits up to `timeout` for messages.
    /// Returns pending messages and marks them as in-flight.
    pub async fn fetch(&mut self, max: usize, timeout: Duration) -> Result<Vec<Message>> {
        // Apply artificial latency if set
        self.error_injector.apply_fetch_latency().await;

        // Check for injected fetch failure
        if self.error_injector.should_fail_fetch() {
            return Err(SimpleBufferError::Fetch(
                "injected fetch failure".to_string(),
            ));
        }

        let start = std::time::Instant::now();

        loop {
            {
                let mut state = self.state.write();
                let mut messages = Vec::new();
                let now = std::time::Instant::now();

                // Find pending messages and mark them as in-flight
                for slot in state.slots.iter_mut() {
                    if slot.state == MessageState::Pending {
                        slot.state = MessageState::InFlight;
                        slot.fetched_at = Some(now);
                        messages.push(slot.message.clone());

                        if messages.len() >= max {
                            break;
                        }
                    }
                }

                if !messages.is_empty() {
                    return Ok(messages);
                }
            }

            // Check timeout
            if start.elapsed() >= timeout {
                return Ok(Vec::new());
            }

            // Wait a bit before checking again
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Acknowledges successful processing of a message.
    ///
    /// The message is marked as acked and can be reclaimed.
    pub async fn ack(&self, offset: &Offset) -> Result<()> {
        // Apply artificial latency if set
        self.error_injector.apply_ack_latency().await;

        // Check for injected ack failure
        if self.error_injector.should_fail_ack() {
            return Err(SimpleBufferError::Ack("injected ack failure".to_string()));
        }

        let mut state = self.state.write();

        let slot = state
            .find_slot_mut(offset)
            .ok_or_else(|| SimpleBufferError::OffsetNotFound(offset.to_string()))?;

        if slot.state != MessageState::InFlight {
            return Err(SimpleBufferError::Ack(format!(
                "message not in-flight, current state: {:?}",
                slot.state
            )));
        }

        slot.state = MessageState::Acked;
        slot.fetched_at = None;

        Ok(())
    }

    /// Negative acknowledgment - indicates the message should be redelivered.
    ///
    /// The message is moved back to pending state.
    pub async fn nack(&self, offset: &Offset) -> Result<()> {
        // Check for injected nack failure
        if self.error_injector.should_fail_nack() {
            return Err(SimpleBufferError::Nack("injected nack failure".to_string()));
        }

        let mut state = self.state.write();

        let slot = state
            .find_slot_mut(offset)
            .ok_or_else(|| SimpleBufferError::OffsetNotFound(offset.to_string()))?;

        if slot.state != MessageState::InFlight {
            return Err(SimpleBufferError::Nack(format!(
                "message not in-flight, current state: {:?}",
                slot.state
            )));
        }

        // Move back to pending for redelivery
        slot.state = MessageState::Pending;
        slot.fetched_at = None;

        Ok(())
    }

    /// Returns the number of pending (unprocessed) messages.
    pub async fn pending(&mut self) -> Result<Option<usize>> {
        Ok(Some(self.state.read().pending_count()))
    }

    /// Returns the name/identifier of this reader.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Marks a message as work-in-progress to prevent redelivery.
    ///
    /// This resets the "fetched_at" timestamp to extend the processing window.
    pub async fn mark_wip(&self, offset: &Offset) -> Result<()> {
        // Check for injected wip ack failure
        if self.error_injector.should_fail_wip_ack() {
            return Err(SimpleBufferError::WipAck(
                "injected wip ack failure".to_string(),
            ));
        }

        let mut state = self.state.write();

        let slot = state
            .find_slot_mut(offset)
            .ok_or_else(|| SimpleBufferError::OffsetNotFound(offset.to_string()))?;

        if slot.state != MessageState::InFlight {
            return Err(SimpleBufferError::WipAck(format!(
                "message not in-flight, current state: {:?}",
                slot.state
            )));
        }

        // Reset the fetched_at timestamp
        slot.fetched_at = Some(std::time::Instant::now());

        Ok(())
    }

    /// Returns the interval at which WIP acks should be sent.
    pub fn wip_ack_interval(&self) -> Option<Duration> {
        Some(self.wip_ack_interval)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;

    use crate::simplebuffer::buffer::MessageID;

    fn create_test_reader() -> (SimpleReader, Arc<RwLock<BufferState>>) {
        let state = Arc::new(RwLock::new(BufferState::new(10, 0.8)));
        let error_injector = Arc::new(ErrorInjector::new());
        let reader = SimpleReader {
            state: Arc::clone(&state),
            name: "test-reader",
            partition_idx: 0,
            error_injector,
            wip_ack_interval: Duration::from_secs(5),
        };
        (reader, state)
    }

    fn create_test_message(seq: i64) -> Message {
        Message {
            keys: Arc::new(["key".to_string()]),
            tags: None,
            value: Bytes::from("test"),
            offset: Offset::new(seq, 0),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "test".to_string(),
                offset: seq.to_string(),
                index: 0,
            },
            headers: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_reader_name() {
        let (reader, _) = create_test_reader();
        assert_eq!(reader.name(), "test-reader");
    }

    #[test]
    fn test_reader_wip_ack_interval() {
        let (reader, _) = create_test_reader();
        assert_eq!(reader.wip_ack_interval(), Some(Duration::from_secs(5)));
    }

    #[tokio::test]
    async fn test_pending_empty_buffer() {
        let (mut reader, _) = create_test_reader();
        let pending = reader.pending().await.unwrap();
        assert_eq!(pending, Some(0));
    }

    #[tokio::test]
    async fn test_pending_with_messages() {
        let (mut reader, state) = create_test_reader();

        // Add pending messages to buffer
        {
            let mut s = state.write();
            let msg = create_test_message(1);
            s.slots.push_back(super::super::buffer::BufferSlot {
                message: msg,
                state: MessageState::Pending,
                sequence: 1,
                fetched_at: None,
            });
        }

        let pending = reader.pending().await.unwrap();
        assert_eq!(pending, Some(1));
    }

    #[tokio::test]
    async fn test_fetch_empty_buffer_timeout() {
        let (mut reader, _) = create_test_reader();
        let start = std::time::Instant::now();
        let messages = reader.fetch(10, Duration::from_millis(50)).await.unwrap();
        let elapsed = start.elapsed();

        assert!(messages.is_empty());
        assert!(elapsed >= Duration::from_millis(50));
    }

    #[tokio::test]
    async fn test_fetch_with_messages() {
        let (mut reader, state) = create_test_reader();

        // Add pending messages
        {
            let mut s = state.write();
            for i in 1..=3 {
                let msg = create_test_message(i);
                s.slots.push_back(super::super::buffer::BufferSlot {
                    message: msg,
                    state: MessageState::Pending,
                    sequence: i,
                    fetched_at: None,
                });
            }
        }

        let messages = reader.fetch(10, Duration::from_millis(100)).await.unwrap();
        assert_eq!(messages.len(), 3);

        // Verify messages are now in-flight
        {
            let s = state.read();
            assert_eq!(s.in_flight_count(), 3);
            assert_eq!(s.pending_count(), 0);
        }
    }

    #[tokio::test]
    async fn test_fetch_respects_max() {
        let (mut reader, state) = create_test_reader();

        // Add 5 pending messages
        {
            let mut s = state.write();
            for i in 1..=5 {
                let msg = create_test_message(i);
                s.slots.push_back(super::super::buffer::BufferSlot {
                    message: msg,
                    state: MessageState::Pending,
                    sequence: i,
                    fetched_at: None,
                });
            }
        }

        let messages = reader.fetch(2, Duration::from_millis(100)).await.unwrap();
        assert_eq!(messages.len(), 2);
    }

    #[tokio::test]
    async fn test_fetch_injected_failure() {
        let (mut reader, _) = create_test_reader();
        reader.error_injector.fail_fetches(1);

        let result = reader.fetch(10, Duration::from_millis(100)).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SimpleBufferError::Fetch(_)));
    }

    #[tokio::test]
    async fn test_ack_success() {
        let (reader, state) = create_test_reader();
        let offset = Offset::new(1, 0);

        // Add an in-flight message
        {
            let mut s = state.write();
            let msg = create_test_message(1);
            s.slots.push_back(super::super::buffer::BufferSlot {
                message: msg,
                state: MessageState::InFlight,
                sequence: 1,
                fetched_at: Some(std::time::Instant::now()),
            });
            s.offset_to_index.insert(offset.clone(), 0);
        }

        reader.ack(&offset).await.unwrap();

        // Verify message is now acked
        {
            let s = state.read();
            assert_eq!(s.slots.front().unwrap().state, MessageState::Acked);
        }
    }

    #[tokio::test]
    async fn test_ack_not_in_flight() {
        let (reader, state) = create_test_reader();
        let offset = Offset::new(1, 0);

        // Add a pending message (not in-flight)
        {
            let mut s = state.write();
            let msg = create_test_message(1);
            s.slots.push_back(super::super::buffer::BufferSlot {
                message: msg,
                state: MessageState::Pending,
                sequence: 1,
                fetched_at: None,
            });
            s.offset_to_index.insert(offset.clone(), 0);
        }

        let result = reader.ack(&offset).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SimpleBufferError::Ack(_)));
    }

    #[tokio::test]
    async fn test_ack_offset_not_found() {
        let (reader, _) = create_test_reader();
        let offset = Offset::new(999, 0);

        let result = reader.ack(&offset).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SimpleBufferError::OffsetNotFound(_)
        ));
    }

    #[tokio::test]
    async fn test_ack_injected_failure() {
        let (reader, state) = create_test_reader();
        let offset = Offset::new(1, 0);
        reader.error_injector.fail_acks(1);

        // Add an in-flight message
        {
            let mut s = state.write();
            let msg = create_test_message(1);
            s.slots.push_back(super::super::buffer::BufferSlot {
                message: msg,
                state: MessageState::InFlight,
                sequence: 1,
                fetched_at: Some(std::time::Instant::now()),
            });
            s.offset_to_index.insert(offset.clone(), 0);
        }

        let result = reader.ack(&offset).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SimpleBufferError::Ack(_)));
    }

    #[tokio::test]
    async fn test_nack_success() {
        let (reader, state) = create_test_reader();
        let offset = Offset::new(1, 0);

        // Add an in-flight message
        {
            let mut s = state.write();
            let msg = create_test_message(1);
            s.slots.push_back(super::super::buffer::BufferSlot {
                message: msg,
                state: MessageState::InFlight,
                sequence: 1,
                fetched_at: Some(std::time::Instant::now()),
            });
            s.offset_to_index.insert(offset.clone(), 0);
        }

        reader.nack(&offset).await.unwrap();

        // Verify message is back to pending
        {
            let s = state.read();
            assert_eq!(s.slots.front().unwrap().state, MessageState::Pending);
        }
    }

    #[tokio::test]
    async fn test_nack_not_in_flight() {
        let (reader, state) = create_test_reader();
        let offset = Offset::new(1, 0);

        // Add a pending message
        {
            let mut s = state.write();
            let msg = create_test_message(1);
            s.slots.push_back(super::super::buffer::BufferSlot {
                message: msg,
                state: MessageState::Pending,
                sequence: 1,
                fetched_at: None,
            });
            s.offset_to_index.insert(offset.clone(), 0);
        }

        let result = reader.nack(&offset).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SimpleBufferError::Nack(_)));
    }

    #[tokio::test]
    async fn test_nack_injected_failure() {
        let (reader, state) = create_test_reader();
        let offset = Offset::new(1, 0);
        reader.error_injector.fail_nacks(1);

        // Add an in-flight message
        {
            let mut s = state.write();
            let msg = create_test_message(1);
            s.slots.push_back(super::super::buffer::BufferSlot {
                message: msg,
                state: MessageState::InFlight,
                sequence: 1,
                fetched_at: Some(std::time::Instant::now()),
            });
            s.offset_to_index.insert(offset.clone(), 0);
        }

        let result = reader.nack(&offset).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SimpleBufferError::Nack(_)));
    }

    #[tokio::test]
    async fn test_mark_wip_success() {
        let (reader, state) = create_test_reader();
        let offset = Offset::new(1, 0);

        // Add an in-flight message
        {
            let mut s = state.write();
            let msg = create_test_message(1);
            s.slots.push_back(super::super::buffer::BufferSlot {
                message: msg,
                state: MessageState::InFlight,
                sequence: 1,
                fetched_at: Some(std::time::Instant::now()),
            });
            s.offset_to_index.insert(offset.clone(), 0);
        }

        // Wait a bit then mark WIP
        tokio::time::sleep(Duration::from_millis(10)).await;
        reader.mark_wip(&offset).await.unwrap();

        // Verify fetched_at was updated (still in-flight)
        {
            let s = state.read();
            let slot = s.slots.front().unwrap();
            assert_eq!(slot.state, MessageState::InFlight);
            assert!(slot.fetched_at.is_some());
        }
    }

    #[tokio::test]
    async fn test_mark_wip_not_in_flight() {
        let (reader, state) = create_test_reader();
        let offset = Offset::new(1, 0);

        // Add a pending message
        {
            let mut s = state.write();
            let msg = create_test_message(1);
            s.slots.push_back(super::super::buffer::BufferSlot {
                message: msg,
                state: MessageState::Pending,
                sequence: 1,
                fetched_at: None,
            });
            s.offset_to_index.insert(offset.clone(), 0);
        }

        let result = reader.mark_wip(&offset).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SimpleBufferError::WipAck(_)));
    }

    #[tokio::test]
    async fn test_mark_wip_injected_failure() {
        let (reader, state) = create_test_reader();
        let offset = Offset::new(1, 0);
        reader.error_injector.fail_wip_acks(1);

        // Add an in-flight message
        {
            let mut s = state.write();
            let msg = create_test_message(1);
            s.slots.push_back(super::super::buffer::BufferSlot {
                message: msg,
                state: MessageState::InFlight,
                sequence: 1,
                fetched_at: Some(std::time::Instant::now()),
            });
            s.offset_to_index.insert(offset.clone(), 0);
        }

        let result = reader.mark_wip(&offset).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SimpleBufferError::WipAck(_)));
    }
}
