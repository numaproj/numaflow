//! Simple buffer reader implementation.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use super::buffer::{BufferState, MessageState, Offset, ReadMessage};
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
    pub async fn fetch(&mut self, max: usize, timeout: Duration) -> Result<Vec<ReadMessage>> {
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
                        messages.push(ReadMessage {
                            payload: slot.payload.clone(),
                            headers: slot.headers.clone(),
                            offset: slot.offset.clone(),
                        });

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
    use std::collections::HashMap;

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

    fn add_slot(state: &Arc<RwLock<BufferState>>, seq: i64, msg_state: MessageState) {
        let mut s = state.write();
        s.slots.push_back(super::super::buffer::BufferSlot {
            payload: Bytes::from("test"),
            headers: HashMap::new(),
            id: format!("msg-{}", seq),
            offset: Offset::new(seq, 0),
            state: msg_state,
            sequence: seq,
            fetched_at: if msg_state == MessageState::InFlight {
                Some(std::time::Instant::now())
            } else {
                None
            },
        });
        let idx = s.slots.len() - 1;
        s.offset_to_index.insert(Offset::new(seq, 0), idx);
    }

    #[test]
    fn test_reader_basics() {
        let (reader, _) = create_test_reader();
        assert_eq!(reader.name(), "test-reader");
        assert_eq!(reader.wip_ack_interval(), Some(Duration::from_secs(5)));
    }

    #[tokio::test]
    async fn test_pending() {
        let (mut reader, state) = create_test_reader();

        // Empty buffer
        assert_eq!(reader.pending().await.unwrap(), Some(0));

        // With pending message
        add_slot(&state, 1, MessageState::Pending);
        assert_eq!(reader.pending().await.unwrap(), Some(1));
    }

    #[tokio::test]
    async fn test_fetch() {
        // Empty buffer times out
        let (mut reader, _) = create_test_reader();
        let start = std::time::Instant::now();
        let messages = reader.fetch(10, Duration::from_millis(50)).await.unwrap();
        assert!(messages.is_empty());
        assert!(start.elapsed() >= Duration::from_millis(50));

        // With messages
        let (mut reader, state) = create_test_reader();
        for i in 1..=5 {
            add_slot(&state, i, MessageState::Pending);
        }

        // Fetch all
        let messages = reader.fetch(10, Duration::from_millis(100)).await.unwrap();
        assert_eq!(messages.len(), 5);
        assert_eq!(state.read().in_flight_count(), 5);

        // Fetch respects max
        let (mut reader, state) = create_test_reader();
        for i in 1..=5 {
            add_slot(&state, i, MessageState::Pending);
        }
        let messages = reader.fetch(2, Duration::from_millis(100)).await.unwrap();
        assert_eq!(messages.len(), 2);

        // Injected failure
        let (mut reader, _) = create_test_reader();
        reader.error_injector.fail_fetches(1);
        let result = reader.fetch(10, Duration::from_millis(100)).await;
        assert!(matches!(result.unwrap_err(), SimpleBufferError::Fetch(_)));
    }

    #[tokio::test]
    async fn test_ack() {
        // Success case
        let (reader, state) = create_test_reader();
        add_slot(&state, 1, MessageState::InFlight);
        reader.ack(&Offset::new(1, 0)).await.unwrap();
        assert_eq!(
            state.read().slots.front().unwrap().state,
            MessageState::Acked
        );

        // Not in-flight fails
        let (reader, state) = create_test_reader();
        add_slot(&state, 1, MessageState::Pending);
        let result = reader.ack(&Offset::new(1, 0)).await;
        assert!(matches!(result.unwrap_err(), SimpleBufferError::Ack(_)));

        // Offset not found
        let (reader, _) = create_test_reader();
        let result = reader.ack(&Offset::new(999, 0)).await;
        assert!(matches!(
            result.unwrap_err(),
            SimpleBufferError::OffsetNotFound(_)
        ));

        // Injected failure
        let (reader, state) = create_test_reader();
        add_slot(&state, 1, MessageState::InFlight);
        reader.error_injector.fail_acks(1);
        let result = reader.ack(&Offset::new(1, 0)).await;
        assert!(matches!(result.unwrap_err(), SimpleBufferError::Ack(_)));
    }

    #[tokio::test]
    async fn test_nack() {
        // Success case
        let (reader, state) = create_test_reader();
        add_slot(&state, 1, MessageState::InFlight);
        reader.nack(&Offset::new(1, 0)).await.unwrap();
        assert_eq!(
            state.read().slots.front().unwrap().state,
            MessageState::Pending
        );

        // Not in-flight fails
        let (reader, state) = create_test_reader();
        add_slot(&state, 1, MessageState::Pending);
        let result = reader.nack(&Offset::new(1, 0)).await;
        assert!(matches!(result.unwrap_err(), SimpleBufferError::Nack(_)));

        // Injected failure
        let (reader, state) = create_test_reader();
        add_slot(&state, 1, MessageState::InFlight);
        reader.error_injector.fail_nacks(1);
        let result = reader.nack(&Offset::new(1, 0)).await;
        assert!(matches!(result.unwrap_err(), SimpleBufferError::Nack(_)));
    }

    #[tokio::test]
    async fn test_mark_wip() {
        // Success case
        let (reader, state) = create_test_reader();
        add_slot(&state, 1, MessageState::InFlight);
        tokio::time::sleep(Duration::from_millis(10)).await;
        reader.mark_wip(&Offset::new(1, 0)).await.unwrap();
        let slot = state.read().slots.front().unwrap().clone();
        assert_eq!(slot.state, MessageState::InFlight);
        assert!(slot.fetched_at.is_some());

        // Not in-flight fails
        let (reader, state) = create_test_reader();
        add_slot(&state, 1, MessageState::Pending);
        let result = reader.mark_wip(&Offset::new(1, 0)).await;
        assert!(matches!(result.unwrap_err(), SimpleBufferError::WipAck(_)));

        // Injected failure
        let (reader, state) = create_test_reader();
        add_slot(&state, 1, MessageState::InFlight);
        reader.error_injector.fail_wip_acks(1);
        let result = reader.mark_wip(&Offset::new(1, 0)).await;
        assert!(matches!(result.unwrap_err(), SimpleBufferError::WipAck(_)));
    }
}
