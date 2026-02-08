//! Simple buffer writer implementation.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use parking_lot::RwLock;

use super::buffer::{BufferSlot, BufferState, Message, MessageState, Offset};
use super::error_injector::ErrorInjector;

/// Error types for write operations.
#[derive(Debug, Clone)]
pub enum WriteError {
    /// Buffer is full, cannot write (retryable)
    BufferFull,
    /// Write operation failed (retryable)
    WriteFailed(String),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::BufferFull => write!(f, "buffer is full"),
            WriteError::WriteFailed(msg) => write!(f, "write failed: {}", msg),
        }
    }
}

impl std::error::Error for WriteError {}

/// Result of a write operation.
#[derive(Debug, Clone)]
pub struct WriteResult {
    pub offset: Offset,
    pub is_duplicate: bool,
}

impl WriteResult {
    pub fn new(offset: Offset) -> Self {
        Self {
            offset,
            is_duplicate: false,
        }
    }

    pub fn duplicate(offset: Offset) -> Self {
        Self {
            offset,
            is_duplicate: true,
        }
    }
}

/// Pending write handle for async write pattern.
#[derive(Debug)]
pub struct PendingWrite {
    pub(super) offset: Offset,
    pub(super) is_duplicate: bool,
}

/// Simple buffer writer.
#[derive(Debug, Clone)]
pub struct SimpleWriter {
    pub(super) state: Arc<RwLock<BufferState>>,
    pub(super) name: &'static str,
    #[allow(dead_code)]
    pub(super) partition_idx: u16,
    pub(super) error_injector: Arc<ErrorInjector>,
}

impl SimpleWriter {
    /// Write a message asynchronously, returning a pending write handle.
    ///
    /// This is the high-performance write method. The returned `PendingWrite` can be
    /// resolved later using `resolve()` to get the offset.
    pub async fn async_write(
        &self,
        message: Message,
    ) -> std::result::Result<PendingWrite, WriteError> {
        // Apply artificial latency if set
        self.error_injector.apply_write_latency().await;

        // Check for injected write failure
        if self.error_injector.should_fail_write() {
            return Err(WriteError::WriteFailed(
                "injected write failure".to_string(),
            ));
        }

        // Check for forced buffer full
        if self.error_injector.force_buffer_full.load(Ordering::SeqCst) {
            return Err(WriteError::BufferFull);
        }

        let mut state = self.state.write();

        // Check if buffer is full
        if state.is_full() {
            return Err(WriteError::BufferFull);
        }

        // Check for duplicate
        if let Some(&existing_seq) = state.dedup_window.get(&message.id) {
            let offset = Offset::new(existing_seq, self.partition_idx);
            return Ok(PendingWrite {
                offset,
                is_duplicate: true,
            });
        }

        // Reclaim acked slots to make room
        state.reclaim_acked();

        // Check capacity after reclaim
        if state.slots.len() >= state.capacity {
            return Err(WriteError::BufferFull);
        }

        // Assign sequence number and create offset
        let sequence = state.next_sequence;
        state.next_sequence += 1;
        let offset = Offset::new(sequence, self.partition_idx);

        // Create the slot
        let slot = BufferSlot {
            message: Message {
                offset: offset.clone(),
                ..message.clone()
            },
            state: MessageState::Pending,
            sequence,
            fetched_at: None,
        };

        // Add to buffer
        let index = state.slots.len();
        state.slots.push_back(slot);
        state.offset_to_index.insert(offset.clone(), index);
        state.dedup_window.insert(message.id, sequence);

        Ok(PendingWrite {
            offset,
            is_duplicate: false,
        })
    }

    /// Resolve a pending write to get the result.
    pub async fn resolve(
        &self,
        pending: PendingWrite,
    ) -> std::result::Result<WriteResult, WriteError> {
        // In-memory implementation resolves immediately
        Ok(WriteResult {
            offset: pending.offset,
            is_duplicate: pending.is_duplicate,
        })
    }

    /// Write a message and wait for confirmation.
    ///
    /// This is a convenience method that combines `async_write` and `resolve`.
    pub async fn write(&self, message: Message) -> std::result::Result<WriteResult, WriteError> {
        let pending = self.async_write(message).await?;
        self.resolve(pending).await
    }

    /// Returns the name of this writer.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Returns whether the buffer is full.
    pub fn is_full(&self) -> bool {
        if self.error_injector.force_buffer_full.load(Ordering::SeqCst) {
            return true;
        }
        self.state.read().is_full()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;

    use crate::simplebuffer::buffer::MessageID;

    fn create_test_writer() -> (SimpleWriter, Arc<RwLock<BufferState>>) {
        let state = Arc::new(RwLock::new(BufferState::new(10, 0.8)));
        let error_injector = Arc::new(ErrorInjector::new());
        let writer = SimpleWriter {
            state: Arc::clone(&state),
            name: "test-writer",
            partition_idx: 0,
            error_injector,
        };
        (writer, state)
    }

    fn create_test_message(id: &str) -> Message {
        Message {
            keys: Arc::new(["key".to_string()]),
            tags: None,
            value: Bytes::from("test"),
            offset: Offset::new(0, 0),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "test".to_string(),
                offset: id.to_string(),
                index: 0,
            },
            headers: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_writer_name() {
        let (writer, _) = create_test_writer();
        assert_eq!(writer.name(), "test-writer");
    }

    #[test]
    fn test_is_full_empty_buffer() {
        let (writer, _) = create_test_writer();
        assert!(!writer.is_full());
    }

    #[test]
    fn test_is_full_forced() {
        let (writer, _) = create_test_writer();
        writer.error_injector.set_buffer_full(true);
        assert!(writer.is_full());
    }

    #[tokio::test]
    async fn test_async_write_success() {
        let (writer, state) = create_test_writer();
        let msg = create_test_message("msg1");

        let pending = writer.async_write(msg).await.unwrap();
        assert!(!pending.is_duplicate);
        assert_eq!(pending.offset.sequence, 1);

        // Verify message was added to buffer
        {
            let s = state.read();
            assert_eq!(s.slots.len(), 1);
            assert_eq!(s.pending_count(), 1);
        }
    }

    #[tokio::test]
    async fn test_async_write_multiple() {
        let (writer, state) = create_test_writer();

        for i in 1..=3 {
            let msg = create_test_message(&format!("msg{}", i));
            let pending = writer.async_write(msg).await.unwrap();
            assert_eq!(pending.offset.sequence, i);
        }

        {
            let s = state.read();
            assert_eq!(s.slots.len(), 3);
        }
    }

    #[tokio::test]
    async fn test_async_write_duplicate_detection() {
        let (writer, _) = create_test_writer();
        let msg1 = create_test_message("same-id");
        let msg2 = create_test_message("same-id");

        let pending1 = writer.async_write(msg1).await.unwrap();
        assert!(!pending1.is_duplicate);

        let pending2 = writer.async_write(msg2).await.unwrap();
        assert!(pending2.is_duplicate);
        assert_eq!(pending1.offset, pending2.offset);
    }

    #[tokio::test]
    async fn test_async_write_buffer_full() {
        let (writer, _) = create_test_writer();

        // Fill the buffer (capacity 10, usage_limit 0.8 = 8 messages)
        for i in 1..=8 {
            let msg = create_test_message(&format!("msg{}", i));
            writer.async_write(msg).await.unwrap();
        }

        // 9th message should fail
        let msg = create_test_message("msg9");
        let result = writer.async_write(msg).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), WriteError::BufferFull));
    }

    #[tokio::test]
    async fn test_async_write_forced_buffer_full() {
        let (writer, _) = create_test_writer();
        writer.error_injector.set_buffer_full(true);

        let msg = create_test_message("msg1");
        let result = writer.async_write(msg).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), WriteError::BufferFull));
    }

    #[tokio::test]
    async fn test_async_write_injected_failure() {
        let (writer, _) = create_test_writer();
        writer.error_injector.fail_writes(1);

        let msg = create_test_message("msg1");
        let result = writer.async_write(msg).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), WriteError::WriteFailed(_)));
    }

    #[tokio::test]
    async fn test_resolve_pending_write() {
        let (writer, _) = create_test_writer();
        let msg = create_test_message("msg1");

        let pending = writer.async_write(msg).await.unwrap();
        let result = writer.resolve(pending).await.unwrap();

        assert_eq!(result.offset.sequence, 1);
        assert!(!result.is_duplicate);
    }

    #[tokio::test]
    async fn test_resolve_duplicate() {
        let (writer, _) = create_test_writer();
        let msg1 = create_test_message("same-id");
        let msg2 = create_test_message("same-id");

        let pending1 = writer.async_write(msg1).await.unwrap();
        let result1 = writer.resolve(pending1).await.unwrap();
        assert!(!result1.is_duplicate);

        let pending2 = writer.async_write(msg2).await.unwrap();
        let result2 = writer.resolve(pending2).await.unwrap();
        assert!(result2.is_duplicate);
    }

    #[tokio::test]
    async fn test_write_convenience_method() {
        let (writer, state) = create_test_writer();
        let msg = create_test_message("msg1");

        let result = writer.write(msg).await.unwrap();
        assert_eq!(result.offset.sequence, 1);
        assert!(!result.is_duplicate);

        {
            let s = state.read();
            assert_eq!(s.slots.len(), 1);
        }
    }

    #[tokio::test]
    async fn test_write_with_failure() {
        let (writer, _) = create_test_writer();
        writer.error_injector.fail_writes(1);

        let msg = create_test_message("msg1");
        let result = writer.write(msg).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_pending_write_debug() {
        let pending = PendingWrite {
            offset: Offset::new(42, 0),
            is_duplicate: false,
        };
        let debug_str = format!("{:?}", pending);
        assert!(debug_str.contains("PendingWrite"));
        assert!(debug_str.contains("42"));
    }

    #[test]
    fn test_writer_clone() {
        let (writer, _) = create_test_writer();
        let writer2 = writer.clone();
        assert_eq!(writer.name(), writer2.name());
    }

    // ========== WriteError tests ==========

    #[test]
    fn test_write_error_buffer_full_display() {
        let err = WriteError::BufferFull;
        assert_eq!(format!("{}", err), "buffer is full");
    }

    #[test]
    fn test_write_error_write_failed_display() {
        let err = WriteError::WriteFailed("connection lost".to_string());
        assert_eq!(format!("{}", err), "write failed: connection lost");
    }

    #[test]
    fn test_write_error_is_error() {
        let err: Box<dyn std::error::Error> = Box::new(WriteError::BufferFull);
        assert!(err.to_string().contains("buffer is full"));
    }

    // ========== WriteResult tests ==========

    #[test]
    fn test_write_result_new() {
        let result = WriteResult::new(Offset::new(42, 0));
        assert_eq!(result.offset.sequence, 42);
        assert!(!result.is_duplicate);
    }

    #[test]
    fn test_write_result_duplicate() {
        let result = WriteResult::duplicate(Offset::new(42, 0));
        assert_eq!(result.offset.sequence, 42);
        assert!(result.is_duplicate);
    }
}
