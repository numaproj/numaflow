//! Simple buffer writer implementation.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use parking_lot::RwLock;

use super::buffer::{BufferSlot, BufferState, MessageState, Offset};
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
    ///
    /// # Arguments
    /// * `id` - Unique message ID for deduplication
    /// * `payload` - The message payload bytes
    /// * `headers` - Optional message headers
    pub async fn async_write(
        &self,
        id: String,
        payload: Bytes,
        headers: HashMap<String, String>,
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
        if let Some(&existing_seq) = state.dedup_window.get(&id) {
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
            payload,
            headers,
            id: id.clone(),
            offset: offset.clone(),
            state: MessageState::Pending,
            sequence,
            fetched_at: None,
        };

        // Add to buffer
        let index = state.slots.len();
        state.slots.push_back(slot);
        state.offset_to_index.insert(offset.clone(), index);
        state.dedup_window.insert(id, sequence);

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
    pub async fn write(
        &self,
        id: String,
        payload: Bytes,
        headers: HashMap<String, String>,
    ) -> std::result::Result<WriteResult, WriteError> {
        let pending = self.async_write(id, payload, headers).await?;
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

    #[test]
    fn test_writer_basics() {
        let (writer, _) = create_test_writer();
        assert_eq!(writer.name(), "test-writer");
        assert!(!writer.is_full());

        // Clone works
        let writer2 = writer.clone();
        assert_eq!(writer.name(), writer2.name());

        // Forced buffer full
        writer.error_injector.set_buffer_full(true);
        assert!(writer.is_full());
    }

    #[tokio::test]
    async fn test_write_operations() {
        // Basic write
        let (writer, state) = create_test_writer();
        let pending = writer
            .async_write("msg1".to_string(), Bytes::from("test"), HashMap::new())
            .await
            .unwrap();
        assert!(!pending.is_duplicate);
        assert_eq!(pending.offset.sequence, 1);
        assert_eq!(state.read().pending_count(), 1);

        // Multiple writes
        let (writer, state) = create_test_writer();
        for i in 1..=3 {
            let pending = writer
                .async_write(format!("msg{}", i), Bytes::from("test"), HashMap::new())
                .await
                .unwrap();
            assert_eq!(pending.offset.sequence, i);
        }
        assert_eq!(state.read().slots.len(), 3);

        // Convenience write method
        let (writer, state) = create_test_writer();
        let result = writer
            .write("msg1".to_string(), Bytes::from("test"), HashMap::new())
            .await
            .unwrap();
        assert_eq!(result.offset.sequence, 1);
        assert!(!result.is_duplicate);
        assert_eq!(state.read().slots.len(), 1);
    }

    #[tokio::test]
    async fn test_duplicate_detection() {
        let (writer, _) = create_test_writer();

        // First write
        let pending1 = writer
            .async_write("same-id".to_string(), Bytes::from("test"), HashMap::new())
            .await
            .unwrap();
        assert!(!pending1.is_duplicate);

        // Duplicate detected
        let pending2 = writer
            .async_write("same-id".to_string(), Bytes::from("test"), HashMap::new())
            .await
            .unwrap();
        assert!(pending2.is_duplicate);
        assert_eq!(pending1.offset, pending2.offset);

        // Resolve works for both
        let result1 = writer
            .resolve(PendingWrite {
                offset: pending1.offset.clone(),
                is_duplicate: false,
            })
            .await
            .unwrap();
        assert!(!result1.is_duplicate);

        let result2 = writer
            .resolve(PendingWrite {
                offset: pending2.offset,
                is_duplicate: true,
            })
            .await
            .unwrap();
        assert!(result2.is_duplicate);
    }

    #[tokio::test]
    async fn test_buffer_full() {
        // Natural buffer full (capacity 10, usage_limit 0.8 = 8 messages)
        let (writer, _) = create_test_writer();
        for i in 1..=8 {
            writer
                .async_write(format!("msg{}", i), Bytes::from("test"), HashMap::new())
                .await
                .unwrap();
        }
        let result = writer
            .async_write("msg9".to_string(), Bytes::from("test"), HashMap::new())
            .await;
        assert!(matches!(result.unwrap_err(), WriteError::BufferFull));

        // Forced buffer full
        let (writer, _) = create_test_writer();
        writer.error_injector.set_buffer_full(true);
        let result = writer
            .async_write("msg1".to_string(), Bytes::from("test"), HashMap::new())
            .await;
        assert!(matches!(result.unwrap_err(), WriteError::BufferFull));
    }

    #[tokio::test]
    async fn test_injected_failure() {
        let (writer, _) = create_test_writer();
        writer.error_injector.fail_writes(1);
        let result = writer
            .async_write("msg1".to_string(), Bytes::from("test"), HashMap::new())
            .await;
        assert!(matches!(result.unwrap_err(), WriteError::WriteFailed(_)));

        // Also test via convenience method
        let (writer, _) = create_test_writer();
        writer.error_injector.fail_writes(1);
        let result = writer
            .write("msg1".to_string(), Bytes::from("test"), HashMap::new())
            .await;
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
    fn test_write_error_and_result_types() {
        // WriteError display
        assert_eq!(format!("{}", WriteError::BufferFull), "buffer is full");
        assert_eq!(
            format!("{}", WriteError::WriteFailed("lost".to_string())),
            "write failed: lost"
        );

        // WriteError is std::error::Error
        let err: Box<dyn std::error::Error> = Box::new(WriteError::BufferFull);
        assert!(err.to_string().contains("buffer is full"));

        // WriteResult constructors
        let result = WriteResult::new(Offset::new(42, 0));
        assert_eq!(result.offset.sequence, 42);
        assert!(!result.is_duplicate);

        let result = WriteResult::duplicate(Offset::new(42, 0));
        assert_eq!(result.offset.sequence, 42);
        assert!(result.is_duplicate);
    }
}
