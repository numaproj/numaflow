//! Simple buffer implementation for testing.
//!
//! This module provides a simple in-memory buffer that mimics the behavior of an ISB
//! but allows for testing of error paths, retry logic, buffer full scenarios, etc.
//!
//! # Features
//! - Fixed-size circular buffer
//! - Full ISB semantics: pending, ack, nack, mark_wip
//! - Error injection for testing error paths
//! - Configurable buffer full behavior
//!
//! # Example
//! ```ignore
//! use numaflow_testing::simplebuffer::{SimpleBuffer, SimpleReader, SimpleWriter};
//!
//! let buffer = SimpleBuffer::new(100, 0, "test-buffer");
//! let writer = buffer.writer();
//! let reader = buffer.reader();
//! ```

/// Buffer state, slot definitions, and core types.
mod buffer;
/// Error types.
mod error;
/// Error injector for testing.
mod error_injector;
/// Reader implementation.
mod reader;
/// Writer implementation.
mod writer;

// Re-exports
pub use buffer::{Message, MessageID, Offset};
pub use error::{Result, SimpleBufferError};
pub use error_injector::ErrorInjector;
pub use reader::SimpleReader;
pub use writer::{PendingWrite, SimpleWriter, WriteError, WriteResult};

use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use buffer::BufferState;

/// Simple buffer for testing.
///
/// This provides a shared buffer that can be accessed by multiple readers and writers.
/// It supports all the features of a real ISB including ack, nack, pending tracking,
/// and work-in-progress markers.
#[derive(Debug, Clone)]
pub struct SimpleBuffer {
    /// Shared buffer state.
    state: Arc<RwLock<BufferState>>,
    /// Buffer name.
    name: &'static str,
    /// Partition index.
    partition_idx: u16,
    /// Error injector for testing.
    error_injector: Arc<ErrorInjector>,
    /// WIP ack interval.
    wip_ack_interval: Duration,
}

impl SimpleBuffer {
    /// Create a new simple buffer with the given capacity.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of messages the buffer can hold.
    /// * `partition_idx` - Partition index for this buffer.
    /// * `name` - Name of the buffer (must be static).
    pub fn new(capacity: usize, partition_idx: u16, name: &'static str) -> Self {
        Self::with_config(capacity, partition_idx, name, 0.8, Duration::from_secs(1))
    }

    /// Create a new simple buffer with custom configuration.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of messages the buffer can hold.
    /// * `partition_idx` - Partition index for this buffer.
    /// * `name` - Name of the buffer (must be static).
    /// * `usage_limit` - Usage limit (0.0 to 1.0) at which buffer is considered full.
    /// * `wip_ack_interval` - Interval for WIP acknowledgments.
    pub fn with_config(
        capacity: usize,
        partition_idx: u16,
        name: &'static str,
        usage_limit: f64,
        wip_ack_interval: Duration,
    ) -> Self {
        Self {
            state: Arc::new(RwLock::new(BufferState::new(capacity, usage_limit))),
            name,
            partition_idx,
            error_injector: Arc::new(ErrorInjector::new()),
            wip_ack_interval,
        }
    }

    /// Get the error injector for this buffer.
    ///
    /// Use this to inject errors for testing.
    pub fn error_injector(&self) -> &Arc<ErrorInjector> {
        &self.error_injector
    }

    /// Create a writer for this buffer.
    pub fn writer(&self) -> SimpleWriter {
        SimpleWriter {
            state: Arc::clone(&self.state),
            name: self.name,
            partition_idx: self.partition_idx,
            error_injector: Arc::clone(&self.error_injector),
        }
    }

    /// Create a reader for this buffer.
    pub fn reader(&self) -> SimpleReader {
        SimpleReader {
            state: Arc::clone(&self.state),
            name: self.name,
            partition_idx: self.partition_idx,
            error_injector: Arc::clone(&self.error_injector),
            wip_ack_interval: self.wip_ack_interval,
        }
    }

    /// Get the current number of pending messages.
    pub fn pending_count(&self) -> usize {
        self.state.read().pending_count()
    }

    /// Get the current number of in-flight messages.
    pub fn in_flight_count(&self) -> usize {
        self.state.read().in_flight_count()
    }

    /// Get the current buffer usage as a fraction.
    pub fn usage(&self) -> f64 {
        self.state.read().usage()
    }

    /// Check if the buffer is full.
    pub fn is_full(&self) -> bool {
        self.state.read().is_full()
    }

    /// Get the total number of messages in the buffer (all states).
    pub fn len(&self) -> usize {
        self.state.read().slots.len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.state.read().slots.is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn create_test_message(id: &str, value: &str) -> Message {
        Message {
            keys: Arc::new(["key1".to_string()]),
            value: Bytes::from(value.to_string()),
            id: MessageID {
                vertex_name: "test-vertex".to_string(),
                offset: id.to_string(),
                index: 0,
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_basic_write_and_read() {
        let buffer = SimpleBuffer::new(10, 0, "test-buffer");
        let writer = buffer.writer();
        let mut reader = buffer.reader();

        // Write a message
        let msg = create_test_message("1", "hello");
        let result = writer.write(msg).await.unwrap();
        assert!(!result.is_duplicate);
        assert_eq!(result.offset.sequence, 1);

        // Read the message
        let messages = reader.fetch(10, Duration::from_millis(100)).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, Bytes::from("hello"));

        // Ack the message
        reader.ack(&messages[0].offset).await.unwrap();

        assert_eq!(buffer.pending_count(), 0);
        assert_eq!(buffer.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn test_nack_redelivers() {
        let buffer = SimpleBuffer::new(10, 0, "test-buffer");
        let writer = buffer.writer();
        let mut reader = buffer.reader();

        // Write a message
        let msg = create_test_message("1", "hello");
        writer.write(msg).await.unwrap();

        // Read the message
        let messages = reader.fetch(10, Duration::from_millis(100)).await.unwrap();
        assert_eq!(messages.len(), 1);
        let offset = messages[0].offset.clone();

        // Nack the message
        reader.nack(&offset).await.unwrap();

        // Message should be pending again
        assert_eq!(buffer.pending_count(), 1);
        assert_eq!(buffer.in_flight_count(), 0);

        // Should be able to fetch it again
        let messages = reader.fetch(10, Duration::from_millis(100)).await.unwrap();
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn test_buffer_full() {
        let buffer = SimpleBuffer::with_config(5, 0, "test-buffer", 0.8, Duration::from_secs(1));
        let writer = buffer.writer();

        // Fill the buffer to 80% (4 messages out of 5)
        for i in 0..4 {
            let msg = create_test_message(&i.to_string(), &format!("msg{}", i));
            writer.write(msg).await.unwrap();
        }

        // Next write should fail with buffer full
        let msg = create_test_message("4", "msg4");
        let result = writer.write(msg).await;
        assert!(matches!(result, Err(WriteError::BufferFull)));
    }

    #[tokio::test]
    async fn test_force_buffer_full() {
        let buffer = SimpleBuffer::new(100, 0, "test-buffer");
        let writer = buffer.writer();

        // Force buffer full
        buffer.error_injector().set_buffer_full(true);

        let msg = create_test_message("1", "hello");
        let result = writer.write(msg).await;
        assert!(matches!(result, Err(WriteError::BufferFull)));

        // Unset buffer full
        buffer.error_injector().set_buffer_full(false);

        let msg = create_test_message("2", "hello2");
        let result = writer.write(msg).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fail_writes() {
        let buffer = SimpleBuffer::new(100, 0, "test-buffer");
        let writer = buffer.writer();

        // Fail next 2 writes
        buffer.error_injector().fail_writes(2);

        let msg = create_test_message("1", "hello");
        assert!(matches!(
            writer.write(msg).await,
            Err(WriteError::WriteFailed(_))
        ));

        let msg = create_test_message("2", "hello2");
        assert!(matches!(
            writer.write(msg).await,
            Err(WriteError::WriteFailed(_))
        ));

        // Third write should succeed
        let msg = create_test_message("3", "hello3");
        assert!(writer.write(msg).await.is_ok());
    }

    #[tokio::test]
    async fn test_fail_acks() {
        let buffer = SimpleBuffer::new(100, 0, "test-buffer");
        let writer = buffer.writer();
        let mut reader = buffer.reader();

        let msg = create_test_message("1", "hello");
        writer.write(msg).await.unwrap();

        let messages = reader.fetch(10, Duration::from_millis(100)).await.unwrap();
        let offset = messages[0].offset.clone();

        // Fail next ack
        buffer.error_injector().fail_acks(1);

        assert!(matches!(
            reader.ack(&offset).await,
            Err(SimpleBufferError::Ack(_))
        ));

        // Second ack should succeed
        assert!(reader.ack(&offset).await.is_ok());
    }

    #[tokio::test]
    async fn test_duplicate_detection() {
        let buffer = SimpleBuffer::new(100, 0, "test-buffer");
        let writer = buffer.writer();

        let msg = create_test_message("1", "hello");
        let result1 = writer.write(msg.clone()).await.unwrap();
        assert!(!result1.is_duplicate);

        // Write same message again
        let result2 = writer.write(msg).await.unwrap();
        assert!(result2.is_duplicate);
        assert_eq!(result1.offset, result2.offset);
    }

    #[tokio::test]
    async fn test_mark_wip() {
        let buffer = SimpleBuffer::new(100, 0, "test-buffer");
        let writer = buffer.writer();
        let mut reader = buffer.reader();

        let msg = create_test_message("1", "hello");
        writer.write(msg).await.unwrap();

        let messages = reader.fetch(10, Duration::from_millis(100)).await.unwrap();
        let offset = messages[0].offset.clone();

        // Mark as WIP
        assert!(reader.mark_wip(&offset).await.is_ok());

        // Should still be able to ack
        assert!(reader.ack(&offset).await.is_ok());
    }

    #[tokio::test]
    async fn test_ack_non_existent_offset() {
        let buffer = SimpleBuffer::new(100, 0, "test-buffer");
        let reader = buffer.reader();

        let fake_offset = Offset::new(999, 0);
        assert!(matches!(
            reader.ack(&fake_offset).await,
            Err(SimpleBufferError::OffsetNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_pending_count() {
        let buffer = SimpleBuffer::new(100, 0, "test-buffer");
        let writer = buffer.writer();
        let mut reader = buffer.reader();

        // Initially no pending
        assert_eq!(reader.pending().await.unwrap(), Some(0));

        // Write messages
        for i in 0..5 {
            let msg = create_test_message(&i.to_string(), &format!("msg{}", i));
            writer.write(msg).await.unwrap();
        }

        // Should have 5 pending
        assert_eq!(reader.pending().await.unwrap(), Some(5));

        // Fetch 3
        let messages = reader.fetch(3, Duration::from_millis(100)).await.unwrap();
        assert_eq!(messages.len(), 3);

        // Should have 2 pending (3 in-flight)
        assert_eq!(reader.pending().await.unwrap(), Some(2));
    }

    #[tokio::test]
    async fn test_fetch_timeout() {
        let buffer = SimpleBuffer::new(100, 0, "test-buffer");
        let mut reader = buffer.reader();

        // Fetch on empty buffer should timeout and return empty
        let messages = reader.fetch(10, Duration::from_millis(50)).await.unwrap();
        assert!(messages.is_empty());
    }

    #[tokio::test]
    async fn test_wip_ack_interval() {
        let buffer =
            SimpleBuffer::with_config(100, 0, "test-buffer", 0.8, Duration::from_millis(500));
        let reader = buffer.reader();

        assert_eq!(reader.wip_ack_interval(), Some(Duration::from_millis(500)));
    }
}
