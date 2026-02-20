//! Simple buffer writer implementation.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

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

/// Pending write handle that implements Future.
///
/// This represents a write operation that has been initiated but not yet confirmed.
/// Await this future to get the final `WriteResult`.
#[derive(Debug)]
pub struct PendingWrite {
    result: std::result::Result<WriteResult, WriteError>,
}

impl PendingWrite {
    /// Create a new pending write with a successful result.
    fn success(offset: Offset, is_duplicate: bool) -> Self {
        Self {
            result: Ok(WriteResult {
                offset,
                is_duplicate,
            }),
        }
    }

    /// Create a new pending write with an error.
    fn error(err: WriteError) -> Self {
        Self { result: Err(err) }
    }

    /// Get the offset if the write was successful.
    pub fn offset(&self) -> Option<&Offset> {
        self.result.as_ref().ok().map(|r| &r.offset)
    }

    /// Check if this was a duplicate write.
    pub fn is_duplicate(&self) -> Option<bool> {
        self.result.as_ref().ok().map(|r| r.is_duplicate)
    }
}

impl Future for PendingWrite {
    type Output = std::result::Result<WriteResult, WriteError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // For in-memory implementation, the result is immediately available
        Poll::Ready(self.get_mut().result.clone())
    }
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
    /// Write a message asynchronously, returning a pending write future.
    ///
    /// This is the high-performance write method. The returned `PendingWrite` is a
    /// future that can be awaited later to get the `WriteResult`.
    ///
    /// # Arguments
    /// * `id` - Unique message ID for deduplication
    /// * `payload` - The message payload bytes
    /// * `headers` - Optional message headers
    ///
    /// # Example
    /// ```ignore
    /// let pending = writer.async_write("id".to_string(), payload, headers);
    /// // ... do other work ...
    /// let result = pending.await?;  // or use resolve()
    /// ```
    pub fn async_write(
        &self,
        id: String,
        payload: Bytes,
        headers: HashMap<String, String>,
    ) -> PendingWrite {
        // Check for injected write failure
        if self.error_injector.should_fail_write() {
            return PendingWrite::error(WriteError::WriteFailed(
                "injected write failure".to_string(),
            ));
        }

        // Check for forced buffer full
        if self.error_injector.force_buffer_full.load(Ordering::SeqCst) {
            return PendingWrite::error(WriteError::BufferFull);
        }

        let mut state = self.state.write();

        // Check if buffer is full
        if state.is_full() {
            return PendingWrite::error(WriteError::BufferFull);
        }

        // Check for duplicate
        if let Some(&existing_seq) = state.dedup_window.get(&id) {
            let offset = Offset::new(existing_seq, self.partition_idx);
            return PendingWrite::success(offset, true);
        }

        // Reclaim acked slots to make room
        state.reclaim_acked();

        // Check capacity after reclaim
        if state.slots.len() >= state.capacity {
            return PendingWrite::error(WriteError::BufferFull);
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

        PendingWrite::success(offset, false)
    }

    /// Resolve a pending write by awaiting it.
    ///
    /// This method applies any configured error injection (latency, failures)
    /// before returning the result.
    pub async fn resolve(
        &self,
        pending: PendingWrite,
    ) -> std::result::Result<WriteResult, WriteError> {
        // Apply artificial latency if set
        self.error_injector.apply_resolve_latency().await;

        // Check for injected resolve failure
        if self.error_injector.should_fail_resolve() {
            return Err(WriteError::WriteFailed(
                "injected resolve failure".to_string(),
            ));
        }

        pending.await
    }

    /// Write a message and wait for confirmation.
    ///
    /// This is a convenience method that combines `async_write` and awaiting the result.
    pub async fn write(
        &self,
        id: String,
        payload: Bytes,
        headers: HashMap<String, String>,
    ) -> std::result::Result<WriteResult, WriteError> {
        self.async_write(id, payload, headers).await
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

    /// Returns a reference to the error injector.
    pub fn error_injector(&self) -> &Arc<ErrorInjector> {
        &self.error_injector
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
        // Basic write - async_write returns a future, await it to get result
        let (writer, state) = create_test_writer();
        let pending = writer.async_write("msg1".to_string(), Bytes::from("test"), HashMap::new());
        // Can inspect before awaiting
        assert_eq!(pending.offset().unwrap().sequence, 1);
        assert!(!pending.is_duplicate().unwrap());
        // Await to get final result
        let result = pending.await.unwrap();
        assert_eq!(result.offset.sequence, 1);
        assert!(!result.is_duplicate);
        assert_eq!(state.read().pending_count(), 1);

        // Multiple writes
        let (writer, state) = create_test_writer();
        for i in 1..=3 {
            let result = writer
                .async_write(format!("msg{}", i), Bytes::from("test"), HashMap::new())
                .await
                .unwrap();
            assert_eq!(result.offset.sequence, i);
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
        let result1 = writer
            .async_write("same-id".to_string(), Bytes::from("test"), HashMap::new())
            .await
            .unwrap();
        assert!(!result1.is_duplicate);

        // Duplicate detected
        let result2 = writer
            .async_write("same-id".to_string(), Bytes::from("test"), HashMap::new())
            .await
            .unwrap();
        assert!(result2.is_duplicate);
        assert_eq!(result1.offset, result2.offset);

        // Test resolve method
        let pending = writer.async_write("new-id".to_string(), Bytes::from("test"), HashMap::new());
        let result3 = writer.resolve(pending).await.unwrap();
        assert!(!result3.is_duplicate);
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

    #[tokio::test]
    async fn test_pending_write_future() {
        let (writer, _) = create_test_writer();

        // Create pending write (not yet awaited)
        let pending = writer.async_write("msg1".to_string(), Bytes::from("test"), HashMap::new());

        // Can inspect the pending state
        assert!(pending.offset().is_some());
        assert_eq!(pending.is_duplicate(), Some(false));

        // Debug works
        let debug_str = format!("{:?}", pending);
        assert!(debug_str.contains("PendingWrite"));

        // Await to get result
        let result = pending.await.unwrap();
        assert_eq!(result.offset.sequence, 1);
    }

    #[tokio::test]
    async fn test_resolve_error_injection() {
        let (writer, _) = create_test_writer();

        // Successful resolve
        let pending = writer.async_write("msg1".to_string(), Bytes::from("test"), HashMap::new());
        let result = writer.resolve(pending).await;
        assert!(result.is_ok());

        // Injected resolve failure
        writer.error_injector.fail_resolves(1);
        let pending = writer.async_write("msg2".to_string(), Bytes::from("test"), HashMap::new());
        let result = writer.resolve(pending).await;
        assert!(
            matches!(result.unwrap_err(), WriteError::WriteFailed(msg) if msg.contains("injected resolve failure"))
        );

        // After countdown expires, resolve works again
        let pending = writer.async_write("msg3".to_string(), Bytes::from("test"), HashMap::new());
        let result = writer.resolve(pending).await;
        assert!(result.is_ok());
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
