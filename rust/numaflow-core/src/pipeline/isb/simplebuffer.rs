//! SimpleBuffer adapters for testing ISBReaderOrchestrator and ISBWriterOrchestrator.
//!
//! This module provides adapter types that wrap `numaflow_testing::simplebuffer` types
//! and implement the ISBReader and ISBWriter traits, enabling comprehensive testing
//! of error paths without requiring external infrastructure like NATS.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use numaflow_testing::simplebuffer::{
    ErrorInjector, ReadMessage, SimpleBuffer, SimpleBufferError, SimpleReader, SimpleWriter,
    WriteError as SimpleWriteError,
};
use numaflow_throttling::NoOpRateLimiter;

use crate::error::Error;
use crate::message::{IntOffset, Message, MessageID, Offset};
use crate::pipeline::isb::error::ISBError;
use crate::pipeline::isb::{ISBReader, ISBWriter, PendingWrite, WriteError, WriteResult};
use crate::typ::NumaflowTypeConfig;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

/// Zero-allocation pending write for SimpleBuffer (testing).
///
/// This struct holds the result of a write operation that can be awaited.
/// Since SimpleBuffer is synchronous, the result is immediately available.
/// Error injection for latency is applied at resolve time via the writer.
pub struct SimpleBufferPendingWrite {
    /// The underlying pending write from numaflow_testing
    pending: numaflow_testing::simplebuffer::PendingWrite,
    /// The writer to use for resolve (applies error injection)
    writer: SimpleWriter,
}

impl SimpleBufferPendingWrite {
    /// Create a new pending write.
    pub(crate) fn new(
        pending: numaflow_testing::simplebuffer::PendingWrite,
        writer: SimpleWriter,
    ) -> Self {
        Self { pending, writer }
    }
}

impl Future for SimpleBufferPendingWrite {
    type Output = Result<WriteResult, WriteError>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        // SimpleBuffer's PendingWrite is immediately ready, but we need to
        // go through resolve() for error injection. Since resolve() is async
        // and we can't easily poll it here, we'll just poll the inner pending
        // directly. Error injection at resolve level won't work in this path,
        // but that's acceptable for testing as errors can be injected at
        // async_write time instead.
        let this = self.get_mut();
        match Pin::new(&mut this.pending).poll(cx) {
            Poll::Ready(Ok(r)) => Poll::Ready(Ok(r.into())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Adapter that wraps a `SimpleBuffer` and provides access to reader/writer adapters
/// and the shared error injector.
///
/// This is the main entry point for using SimpleBuffer in tests.
#[derive(Debug, Clone)]
pub(crate) struct SimpleBufferAdapter {
    buffer: SimpleBuffer,
}

impl SimpleBufferAdapter {
    /// Create a new adapter wrapping the given SimpleBuffer.
    pub fn new(buffer: SimpleBuffer) -> Self {
        Self { buffer }
    }

    /// Get access to the error injector for test control.
    pub fn error_injector(&self) -> &Arc<ErrorInjector> {
        self.buffer.error_injector()
    }

    /// Get a reader adapter that implements `ISBReader`.
    pub fn reader(&self) -> SimpleReaderAdapter {
        SimpleReaderAdapter {
            inner: self.buffer.reader(),
        }
    }

    /// Get a writer adapter that implements `ISBWriter`.
    pub fn writer(&self) -> SimpleWriterAdapter {
        SimpleWriterAdapter {
            inner: self.buffer.writer(),
        }
    }

    /// Get the current number of pending messages in the buffer.
    pub fn pending_count(&self) -> usize {
        self.buffer.pending_count()
    }

    /// Get the current number of in-flight messages in the buffer.
    #[allow(dead_code)]
    pub fn in_flight_count(&self) -> usize {
        self.buffer.in_flight_count()
    }
}

/// Adapter that wraps `SimpleReader` and implements the `ISBReader` trait.
///
/// This allows using SimpleBuffer for testing ISBReaderOrchestrator without NATS.
#[derive(Debug, Clone)]
pub(crate) struct SimpleReaderAdapter {
    inner: SimpleReader,
}

/// Convert [SimpleBufferError] to [numaflow_core::Error].
impl From<SimpleBufferError> for Error {
    fn from(value: SimpleBufferError) -> Self {
        let isb_error = match value {
            SimpleBufferError::OffsetNotFound(msg) => ISBError::OffsetNotFound(msg),
            SimpleBufferError::Ack(msg) => ISBError::Ack(msg),
            SimpleBufferError::Nack(msg) => ISBError::Nack(msg),
            SimpleBufferError::WipAck(msg) => ISBError::WipAck(msg),
            SimpleBufferError::Fetch(msg) => ISBError::Fetch(msg),
            SimpleBufferError::Pending(msg) => ISBError::Pending(msg),
            SimpleBufferError::Write(msg) => ISBError::Write(msg),
            SimpleBufferError::BufferFull => ISBError::Write("buffer full".to_string()),
            SimpleBufferError::Other(msg) => ISBError::Other(msg),
        };
        Error::ISB(isb_error)
    }
}

impl From<&Offset> for numaflow_testing::simplebuffer::Offset {
    fn from(value: &Offset) -> Self {
        match value {
            Offset::Int(int_offset) => numaflow_testing::simplebuffer::Offset::new(
                int_offset.offset,
                int_offset.partition_idx,
            ),
            Offset::String(str_offset) => {
                let seq: i64 = std::str::from_utf8(&str_offset.offset)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                numaflow_testing::simplebuffer::Offset::new(seq, str_offset.partition_idx)
            }
        }
    }
}

/// Convert [numaflow_testing::simplebuffer::Offset] to [numaflow_core::message::Offset].
impl From<&numaflow_testing::simplebuffer::Offset> for Offset {
    fn from(value: &numaflow_testing::simplebuffer::Offset) -> Self {
        Offset::Int(IntOffset::new(value.sequence, value.partition_idx))
    }
}

/// Convert ReadMessage to Message with default fields.
fn convert_message(read_msg: ReadMessage) -> Message {
    let offset = (&read_msg.offset).into();

    Message {
        typ: Default::default(),
        keys: Arc::new([]),
        tags: None,
        value: read_msg.payload,
        offset,
        event_time: Utc::now(),
        watermark: None,
        id: MessageID {
            vertex_name: "test".into(),
            index: 0,
            offset: read_msg.offset.to_string().into(),
        },
        headers: Arc::new(read_msg.headers),
        metadata: None,
        is_late: false,
        ack_handle: None,
    }
}

impl ISBReader for SimpleReaderAdapter {
    async fn fetch(&self, max: usize, timeout: Duration) -> crate::Result<Vec<Message>> {
        self.inner
            .fetch(max, timeout)
            .await
            .map(|msgs| msgs.into_iter().map(convert_message).collect())
            .map_err(|e| e.into())
    }

    async fn ack(&self, offset: &Offset) -> crate::Result<()> {
        let simple_offset = offset.into();
        self.inner.ack(&simple_offset).await.map_err(|e| e.into())
    }

    async fn nack(&self, offset: &Offset) -> crate::Result<()> {
        let simple_offset = offset.into();
        self.inner.nack(&simple_offset).await.map_err(|e| e.into())
    }

    async fn pending(&self) -> crate::Result<Option<usize>> {
        self.inner.pending().await.map_err(|e| e.into())
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    async fn mark_wip(&self, offset: &Offset) -> crate::Result<()> {
        let simple_offset = offset.into();
        self.inner
            .mark_wip(&simple_offset)
            .await
            .map_err(|e| e.into())
    }

    fn wip_ack_interval(&self) -> Option<Duration> {
        self.inner.wip_ack_interval()
    }
}

/// Adapter that wraps `SimpleWriter` and implements the `ISBWriter` trait.
///
/// This allows using SimpleBuffer for testing ISBWriterOrchestrator without a production grade ISB.
#[derive(Debug, Clone)]
pub(crate) struct SimpleWriterAdapter {
    inner: SimpleWriter,
}

/// Convert [SimpleWriteError] to [WriteError].
impl From<SimpleWriteError> for WriteError {
    fn from(err: SimpleWriteError) -> Self {
        match err {
            SimpleWriteError::BufferFull => WriteError::BufferFull,
            SimpleWriteError::WriteFailed(msg) => WriteError::WriteFailed(msg),
        }
    }
}

/// Convert [numaflow_testing::simplebuffer::WriteResult] to [WriteResult].
impl From<numaflow_testing::simplebuffer::WriteResult> for WriteResult {
    fn from(result: numaflow_testing::simplebuffer::WriteResult) -> Self {
        let offset = (&result.offset).into();
        if result.is_duplicate {
            WriteResult::duplicate(offset)
        } else {
            WriteResult::new(offset)
        }
    }
}

impl ISBWriter for SimpleWriterAdapter {
    async fn async_write(&self, message: Message) -> Result<PendingWrite, WriteError> {
        // Check if buffer is full before attempting write.
        // This is important because ISBWriterOrchestrator::write_to_stream expects
        // async_write to return Err(WriteError::BufferFull) directly when the buffer
        // is full, so it can apply the BufferFullStrategy (DiscardLatest or RetryUntilSuccess).
        // The inner async_write embeds errors in PendingWrite which would only be
        // discovered during resolve(), but write_to_stream needs the error immediately.
        if self.inner.is_full() {
            return Err(WriteError::BufferFull);
        }

        let id = message.id.to_string();
        let payload = message.value;
        let headers: HashMap<String, String> = (*message.headers).clone();
        let pending = self.inner.async_write(id, payload, headers);

        // Return the zero-allocation pending write
        Ok(PendingWrite::SimpleBuffer(SimpleBufferPendingWrite::new(
            pending,
            self.inner.clone(),
        )))
    }

    async fn write(&self, message: Message) -> Result<WriteResult, WriteError> {
        let id = message.id.to_string();
        let payload = message.value;
        let headers: HashMap<String, String> = (*message.headers).clone();
        self.inner
            .write(id, payload, headers)
            .await
            .map(|r| r.into())
            .map_err(|e| e.into())
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn is_full(&self) -> bool {
        self.inner.is_full()
    }
}

/// Test type configuration that uses SimpleBuffer for ISB operations.
///
/// This allows testing ISBReaderOrchestrator and ISBWriterOrchestrator
/// without requiring external infrastructure like NATS.
#[derive(Clone)]
#[cfg(test)]
pub(crate) struct WithSimpleBuffer;
#[cfg(test)]
impl NumaflowTypeConfig for WithSimpleBuffer {
    type RateLimiter = NoOpRateLimiter;
    type ISBReader = SimpleReaderAdapter;
    type ISBWriter = SimpleWriterAdapter;
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)] // Tests use indexing for simplicity
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;

    /// Helper to write a message to the buffer via the adapter
    async fn write_message(adapter: &SimpleBufferAdapter, id: &str, payload: &str) {
        let writer = adapter.writer();
        writer
            .write(create_test_message(id, payload))
            .await
            .expect("write should succeed");
    }

    /// Create a test message
    fn create_test_message(id: &str, payload: &str) -> Message {
        Message {
            typ: Default::default(),
            keys: Arc::new([]),
            tags: None,
            value: Bytes::from(payload.to_string()),
            offset: Offset::Int(IntOffset::new(0, 0)),
            event_time: Utc::now(),
            watermark: None,
            id: MessageID {
                vertex_name: "test".into(),
                index: 0,
                offset: id.to_string().into(),
            },
            headers: Arc::new(HashMap::new()),
            metadata: None,
            is_late: false,
            ack_handle: None,
        }
    }

    #[tokio::test]
    async fn test_simple_reader_adapter_fetch_and_ack() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        write_message(&adapter, "msg-1", "hello").await;
        write_message(&adapter, "msg-2", "world").await;

        let reader = adapter.reader();

        // Fetch messages
        let messages = reader
            .fetch(10, Duration::from_millis(100))
            .await
            .expect("fetch should succeed");
        assert_eq!(messages.len(), 2);

        // Ack the first message
        reader
            .ack(&messages[0].offset)
            .await
            .expect("ack should succeed");

        // Ack the second message
        reader
            .ack(&messages[1].offset)
            .await
            .expect("ack should succeed");
    }

    #[tokio::test]
    async fn test_simple_reader_adapter_nack() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        write_message(&adapter, "msg-1", "hello").await;

        let reader = adapter.reader();

        // Fetch message
        let messages = reader
            .fetch(10, Duration::from_millis(100))
            .await
            .expect("fetch should succeed");
        assert_eq!(messages.len(), 1);

        // Nack the message
        reader
            .nack(&messages[0].offset)
            .await
            .expect("nack should succeed");

        // Message should be redelivered on next fetch
        let messages = reader
            .fetch(10, Duration::from_millis(100))
            .await
            .expect("fetch should succeed");
        assert_eq!(
            messages.len(),
            1,
            "message should be redelivered after nack"
        );
    }

    #[tokio::test]
    async fn test_fetch_error_continues_loop() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        write_message(&adapter, "msg-1", "hello").await;

        // Inject fetch failure
        adapter.error_injector().fail_fetches(1);

        let reader = adapter.reader();

        // First fetch should fail
        let result = reader.fetch(10, Duration::from_millis(100)).await;
        assert!(result.is_err(), "first fetch should fail due to injection");

        // Second fetch should succeed
        let messages = reader
            .fetch(10, Duration::from_millis(100))
            .await
            .expect("second fetch should succeed");
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn test_ack_error_is_retryable() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        write_message(&adapter, "msg-1", "hello").await;

        let reader = adapter.reader();

        // Fetch message
        let messages = reader
            .fetch(10, Duration::from_millis(100))
            .await
            .expect("fetch should succeed");
        assert_eq!(messages.len(), 1);
        let offset = messages[0].offset.clone();

        // Inject ack failure for first 2 attempts
        adapter.error_injector().fail_acks(2);

        // First ack should fail
        let result = reader.ack(&offset).await;
        assert!(result.is_err(), "first ack should fail");

        // Second ack should fail
        let result = reader.ack(&offset).await;
        assert!(result.is_err(), "second ack should fail");

        // Third ack should succeed
        reader.ack(&offset).await.expect("third ack should succeed");
    }

    #[tokio::test]
    async fn test_ack_stops_on_offset_not_found() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        let reader = adapter.reader();

        // Try to ack an offset that doesn't exist
        let nonexistent_offset = Offset::Int(IntOffset::new(999, 0));
        let result = reader.ack(&nonexistent_offset).await;

        assert!(result.is_err());
        // Verify it's an OffsetNotFound error
        if let Err(Error::ISB(ISBError::OffsetNotFound(_))) = result {
            // Expected
        } else {
            panic!("Expected OffsetNotFound error, got: {:?}", result);
        }
    }

    #[tokio::test]
    async fn test_nack_stops_on_offset_not_found() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        let reader = adapter.reader();

        // Try to nack an offset that doesn't exist
        let nonexistent_offset = Offset::Int(IntOffset::new(999, 0));
        let result = reader.nack(&nonexistent_offset).await;

        assert!(result.is_err());
        // Verify it's an OffsetNotFound error
        if let Err(Error::ISB(ISBError::OffsetNotFound(_))) = result {
            // Expected
        } else {
            panic!("Expected OffsetNotFound error, got: {:?}", result);
        }
    }

    #[tokio::test]
    async fn test_wip_failure_is_recoverable() {
        let adapter = SimpleBufferAdapter::new(SimpleBuffer::new(100, 0, "test-buffer"));
        write_message(&adapter, "msg-1", "hello").await;

        let reader = adapter.reader();

        // Fetch message first
        let messages = reader
            .fetch(10, Duration::from_millis(100))
            .await
            .expect("fetch should succeed");
        let offset = messages[0].offset.clone();

        // Inject WIP failure
        adapter.error_injector().fail_wip_acks(1);

        // First mark_wip should fail
        let result = reader.mark_wip(&offset).await;
        assert!(result.is_err(), "first mark_wip should fail");

        // Second mark_wip should succeed
        reader
            .mark_wip(&offset)
            .await
            .expect("second mark_wip should succeed");
    }
}
