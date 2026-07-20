//! SimpleBuffer adapters implementing [`ISBReader`] / [`ISBWriter`] with full proto
//! round-trip fidelity.
//!
//! Writes serialize the complete [`Message`] via `TryFrom<Message> for BytesMut`
//! (same encoding as JetStream). Reads decode the protobuf and map fields via
//! [`message_from_isb_proto`], then assign the broker offset from the buffer slot.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use prost::Message as ProstMessage;

use crate::error::Error;
use crate::message::{IntOffset, Message, NackOptions, Offset, message_from_isb_proto};
use crate::pipeline::isb::error::ISBError;
use crate::pipeline::isb::inmemory::{
    ErrorInjector, Offset as SimpleOffset, ReadMessage, SimpleBuffer, SimpleBufferError,
    SimpleReader, SimpleWriter, WriteError as SimpleWriteError, WriteResult as SimpleWriteResult,
};
use crate::pipeline::isb::{ISBReader, ISBWriter, PendingWrite, WriteError, WriteResult};

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

impl SimpleReaderAdapter {
    /// Create a reader adapter from an existing [`SimpleReader`].
    pub(crate) fn new(inner: SimpleReader) -> Self {
        Self { inner }
    }
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

impl From<&Offset> for SimpleOffset {
    fn from(value: &Offset) -> Self {
        match value {
            Offset::Int(int_offset) => {
                SimpleOffset::new(int_offset.offset, int_offset.partition_idx)
            }
            Offset::String(str_offset) => {
                let seq: i64 = std::str::from_utf8(&str_offset.offset)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                SimpleOffset::new(seq, str_offset.partition_idx)
            }
        }
    }
}

/// Convert in-memory buffer [`Offset`] to [numaflow_core::message::Offset].
impl From<&SimpleOffset> for Offset {
    fn from(value: &SimpleOffset) -> Self {
        Offset::Int(IntOffset::new(value.sequence, value.partition_idx))
    }
}

/// Decode a buffer [`ReadMessage`] into a core [`Message`].
///
/// Keep field mapping in sync with `js_reader.rs` / [`message_from_isb_proto`].
/// Unlike JetStream, WMB messages retain their real broker offset (required for
/// orchestrator ack at `reader.rs`). Decode failure returns `Err` (not dropped).
fn convert_message(read_msg: ReadMessage) -> crate::Result<Message> {
    let offset = Offset::Int(IntOffset::new(
        read_msg.offset.sequence,
        read_msg.offset.partition_idx,
    ));
    let proto = numaflow_pb::objects::isb::Message::decode(read_msg.payload)
        .map_err(|e| Error::Proto(e.to_string()))?;
    message_from_isb_proto(proto, offset)
}

impl ISBReader for SimpleReaderAdapter {
    async fn fetch(&self, max: usize, timeout: Duration) -> crate::Result<Vec<Message>> {
        let msgs = self.inner.fetch(max, timeout).await.map_err(Error::from)?;
        msgs.into_iter().map(convert_message).collect()
    }

    async fn ack(&self, offset: &Offset) -> crate::Result<()> {
        let simple_offset = offset.into();
        self.inner.ack(&simple_offset).await.map_err(|e| e.into())
    }

    async fn nack(&self, offset: &Offset, _nack_options: Option<NackOptions>) -> crate::Result<()> {
        // SimpleBuffer has no broker-side delay; the optional `delay` is ignored.
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

impl SimpleWriterAdapter {
    /// Create a writer adapter from an existing [`SimpleWriter`].
    pub(crate) fn new(inner: SimpleWriter) -> Self {
        Self { inner }
    }
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

/// Convert in-memory [`SimpleWriteResult`] to [WriteResult].
impl From<SimpleWriteResult> for WriteResult {
    fn from(result: SimpleWriteResult) -> Self {
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
        // Serialize the full message exactly like JetStream (headers live inside the proto).
        let encoded: bytes::BytesMut = message
            .try_into()
            .map_err(|e: Error| WriteError::WriteFailed(e.to_string()))?;
        let pending = self.inner.async_write(id, encoded.freeze(), HashMap::new());

        // Clone inner writer to capture in the future for resolve() which applies latency/errors
        let writer = self.inner.clone();

        // Return a boxed future that resolves the inner pending write to WriteResult
        // Using resolve() ensures error injection (latency, failures) is applied
        Ok(Box::pin(async move {
            writer
                .resolve(pending)
                .await
                .map(|r| r.into())
                .map_err(|e| e.into())
        }))
    }

    async fn write(&self, message: Message) -> Result<WriteResult, WriteError> {
        let id = message.id.to_string();
        let encoded: bytes::BytesMut = message
            .try_into()
            .map_err(|e: Error| WriteError::WriteFailed(e.to_string()))?;
        self.inner
            .write(id, encoded.freeze(), HashMap::new())
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
impl crate::typ::NumaflowTypeConfig for WithSimpleBuffer {
    type RateLimiter = numaflow_throttling::NoOpRateLimiter;
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)] // Tests use indexing for simplicity
mod tests {
    use super::*;
    use crate::message::MessageID;
    use bytes::Bytes;
    use chrono::Utc;
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
            nack_options: None,
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
            .nack(&messages[0].offset, None)
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
        let result = reader.nack(&nonexistent_offset, None).await;

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
