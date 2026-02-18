//! [Vertex] are connected via [ISB].
//!
//! [Vertex]: https://numaflow.numaproj.io/core-concepts/vertex/
//! [ISB]: https://numaflow.numaproj.io/core-concepts/inter-step-buffer/

use std::time::Duration;

use async_trait::async_trait;

use crate::error::Result;
use crate::message::{Message, Offset};

pub(crate) mod compression;
pub(crate) mod error;
pub(crate) mod factory;
pub(crate) mod jetstream;
pub(crate) mod reader;
pub(crate) mod writer;
// SimpleBuffer for integration tests
#[cfg(test)]
pub(crate) mod simplebuffer;

pub(crate) use factory::ISBFactory;

/// Trait for reading messages from an Inter Step Buffer (ISB).
///
/// Implementations handle the low-level details of fetching messages,
/// acknowledging them, and tracking pending counts. The ack/nack operations
/// use offsets, and implementations are responsible for maintaining any
/// internal state needed to perform these operations (e.g., JetStream needs
/// the original message object).
///
/// Implementations must be cheaply cloneable (e.g., using Arc internally).
#[async_trait]
pub(crate) trait ISBReader: Send + Sync + Clone {
    /// Fetches a batch of messages from the ISB.
    ///
    /// This is a blocking call that waits up to `timeout` for messages.
    ///
    /// # Arguments
    /// * `max` - Maximum number of messages to fetch
    /// * `timeout` - Maximum time to wait for messages
    async fn fetch(&mut self, max: usize, timeout: Duration) -> Result<Vec<Message>>;

    /// Acknowledges successful processing of a message.
    ///
    /// The implementation uses the offset to identify which message to ack.
    async fn ack(&self, offset: &Offset) -> Result<()>;

    /// Negative acknowledgment - indicates the message should be redelivered.
    ///
    /// The implementation uses the offset to identify which message to nack.
    async fn nack(&self, offset: &Offset) -> Result<()>;

    /// Returns the number of pending (unprocessed) messages, if available.
    ///
    /// Returns `None` if the ISB implementation doesn't support pending counts.
    async fn pending(&mut self) -> Result<Option<usize>>;

    /// Returns the name/identifier of this reader (e.g., stream name).
    fn name(&self) -> &'static str;

    /// Marks a message as work-in-progress to prevent redelivery during long processing.
    ///
    /// This is optional - implementations that don't support WIP can use the default
    /// implementation which does nothing.
    async fn mark_wip(&self, _offset: &Offset) -> Result<()> {
        Ok(())
    }

    /// Returns the interval at which WIP acks should be sent, if WIP is supported.
    ///
    /// Returns `None` if WIP is not supported or not needed.
    /// Default implementation returns `None`.
    fn wip_ack_interval(&self) -> Option<Duration> {
        None
    }
}

/// Error types for ISB write operations.
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
///
/// Contains the offset of the written message along with additional metadata
/// that may be useful for logging, metrics, or debugging.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// The offset of the written message
    pub offset: Offset,
    /// Whether this was a duplicate message (already existed in the buffer)
    pub is_duplicate: bool,
}

impl WriteResult {
    /// Creates a new WriteResult with the given offset and no duplicate flag.
    pub fn new(offset: Offset) -> Self {
        Self {
            offset,
            is_duplicate: false,
        }
    }

    /// Creates a new WriteResult marked as a duplicate.
    pub fn duplicate(offset: Offset) -> Self {
        Self {
            offset,
            is_duplicate: true,
        }
    }
}

/// Trait for writing messages to an Inter Step Buffer (ISB).
///
/// This trait supports two write patterns:
/// 1. **High-performance async pattern**: Use `async_write()` to get a `PendingWrite` handle
///    immediately, then resolve it later with `resolve()`. This allows batching acknowledgments
///    for higher throughput.
/// 2. **Simple synchronous pattern**: Use `write()` which writes and waits for confirmation
///    in a single call. This is useful as a fallback when async writes fail.
///
/// Both `resolve()` and `write()` return `Result<WriteResult, WriteError>` for consistency.
/// The orchestrator is responsible for retry logic and handling cancellation.
///
/// Implementations must be cheaply cloneable (e.g., using Arc internally).
#[async_trait]
pub(crate) trait ISBWriter: Send + Sync + Clone {
    /// The pending write handle returned by `async_write()`.
    /// For JetStream, this is `PublishAckFuture`. For simple implementations,
    /// this can be `()` if acknowledgments are immediate.
    type PendingWrite: Send + 'static;

    /// Writes a message and returns immediately with a pending write handle.
    ///
    /// This is the high-performance write method. The returned `PendingWrite` can be
    /// resolved later using `resolve()` to get the offset. This allows batching
    /// multiple writes and resolving them in parallel.
    ///
    /// Returns `Err(WriteError::BufferFull)` if the buffer is full.
    /// Returns `Err(WriteError::WriteFailed)` if the publish operation fails.
    ///
    /// # Arguments
    /// * `message` - The message to write
    async fn async_write(
        &self,
        message: Message,
    ) -> std::result::Result<Self::PendingWrite, WriteError>;

    /// Resolves a pending write to get the result.
    ///
    /// This waits for the write acknowledgment and returns a `WriteResult` containing
    /// the offset of the written message along with additional metadata (e.g., whether
    /// the message was a duplicate).
    ///
    /// # Arguments
    /// * `pending` - The pending write handle from `async_write()`
    async fn resolve(
        &self,
        pending: Self::PendingWrite,
    ) -> std::result::Result<WriteResult, WriteError>;

    /// Writes a message and waits for confirmation, returning the result.
    ///
    /// This is a single-attempt write that returns immediately after the write
    /// completes or fails. The orchestrator is responsible for retry logic.
    ///
    /// Returns `Err(WriteError::BufferFull)` if the buffer is full.
    /// Returns `Err(WriteError::WriteFailed)` if the write operation fails.
    ///
    /// # Arguments
    /// * `message` - The message to write
    async fn write(&self, message: Message) -> std::result::Result<WriteResult, WriteError>;

    /// Returns the name/identifier of this writer (e.g., stream name).
    #[allow(dead_code)]
    fn name(&self) -> &'static str;

    /// Returns whether the buffer is full.
    ///
    /// This is useful for proactive checking before attempting writes.
    /// Default implementation returns `false`.
    #[allow(dead_code)]
    fn is_full(&self) -> bool {
        false
    }
}
