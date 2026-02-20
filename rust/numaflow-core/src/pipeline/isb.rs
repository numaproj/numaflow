//! [Vertex] are connected via [ISB].
//!
//! [Vertex]: https://numaflow.numaproj.io/core-concepts/vertex/
//! [ISB]: https://numaflow.numaproj.io/core-concepts/inter-step-buffer/

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

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
pub use jetstream::js_writer::JetStreamPendingWrite;
#[cfg(test)]
pub(crate) use simplebuffer::SimpleBufferPendingWrite;

/// A pending write operation that can be awaited to get the final `WriteResult`.
///
/// This enum represents pending writes from different ISB implementations.
/// It implements `Future` directly, avoiding heap allocation that would be
/// required by `Pin<Box<dyn Future>>`.
///
/// # Adding new ISB implementations
///
/// When adding a new ISB implementation:
/// 1. Create a new pending write struct (e.g., `NewISBPendingWrite`)
/// 2. Implement `Future` for it
/// 3. Add a new variant to this enum
/// 4. Update the `Future` impl below to handle the new variant
///
/// Note: If the new pending write type is NOT `Unpin`, the `self.get_mut()` call
/// in the `Future` impl will fail to compile. In that case, you'll need to use
/// `pin_project_lite` for safe pin projection.
pub enum PendingWrite {
    /// Pending write for JetStream ISB
    JetStream(JetStreamPendingWrite),
    /// Pending write for SimpleBuffer (testing only)
    #[cfg(test)]
    SimpleBuffer(SimpleBufferPendingWrite),
}

impl Future for PendingWrite {
    type Output = std::result::Result<WriteResult, WriteError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // This only compiles if all variants are Unpin.
        // If a non-Unpin variant is added, this will fail to compile,
        // signaling that pin_project_lite needs to be used.
        match self.get_mut() {
            PendingWrite::JetStream(inner) => Pin::new(inner).poll(cx),
            #[cfg(test)]
            PendingWrite::SimpleBuffer(inner) => Pin::new(inner).poll(cx),
        }
    }
}

/// Trait for reading messages from an Inter Step Buffer (ISB).
///
/// Implementations handle the low-level details of fetching messages,
/// acknowledging them, and tracking pending counts. The ack/nack operations
/// use offsets, and implementations are responsible for maintaining any
/// internal state needed to perform these operations (e.g., JetStream needs
/// the original message object).
///
/// Uses `trait_variant::make` to generate an object-safe `ISBReader` trait with `Send` bound.
#[allow(dead_code)]
#[trait_variant::make(ISBReader: Send)]
pub(crate) trait LocalISBReader: Sync {
    /// Fetches a batch of messages from the ISB.
    ///
    /// This is a blocking call that waits up to `timeout` for messages.
    ///
    /// # Arguments
    /// * `max` - Maximum number of messages to fetch
    /// * `timeout` - Maximum time to wait for messages
    async fn fetch(&self, max: usize, timeout: Duration) -> Result<Vec<Message>>;

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
    async fn pending(&self) -> Result<Option<usize>>;

    /// Returns the name/identifier of this reader (e.g., stream name).
    fn name(&self) -> &'static str;

    /// Marks a message as work-in-progress to prevent redelivery during long processing.
    ///
    /// This is optional - implementations that don't support WIP can use the default
    /// implementation which does nothing.
    async fn mark_wip(&self, offset: &Offset) -> Result<()>;

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
///    immediately, then resolve it later by awaiting the future. This allows batching
///    acknowledgments for higher throughput.
/// 2. **Simple synchronous pattern**: Use `write()` which writes and waits for confirmation
///    in a single call. This is useful as a fallback when async writes fail.
///
/// Both patterns return `Result<WriteResult, WriteError>` for consistency.
/// The orchestrator is responsible for retry logic and handling cancellation.
///
/// Uses `trait_variant::make` to generate an object-safe `ISBWriter` trait with `Send` bound.
#[allow(dead_code)]
#[trait_variant::make(ISBWriter: Send)]
pub(crate) trait LocalISBWriter: Sync {
    /// Writes a message and returns immediately with a pending write handle.
    ///
    /// This is the high-performance write method. The returned `PendingWrite` is a
    /// boxed future that can be awaited later to get the `WriteResult`. This allows
    /// batching multiple writes and resolving them in parallel.
    ///
    /// Returns `Err(WriteError::BufferFull)` if the buffer is full.
    /// Returns `Err(WriteError::WriteFailed)` if the publish operation fails.
    ///
    /// # Arguments
    /// * `message` - The message to write
    async fn async_write(&self, message: Message) -> std::result::Result<PendingWrite, WriteError>;

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
