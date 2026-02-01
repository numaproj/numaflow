//! [Vertex] are connected via [ISB].
//!
//! [Vertex]: https://numaflow.numaproj.io/core-concepts/vertex/
//! [ISB]: https://numaflow.numaproj.io/core-concepts/inter-step-buffer/

use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::message::{Message, Offset};

pub(crate) mod compression;
pub(crate) mod error;
pub(crate) mod jetstream;
pub(crate) mod reader;
pub(crate) mod writer;

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
    #[allow(dead_code)]
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

/// Trait for writing messages to an Inter Step Buffer (ISB).
///
/// Implementations handle the low-level details of writing messages
/// and waiting for confirmation. The write operation blocks until
/// the message is confirmed written (or an error occurs).
///
/// Implementations must be cheaply cloneable (e.g., using Arc internally).
#[async_trait]
pub(crate) trait ISBWriter: Send + Sync + Clone {
    /// Writes a message to the ISB and blocks until confirmed.
    ///
    /// Returns `Ok(())` on successful write, or an error if the write fails.
    /// `WriteError::BufferFull` indicates the buffer is full (retryable).
    /// `WriteError::WriteFailed` indicates a write failure (retryable).
    ///
    /// # Arguments
    /// * `message` - The message to write
    /// * `cln_token` - Cancellation token for graceful shutdown
    async fn write(
        &self,
        message: Message,
        cln_token: CancellationToken,
    ) -> std::result::Result<(), WriteError>;

    /// Returns the name/identifier of this writer (e.g., stream name).
    #[allow(dead_code)]
    fn name(&self) -> &'static str;

    /// Returns whether the buffer is full.
    ///
    /// This is useful for proactive checking before attempting writes.
    /// Default implementation returns `false`.
    fn is_full(&self) -> bool {
        false
    }
}
