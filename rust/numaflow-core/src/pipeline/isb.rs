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
pub mod jetstream;
pub(crate) mod reader;
pub(crate) mod writer;

/// Trait for reading messages from an Inter-Stage Buffer (ISB).
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
