use thiserror::Error;

/// ISB-specific error types for Inter-Step Buffer operations.
/// These errors are specific to ISB reader and writer operations.
#[derive(Error, Debug, Clone)]
pub enum ISBError {
    /// Error when trying to ack/nack/mark_wip an offset that doesn't exist in the tracking map.
    /// This typically indicates a bug or timing issue where the offset was already processed or never fetched.
    #[error("Offset not found in tracking map: {0}")]
    OffsetNotFound(String),

    /// Error during message acknowledgment operation.
    /// This can occur due to network issues, JetStream errors, or timeout.
    #[error("Failed to acknowledge message: {0}")]
    Ack(String),

    /// Error during negative acknowledgment operation.
    /// This can occur due to network issues, JetStream errors, or timeout.
    #[error("Failed to negatively acknowledge message: {0}")]
    Nack(String),

    /// Error during work-in-progress acknowledgment operation.
    /// WIP acks are sent periodically to prevent message redelivery during long processing.
    #[error("Failed to send work-in-progress acknowledgment: {0}")]
    WipAck(String),

    /// Error when fetching messages from the ISB.
    /// This can occur due to network issues, consumer errors, or stream errors.
    #[error("Failed to fetch messages from ISB: {0}")]
    Fetch(String),

    /// Error when encoding/compressing messages for writing to the ISB.
    /// This can occur due to compression errors or serialization failures.
    #[error("Failed to encode message: {0}")]
    Encode(String),

    /// Error when decoding/decompressing messages from the ISB.
    /// This can occur due to protocol buffer errors, decompression errors, or data corruption.
    #[error("Failed to decode message: {0}")]
    Decode(String),

    /// Error when querying pending message count.
    /// This can occur due to network issues or JetStream API errors.
    #[error("Failed to query pending messages: {0}")]
    Pending(String),

    /// Error when writing/publishing messages to the ISB.
    /// This can occur due to network issues, JetStream errors, timeout, or buffer full.
    #[error("Failed to write message to ISB: {0}")]
    Write(String),

    /// Error when fetching buffer information (stream/consumer info).
    /// This can occur due to network issues or JetStream API errors.
    #[error("Failed to fetch buffer information: {0}")]
    BufferInfo(String),

    /// Generic ISB error for cases not covered by specific variants.
    /// TODO: As we identify more specific error cases, we should add dedicated variants.
    #[error("ISB operation failed: {0}")]
    Other(String),
}
