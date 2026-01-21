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

    /// Error when decoding/deserializing messages from the ISB.
    /// This can occur due to protocol buffer errors, compression errors, or data corruption.
    #[error("Failed to decode message: {0}")]
    Decode(String),

    /// Error when querying pending message count.
    /// This can occur due to network issues or JetStream API errors.
    #[error("Failed to query pending messages: {0}")]
    Pending(String),

    /// Generic ISB error for cases not covered by specific variants.
    /// TODO: As we identify more specific error cases, we should add dedicated variants.
    #[error("ISB operation failed: {0}")]
    Other(String),
}

impl ISBError {
    /// Determines if this error is retryable.
    /// 
    /// Currently, all ISB errors are considered retryable as they are typically
    /// transient (network issues, timeouts, etc.).
    /// 
    /// TODO: As we identify non-retryable errors (e.g., authentication failures,
    /// invalid configuration), we should return false for those cases.
    pub fn is_retryable(&self) -> bool {
        // All current errors are retryable
        true
    }
}

