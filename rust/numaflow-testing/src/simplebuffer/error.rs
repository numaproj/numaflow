//! Error types for the simple buffer.

use thiserror::Error;

/// Result type for simple buffer operations.
pub type Result<T> = std::result::Result<T, SimpleBufferError>;

/// Errors specific to the simple buffer implementation.
#[derive(Error, Debug, Clone)]
pub enum SimpleBufferError {
    #[error("Offset not found: {0}")]
    OffsetNotFound(String),

    #[error("Failed to acknowledge message: {0}")]
    Ack(String),

    #[error("Failed to negatively acknowledge message: {0}")]
    Nack(String),

    #[error("Failed to send work-in-progress acknowledgment: {0}")]
    WipAck(String),

    #[error("Failed to fetch messages: {0}")]
    Fetch(String),

    #[error("Failed to query pending messages: {0}")]
    Pending(String),

    #[error("Failed to write message: {0}")]
    Write(String),

    #[error("Buffer is full")]
    BufferFull,

    #[error("Buffer operation failed: {0}")]
    Other(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_all_variants() {
        // Test display format for all error variants
        assert_eq!(
            format!("{}", SimpleBufferError::OffsetNotFound("42-0".to_string())),
            "Offset not found: 42-0"
        );
        assert_eq!(
            format!("{}", SimpleBufferError::Ack("not in-flight".to_string())),
            "Failed to acknowledge message: not in-flight"
        );
        assert_eq!(
            format!("{}", SimpleBufferError::Nack("not in-flight".to_string())),
            "Failed to negatively acknowledge message: not in-flight"
        );
        assert_eq!(
            format!("{}", SimpleBufferError::WipAck("timeout".to_string())),
            "Failed to send work-in-progress acknowledgment: timeout"
        );
        assert_eq!(
            format!(
                "{}",
                SimpleBufferError::Fetch("connection lost".to_string())
            ),
            "Failed to fetch messages: connection lost"
        );
        assert_eq!(
            format!("{}", SimpleBufferError::Pending("query failed".to_string())),
            "Failed to query pending messages: query failed"
        );
        assert_eq!(
            format!("{}", SimpleBufferError::Write("disk full".to_string())),
            "Failed to write message: disk full"
        );
        assert_eq!(
            format!("{}", SimpleBufferError::BufferFull),
            "Buffer is full"
        );
        assert_eq!(
            format!("{}", SimpleBufferError::Other("unknown".to_string())),
            "Buffer operation failed: unknown"
        );
    }

    #[test]
    fn test_error_traits() {
        // Test std::error::Error trait
        let err: Box<dyn std::error::Error> =
            Box::new(SimpleBufferError::OffsetNotFound("1-0".to_string()));
        assert!(err.to_string().contains("Offset not found"));

        // Test Clone
        let err1 = SimpleBufferError::Ack("test".to_string());
        let err2 = err1.clone();
        assert_eq!(format!("{}", err1), format!("{}", err2));

        // Test Debug
        let err = SimpleBufferError::BufferFull;
        assert!(format!("{:?}", err).contains("BufferFull"));

        // Test Result type alias
        fn returns_ok() -> Result<i32> {
            Ok(42)
        }
        fn returns_err() -> Result<i32> {
            Err(SimpleBufferError::BufferFull)
        }
        assert_eq!(returns_ok().unwrap(), 42);
        assert!(returns_err().is_err());
    }
}
