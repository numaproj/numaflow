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
    fn test_offset_not_found_display() {
        let err = SimpleBufferError::OffsetNotFound("42-0".to_string());
        assert_eq!(format!("{}", err), "Offset not found: 42-0");
    }

    #[test]
    fn test_ack_error_display() {
        let err = SimpleBufferError::Ack("message not in-flight".to_string());
        assert_eq!(
            format!("{}", err),
            "Failed to acknowledge message: message not in-flight"
        );
    }

    #[test]
    fn test_nack_error_display() {
        let err = SimpleBufferError::Nack("message not in-flight".to_string());
        assert_eq!(
            format!("{}", err),
            "Failed to negatively acknowledge message: message not in-flight"
        );
    }

    #[test]
    fn test_wip_ack_error_display() {
        let err = SimpleBufferError::WipAck("timeout".to_string());
        assert_eq!(
            format!("{}", err),
            "Failed to send work-in-progress acknowledgment: timeout"
        );
    }

    #[test]
    fn test_fetch_error_display() {
        let err = SimpleBufferError::Fetch("connection lost".to_string());
        assert_eq!(
            format!("{}", err),
            "Failed to fetch messages: connection lost"
        );
    }

    #[test]
    fn test_pending_error_display() {
        let err = SimpleBufferError::Pending("query failed".to_string());
        assert_eq!(
            format!("{}", err),
            "Failed to query pending messages: query failed"
        );
    }

    #[test]
    fn test_write_error_display() {
        let err = SimpleBufferError::Write("disk full".to_string());
        assert_eq!(format!("{}", err), "Failed to write message: disk full");
    }

    #[test]
    fn test_buffer_full_display() {
        let err = SimpleBufferError::BufferFull;
        assert_eq!(format!("{}", err), "Buffer is full");
    }

    #[test]
    fn test_other_error_display() {
        let err = SimpleBufferError::Other("unknown error".to_string());
        assert_eq!(format!("{}", err), "Buffer operation failed: unknown error");
    }

    #[test]
    fn test_error_is_std_error() {
        let err: Box<dyn std::error::Error> =
            Box::new(SimpleBufferError::OffsetNotFound("1-0".to_string()));
        assert!(err.to_string().contains("Offset not found"));
    }

    #[test]
    fn test_error_clone() {
        let err1 = SimpleBufferError::Ack("test".to_string());
        let err2 = err1.clone();
        assert_eq!(format!("{}", err1), format!("{}", err2));
    }

    #[test]
    fn test_error_debug() {
        let err = SimpleBufferError::BufferFull;
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("BufferFull"));
    }

    #[test]
    fn test_result_type_alias() {
        fn returns_result() -> Result<i32> {
            Ok(42)
        }

        fn returns_error() -> Result<i32> {
            Err(SimpleBufferError::BufferFull)
        }

        assert_eq!(returns_result().unwrap(), 42);
        assert!(returns_error().is_err());
    }
}
