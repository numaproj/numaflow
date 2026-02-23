//! Error types for the simple KV store.

use thiserror::Error;

/// Result type for simple KV store operations.
pub type Result<T> = std::result::Result<T, SimpleKVStoreError>;

/// Errors specific to the simple KV store implementation.
#[derive(Error, Debug, Clone)]
pub enum SimpleKVStoreError {
    #[error("Failed to get keys: {0}")]
    Keys(String),

    #[error("Failed to get value: {0}")]
    Get(String),

    #[error("Failed to put value: {0}")]
    Put(String),

    #[error("Failed to delete key: {0}")]
    Delete(String),

    #[error("Failed to create watch: {0}")]
    Watch(String),

    #[error("Watch stream closed")]
    WatchStreamClosed,

    #[error("KV store operation failed: {0}")]
    Other(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_all_variants() {
        assert_eq!(
            format!("{}", SimpleKVStoreError::Keys("connection lost".to_string())),
            "Failed to get keys: connection lost"
        );
        assert_eq!(
            format!("{}", SimpleKVStoreError::Get("timeout".to_string())),
            "Failed to get value: timeout"
        );
        assert_eq!(
            format!("{}", SimpleKVStoreError::Put("disk full".to_string())),
            "Failed to put value: disk full"
        );
        assert_eq!(
            format!("{}", SimpleKVStoreError::Delete("not found".to_string())),
            "Failed to delete key: not found"
        );
        assert_eq!(
            format!("{}", SimpleKVStoreError::Watch("subscription failed".to_string())),
            "Failed to create watch: subscription failed"
        );
        assert_eq!(
            format!("{}", SimpleKVStoreError::WatchStreamClosed),
            "Watch stream closed"
        );
        assert_eq!(
            format!("{}", SimpleKVStoreError::Other("unknown".to_string())),
            "KV store operation failed: unknown"
        );
    }

    #[test]
    fn test_error_traits() {
        // Test std::error::Error trait
        let err: Box<dyn std::error::Error> =
            Box::new(SimpleKVStoreError::Keys("test".to_string()));
        assert!(err.to_string().contains("Failed to get keys"));

        // Test Clone
        let err1 = SimpleKVStoreError::Put("test".to_string());
        let err2 = err1.clone();
        assert_eq!(format!("{}", err1), format!("{}", err2));

        // Test Debug
        let err = SimpleKVStoreError::WatchStreamClosed;
        assert!(format!("{:?}", err).contains("WatchStreamClosed"));

        // Test Result type alias
        fn returns_ok() -> Result<i32> {
            Ok(42)
        }
        fn returns_err() -> Result<i32> {
            Err(SimpleKVStoreError::WatchStreamClosed)
        }
        assert_eq!(returns_ok().unwrap(), 42);
        assert!(returns_err().is_err());
    }
}

