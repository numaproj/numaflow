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
