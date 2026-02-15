//! Error types for the daemon server.

/// Result type for daemon server operations.
pub(crate) type Result<T> = std::result::Result<T, Error>;

/// Errors specific to the daemon server implementation.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to complete a task: {0}")]
    NotComplete(String),

    #[error("Failed to configure TLS: {0}")]
    TlsConfiguration(String),

    #[error("Failed to parse address: {0}")]
    Address(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
