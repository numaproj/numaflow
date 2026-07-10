/// Result type returned by the shared UDF client.
pub type Result<T> = std::result::Result<T, UdfClientError>;

/// Errors produced while opening or using a UDF client session.
#[derive(Debug, Clone, thiserror::Error)]
pub enum UdfClientError {
    #[error("invalid UDF client configuration: {0}")]
    InvalidConfig(String),

    #[error("failed to start MapFn: {0}")]
    MapFnStart(#[source] tonic::Status),

    #[error("map gRPC stream failed: {0}")]
    Grpc(#[source] tonic::Status),

    #[error("map stream closed before handshake response")]
    HandshakeStreamClosed,

    #[error("invalid map handshake response")]
    InvalidHandshake,

    #[error("map request stream is closed")]
    RequestStreamClosed,

    #[error("map response stream closed")]
    ResponseStreamClosed,

    #[error("unary map session is closed: {0}")]
    SessionClosed(String),

    #[error("unary map request id is empty")]
    EmptyRequestId,

    #[error("duplicate in-flight unary map request id: {0}")]
    DuplicateRequestId(String),

    #[error("unexpected unary map response id: {0}")]
    UnexpectedResponseId(String),

    #[error("unexpected unary map control frame: {0}")]
    UnexpectedControlFrame(String),

    #[error("unary map response channel closed for id: {0}")]
    ResponseChannelClosed(String),
}
