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

    #[error("map session is closed: {0}")]
    SessionClosed(String),

    #[error("map request id is empty")]
    EmptyRequestId,

    #[error("duplicate in-flight map request id: {0}")]
    DuplicateRequestId(String),

    #[error("unexpected map response id: {0}")]
    UnexpectedResponseId(String),

    #[error("unexpected map control frame: {0}")]
    UnexpectedControlFrame(String),

    #[error("map response channel closed for id: {0}")]
    ResponseChannelClosed(String),

    #[error(
        "UDF_PARTIAL_RESPONSE(batch_map): received EOT before all batch responses; \
         unanswered ids: {pending_ids:?}"
    )]
    PartialBatchResponse { pending_ids: Vec<String> },

    #[error("batch map operation was abandoned before EOT")]
    BatchOperationAbandoned,

    #[error("invalid reduce response timestamp: {0}")]
    InvalidReduceResponseTimestamp(String),

    #[error("failed to start ReduceFn: {0}")]
    ReduceFnStart(#[source] tonic::Status),

    #[error("reduce gRPC stream failed: {0}")]
    ReduceGrpc(#[source] tonic::Status),

    #[error("reduce request stream is closed")]
    ReduceRequestStreamClosed,

    #[error("reduce response channel closed")]
    ReduceResponseChannelClosed,

    #[error("invalid reduce response result: {0}")]
    InvalidReduceResponseResult(String),

    #[error("invalid reduce response window: {0}")]
    InvalidReduceResponseWindow(String),

    #[error("reduce response window mismatch: expected {expected}, got {actual}")]
    ReduceResponseWindowMismatch { expected: String, actual: String },
}
