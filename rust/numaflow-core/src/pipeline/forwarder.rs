/// Forwarder consists
/// (Read) +-------> (UDF) -------> (Write) +  
///        |                                |
///        |                                |
///        +-------> {Ack} <----------------+
///
/// {} -> Listens on a OneShot
/// () -> Streaming Interface
///

/// Forwarder specific to Sink where reader is ISB, UDF is not present, while
/// the Write is User-defined Sink or builtin.
pub(crate) mod sink_forwarder;

/// Source where the Reader is builtin or User-defined Source, Write is ISB,
/// with an optional Transformer.
pub(crate) mod source_forwarder;
