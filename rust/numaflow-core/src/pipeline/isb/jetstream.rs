/// JetStream Writer is responsible for writing messages to JetStream ISB.
/// it exposes both sync and async methods to write messages. It has gates
/// to prevent writing into the buffer if the buffer is full. After successful
/// writes, it will let the callee know the status (or return a non-retryable
/// exception).
pub(crate) mod writer;

pub(crate) mod reader;

/// Stream is a combination of stream name and partition id.
type Stream = (String, u16);
