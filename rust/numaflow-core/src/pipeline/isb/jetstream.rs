/// JetStream Writer is responsible for writing messages to JetStream ISB.
/// it exposes both sync and async methods to write messages. It has gates
/// to prevent writing into the buffer if the buffer is full. After successful
/// writes, it will let the callee know the status (or return a non-retryable
/// exception).
pub(crate) mod writer;

/// JetStream Reader is responsible for reading messages from JetStream ISB.
/// It exposes method to read messages in the form of batches and also ack/nack
/// the messages.
pub(crate) mod reader;
