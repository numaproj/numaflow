use crate::message::{Message, ResponseFromSink};

/// [User-Defined Sink] extends Numaflow to add custom sources supported outside the builtins.
///
/// [User-Defined Sink]: https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/
pub(crate) mod user_defined;

/// Set of items to be implemented be a Numaflow Sink.
///
/// [Sink]: https://numaflow.numaproj.io/user-guide/sinks/overview/
pub(crate) trait Sink {
    /// Write the messages to the Sink.
    async fn sink(&mut self, _: Vec<Message>) -> crate::Result<Vec<ResponseFromSink>>;
}
