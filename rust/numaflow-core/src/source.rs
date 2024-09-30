use crate::message::{Message, Offset};

/// [User-Defined Source] extends Numaflow to add custom sources supported outside the builtins.
///
/// [User-Defined Source]: https://numaflow.numaproj.io/user-guide/sources/user-defined-sources/
pub(crate) mod user_defined;

/// Set of items that has to be implemented to become a Source.
pub(crate) trait Source {
    /// Name of the source.
    fn name(&self) -> &'static str;

    async fn read(&mut self) -> crate::Result<Vec<Message>>;

    /// acknowledge an offset. The implementor might choose to do it in an asynchronous way.
    async fn ack(&mut self, _: Vec<Offset>) -> crate::Result<()>;

    /// number of partitions processed by this source.
    fn partitions(&self) -> Vec<u16>;
}

/// Lag reader reports the pending information at source, this information is used by the auto-scaler.
pub(crate) trait LagReader {
    /// Pending elements yet to be processed at the source. It may or may not included unacknowledged
    /// messages.
    fn pending(&self) -> crate::Result<usize>;
}
