/// Lag reader reports the pending information at Reader (source, ISBs), this information is used by
/// the auto-scaler.
#[trait_variant::make(LagReader: Send)]
#[allow(dead_code)]
pub(crate) trait LocalLagReader {
    /// Pending elements yet to be read from the stream. The stream could be the [crate::source], or ISBs
    /// It may or may not include unacknowledged messages.
    async fn pending(&mut self) -> crate::error::Result<Option<usize>>;
}
