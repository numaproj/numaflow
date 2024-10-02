/// Lag reader reports the pending information at Reader (source, ISBs), this information is used by
/// the auto-scaler.
#[trait_variant::make(LagReader: Send)]
#[allow(dead_code)]
pub(crate) trait LocalLagReader {
    /// Pending elements yet to be processed at the source. It may or may not included unacknowledged
    /// messages.
    async fn pending(&mut self) -> crate::error::Result<Option<usize>>;
}
