/// Lag reader reports the pending information at Reader (source, ISBs), this information is used by
/// the auto-scaler.
#[trait_variant::make(LagReader: Send)]
#[allow(dead_code)]
pub(crate) trait LocalLagReader {
    /// Pending elements yet to be read from the stream. The stream could be the [crate::source], or ISBs
    /// It may or may not include unacknowledged messages.
    /// Pending should only be supported for sources which are not bounded like kafka, jetstream
    /// pulsar, sqs, etc.
    /// if we return Some(x) from this method, the autoscaler will scale up thinking
    /// there are pending messages and will start autoscaling. It will also scale down to 0 replicas
    /// if the pending is 0. This is not ideal for non-bounded source for HTTP source.
    async fn pending(&mut self) -> crate::error::Result<Option<usize>>;
}
