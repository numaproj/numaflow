use async_nats::jetstream::Context;

/// Writes to JetStream ISB.
pub(super) struct JetstreamWriter {
    js_ctx: Context,
    batch_size: usize,
}

impl JetstreamWriter {
    pub(super) fn new(js_ctx: Context, batch_size: usize) -> Self {
        Self { js_ctx, batch_size }
    }
}
