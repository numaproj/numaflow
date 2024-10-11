use async_trait::async_trait;

/// Lag reader reports the pending information at Reader (source, ISBs), this information is used by
/// the auto-scaler.

#[async_trait]
pub(crate) trait LagReader {
    /// Pending elements yet to be read from the stream. The stream could be the [crate::source], or ISBs
    /// It may or may not include unacknowledged messages.
    async fn pending(&mut self) -> crate::error::Result<Option<usize>>;
}

impl<R: LagReader + ?Sized> LagReader for Box<R> {
    fn pending<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = crate::error::Result<Option<usize>>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        (**self).pending()
    }
}
