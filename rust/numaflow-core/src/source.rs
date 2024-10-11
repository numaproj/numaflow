use crate::message::{Message, Offset};
use async_trait::async_trait;

/// [User-Defined Source] extends Numaflow to add custom sources supported outside the builtins.
///
/// [User-Defined Source]: https://numaflow.numaproj.io/user-guide/sources/user-defined-sources/
pub(crate) mod user_defined;

/// [Generator] is a builtin to generate data for load testing and other internal use-cases.
///
/// [Generator]: https://numaflow.numaproj.io/user-guide/sources/generator/
pub(crate) mod generator;

/// Set of Read related items that has to be implemented to become a Source.
#[async_trait]
pub(crate) trait SourceReader {
    #[allow(dead_code)]
    /// Name of the source.
    fn name(&self) -> &'static str;

    async fn read(&mut self) -> crate::Result<Vec<Message>>;

    #[allow(dead_code)]
    /// number of partitions processed by this source.
    fn partitions(&self) -> Vec<u16>;
}

/// Set of Ack related items that has to be implemented to become a Source.
#[async_trait]
pub(crate) trait SourceAcker {
    /// acknowledge an offset. The implementor might choose to do it in an asynchronous way.
    async fn ack(&mut self, _: Vec<Offset>) -> crate::Result<()>;
}

// #[async_trait]
impl<R: SourceReader + ?Sized> SourceReader for Box<R> {
    fn name(&self) -> &'static str {
        (**self).name()
    }
    fn partitions(&self) -> Vec<u16> {
        (**self).partitions()
    }

    fn read<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = crate::Result<Vec<Message>>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        (**self).read()
    }
    // async fn read(&mut self) -> crate::Result<Vec<Message>> {
    //     (**self).read().await
    // }
}

impl<A: SourceAcker + ?Sized> SourceAcker for Box<A> {
    fn ack<'life0, 'async_trait>(
        &'life0 mut self,
        offsets: Vec<Offset>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = crate::Result<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        (**self).ack(offsets)
    }
}
