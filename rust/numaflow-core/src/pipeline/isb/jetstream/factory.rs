//! JetStream ISB Factory implementation.
//!
//! This module provides a factory for creating JetStream-based ISB readers and writers.

use async_nats::jetstream::Context;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::Result;
use crate::config::pipeline::isb::{BufferWriterConfig, ISBConfig, Stream};
use crate::pipeline::isb::ISBFactory;
use crate::pipeline::isb::jetstream::js_reader::JetStreamReader;
use crate::pipeline::isb::jetstream::js_writer::JetStreamWriter;

/// Factory for creating JetStream-based ISB readers and writers.
///
/// This factory encapsulates the JetStream context and provides methods
/// to create readers and writers for specific streams.
#[derive(Clone)]
pub struct JetStreamFactory {
    /// The JetStream context used to create readers and writers
    context: Context,
}

impl JetStreamFactory {
    /// Creates a new JetStreamFactory with the given JetStream context.
    ///
    /// # Arguments
    /// * `context` - The JetStream context to use for creating readers and writers
    pub fn new(context: Context) -> Self {
        Self { context }
    }

    /// Returns a reference to the underlying JetStream context.
    ///
    /// This can be useful for operations that need direct access to the context,
    /// such as watermark handling.
    #[allow(dead_code)] // May be used for watermark handling or other direct context access
    pub fn context(&self) -> &Context {
        &self.context
    }
}

#[async_trait]
impl ISBFactory for JetStreamFactory {
    type Reader = JetStreamReader;
    type Writer = JetStreamWriter;

    async fn create_reader(
        &self,
        stream: Stream,
        isb_config: Option<&ISBConfig>,
    ) -> Result<Self::Reader> {
        JetStreamReader::new(stream, self.context.clone(), isb_config.cloned()).await
    }

    async fn create_writer(
        &self,
        stream: Stream,
        writer_config: BufferWriterConfig,
        isb_config: Option<&ISBConfig>,
        cln_token: CancellationToken,
    ) -> Result<Self::Writer> {
        let compression_type = isb_config.map(|c| c.compression.compress_type);
        JetStreamWriter::new(
            stream,
            self.context.clone(),
            writer_config,
            compression_type,
            cln_token,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_factory_creation() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let context = async_nats::jetstream::new(client);
        let factory = JetStreamFactory::new(context.clone());

        // Verify the context is accessible
        assert!(std::ptr::eq(factory.context(), &factory.context));
    }
}
