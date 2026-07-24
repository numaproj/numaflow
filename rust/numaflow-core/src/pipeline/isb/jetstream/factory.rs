//! JetStream ISB Factory implementation.
//!
//! This module provides a factory for creating JetStream-based ISB readers and writers.

use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::Context;
use async_nats::{ConnectOptions, jetstream};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::Result;
use crate::config::pipeline::isb::jetstream::ClientConfig;
use crate::config::pipeline::isb::{BufferWriterConfig, ISBConfig, Stream};
use crate::error;
use crate::error::Error;
use crate::pipeline::isb::ISBFactory;
use crate::pipeline::isb::dyn_adapter::{ISBReaderRef, ISBWriterRef};
use crate::pipeline::isb::jetstream::js_reader::JetStreamReader;
use crate::pipeline::isb::jetstream::js_writer::JetStreamWriter;
use numaflow_shared::kv::KVStore;
use numaflow_shared::kv::jetstream::JetstreamKVStore;

/// Creates a jetstream context based on the provided configuration.
pub(crate) async fn create_js_context(config: ClientConfig) -> Result<Context> {
    // TODO: make these configurable. today this is hardcoded on Golang code too.
    let mut opts = ConnectOptions::new()
        .max_reconnects(None) // unlimited reconnects
        .ping_interval(Duration::from_secs(3))
        .retry_on_initial_connect();

    if let (Some(user), Some(password)) = (config.user, config.password) {
        opts = opts.user_and_password(user, password);
    }

    let js_client = async_nats::connect_with_options(&config.url, opts)
        .await
        .map_err(|e| error::Error::Connection(e.to_string()))?;

    Ok(jetstream::new(js_client))
}

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
    async fn create_reader(
        &self,
        stream: Stream,
        isb_config: Option<&ISBConfig>,
    ) -> Result<ISBReaderRef> {
        Ok(Arc::new(
            JetStreamReader::new(stream, self.context.clone(), isb_config.cloned()).await?,
        ))
    }

    async fn create_writer(
        &self,
        stream: Stream,
        writer_config: BufferWriterConfig,
        isb_config: Option<&ISBConfig>,
        cln_token: CancellationToken,
    ) -> Result<ISBWriterRef> {
        let compression_type = isb_config.map(|c| c.compression.compress_type);
        Ok(Arc::new(
            JetStreamWriter::new(
                stream,
                self.context.clone(),
                writer_config,
                compression_type,
                cln_token,
            )
            .await?,
        ))
    }

    async fn create_kv_store(&self, bucket: String) -> Result<Arc<dyn KVStore>> {
        let store =
            self.context.get_key_value(&bucket).await.map_err(|e| {
                Error::Connection(format!("Failed to get KV bucket '{bucket}': {e}"))
            })?;
        let name: &'static str = Box::leak(bucket.into_boxed_str());
        Ok(Arc::new(JetstreamKVStore::new(store, name)))
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
