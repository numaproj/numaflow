//! JetStream ISB Factory implementation.
//!
//! This module provides a factory for creating JetStream-based ISB readers and writers.

use std::sync::Arc;

use async_nats::jetstream::Context;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::Result;
use crate::config::pipeline::isb::{BufferWriterConfig, ISBConfig, Stream};
use crate::error::Error;
use crate::metrics::MetricLabels;
use crate::pipeline::isb::ISBFactory;
use crate::pipeline::isb::dyn_adapter::{ISBReaderRef, ISBWriterRef};
use crate::pipeline::isb::jetstream::js_reader::JetStreamReader;
use crate::pipeline::isb::jetstream::js_writer::JetStreamWriter;
use numaflow_shared::kv::KVStore;
use numaflow_shared::kv::KVStoreFactory;
use numaflow_shared::kv::jetstream::JetstreamKVStoreFactory;

/// Factory for creating JetStream-based ISB readers and writers.
///
/// This factory encapsulates the JetStream context and provides methods
/// to create readers and writers for specific streams.
#[derive(Clone)]
pub struct JetStreamFactory {
    /// The shared JetStream KV store factory, which also carries the JetStream context
    kv: JetstreamKVStoreFactory,
}

impl JetStreamFactory {
    /// Creates a new JetStreamFactory with the given JetStream context.
    ///
    /// # Arguments
    /// * `context` - The JetStream context to use for creating readers and writers
    pub fn new(context: Context) -> Self {
        Self {
            kv: JetstreamKVStoreFactory::new(context),
        }
    }

    /// Returns a reference to the underlying JetStream context.
    ///
    /// This can be useful for operations that need direct access to the context,
    /// such as watermark handling.
    #[allow(dead_code)] // May be used for watermark handling or other direct context access
    pub fn context(&self) -> &Context {
        self.kv.context()
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
            JetStreamReader::new(stream, self.kv.context().clone(), isb_config.cloned()).await?,
        ))
    }

    async fn create_writer(
        &self,
        stream: Stream,
        writer_config: BufferWriterConfig,
        isb_config: Option<&ISBConfig>,
        metric_labels: Option<MetricLabels>,
        cln_token: CancellationToken,
    ) -> Result<ISBWriterRef> {
        let compression_type = isb_config.map(|c| c.compression.compress_type);
        Ok(Arc::new(
            JetStreamWriter::new(
                stream,
                self.kv.context().clone(),
                writer_config,
                compression_type,
                metric_labels,
                cln_token,
            )
            .await?,
        ))
    }

    async fn create_kv_store(&self, bucket: String) -> Result<Arc<dyn KVStore>> {
        self.kv
            .create_kv_store(bucket)
            .await
            .map_err(|e| Error::Connection(e.to_string()))
    }
}