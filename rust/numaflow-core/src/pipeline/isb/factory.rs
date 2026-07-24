//! ISB Factory trait for creating ISB readers and writers.
//!
//! This module provides an abstraction layer for creating ISB components,
//! allowing different ISB implementations (JetStream, SimpleBuffer, etc.)
//! to be used interchangeably.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::Result;
use crate::config::pipeline::ToVertexConfig;
use crate::config::pipeline::isb::{BufferWriterConfig, ISBClientConfig, ISBConfig, Stream};
use crate::pipeline::isb::dyn_adapter::{ISBReaderRef, ISBWriterRef};
use crate::pipeline::isb::inmemory::InMemoryFactory;
use crate::pipeline::isb::jetstream::JetStreamFactory;
use crate::pipeline::isb::jetstream::factory::create_js_context;
use numaflow_shared::kv::KVStore;

/// Trait for creating ISB readers and writers.
///
/// This factory pattern allows the pipeline to be agnostic of the underlying
/// ISB implementation. Different implementations (JetStream, SimpleBuffer, etc.)
/// can provide their own factory that creates type-erased reader/writer handles.
#[async_trait]
pub(crate) trait ISBFactory: Send + Sync {
    /// Creates a reader for the given stream.
    ///
    /// # Arguments
    /// * `stream` - The stream configuration to read from
    /// * `isb_config` - Optional ISB-specific configuration (e.g., compression settings)
    async fn create_reader(
        &self,
        stream: Stream,
        isb_config: Option<&ISBConfig>,
    ) -> Result<ISBReaderRef>;

    /// Creates a writer for the given stream.
    ///
    /// # Arguments
    /// * `stream` - The stream configuration to write to
    /// * `writer_config` - Writer configuration (buffer limits, strategies, etc.)
    /// * `isb_config` - Optional ISB-specific configuration (e.g., compression settings)
    /// * `cln_token` - Cancellation token for graceful shutdown
    async fn create_writer(
        &self,
        stream: Stream,
        writer_config: BufferWriterConfig,
        isb_config: Option<&ISBConfig>,
        cln_token: CancellationToken,
    ) -> Result<ISBWriterRef>;

    /// Creates writers for all streams in the given vertex configurations.
    ///
    /// This is a convenience method that creates writers for all streams
    /// across all vertex configurations.
    ///
    /// # Arguments
    /// * `to_vertex_config` - List of vertex configurations containing stream info
    /// * `isb_config` - Optional ISB-specific configuration
    /// * `cln_token` - Cancellation token for graceful shutdown
    async fn create_writers(
        &self,
        to_vertex_config: &[ToVertexConfig],
        isb_config: Option<&ISBConfig>,
        cln_token: CancellationToken,
    ) -> Result<HashMap<&'static str, ISBWriterRef>> {
        let mut writers = HashMap::new();
        for vertex_config in to_vertex_config {
            for stream in &vertex_config.writer_config.streams {
                let writer = self
                    .create_writer(
                        stream.clone(),
                        vertex_config.writer_config.clone(),
                        isb_config,
                        cln_token.clone(),
                    )
                    .await?;
                writers.insert(stream.name, writer);
            }
        }
        Ok(writers)
    }

    /// Creates a key-value store for the given bucket.
    ///
    /// Used by watermark (offset timeline), serving callbacks/response store,
    /// and other features that need backend-neutral KV access.
    async fn create_kv_store(&self, bucket: String) -> Result<Arc<dyn KVStore>>;
}

/// Creates an ISB factory for the configured backend.
pub(crate) async fn create_isb_factory(
    config: &ISBClientConfig,
    _cln_token: CancellationToken,
) -> Result<Arc<dyn ISBFactory>> {
    match config {
        ISBClientConfig::Jetstream(cfg) => {
            let js_context = create_js_context(cfg.clone()).await?;
            Ok(Arc::new(JetStreamFactory::new(js_context)))
        }
        ISBClientConfig::InMemory => {
            tracing::warn!("in-memory ISB backend selected — for local testing only");
            Ok(Arc::new(InMemoryFactory::new()))
        }
    }
}
