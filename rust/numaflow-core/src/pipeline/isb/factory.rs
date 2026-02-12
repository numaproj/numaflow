//! ISB Factory trait for creating ISB readers and writers.
//!
//! This module provides an abstraction layer for creating ISB components,
//! allowing different ISB implementations (JetStream, SimpleBuffer, etc.)
//! to be used interchangeably.

use std::collections::HashMap;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::Result;
use crate::config::pipeline::ToVertexConfig;
use crate::config::pipeline::isb::{BufferWriterConfig, ISBConfig, Stream};
use crate::pipeline::isb::{ISBReader, ISBWriter};

/// Trait for creating ISB readers and writers.
///
/// This factory pattern allows the pipeline to be agnostic of the underlying
/// ISB implementation. Different implementations (JetStream, SimpleBuffer, etc.)
/// can provide their own factory that creates the appropriate reader/writer types.
///
/// The factory is parameterized by the reader and writer types it creates,
/// which must implement the `ISBReader` and `ISBWriter` traits respectively.
#[async_trait]
pub(crate) trait ISBFactory: Send + Sync {
    /// The reader type created by this factory
    type Reader: ISBReader + 'static;
    /// The writer type created by this factory
    type Writer: ISBWriter + 'static;

    /// Creates a reader for the given stream.
    ///
    /// # Arguments
    /// * `stream` - The stream configuration to read from
    /// * `isb_config` - Optional ISB-specific configuration (e.g., compression settings)
    async fn create_reader(
        &self,
        stream: Stream,
        isb_config: Option<&ISBConfig>,
    ) -> Result<Self::Reader>;

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
    ) -> Result<Self::Writer>;

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
    ) -> Result<HashMap<&'static str, Self::Writer>> {
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
}
