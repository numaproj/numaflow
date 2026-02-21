//! The [Watermark] is propagated across all Vertices from the [crate::source] all the way to
//! the [crate::sink]. The watermark is generated at the [crate::source] based
//! on the event-time, and if it is missing, it will be the ingestion time (the time the data was
//! inserted into the pipeline). Once the watermark is created, it just flows through the subsequent
//! vertices of the pipeline.
//! Every vertex (Source, UDFs, Sink) should implement two interfaces
//!   - Publish Watermark - this will be per unit per-vertex information
//!   - Fetch Watermark - this will be the merged WM of the vertex across all units
//!
//! ## Publish Watermark
//! Publish publishes the watermark of the [processor] of the vertex, always the smallest watermark
//! is published (WM < the oldest message still being processed). We cannot measure the WM of the vertex
//! by looking at the WM of a [processor] of the vertex but by looking into all the WMs published
//! by all the [processor] in that vertex.
//!
//! ## Fetch Watermark
//! Fetch at vertex Vn will fetch the watermarks of all the [processor] of the vertex Vn-1. Fetch
//! happens for every message (looked up using offset) read from the stream (source or isb). We cannot
//! measure the WM of the vertex by looking at the WM of a single [processor] of the vertex but by looking
//! into all the WMs published by all the [processor] in that vertex. The vertex Vn to determine the
//! watermark of an offset will have to query the watermarks of each [processor] of the vertex Vn-1. The
//! lowest watermark among all the [processor] will be set as the watermark of that offset.
//!
//! [Watermark]: https://numaflow.numaproj.io/core-concepts/watermarks/

use async_trait::async_trait;
use numaflow_shared::kv::{KVResult, KVStorer};
use std::sync::Arc;

use crate::watermark::isb::ISBWatermarkHandle;
use crate::watermark::source::SourceWatermarkHandle;

/// Responsible for fetch/publish cycle of watermark per offset for each stream in the ISB.
pub(crate) mod isb;

/// Manages the processors for watermark. Watermark is specific to each processor.
mod processor;

/// Responsible for fetching and publishing watermarks for the Source. A Source could have multiple
/// partitions, similar partitions in the ISB. The main difference between Source and [isb] is that
/// the watermark starts at source, so we will have to do a publishing followed by a fetch and publish.
pub(crate) mod source;

/// Publishes idle watermarks for the source and ISB when they are idling.
mod idle;

/// Stores WMB related data.
pub(crate) mod wmb;

/// Watermark store implementations for pluggable storage backends.
pub(crate) mod store;

/// WatermarkStore defines a pair of heartbeat KV store and offset timeline KV store.
///
/// In Numaflow's watermark propagation, we use two KV buckets:
/// - **Heartbeat (HB) bucket**: Tracks processor liveness via heartbeat timestamps
/// - **Offset Timeline (OT) bucket**: Stores watermark-offset pairs for each processor
///
/// This trait abstracts the storage backend, allowing different implementations
/// (JetStream, Redis, in-memory for testing, etc.).
///
/// This trait is object-safe and can be used as `Arc<dyn WatermarkStore>` for dynamic dispatch.
#[allow(dead_code)] // FIXME: remove after integration
#[async_trait]
pub trait WatermarkStore: Send + Sync {
    /// Returns the heartbeat KV store.
    ///
    /// The heartbeat store is used to track processor liveness. Each processor
    /// publishes its heartbeat timestamp periodically, and watchers use this
    /// to detect dead processors.
    fn heartbeat_store(&self) -> Arc<dyn KVStorer>;

    /// Returns the offset timeline KV store.
    ///
    /// The offset timeline store holds watermark-offset pairs for each processor.
    /// This is used to determine the watermark for a given offset by looking up
    /// the timeline entries from all active processors.
    fn offset_timeline_store(&self) -> Arc<dyn KVStorer>;

    /// Close both stores and release resources.
    ///
    /// This is called during graceful shutdown to clean up any resources
    /// held by the stores.
    async fn close(&self) -> KVResult<()>;
}

/// Watermark handle, enum to hold both edge and source watermark handles
/// This is used to fetch and publish watermarks
#[derive(Clone)]
pub(crate) enum WatermarkHandle {
    #[allow(clippy::upper_case_acronyms)]
    ISB(ISBWatermarkHandle),
    Source(SourceWatermarkHandle),
}
