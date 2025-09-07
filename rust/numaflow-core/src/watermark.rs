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

/// Watermark handle, enum to hold both edge and source watermark handles
/// This is used to fetch and publish watermarks
#[derive(Clone)]
pub(crate) enum WatermarkHandle {
    #[allow(clippy::upper_case_acronyms)]
    ISB(ISBWatermarkHandle),
    Source(SourceWatermarkHandle),
}
