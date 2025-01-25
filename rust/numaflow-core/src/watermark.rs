//! [Watermark] _is a monotonically increasing timestamp of the oldest work/event not yet completed_
//!
//! [Watermark]: https://numaflow.numaproj.io/core-concepts/watermarks/

use crate::watermark::isb::ISBWatermarkHandle;
use crate::watermark::source::SourceWatermarkHandle;

/// Responsible for fetching and publishing watermarks for edge
pub(crate) mod isb;

/// Manages the processors for watermark.
mod processor;

/// some common utilities for watermark.
mod shared;

/// Responsible for fetching and publishing watermarks for source.
pub(crate) mod source;

/// Stores WMB related data.
mod wmb;

/// Watermark handle, enum to hold both edge and source watermark handles
/// This is used to fetch and publish watermarks
#[derive(Clone)]
pub(crate) enum WatermarkHandle {
    Edge(ISBWatermarkHandle),
    Source(SourceWatermarkHandle),
}
