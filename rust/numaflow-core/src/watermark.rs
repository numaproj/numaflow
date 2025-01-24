use crate::watermark::edge::EdgeWatermarkHandle;
use crate::watermark::source::SourceWatermarkHandle;

/// Responsible for fetching and publishing watermarks for edge
pub(crate) mod edge;

/// Manages the processors for watermark
mod processor;

/// some common utilities for watermark
mod shared;

/// Responsible for fetching and publishing watermarks for source
pub(crate) mod source;

/// Stores WMB related data
mod wmb;

/// Watermark handle, enum to hold both edge and source watermark handles
/// This is used to fetch and publish watermarks
#[derive(Clone)]
pub(crate) enum WatermarkHandle {
    Edge(EdgeWatermarkHandle),
    Source(SourceWatermarkHandle),
}
