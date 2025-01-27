//! [Watermark] _is a monotonically increasing timestamp of the oldest work/event not yet completed_
//! 
//! 
//! [Watermark]: https://numaflow.numaproj.io/core-concepts/watermarks/

use crate::watermark::isb::ISBWatermarkHandle;
use crate::watermark::source::SourceWatermarkHandle;

/// Responsible for fetch/publish cycle of watermark per offset for each stream in the ISB.
pub(crate) mod isb;

/// Manages the processors for watermark.
mod processor;

/// Responsible for fetching and publishing watermarks for the Source. A Source could have multiple
/// partitions, similar partitions in the ISB. The main difference between Source and [isb] is that
/// the watermark starts at source, so we will have to do a publish followed by a fetch and publish.
pub(crate) mod source;

/// Stores WMB related data.
mod wmb;

/// Watermark handle, enum to hold both edge and source watermark handles
/// This is used to fetch and publish watermarks
#[derive(Clone)]
pub(crate) enum WatermarkHandle {
    ISB(ISBWatermarkHandle),
    Source(SourceWatermarkHandle),
}
