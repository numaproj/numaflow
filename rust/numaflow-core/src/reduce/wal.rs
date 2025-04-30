//! Write Ahead Log for both Aligned and Unaligned Reduce Operation. The WAL is to persist the data
//! we read from the ISB and store it until the processing is complete.

/// A WAL Segment.
pub(crate) mod segment;

/// All the errors WAL could face.
pub(crate) mod error;
