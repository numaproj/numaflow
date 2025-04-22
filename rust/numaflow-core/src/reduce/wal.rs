/// A WAL Segment.
pub(crate) mod segment;

/// A compactor to truncate processed WAL segments. It can be both Data WAL and GC WAL.
pub(crate) mod compactor;

/// All the errors WAL could face.
pub(crate) mod error;
