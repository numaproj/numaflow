/// Write Head Log for storing reduce data until the reduction is complete.
pub(crate) mod wal;

/// All the errors Reduce could face.
mod error;

/// Persistent Buffer Queue (PBQ) for storing reduce data until the reduction is complete. It is
/// responsible for reading from ISB and persisting the data to WAL. It is also responsible for
/// reading the data it read from ISB or WAL (during replay) to Process and Forward (PNF).
pub(crate) mod pbq;

/// Reducer for processing the reduce data and forwarding it to the next vertex.
pub(crate) mod reducer;
