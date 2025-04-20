use chrono::{DateTime, Utc};

/// Write Head Log for storing reduce data until the reduction is complete.
pub(crate) mod wal;

/// Shard is how Numaflow builds time based boundary for Reduce operation. For Aligned Windows,
/// the start and end will be based on the Window boundary, while for Unaligned it will be -oo to
/// +oo.
pub(crate) struct Shard {
    /// The start time of the boundary.
    pub(crate) start_time: DateTime<Utc>,
    /// The end time of the boundary.
    pub(crate) end_time: DateTime<Utc>,
}

impl Shard {
    pub(crate) fn new(start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> Self {
        Self {
            start_time,
            end_time,
        }
    }
}
