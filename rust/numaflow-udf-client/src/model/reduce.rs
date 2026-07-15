use bytes::Bytes;
use chrono::{DateTime, Utc};

use super::UdfMetadata;

/// A half-open aligned window `[start_time, end_time)`.
///
/// Windows are ordered by end time when sorted (for example in a `BTreeSet`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window {
    /// Start time of the window.
    pub start_time: DateTime<Utc>,
    /// End time of the window.
    pub end_time: DateTime<Utc>,
}

impl Window {
    /// Creates a new Window.
    pub fn new(start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> Self {
        Self {
            start_time,
            end_time,
        }
    }
}

impl Ord for Window {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.end_time.cmp(&other.end_time)
    }
}

impl PartialOrd for Window {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for Window {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{} → {})",
            self.start_time.to_rfc3339(),
            self.end_time.to_rfc3339()
        )
    }
}

/// One decoded ReduceFn result for an aligned window stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlignedReduceResult {
    pub window: Window,
    pub keys: Vec<String>,
    pub value: Bytes,
    pub tags: Vec<String>,
    pub metadata: Option<UdfMetadata>,
}
