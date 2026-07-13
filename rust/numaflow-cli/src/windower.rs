//! Session Reduce window helpers pending unaligned migration to shared managers.
//! Aligned fixed/sliding assignment lives in `numaflow-udf-client`.

use chrono::{DateTime, Utc};

use crate::message::Message;

/// A half-open window `[start_ms, end_ms)` in epoch milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WindowMs {
    pub start: i64,
    pub end: i64,
}

impl WindowMs {
    #[allow(dead_code)]
    pub fn contains(&self, t: i64) -> bool {
        self.start <= t && t < self.end
    }
    /// Two windows overlap iff `a.start < b.end && b.start < a.end`.
    pub fn overlaps(&self, other: &WindowMs) -> bool {
        self.start < other.end && other.start < self.end
    }
    pub fn start_dt(&self) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp_millis(self.start)
    }
    pub fn end_dt(&self) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp_millis(self.end)
    }
}

impl std::fmt::Display for WindowMs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fmt_dt =
            |t: Option<DateTime<Utc>>| t.map(|d| d.to_rfc3339()).unwrap_or_else(|| "?".to_string());
        write!(
            f,
            "[{} → {})",
            fmt_dt(self.start_dt()),
            fmt_dt(self.end_dt())
        )
    }
}

pub fn event_ms(m: &Message) -> i64 {
    m.event_time.timestamp_millis()
}

/// The initial SESSION window for an event: `[event_ms, event_ms + gap)` (no epoch alignment).
pub fn session_window(event_ms: i64, gap_ms: i64) -> WindowMs {
    WindowMs {
        start: event_ms,
        end: event_ms + gap_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_no_alignment() {
        assert_eq!(
            session_window(12_345, 10_000),
            WindowMs {
                start: 12_345,
                end: 22_345
            }
        );
    }

    #[test]
    fn overlap_end_exclusive() {
        let a = WindowMs { start: 5, end: 10 };
        let b = WindowMs { start: 10, end: 15 };
        assert!(!a.overlaps(&b)); // touching, not overlapping
        let c = WindowMs { start: 9, end: 15 };
        assert!(a.overlaps(&c));
    }
}
