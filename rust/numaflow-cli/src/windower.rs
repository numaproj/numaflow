//! Window-assignment math, ported from `numaflow-core/src/reduce/reducer`. All arithmetic is
//! in epoch **milliseconds** (`i64`); intervals are half-open `[start, end)`. Fixed and sliding
//! windows are epoch-aligned (truncated via Euclidean floor); session and accumulator windows
//! start at the raw event time.

use chrono::{DateTime, Utc};

use crate::message::Message;

/// A half-open window `[start_ms, end_ms)` in epoch milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WindowMs {
    pub start: i64,
    pub end: i64,
}

impl WindowMs {
    #[cfg_attr(not(test), allow(dead_code))]
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

/// Epoch-aligned floor: largest multiple of `duration_ms` not exceeding `ts_ms`. Euclidean so
/// it is correct for negative timestamps too.
fn truncate(ts_ms: i64, duration_ms: i64) -> i64 {
    ts_ms.div_euclid(duration_ms) * duration_ms
}

pub fn event_ms(m: &Message) -> i64 {
    m.event_time.timestamp_millis()
}

/// FIXED: the single window `[floor(event/length)*length, +length)`.
pub fn fixed_window(event_ms: i64, length_ms: i64) -> WindowMs {
    let start = truncate(event_ms, length_ms);
    WindowMs {
        start,
        end: start + length_ms,
    }
}

/// SLIDING: every window `[start, start+length)` that contains `event_ms`, where starts are
/// epoch-aligned multiples of `slide`. Returned in descending-start order (matching numa).
pub fn sliding_windows(event_ms: i64, length_ms: i64, slide_ms: i64) -> Vec<WindowMs> {
    let mut out = Vec::new();
    let mut start = truncate(event_ms, slide_ms);
    let mut end = start + length_ms;
    while start <= event_ms && event_ms < end {
        out.push(WindowMs { start, end });
        start -= slide_ms;
        end -= slide_ms;
    }
    out
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
    fn truncate_matches_numa() {
        // From numaflow-core aligned/windower.rs tests.
        assert_eq!(truncate(100, 30), 90);
        assert_eq!(truncate(59, 30), 30);
        assert_eq!(truncate(60, 30), 60);
        assert_eq!(truncate(89, 30), 60);
        assert_eq!(truncate(810, 70), 770);
    }

    #[test]
    fn fixed_basic() {
        assert_eq!(
            fixed_window(100, 30),
            WindowMs {
                start: 90,
                end: 120
            }
        );
        // event at 60000ms, 60s length → [60000, 120000)
        assert_eq!(
            fixed_window(60_000, 60_000),
            WindowMs {
                start: 60_000,
                end: 120_000
            }
        );
    }

    #[test]
    fn fixed_boundary_is_half_open() {
        // event exactly on the boundary belongs to the window that starts there.
        assert_eq!(fixed_window(60_000, 60_000).start, 60_000);
        // one ms before → previous window
        assert_eq!(fixed_window(59_999, 60_000).start, 0);
    }

    #[test]
    fn sliding_six_windows() {
        // event 105000ms, length 60s, slide 10s → 6 windows, descending start.
        let ws = sliding_windows(105_000, 60_000, 10_000);
        let starts: Vec<i64> = ws.iter().map(|w| w.start).collect();
        assert_eq!(
            starts,
            vec![100_000, 90_000, 80_000, 70_000, 60_000, 50_000]
        );
        for w in &ws {
            assert_eq!(w.end - w.start, 60_000);
            assert!(w.contains(105_000));
        }
    }

    #[test]
    fn sliding_length_60_slide_40() {
        // event 610s, length 60s, slide 40s → [600,660),[560,620)
        let ws = sliding_windows(610_000, 60_000, 40_000);
        let starts: Vec<i64> = ws.iter().map(|w| w.start).collect();
        assert_eq!(starts, vec![600_000, 560_000]);
    }

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
