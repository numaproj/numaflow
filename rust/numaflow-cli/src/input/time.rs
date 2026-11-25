//! Time parsing for event/watermark fields and base-time logic.
//!
//! Two grammars are accepted anywhere a time is expected:
//! - RFC3339 absolute (`2023-01-01T00:00:00Z`)
//! - relative to the base time: `+<dur>` (`+5s`, `+1m30s`) using `humantime`.

use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};

use crate::error::{CliError, CliResult};

/// Parse a time string against `base`. `+dur` → `base + dur`; otherwise RFC3339 absolute.
pub fn parse_time(input: &str, base: DateTime<Utc>) -> CliResult<DateTime<Utc>> {
    let input = input.trim();
    if let Some(rest) = input.strip_prefix('+') {
        let dur = humantime::parse_duration(rest.trim())
            .map_err(|e| CliError::Usage(format!("invalid relative time '+{rest}': {e}")))?;
        let chrono_dur = Duration::from_std(dur)
            .map_err(|e| CliError::Usage(format!("relative time out of range: {e}")))?;
        return Ok(base + chrono_dur);
    }
    DateTime::parse_from_rfc3339(input)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| CliError::Usage(format!("invalid RFC3339 time '{input}': {e}")))
}

/// Compute the base time for a run.
///
/// If `base_time` is given it is parsed as RFC3339; otherwise `now`. For reduce-family windows the
/// base is truncated *down* to a window boundary (a multiple of `align_to` since the epoch) so
/// relative event times land cleanly inside the first window.
pub fn compute_base_time(
    base_time: Option<&str>,
    align_to: Option<StdDuration>,
    now: DateTime<Utc>,
) -> CliResult<DateTime<Utc>> {
    let base = match base_time {
        Some(s) => DateTime::parse_from_rfc3339(s.trim())
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| CliError::Usage(format!("invalid --base-time '{s}': {e}")))?,
        None => now,
    };

    match align_to {
        Some(align) if !align.is_zero() => {
            let align_ms = align.as_millis() as i64;
            let base_ms = base.timestamp_millis();
            // Floor to a multiple of the alignment window.
            let floored = base_ms - base_ms.rem_euclid(align_ms);
            Ok(DateTime::from_timestamp_millis(floored).unwrap_or(base))
        }
        _ => Ok(base),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn relative_time_adds_to_base() {
        let base = Utc.timestamp_opt(1000, 0).unwrap();
        let t = parse_time("+5s", base).unwrap();
        assert_eq!(t.timestamp(), 1005);
    }

    #[test]
    fn absolute_time_ignores_base() {
        let base = Utc.timestamp_opt(1000, 0).unwrap();
        let t = parse_time("1970-01-01T00:00:10Z", base).unwrap();
        assert_eq!(t.timestamp(), 10);
    }

    #[test]
    fn base_time_floors_to_window() {
        // base = 1m35s; align 1m → floor to 1m.
        let base = Utc.timestamp_opt(95, 0).unwrap();
        let aligned = compute_base_time(None, Some(StdDuration::from_secs(60)), base).unwrap();
        assert_eq!(aligned.timestamp(), 60);
    }
}
