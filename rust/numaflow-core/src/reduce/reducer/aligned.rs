//! Aligned windows in stream processing are time-based windowing strategies where window boundaries
//! are synchronized with a global clock (e.g., system time), ensuring consistent window intervals
//! across all data streams. This alignment simplifies aggregation and analysis by providing uniform
//! time slices for grouping events.
//! There are two main types of aligned windows:
//!   * [Fixed](windower::fixed) (tumbling)
//!   * [Sliding](windower::sliding).
//! Fixed windows divide the timeline into non-overlapping intervals of equal length,
//! such as every 10 seconds, with each event assigned to exactly one window. In contrast, sliding
//! windows also have a fixed duration but move forward by a smaller slide interval, resulting in
//! overlapping windows. This allows each event to potentially be part of multiple windows, enabling
//! more fine-grained, continuous analysis. Both types ensure deterministic and predictable windowing
//! based on time.

/// Aligned Reduce for Fixed and Sliding Windows.
pub(crate) mod reducer;
/// User-defined Reduce for Aligned and Unaligned Windows.
pub(crate) mod user_defined;
/// Windower for Aligned Windows.
pub(crate) mod windower;
