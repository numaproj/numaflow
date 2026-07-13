//! Implementation of [Sliding Window] requires a bit more bookkeeping than [Fixed Window],
//! because a single event can belong to multiple windows, but we only have a single [WAL] (we do not
//! have per window WALs). An example of sliding window with length 5s and slide 1s, and an event
//! time at 100 will belong to 5 windows `[95, 100)`, `[96, 101)`, `[97, 102)`, `[98, 103)`, `[99, 104)`.
//! This means that the element has to be to sent to multiple windows. To understand the implementation,
//! we need to look in how we handle window creation in different scenarios (startup, normal operation,
//! restart) but the code is oblivious to the scenario.
//!
//! ## Startup
//! During pipeline startup is the very first time a pipeline is started. It has not materialized any
//! windows yet and just started reading data from the ISB. When we read an element from the ISB with event-time
//! of `t1`, we need to create all windows that can contain `t1`. For example, if the window length is 30s
//! and slide is 10s, and the event-time is 100, we need to create windows `[100, 130)`, `[90, 120)`,
//! and `[80, 110)`.
//!
//! ## Normal Operation
//! During normal operation, when we read an element from the ISB, we need to create all windows that
//! can contain the event-time of the message. For example, if the window length is 30s and slide is
//! 10s, and the event-time is 60, we need to create windows `[60, 90)` but we can assume that
//! the windows `[50, 80)`, and `[40, 70)` has already been created when elements with event-time
//! 90 and 80 were received.
//! Out-of-order can cause the assumption to fail, so we make sure that the previous windows are created
//! if they do not exist. This can happen when we have watermark withholding, and we receive out of order
//! messages.
//!
//! ## Restart
//! When a pipeline is restarted, we will replay from the WAL to restore the state. Restart is a bit
//! tricky because we need to make sure that we do not create duplicate windows. To do that we keep
//! track of the min start time of the active windows. Any window with start time less than this value
//! should not be created again.
//! Let me explain this with an example. Let's say we have a window length of 30s and slide of 10s.
//! and we have already processed elements with event-time 100 and 110 and the watermark has advanced to 110.
//! So we have created windows `[100, 130)`, `[90, 120)`, `[80, 110)`, `[110, 140)`, `[100, 130)`, now
//! let's assume that the window `[80, 110)` is closed and deleted (WM is 110). Now when we restart, we will
//! replay the elements with event-time 100, etc. The reason for replaying element with event-time 100
//! even if the watermark has progressed to 110, is that there are widows with start time less that the
//! watermark. E.g., window`[100, 130)` is still open and data has to be sent to this window. This
//! is the reason why we need the min start time of the active windows.
//! Now to track the state of active windows across restarted, we write this information to a file on
//! shutdown and read it on startup. In case of SIGKILL, the state file will not be created, so
//! we will recreate the closed windows and there will be duplicates causing at-least-once
//! behavior and not a data-loss scenario.
//!
//! [Sliding Window]: (https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/windowing/sliding/)
//! [Fixed Window]: super::fixed
//! [WAL]: crate::reduce::wal

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock, atomic::AtomicI64};
use std::time::Duration;

use super::{AlignedWindowAction, truncate_to_duration};
use crate::model::Window;
use chrono::{DateTime, TimeZone, Utc};

/// Snapshot of sliding window manager state for restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlidingWindowSnapshot {
    pub windows: Vec<Window>,
    pub max_deleted_window_end_time: i64,
}

/// Sliding window manager is responsible for managing the sliding windows.
#[derive(Debug, Clone)]
pub struct SlidingWindowManager {
    /// Duration of each window.
    window_length: Duration,
    /// Slide duration.
    slide: Duration,
    /// Active windows sorted by end time.
    /// NOTE: During replay we will assume that these windows are opened when we get the first message.
    /// Since we do not differentiate between the first message and subsequent messages, we does an
    /// "append" and in the "append" we "open" the stream if stream doesn't exist.
    /// TODO: perhaps we can differentiate between replayed window vs new window during normal operation.
    pub(crate) active_windows: Arc<RwLock<BTreeSet<Window>>>,
    /// Closed windows sorted by end time. We need to keep track of closed windows so that we can
    /// find the oldest window computed and forwarded. The watermark is progressed based on the latest
    /// closed window end time. The oldest window in the active_window will be greater than the latest
    /// closed window.
    pub(crate) closed_windows: Arc<RwLock<BTreeSet<Window>>>,
    /// max end time of the deleted windows, used to avoid creating
    /// windows twice during replay.
    max_deleted_window_end_time: Arc<AtomicI64>,
}

impl SlidingWindowManager {
    pub fn new(window_length: Duration, slide: Duration) -> Self {
        Self {
            window_length,
            slide,
            active_windows: Arc::new(RwLock::new(BTreeSet::new())),
            closed_windows: Arc::new(RwLock::new(BTreeSet::new())),
            max_deleted_window_end_time: Arc::new(AtomicI64::new(-1)),
        }
    }

    /// Restores manager state from a snapshot (for example after restart).
    pub fn from_snapshot(
        window_length: Duration,
        slide: Duration,
        snapshot: SlidingWindowSnapshot,
    ) -> Self {
        let manager = Self::new(window_length, slide);
        manager
            .active_windows
            .write()
            .expect("Poisoned lock for active_windows")
            .extend(snapshot.windows);
        manager
            .max_deleted_window_end_time
            .store(snapshot.max_deleted_window_end_time, Ordering::Relaxed);
        manager
    }

    /// Returns a snapshot suitable for persisting across restarts.
    pub fn snapshot_for_restart(&self) -> SlidingWindowSnapshot {
        let windows: Vec<Window> = {
            let active_windows = self
                .active_windows
                .read()
                .expect("Poisoned lock for active_windows");
            let closed_windows = self
                .closed_windows
                .read()
                .expect("Poisoned lock for closed_windows");

            closed_windows
                .iter()
                .chain(active_windows.iter())
                .cloned()
                .collect()
        };

        SlidingWindowSnapshot {
            windows,
            max_deleted_window_end_time: self.max_deleted_window_end_time.load(Ordering::Relaxed),
        }
    }

    /// Creates windows for the given event time (descending start-time order).
    fn create_windows(&self, event_time: DateTime<Utc>) -> Vec<Window> {
        let mut windows = Vec::new();
        let window_length_millis = self.window_length.as_millis() as i64;
        let slide_millis = self.slide.as_millis() as i64;
        let event_time_millis = event_time.timestamp_millis();

        // Calculate the start time of the largest window that contains the event
        // use the highest integer multiple of slide length which is less than the eventTime
        // as the start time for the window. For example, if the eventTime is 810 and slide
        // length is 70, use 770 as the startTime of the window. In that way, we can guarantee
        // consistency while assigning the messages to the windows.
        let start_time_millis = truncate_to_duration(event_time_millis, slide_millis);
        let mut current_start = start_time_millis;
        let mut current_end = current_start + window_length_millis;

        // Create all windows that contain this event for example if the event time of the message
        // is 25 with length 5s and slide 1s we will have [21, 26), [22, 27), [23, 28), [24, 29), [25, 30)
        // windows.

        // startTime and endTime will be the largest timestamp window for the given eventTime,
        // using that we can create other windows by subtracting the slide length
        while current_start <= event_time_millis && event_time_millis < current_end {
            let start_time = Utc.timestamp_millis_opt(current_start).unwrap();
            let end_time = Utc.timestamp_millis_opt(current_end).unwrap();

            windows.push(Window::new(start_time, end_time));

            // Move to the previous window
            current_start -= slide_millis;
            current_end -= slide_millis;
        }

        windows
    }

    /// Closes any windows that can be closed because the Watermark has advanced beyond the window
    pub fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<AlignedWindowAction> {
        let windows_to_close: Vec<Window> = {
            let mut active_windows = self
                .active_windows
                .write()
                .expect("Poisoned lock for active_windows");
            active_windows
                .extract_if(.., |window| window.end_time <= watermark)
                .collect()
        };

        {
            let mut closed_windows = self
                .closed_windows
                .write()
                .expect("Poisoned lock for closed_windows");
            for window in &windows_to_close {
                closed_windows.insert(window.clone());
            }
        }

        windows_to_close
            .into_iter()
            .map(|window| AlignedWindowAction::Close { window })
            .collect()
    }

    /// Closes all active windows (same end-time order as [`Self::close_windows`] with a terminal watermark).
    pub fn close_all(&self) -> Vec<AlignedWindowAction> {
        let watermark = self
            .active_windows
            .read()
            .expect("Poisoned lock for active_windows")
            .iter()
            .last()
            .map(|window| window.end_time)
            .unwrap_or_else(|| Utc.timestamp_millis_opt(0).unwrap());
        self.close_windows(watermark)
    }

    /// Deletes a window after it is closed and GC is done.
    pub fn gc_window(&self, window: Window) {
        // Update max_deleted_window_end_time if this window's end time is greater
        self.max_deleted_window_end_time
            .fetch_max(window.end_time.timestamp_millis(), Ordering::Relaxed);

        // Remove the window from closed_windows
        self.closed_windows
            .write()
            .expect("Poisoned lock for closed_windows")
            .remove(&window);
    }

    /// Returns the oldest window yet to be completed. This will be the lowest Watermark in the Vertex.
    pub fn oldest_window(&self) -> Option<Window> {
        // get the oldest window from closed_windows, if closed_windows is empty, get the oldest
        // from active_windows
        // NOTE: closed windows will always have a lower end time than active_windows
        {
            let closed_windows = self
                .closed_windows
                .read()
                .expect("Poisoned lock for closed_windows");
            if let Some(window) = closed_windows.iter().next() {
                return Some(window.clone());
            }
        }

        self.active_windows
            .read()
            .expect("Poisoned lock for active_windows")
            .iter()
            .next()
            .cloned()
    }

    /// Returns the number of currently active windows.
    pub fn active_window_count(&self) -> usize {
        self.active_windows
            .read()
            .expect("Poisoned lock for active_windows")
            .len()
    }

    /// Returns the number of closed windows awaiting GC.
    pub fn closed_window_count(&self) -> usize {
        self.closed_windows
            .read()
            .expect("Poisoned lock for closed_windows")
            .len()
    }

    /// Assigns windows to an event time. It first figures all the windows that the event belongs to and
    /// then checks if the window already exists. If it does, it appends to the window,
    /// else it creates a new window.
    pub fn assign_windows(&self, event_time: DateTime<Utc>) -> Vec<AlignedWindowAction> {
        let windows = self.create_windows(event_time);
        let mut result = Vec::new();

        // Check if windows already exist and create appropriate operations
        let mut active_windows = self.active_windows.write().unwrap();

        for window in &windows {
            if active_windows.contains(window) {
                result.push(AlignedWindowAction::Append {
                    window: window.clone(),
                });
            } else if window.end_time.timestamp_millis()
                > self.max_deleted_window_end_time.load(Ordering::Relaxed)
            {
                // we cannot use active_window because that could be misaligned due to out-of-order
                // messages.
                // only create the window if the end time is greater than the max deleted window end
                // time, during replay we might get the same message again and again, so we need to
                // avoid creating the materialized window again.
                active_windows.insert(window.clone());
                result.push(AlignedWindowAction::Open {
                    window: window.clone(),
                });
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assign_windows_length_divisible_by_slide() {
        // Create a sliding windower with 60s window length and 20s slide
        let windower = SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(20));

        // Base time: 60000ms (60s)
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

        let event_time = Utc
            .timestamp_millis_opt(base_time.timestamp_millis() + 10000)
            .unwrap();

        // Assign windows
        let window_actions = windower.assign_windows(event_time);

        // Verify results - should be assigned to exactly 3 windows
        assert_eq!(window_actions.len(), 3);

        // Check first window: [60000, 120000)
        match window_actions
            .first()
            .expect("Expected at least one window action")
        {
            AlignedWindowAction::Open { window } => {
                assert_eq!(window.start_time.timestamp_millis(), 60000);
                assert_eq!(window.end_time.timestamp_millis(), 120000);
            }
            _ => panic!("Expected Open operation"),
        }

        let event_time2 = Utc
            .timestamp_millis_opt(base_time.timestamp_millis() + 1000)
            .unwrap();

        let window_actions2 = windower.assign_windows(event_time2);

        // Verify results - should be assigned to exactly 3 windows
        assert_eq!(window_actions2.len(), 3);

        // All operations should be Append
        for action in &window_actions2 {
            match action {
                AlignedWindowAction::Append { .. } => {}
                _ => panic!("Expected Append operation"),
            }
        }
    }

    #[test]
    fn test_assign_windows_length_not_divisible_by_slide() {
        // Create a sliding windower with 60s window length and 40s slide
        let windower = SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(40));

        // Base time: 600s
        let base_time = Utc.timestamp_opt(600, 0).unwrap();

        let event_time = Utc.timestamp_opt(base_time.timestamp() + 10, 0).unwrap();

        // Assign windows
        let window_actions = windower.assign_windows(event_time);

        // Verify results - should be assigned to exactly 2 windows
        assert_eq!(window_actions.len(), 2);

        match window_actions
            .first()
            .expect("Expected at least one window action")
        {
            AlignedWindowAction::Open { window } => {
                assert_eq!(window.start_time.timestamp(), 600);
                assert_eq!(window.end_time.timestamp(), 660);
            }
            _ => panic!("Expected Open operation"),
        }

        // Check second window: [560, 620)
        match window_actions
            .get(1)
            .expect("Expected at least two window actions")
        {
            AlignedWindowAction::Open { window } => {
                assert_eq!(window.start_time.timestamp(), 560);
                assert_eq!(window.end_time.timestamp(), 620);
            }
            _ => panic!("Expected Open operation"),
        }

        // Create and insert the second window manually to test append behavior
        let window2 = Window::new(
            Utc.timestamp_opt(560, 0).unwrap(),
            Utc.timestamp_opt(620, 0).unwrap(),
        );
        {
            let mut active_windows = windower.active_windows.write().unwrap();
            active_windows.insert(window2.clone());
        }

        let event_time2 = Utc.timestamp_opt(base_time.timestamp() + 1, 0).unwrap();

        let window_actions2 = windower.assign_windows(event_time2);

        // Should also be assigned to 2 windows
        assert_eq!(window_actions2.len(), 2);

        // All operations should be Append
        for action in &window_actions2 {
            match action {
                AlignedWindowAction::Append { .. } => {}
                _ => panic!("Expected Append operation"),
            }
        }
    }

    #[test]
    fn test_close_windows() {
        // Base time: 60000ms (60s)
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

        // Create a sliding windower with 60s window length and 10s slide
        let windower = SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(10));

        // Create windows
        let window1 = Window::new(base_time, base_time + chrono::Duration::seconds(60));

        let window2 = Window::new(
            base_time - chrono::Duration::seconds(10),
            base_time + chrono::Duration::seconds(50),
        );

        let window3 = Window::new(
            base_time - chrono::Duration::seconds(20),
            base_time + chrono::Duration::seconds(40),
        );

        // Insert windows manually
        {
            let mut active_windows = windower.active_windows.write().unwrap();
            active_windows.insert(window1.clone());
            active_windows.insert(window2.clone());
            active_windows.insert(window3.clone());
        }

        // Close windows with watermark at base_time + 120s
        let closed = windower.close_windows(base_time + chrono::Duration::seconds(120));

        // Should close all 3 windows
        assert_eq!(closed.len(), 3);

        // Check that all windows are closed in order of end time
        match closed.first().expect("Expected at least one closed action") {
            AlignedWindowAction::Close { window } => {
                assert_eq!(window.start_time, base_time - chrono::Duration::seconds(20));
                assert_eq!(window.end_time, base_time + chrono::Duration::seconds(40));
            }
            _ => panic!("Expected Close operation"),
        }

        match closed.get(1).expect("Expected at least two closed actions") {
            AlignedWindowAction::Close { window } => {
                assert_eq!(window.start_time, base_time - chrono::Duration::seconds(10));
                assert_eq!(window.end_time, base_time + chrono::Duration::seconds(50));
            }
            _ => panic!("Expected Close operation"),
        }

        match closed
            .get(2)
            .expect("Expected at least three closed actions")
        {
            AlignedWindowAction::Close { window } => {
                assert_eq!(window.start_time, base_time);
                assert_eq!(window.end_time, base_time + chrono::Duration::seconds(60));
            }
            _ => panic!("Expected Close operation"),
        }

        // Verify all windows are closed
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 0);
        }
    }

    #[test]
    fn test_delete_window() {
        // Base time: 60000ms (60s)
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

        // Create a sliding windower with 60s window length and 10s slide
        let windower = SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(10));

        // Create windows
        let window1 = Window::new(base_time, base_time + chrono::Duration::seconds(60));

        let window2 = Window::new(
            base_time - chrono::Duration::seconds(10),
            base_time + chrono::Duration::seconds(50),
        );

        let window3 = Window::new(
            base_time - chrono::Duration::seconds(20),
            base_time + chrono::Duration::seconds(40),
        );

        // Insert windows manually
        {
            let mut active_windows = windower.active_windows.write().unwrap();
            active_windows.insert(window1.clone());
            active_windows.insert(window2.clone());
            active_windows.insert(window3.clone());
        }

        // Verify all windows exist
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 3);
        }

        // Close window1
        windower.close_windows(base_time + chrono::Duration::seconds(60));

        // Delete window1
        windower.gc_window(window1.clone());

        // Verify window1 is deleted
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 0);
            let closed_windows = windower.closed_windows.read().unwrap();
            assert_eq!(closed_windows.len(), 2);
            assert!(!closed_windows.contains(&window1));
            assert!(closed_windows.contains(&window2));
            assert!(closed_windows.contains(&window3));
        }

        // Delete window2
        windower.gc_window(window2.clone());

        // Verify window2 is deleted
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 0);
            assert!(!active_windows.contains(&window1));
            assert!(!active_windows.contains(&window2));
            assert!(!active_windows.contains(&window3));
        }

        // Delete window3
        windower.gc_window(window3.clone());

        // Verify window3 is deleted
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 0);
        }
    }

    #[test]
    fn test_oldest_window_endtime() {
        // Base time: 60000ms (60s)
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

        // Create a sliding windower with 60s window length and 10s slide
        let windower = SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(10));

        // Create windows
        let window1 = Window::new(base_time, base_time + chrono::Duration::seconds(60));

        let window2 = Window::new(
            base_time - chrono::Duration::seconds(10),
            base_time + chrono::Duration::seconds(50),
        );

        let window3 = Window::new(
            base_time - chrono::Duration::seconds(20),
            base_time + chrono::Duration::seconds(40),
        );

        // Insert windows manually
        {
            let mut active_windows = windower.active_windows.write().unwrap();
            active_windows.insert(window1.clone());
            active_windows.insert(window2.clone());
            active_windows.insert(window3.clone());
        }

        // Verify oldest window end time
        assert_eq!(
            windower
                .oldest_window()
                .unwrap()
                .end_time
                .timestamp_millis(),
            (base_time + chrono::Duration::seconds(40)).timestamp_millis()
        );

        // Close window3
        windower.close_windows(base_time + chrono::Duration::seconds(40));

        // Delete window3 (the oldest)
        windower.gc_window(window3.clone());

        // Verify the oldest window end time is now window2
        assert_eq!(
            windower
                .oldest_window()
                .unwrap()
                .end_time
                .timestamp_millis(),
            (base_time + chrono::Duration::seconds(50)).timestamp_millis()
        );

        // Close window2
        windower.close_windows(base_time + chrono::Duration::seconds(50));

        // Delete window2
        windower.gc_window(window2.clone());

        // Verify oldest window end time is now window1
        assert_eq!(
            windower
                .oldest_window()
                .unwrap()
                .end_time
                .timestamp_millis(),
            (base_time + chrono::Duration::seconds(60)).timestamp_millis()
        );

        // Close window1
        windower.close_windows(base_time + chrono::Duration::seconds(60));

        // Delete window1
        windower.gc_window(window1.clone());
        assert_eq!(windower.oldest_window(), None);
    }

    #[test]
    fn test_assign_windows_with_small_slide() {
        // prepopulate active windows
        let active_windows = [
            Window::new(
                Utc.timestamp_millis_opt(90000).unwrap(),
                Utc.timestamp_millis_opt(150000).unwrap(),
            ),
            Window::new(
                Utc.timestamp_millis_opt(80000).unwrap(),
                Utc.timestamp_millis_opt(140000).unwrap(),
            ),
            Window::new(
                Utc.timestamp_millis_opt(70000).unwrap(),
                Utc.timestamp_millis_opt(130000).unwrap(),
            ),
            Window::new(
                Utc.timestamp_millis_opt(60000).unwrap(),
                Utc.timestamp_millis_opt(120000).unwrap(),
            ),
            Window::new(
                Utc.timestamp_millis_opt(50000).unwrap(),
                Utc.timestamp_millis_opt(110000).unwrap(),
            ),
        ];

        let windower = SlidingWindowManager::from_snapshot(
            Duration::from_secs(60),
            Duration::from_secs(10),
            SlidingWindowSnapshot {
                windows: active_windows.to_vec(),
                max_deleted_window_end_time: -1,
            },
        );

        let event_time = Utc.timestamp_millis_opt(105000).unwrap();

        // Pre-populate expected windows
        // For event time 105000ms (105s):
        // Window 1: [100000, 160000) - event falls in this window
        // Window 2: [90000, 150000) - event falls in this window
        // Window 3: [80000, 140000) - event falls in this window
        // Window 4: [70000, 130000) - event falls in this window
        // Window 5: [60000, 120000) - event falls in this window
        // Window 6: [50000, 110000) - event falls in this window
        let expected_windows = [
            Window::new(
                Utc.timestamp_millis_opt(100000).unwrap(),
                Utc.timestamp_millis_opt(160000).unwrap(),
            ),
            Window::new(
                Utc.timestamp_millis_opt(90000).unwrap(),
                Utc.timestamp_millis_opt(150000).unwrap(),
            ),
            Window::new(
                Utc.timestamp_millis_opt(80000).unwrap(),
                Utc.timestamp_millis_opt(140000).unwrap(),
            ),
            Window::new(
                Utc.timestamp_millis_opt(70000).unwrap(),
                Utc.timestamp_millis_opt(130000).unwrap(),
            ),
            Window::new(
                Utc.timestamp_millis_opt(60000).unwrap(),
                Utc.timestamp_millis_opt(120000).unwrap(),
            ),
            Window::new(
                Utc.timestamp_millis_opt(50000).unwrap(),
                Utc.timestamp_millis_opt(110000).unwrap(),
            ),
        ];

        // Assign windows
        let window_actions = windower.assign_windows(event_time);

        // Should also be assigned to 6 windows (5 are already open, 1 is new)
        assert_eq!(window_actions.len(), expected_windows.len());

        // Check all windows match expected windows
        for (i, action) in window_actions.iter().enumerate() {
            let window = match action {
                AlignedWindowAction::Open { window } => {
                    assert_eq!(window.start_time, Utc.timestamp_millis_opt(100000).unwrap());
                    assert_eq!(window.end_time, Utc.timestamp_millis_opt(160000).unwrap());
                    window
                }
                AlignedWindowAction::Append { window } => window,
                AlignedWindowAction::Close { .. } => {
                    panic!("Expected Open or Append operation");
                }
            };

            let expected = expected_windows
                .get(i)
                .expect("Expected window should exist at index");
            assert_eq!(window.start_time, expected.start_time);
            assert_eq!(window.end_time, expected.end_time);
        }

        let window_actions_two = windower.assign_windows(event_time);

        // Should also be assigned to 6 windows, all append
        assert_eq!(window_actions_two.len(), 6);

        for action in &window_actions_two {
            match action {
                AlignedWindowAction::Append { .. } => {}
                AlignedWindowAction::Close { .. } | AlignedWindowAction::Open { .. } => {
                    panic!("Expected Append operation");
                }
            }
        }
    }

    #[test]
    fn deleted_window_bound_suppresses_recreation() {
        let windower = SlidingWindowManager::from_snapshot(
            Duration::from_secs(60),
            Duration::from_secs(10),
            SlidingWindowSnapshot {
                windows: Vec::new(),
                max_deleted_window_end_time: 110_000,
            },
        );

        let actions = windower.assign_windows(Utc.timestamp_millis_opt(105_000).unwrap());
        let windows: Vec<&Window> = actions
            .iter()
            .map(|action| match action {
                AlignedWindowAction::Open { window } => window,
                other => panic!("expected OPEN, got {other:?}"),
            })
            .collect();

        assert_eq!(windows.len(), 5);
        assert!(
            windows
                .iter()
                .all(|window| window.end_time.timestamp_millis() > 110_000)
        );
        assert!(
            windows
                .iter()
                .all(|window| window.start_time.timestamp_millis() != 50_000)
        );
    }

    #[test]
    fn close_all_matches_a_terminal_watermark() {
        let close_all_manager =
            SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(10));
        let watermark_manager =
            SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(10));
        for event_ms in [105_000, 125_000] {
            let event_time = Utc.timestamp_millis_opt(event_ms).unwrap();
            close_all_manager.assign_windows(event_time);
            watermark_manager.assign_windows(event_time);
        }

        let close_all = close_all_manager.close_all();
        let terminal = watermark_manager.close_windows(Utc.timestamp_millis_opt(180_000).unwrap());

        assert_eq!(close_all, terminal);
        assert_eq!(close_all_manager.active_window_count(), 0);
        assert_eq!(close_all_manager.closed_window_count(), terminal.len());
    }

    #[test]
    fn test_snapshot_for_restart_round_trip() {
        let windower = SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(10));
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();
        let window1 = Window::new(base_time, base_time + chrono::Duration::seconds(60));
        {
            let mut active = windower.active_windows.write().unwrap();
            active.insert(window1.clone());
        }
        windower.close_windows(base_time + chrono::Duration::seconds(30));
        windower.gc_window(window1.clone());

        let snapshot = windower.snapshot_for_restart();
        let max_deleted = snapshot.max_deleted_window_end_time;
        let restored = SlidingWindowManager::from_snapshot(
            Duration::from_secs(60),
            Duration::from_secs(10),
            snapshot,
        );

        assert_eq!(
            restored.active_window_count(),
            windower.active_window_count()
        );
        assert_eq!(
            restored.snapshot_for_restart().max_deleted_window_end_time,
            max_deleted
        );
    }
}
