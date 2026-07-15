//! In [Fixed Window] each event belongs to exactly one window, and we can assign the message to the window
//! directly using the event time after rounding it to the nearest window [boundary](super::truncate_to_duration).
//! The window is defined by the length of the window. The window is aligned to the epoch. For example,
//! if the window length is 30s, and the event time is 100, then the window this event belongs to will
//! be `[90, 120)`. We only have a single [WAL] for all the windows (we do not have per window WALs).
//! The compactor takes care of compacting the WALs based on the deleted windows.
//!
//! [Fixed Window]: https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/windowing/fixed/
//! [WAL]: crate::reduce::wal

use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use super::{AlignedWindowAction, truncate_to_duration};
use crate::model::Window;
use chrono::{DateTime, TimeZone, Utc};

#[derive(Debug, Clone)]
pub struct FixedWindowManager {
    /// Duration of each window
    window_length: Duration,
    /// Active windows sorted by end time.
    pub(crate) active_windows: Arc<RwLock<BTreeSet<Window>>>,
    /// Closed windows sorted by end time. We need to keep track of closed windows so that we can
    /// find the oldest window computed and forwarded. The watermark is progressed based on the latest
    /// closed window end time. The oldest window in the active_window will be greater than the latest
    /// closed window.
    pub(crate) closed_windows: Arc<RwLock<BTreeSet<Window>>>,
}

impl FixedWindowManager {
    pub fn new(window_length: Duration) -> Self {
        Self {
            window_length,
            active_windows: Arc::new(RwLock::new(BTreeSet::new())),
            closed_windows: Arc::new(RwLock::new(BTreeSet::new())),
        }
    }

    /// Creates a new window for the given event time.
    fn create_window(&self, event_time: DateTime<Utc>) -> Window {
        // Truncate event time to window length
        let window_length_millis = self.window_length.as_millis() as i64;
        let event_time_millis = event_time.timestamp_millis();
        let start_time_millis = truncate_to_duration(event_time_millis, window_length_millis);
        let end_time_millis = start_time_millis + window_length_millis;

        let start_time = Utc.timestamp_millis_opt(start_time_millis).unwrap();
        let end_time = Utc.timestamp_millis_opt(end_time_millis).unwrap();

        Window::new(start_time, end_time)
    }

    /// Assigns windows to an event time.
    pub fn assign_windows(&self, event_time: DateTime<Utc>) -> Vec<AlignedWindowAction> {
        let window = self.create_window(event_time);

        // Check if window already exists
        let mut active_windows = self
            .active_windows
            .write()
            .expect("Poisoned lock for active_windows");
        if active_windows.contains(&window) {
            vec![AlignedWindowAction::Append { window }]
        } else {
            active_windows.insert(window.clone());
            vec![AlignedWindowAction::Open { window }]
        }
    }

    /// Closes any windows that can be closed because the Watermark has advanced beyond the window
    /// end time.
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
        let mut closed_windows = self
            .closed_windows
            .write()
            .expect("Poisoned lock for closed_windows");
        closed_windows.remove(&window);
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assign_windows() {
        // Create a fixed windower with 60s window length
        let windower = FixedWindowManager::new(Duration::from_secs(60));
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

        // Assign windows
        let window_actions = windower.assign_windows(base_time);

        // Verify results - first message should create a new window with Open operation
        assert_eq!(window_actions.len(), 1);

        // Check operation type and extract window
        let window = match window_actions
            .first()
            .expect("Expected at least one window action")
        {
            AlignedWindowAction::Open { window } => window,
            _ => panic!("Expected Open operation"),
        };

        // Check window boundaries
        assert_eq!(window.start_time, base_time);
        assert_eq!(window.end_time, base_time + chrono::Duration::seconds(60));

        // Assign another event to the same window (base_time + 1s)
        let event_time2 = base_time + chrono::Duration::seconds(1);
        let window_actions2 = windower.assign_windows(event_time2);

        match window_actions2
            .first()
            .expect("Expected at least one window action")
        {
            AlignedWindowAction::Append { window } => {
                assert_eq!(window.start_time, base_time);
                assert_eq!(window.end_time, base_time + chrono::Duration::seconds(60));
            }
            _ => panic!("Expected Append operation"),
        }
    }

    #[test]
    fn exact_boundary_opens_the_next_half_open_window() {
        let windower = FixedWindowManager::new(Duration::from_secs(60));

        windower.assign_windows(Utc.timestamp_millis_opt(60_001).unwrap());
        let actions = windower.assign_windows(Utc.timestamp_millis_opt(120_000).unwrap());

        assert_eq!(
            actions,
            vec![AlignedWindowAction::Open {
                window: Window::new(
                    Utc.timestamp_millis_opt(120_000).unwrap(),
                    Utc.timestamp_millis_opt(180_000).unwrap(),
                ),
            }]
        );
    }

    #[test]
    fn negative_timestamp_uses_production_truncation() {
        let windower = FixedWindowManager::new(Duration::from_secs(60));

        let actions = windower.assign_windows(Utc.timestamp_millis_opt(-1).unwrap());

        assert_eq!(
            actions,
            vec![AlignedWindowAction::Open {
                window: Window::new(
                    Utc.timestamp_millis_opt(-60_000).unwrap(),
                    Utc.timestamp_millis_opt(0).unwrap(),
                ),
            }]
        );
    }

    #[test]
    fn close_all_matches_a_terminal_watermark() {
        let close_all_manager = FixedWindowManager::new(Duration::from_secs(60));
        let watermark_manager = FixedWindowManager::new(Duration::from_secs(60));
        for event_ms in [1_000, 61_000, 121_000] {
            let event_time = Utc.timestamp_millis_opt(event_ms).unwrap();
            close_all_manager.assign_windows(event_time);
            watermark_manager.assign_windows(event_time);
        }

        let close_all = close_all_manager.close_all();
        let terminal = watermark_manager.close_windows(Utc.timestamp_millis_opt(180_000).unwrap());

        assert_eq!(close_all, terminal);
        assert_eq!(close_all_manager.active_window_count(), 0);
        assert_eq!(close_all_manager.closed_window_count(), 3);
    }

    #[test]
    fn test_insert_window() {
        // Create a fixed windower with 60s window length
        let windower = FixedWindowManager::new(Duration::from_secs(60));
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

        // Create a window
        let window1 = Window::new(base_time, base_time + chrono::Duration::seconds(60));

        // Insert window manually
        {
            let mut active_windows = windower.active_windows.write().unwrap();
            active_windows.insert(window1.clone());
        }

        // Verify window exists and count is 1
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 1);
            assert!(active_windows.contains(&window1));
        }

        // Insert the same window again (should not increase count)
        {
            let mut active_windows = windower.active_windows.write().unwrap();
            active_windows.insert(window1.clone());
        }

        // Verify count is still 1
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 1);
        }

        // Create and insert a different window
        let window2 = Window::new(
            base_time + chrono::Duration::seconds(60),
            base_time + chrono::Duration::seconds(120),
        );

        {
            let mut active_windows = windower.active_windows.write().unwrap();
            active_windows.insert(window2.clone());
        }

        // Verify count is now 2
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 2);
        }
    }

    #[test]
    fn test_close_windows() {
        // Create a fixed windower with 60s window length
        let windower = FixedWindowManager::new(Duration::from_secs(60));
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

        // Create windows
        let window1 = Window::new(base_time, base_time + chrono::Duration::seconds(60));

        let window2 = Window::new(
            base_time + chrono::Duration::seconds(60),
            base_time + chrono::Duration::seconds(120),
        );

        let window3 = Window::new(
            base_time + chrono::Duration::seconds(120),
            base_time + chrono::Duration::seconds(180),
        );

        // Insert windows manually
        {
            let mut active_windows = windower.active_windows.write().unwrap();
            active_windows.insert(window1.clone());
            active_windows.insert(window2.clone());
            active_windows.insert(window3.clone());
        }

        // Close windows with watermark at base_time + 120s
        let closed_actions = windower.close_windows(base_time + chrono::Duration::seconds(120));

        // Should close 2 windows (window1 and window2)
        assert_eq!(closed_actions.len(), 2);

        match closed_actions
            .first()
            .expect("Expected at least one closed action")
        {
            AlignedWindowAction::Close { window } => {
                assert_eq!(window.start_time, base_time);
                assert_eq!(window.end_time, base_time + chrono::Duration::seconds(60));
            }
            _ => panic!("Expected Close operation"),
        }

        match closed_actions
            .get(1)
            .expect("Expected at least two closed actions")
        {
            AlignedWindowAction::Close { window } => {
                assert_eq!(window.start_time, base_time + chrono::Duration::seconds(60));
                assert_eq!(window.end_time, base_time + chrono::Duration::seconds(120));
            }
            _ => panic!("Expected Close operation"),
        }

        // Verify only window3 remains in active windows
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 1);
            assert!(active_windows.contains(&window3));
        }
    }

    #[test]
    fn test_delete_window() {
        // Create a fixed windower with 60s window length
        let windower = FixedWindowManager::new(Duration::from_secs(60));
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

        // Create windows
        let window1 = Window::new(base_time, base_time + chrono::Duration::seconds(60));

        let window2 = Window::new(
            base_time + chrono::Duration::seconds(60),
            base_time + chrono::Duration::seconds(120),
        );

        let window3 = Window::new(
            base_time + chrono::Duration::seconds(120),
            base_time + chrono::Duration::seconds(180),
        );

        // Insert windows manually
        {
            let mut active_windows = windower.active_windows.write().unwrap();
            active_windows.insert(window1.clone());
            active_windows.insert(window2.clone());
            active_windows.insert(window3.clone());
        }

        // close windows so that we can delete them
        windower.close_windows(base_time + chrono::Duration::seconds(120));

        assert_eq!(windower.closed_windows.read().unwrap().len(), 2);

        // Delete window2
        windower.gc_window(window2.clone());

        // Verify window2 was deleted
        {
            let active_windows = windower.active_windows.read().unwrap();
            let closed_windows = windower.closed_windows.read().unwrap();
            assert_eq!(active_windows.len(), 1);
            assert!(!active_windows.contains(&window1));
            assert!(!active_windows.contains(&window2));
            assert!(active_windows.contains(&window3));
            assert_eq!(closed_windows.len(), 1);
            assert!(closed_windows.contains(&window1));
            assert!(!closed_windows.contains(&window2));
        }
    }

    #[test]
    fn test_oldest_window_endtime() {
        // Create a fixed windower with 60s window length
        let windower = FixedWindowManager::new(Duration::from_secs(60));
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

        // Create windows
        let window1 = Window::new(base_time, base_time + chrono::Duration::seconds(60));

        let window2 = Window::new(
            base_time + chrono::Duration::seconds(60),
            base_time + chrono::Duration::seconds(120),
        );

        let window3 = Window::new(
            base_time + chrono::Duration::seconds(120),
            base_time + chrono::Duration::seconds(180),
        );

        // Insert windows manually
        {
            let mut active_windows = windower.active_windows.write().unwrap();
            active_windows.insert(window1.clone());
            active_windows.insert(window2.clone());
            active_windows.insert(window3.clone());
        }

        // Verify the oldest window end time is window1's end time
        assert_eq!(
            windower.oldest_window().unwrap().end_time,
            base_time + chrono::Duration::seconds(60)
        );

        // Close window1
        windower.close_windows(base_time + chrono::Duration::seconds(60));

        // Delete window1
        windower.gc_window(window1);

        // Verify the oldest window end time is now window2's end time
        assert_eq!(
            windower.oldest_window().unwrap().end_time,
            base_time + chrono::Duration::seconds(120)
        );
    }
}
