//! In [Fixed Window] each event belongs to exactly one window and we can assign the message to the window
//! directly using the event time after rounding it to the nearest window [boundary](windower::truncate_to_duration).
//! The window is defined by the length of the window. The window is aligned to the epoch. For example,
//! if the window length is 30s, and the event time is 100, then the window this event belongs to will
//! be `[90, 120)`. There will be a [WAL] for each window. The GC (garbage collection) of WAL is
//! very simple since each window has its own WAL and the GC can be done by deleting the WAL. During
//! startup, we will replay the WALs and recreate each per WAL and simply replay the messages to that window.
//!
//! [Fixed Window]: https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/windowing/fixed/
//! [WAL]: crate::reduce::wal

use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::message::Message;
use crate::reduce::reducer::aligned::windower;
use crate::reduce::reducer::aligned::windower::{AlignedWindowMessage, Window, WindowOperation};
use chrono::{DateTime, TimeZone, Utc};

#[derive(Debug, Clone)]
pub(crate) struct FixedWindowManager {
    /// Duration of each window
    window_length: Duration,
    /// Active windows sorted by end time.
    active_windows: Arc<RwLock<BTreeSet<Window>>>,
    /// Closed windows sorted by end time. We need to keep track of closed windows so that we can
    /// find the oldest window computed and forwarded. The watermark is progressed based on the latest
    /// closed window end time. The oldest window in the active_window will be greater than the latest
    /// closed window.
    closed_windows: Arc<RwLock<BTreeSet<Window>>>,
}

impl FixedWindowManager {
    pub(crate) fn new(window_length: Duration) -> Self {
        Self {
            window_length,
            active_windows: Arc::new(RwLock::new(BTreeSet::new())),
            closed_windows: Arc::new(RwLock::new(BTreeSet::new())),
        }
    }

    /// Creates a new window for the given message.
    fn create_window(&self, msg: &Message) -> Window {
        // Truncate event time to window length
        let window_length_millis = self.window_length.as_millis() as i64;
        let event_time_millis = msg.event_time.timestamp_millis();
        let start_time_millis =
            windower::truncate_to_duration(event_time_millis, window_length_millis);
        let end_time_millis = start_time_millis + window_length_millis;

        let start_time = Utc.timestamp_millis_opt(start_time_millis).unwrap();
        let end_time = Utc.timestamp_millis_opt(end_time_millis).unwrap();

        Window::new(start_time, end_time)
    }

    /// Assigns windows to a message
    pub(crate) fn assign_windows(&self, msg: Message) -> Vec<AlignedWindowMessage> {
        let window = self.create_window(&msg);

        // Check if window already exists
        let mut active_windows = self
            .active_windows
            .write()
            .expect("Poisoned lock for active_windows");
        let operation = if active_windows.contains(&window) {
            // Window exists, append message
            WindowOperation::Append(msg)
        } else {
            // New window, insert it
            active_windows.insert(window.clone());
            WindowOperation::Open(msg)
        };

        // Create window message
        vec![AlignedWindowMessage { operation, window }]
    }

    /// Closes any windows that can be closed because the Watermark has advanced beyond the window
    /// end time.
    pub(crate) fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<AlignedWindowMessage> {
        let mut result = Vec::new();

        let mut active_windows = self
            .active_windows
            .write()
            .expect("Poisoned lock for active_windows");
        let mut closed_windows = self
            .closed_windows
            .write()
            .expect("Poisoned lock for closed_windows");

        let mut windows_to_close = Vec::new();

        for window in active_windows.iter() {
            if window.end_time <= watermark {
                // Create close message
                let window_msg = AlignedWindowMessage {
                    operation: WindowOperation::Close,
                    window: window.clone(),
                };

                result.push(window_msg);
                windows_to_close.push(window.clone());
            }
        }

        // Move windows from active to closed
        for window in windows_to_close {
            active_windows.remove(&window);
            closed_windows.insert(window);
        }

        result
    }

    /// Deletes a window after it is closed and GC is done.
    pub(crate) fn gc_window(&self, window: Window) {
        let mut closed_windows = self
            .closed_windows
            .write()
            .expect("Poisoned lock for closed_windows");
        closed_windows.remove(&window);
    }

    /// Returns the oldest window yet to be completed. This will be the lowest Watermark in the Vertex.
    pub(crate) fn oldest_window(&self) -> Option<Window> {
        // get the oldest window from closed_windows, if closed_windows is empty, get the oldest
        // from active_windows
        // NOTE: closed windows will always have a lower end time than active_windows
        self.closed_windows
            .read()
            .expect("Poisoned lock for closed_windows")
            .iter()
            .next()
            .cloned()
            .or_else(|| {
                self.active_windows
                    .read()
                    .expect("Poisoned lock for active_windows")
                    .iter()
                    .next()
                    .cloned()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_assign_windows() {
        // Create a fixed windower with 60s window length
        let windower = FixedWindowManager::new(Duration::from_secs(60));
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

        // Create a test message with event time at base_time
        let msg = Message {
            event_time: base_time,
            ..Default::default()
        };

        // Assign windows
        let window_msgs = windower.assign_windows(msg.clone());

        // Verify results - first message should create a new window with Open operation
        assert_eq!(window_msgs.len(), 1);
        // Check window boundaries
        assert_eq!(window_msgs[0].window.start_time, base_time);
        assert_eq!(
            window_msgs[0].window.end_time,
            base_time + chrono::Duration::seconds(60)
        );

        // Check operation type
        match &window_msgs[0].operation {
            WindowOperation::Open(_) => {}
            _ => panic!("Expected Open operation"),
        }

        // Assign another message to the same window (base_time + 1s)
        let msg2 = Message {
            event_time: base_time + chrono::Duration::seconds(1),
            ..Default::default()
        };

        let window_msgs2 = windower.assign_windows(msg2.clone());

        assert_eq!(window_msgs2[0].window.start_time, base_time);
        assert_eq!(
            window_msgs2[0].window.end_time,
            base_time + chrono::Duration::seconds(60)
        );

        match &window_msgs2[0].operation {
            WindowOperation::Append(append_msg) => {
                assert_eq!(append_msg.event_time, msg2.event_time);
            }
            _ => panic!("Expected Append operation"),
        }
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
        let closed_msgs = windower.close_windows(base_time + chrono::Duration::seconds(120));

        // Should close 2 windows (window1 and window2)
        assert_eq!(closed_msgs.len(), 2);

        assert_eq!(closed_msgs[0].window.start_time, base_time);
        assert_eq!(
            closed_msgs[0].window.end_time,
            base_time + chrono::Duration::seconds(60)
        );
        match &closed_msgs[0].operation {
            WindowOperation::Close => {}
            _ => panic!("Expected Close operation"),
        }

        assert_eq!(
            closed_msgs[1].window.start_time,
            base_time + chrono::Duration::seconds(60)
        );
        assert_eq!(
            closed_msgs[1].window.end_time,
            base_time + chrono::Duration::seconds(120)
        );
        match &closed_msgs[1].operation {
            WindowOperation::Close => {}
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
