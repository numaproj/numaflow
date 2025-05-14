use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::message::Message;
use crate::reduce::reducer::aligned::windower::{AlignedWindowMessage, Window, WindowOperation};
use chrono::{DateTime, TimeZone, Utc};

#[derive(Debug, Clone)]
pub(crate) struct FixedWindowManager {
    /// Duration of each window
    window_length: Duration,
    /// Active windows sorted by end time
    active_windows: Arc<Mutex<BTreeSet<Window>>>,
    /// Closed windows sorted by end time
    closed_windows: Arc<Mutex<BTreeSet<Window>>>,
}

impl FixedWindowManager {
    pub(crate) fn new(window_length: Duration) -> Self {
        Self {
            window_length,
            active_windows: Arc::new(Mutex::new(BTreeSet::new())),
            closed_windows: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    /// Truncates a timestamp to the nearest multiple of the given duration
    fn truncate_to_duration(timestamp_millis: i64, duration_millis: i64) -> i64 {
        (timestamp_millis / duration_millis) * duration_millis
    }

    /// Creates a new window for the given message
    fn create_window(&self, msg: &Message) -> Window {
        // Truncate event time to window length
        let window_length_millis = self.window_length.as_millis() as i64;
        let event_time_millis = msg.event_time.timestamp_millis();
        let start_time_millis = Self::truncate_to_duration(event_time_millis, window_length_millis);
        let end_time_millis = start_time_millis + window_length_millis;

        let start_time = Utc.timestamp_millis_opt(start_time_millis).unwrap();
        let end_time = Utc.timestamp_millis_opt(end_time_millis).unwrap();

        Window::new(start_time, end_time)
    }

    /// Assigns windows to a message
    pub(crate) fn assign_windows(&self, msg: Message) -> Vec<AlignedWindowMessage> {
        let window = self.create_window(&msg);

        // Check if window already exists
        let mut active_windows = self.active_windows.lock().unwrap();
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

        let mut active_windows = self.active_windows.lock().unwrap();
        let mut closed_windows = self.closed_windows.lock().unwrap();

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
        let mut closed_windows = self.closed_windows.lock().unwrap();
        closed_windows.remove(&window);
    }

    /// Returns the oldest window yet to be completed. This will be the lowest Watermark in the Vertex.
    pub(crate) fn oldest_window(&self) -> Option<Window> {
        // First check closed windows
        let closed_windows = self.closed_windows.lock().unwrap();
        if !closed_windows.is_empty() {
            return closed_windows.iter().next().cloned();
        }

        // If no closed windows, check active windows
        let active_windows = self.active_windows.lock().unwrap();
        active_windows.iter().next().cloned()
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
            let mut active_windows = windower.active_windows.lock().unwrap();
            active_windows.insert(window1.clone());
        }

        // Verify window exists and count is 1
        {
            let active_windows = windower.active_windows.lock().unwrap();
            assert_eq!(active_windows.len(), 1);
            assert!(active_windows.contains(&window1));
        }

        // Insert the same window again (should not increase count)
        {
            let mut active_windows = windower.active_windows.lock().unwrap();
            active_windows.insert(window1.clone());
        }

        // Verify count is still 1
        {
            let active_windows = windower.active_windows.lock().unwrap();
            assert_eq!(active_windows.len(), 1);
        }

        // Create and insert a different window
        let window2 = Window::new(
            base_time + chrono::Duration::seconds(60),
            base_time + chrono::Duration::seconds(120),
        );

        {
            let mut active_windows = windower.active_windows.lock().unwrap();
            active_windows.insert(window2.clone());
        }

        // Verify count is now 2
        {
            let active_windows = windower.active_windows.lock().unwrap();
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
            let mut active_windows = windower.active_windows.lock().unwrap();
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
            let active_windows = windower.active_windows.lock().unwrap();
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
            let mut active_windows = windower.active_windows.lock().unwrap();
            active_windows.insert(window1.clone());
            active_windows.insert(window2.clone());
            active_windows.insert(window3.clone());
        }

        // Delete window2
        windower.gc_window(window2.clone());

        // Verify window2 was deleted
        {
            let active_windows = windower.active_windows.lock().unwrap();
            assert_eq!(active_windows.len(), 2);
            assert!(active_windows.contains(&window1));
            assert!(!active_windows.contains(&window2));
            assert!(active_windows.contains(&window3));
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
            let mut active_windows = windower.active_windows.lock().unwrap();
            active_windows.insert(window1.clone());
            active_windows.insert(window2.clone());
            active_windows.insert(window3.clone());
        }

        // Verify the oldest window end time is window1's end time
        assert_eq!(
            windower.oldest_window().unwrap().end_time,
            base_time + chrono::Duration::seconds(60)
        );

        // Delete window1
        windower.gc_window(window1);

        // Verify the oldest window end time is now window2's end time
        assert_eq!(
            windower.oldest_window().unwrap().end_time,
            base_time + chrono::Duration::seconds(120)
        );
    }
}
