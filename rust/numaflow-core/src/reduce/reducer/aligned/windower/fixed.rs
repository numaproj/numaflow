use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};

use crate::message::Message;
use crate::reduce::reducer::aligned::windower::{
    AlignedWindowMessage, Window, WindowManager, WindowOperation,
};

#[derive(Debug, Clone)]
pub(crate) struct FixedWindowManager {
    /// Duration of each window
    window_length: Duration,
    /// Active windows sorted by end time
    active_windows: Arc<Mutex<BTreeMap<DateTime<Utc>, Window>>>,
}

impl FixedWindowManager {
    pub(crate) fn new(window_length: Duration) -> Self {
        Self {
            window_length,
            active_windows: Arc::new(Mutex::new(BTreeMap::new())),
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
}

impl WindowManager for FixedWindowManager {
    fn assign_windows(&self, msg: Message) -> Vec<AlignedWindowMessage> {
        let window = self.create_window(&msg);

        // Check if window already exists
        let mut active_windows = self.active_windows.lock().unwrap();
        let operation = if let Entry::Vacant(e) = active_windows.entry(window.end_time) {
            // New window, insert it
            e.insert(window.clone());
            WindowOperation::Open(msg)
        } else {
            // Window exists, append message
            WindowOperation::Append(msg)
        };

        // Create window message
        vec![AlignedWindowMessage { operation, window }]
    }

    fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<AlignedWindowMessage> {
        let mut result = Vec::new();
        let mut windows_to_close = Vec::new();

        // Find windows that need to be closed
        {
            let active_windows = self.active_windows.lock().unwrap();
            for (&end_time, window) in active_windows.iter() {
                if end_time <= watermark {
                    windows_to_close.push(window.clone());
                }
            }
        }

        // Create close messages for each window
        for window in windows_to_close {
            // Remove from active windows
            self.delete_window(window.clone());

            // Create close message
            let window_msg = AlignedWindowMessage {
                operation: WindowOperation::Close,
                window,
            };

            result.push(window_msg);
        }

        result
    }

    fn delete_window(&self, window: Window) {
        let mut active_windows = self.active_windows.lock().unwrap();
        active_windows.remove(&window.end_time);
    }

    fn oldest_window(&self) -> Option<Window> {
        let active_windows = self.active_windows.lock().unwrap();

        active_windows.values().next().cloned()
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
            active_windows.insert(window1.end_time, window1.clone());
        }

        // Verify window exists and count is 1
        {
            let active_windows = windower.active_windows.lock().unwrap();
            assert_eq!(active_windows.len(), 1);
            assert!(active_windows.contains_key(&window1.end_time));
        }

        // Insert the same window again (should not increase count)
        {
            let mut active_windows = windower.active_windows.lock().unwrap();
            active_windows.insert(window1.end_time, window1.clone());
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
            active_windows.insert(window2.end_time, window2.clone());
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
            active_windows.insert(window1.end_time, window1.clone());
            active_windows.insert(window2.end_time, window2.clone());
            active_windows.insert(window3.end_time, window3.clone());
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
            assert!(active_windows.contains_key(&window3.end_time));
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
            active_windows.insert(window1.end_time, window1.clone());
            active_windows.insert(window2.end_time, window2.clone());
            active_windows.insert(window3.end_time, window3.clone());
        }

        // Delete window2
        windower.delete_window(window2.clone());

        // Verify window2 was deleted
        {
            let active_windows = windower.active_windows.lock().unwrap();
            assert_eq!(active_windows.len(), 2);
            assert!(active_windows.contains_key(&window1.end_time));
            assert!(!active_windows.contains_key(&window2.end_time));
            assert!(active_windows.contains_key(&window3.end_time));
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
            active_windows.insert(window1.end_time, window1.clone());
            active_windows.insert(window2.end_time, window2.clone());
            active_windows.insert(window3.end_time, window3.clone());
        }

        // Verify the oldest window end time is window1's end time
        assert_eq!(
            windower.oldest_window().unwrap().end_time,
            base_time + chrono::Duration::seconds(60)
        );

        // Delete window1
        windower.delete_window(window1);

        // Verify the oldest window end time is now window2's end time
        assert_eq!(
            windower.oldest_window().unwrap().end_time,
            base_time + chrono::Duration::seconds(120)
        );
    }
}
