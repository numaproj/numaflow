use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};

use crate::message::Message;
use crate::reduce::reducer::aligned::windower::{
    AlignedWindowMessage, Window, WindowManager, WindowOperation,
};

#[derive(Debug, Clone)]
pub(crate) struct SlidingWindowManager {
    /// Duration of each window
    window_length: Duration,
    /// Slide duration
    slide: Duration,
    /// Active windows sorted by end time
    active_windows: Arc<Mutex<BTreeSet<Window>>>,
}

impl SlidingWindowManager {
    pub(crate) fn new(window_length: Duration, slide: Duration) -> Self {
        Self {
            window_length,
            slide,
            active_windows: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    /// Truncates a timestamp to the nearest multiple of the given duration
    fn truncate_to_duration(timestamp_millis: i64, duration_millis: i64) -> i64 {
        (timestamp_millis / duration_millis) * duration_millis
    }

    /// Creates windows for the given message
    fn create_windows(&self, msg: &Message) -> Vec<Window> {
        let mut windows = Vec::new();
        let window_length_millis = self.window_length.as_millis() as i64;
        let slide_millis = self.slide.as_millis() as i64;
        let event_time_millis = msg.event_time.timestamp_millis();

        // Calculate the start time of the largest window that contains the event
        let start_time_millis = Self::truncate_to_duration(event_time_millis, slide_millis);
        let mut current_start = start_time_millis;
        let mut current_end = current_start + window_length_millis;

        // Create all windows that contain this event
        // Check if event time is within the window: start_time <= event_time < end_time
        while current_start <= msg.event_time.timestamp_millis()
            && msg.event_time.timestamp_millis() < current_end
        {
            let start_time = Utc.timestamp_millis_opt(current_start).unwrap();
            let end_time = Utc.timestamp_millis_opt(current_end).unwrap();

            windows.push(Window::new(start_time, end_time));

            // Move to the previous window
            current_start -= slide_millis;
            current_end -= slide_millis;
        }

        windows
    }
}

impl WindowManager for SlidingWindowManager {
    fn assign_windows(&self, msg: Message) -> Vec<AlignedWindowMessage> {
        let windows = self.create_windows(&msg);
        let mut result = Vec::new();

        // Check if windows already exist and create appropriate operations
        let mut active_windows = self.active_windows.lock().unwrap();

        for window in windows {
            let operation = if active_windows.contains(&window) {
                // Window exists, append message
                WindowOperation::Append(msg.clone())
            } else {
                // New window, insert it
                active_windows.insert(window.clone());
                WindowOperation::Open(msg.clone())
            };

            result.push(AlignedWindowMessage { operation, window });
        }

        result
    }

    fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<AlignedWindowMessage> {
        let mut result = Vec::new();
        let mut windows_to_close = Vec::new();

        // Find windows that need to be closed
        let mut active_windows = self.active_windows.lock().unwrap();
        for window in active_windows.iter() {
            if window.end_time <= watermark {
                windows_to_close.push(window.clone());
            }
        }

        // Create close messages for each window
        for window in windows_to_close {
            // Remove from active windows
            active_windows.remove(&window);

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
        active_windows.remove(&window);
    }

    fn oldest_window(&self) -> Option<Window> {
        let active_windows = self.active_windows.lock().unwrap();
        active_windows.iter().next().cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_assign_windows_length_divisible_by_slide() {
        // Create a sliding windower with 60s window length and 20s slide
        let windower = SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(20));

        // Base time: 60000ms (60s)
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

        // Create a test message with event time at base_time + 10s
        let msg = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["test_key".to_string()]),
            tags: None,
            value: "test_value".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: Utc
                .timestamp_millis_opt(base_time.timestamp_millis() + 10000)
                .unwrap(),
            watermark: None,
            id: Default::default(),
            headers: HashMap::new(),
            metadata: None,
        };

        // Assign windows
        let window_msgs = windower.assign_windows(msg.clone());

        // Verify results - should be assigned to exactly 3 windows
        assert_eq!(window_msgs.len(), 3);

        // Check first window: [60000, 120000)
        assert_eq!(window_msgs[0].window.start_time.timestamp_millis(), 60000);
        assert_eq!(window_msgs[0].window.end_time.timestamp_millis(), 120000);
        match &window_msgs[0].operation {
            WindowOperation::Open(_) => {}
            _ => panic!("Expected Open operation"),
        }

        assert_eq!(window_msgs[1].window.start_time.timestamp_millis(), 40000);
        assert_eq!(window_msgs[1].window.end_time.timestamp_millis(), 100000);
        match &window_msgs[1].operation {
            WindowOperation::Open(_) => {}
            _ => panic!("Expected Open operation"),
        }

        assert_eq!(window_msgs[2].window.start_time.timestamp_millis(), 20000);
        assert_eq!(window_msgs[2].window.end_time.timestamp_millis(), 80000);
        match &window_msgs[2].operation {
            WindowOperation::Open(_) => {}
            _ => panic!("Expected Open operation"),
        }

        // Assign another message to the same windows
        let msg2 = Message {
            event_time: Utc
                .timestamp_millis_opt(base_time.timestamp_millis() + 1000)
                .unwrap(),
            ..msg.clone()
        };

        let window_msgs2 = windower.assign_windows(msg2.clone());

        // Should also be assigned to 3 windows
        assert_eq!(window_msgs2.len(), 3);

        // All operations should be Append
        for window_msg in &window_msgs2 {
            match &window_msg.operation {
                WindowOperation::Append(_) => {}
                _ => panic!("Expected Append operation"),
            }
        }
    }

    #[tokio::test]
    async fn test_assign_windows_length_not_divisible_by_slide() {
        // Create a sliding windower with 60s window length and 40s slide
        let windower = SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(40));

        // Base time: 600s
        let base_time = Utc.timestamp_opt(600, 0).unwrap();

        // Create a test message with event time at base_time + 10s
        let msg = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["test_key".to_string()]),
            tags: None,
            value: "test_value".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: Utc.timestamp_opt(base_time.timestamp() + 10, 0).unwrap(),
            watermark: None,
            id: Default::default(),
            headers: HashMap::new(),
            metadata: None,
        };

        // Assign windows
        let window_msgs = windower.assign_windows(msg.clone());

        // Verify results - should be assigned to exactly 2 windows
        assert_eq!(window_msgs.len(), 2);

        assert_eq!(window_msgs[0].window.start_time.timestamp(), 600);
        assert_eq!(window_msgs[0].window.end_time.timestamp(), 660);
        match &window_msgs[0].operation {
            WindowOperation::Open(_) => {}
            _ => panic!("Expected Open operation"),
        }

        // Check second window: [560, 620)
        assert_eq!(window_msgs[1].window.start_time.timestamp(), 560);
        assert_eq!(window_msgs[1].window.end_time.timestamp(), 620);
        match &window_msgs[1].operation {
            WindowOperation::Open(_) => {}
            _ => panic!("Expected Open operation"),
        }

        // Assign another message to the same windows
        let msg2 = Message {
            event_time: Utc.timestamp_opt(base_time.timestamp() + 1, 0).unwrap(),
            ..msg.clone()
        };

        let window_msgs2 = windower.assign_windows(msg2.clone());

        // Should also be assigned to 2 windows
        assert_eq!(window_msgs2.len(), 2);

        // All operations should be Append
        for window_msg in &window_msgs2 {
            match &window_msg.operation {
                WindowOperation::Append(_) => {}
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
            let mut active_windows = windower.active_windows.lock().unwrap();
            active_windows.insert(window1.clone());
            active_windows.insert(window2.clone());
            active_windows.insert(window3.clone());
        }

        // Close windows with watermark at base_time + 120s
        let closed = windower.close_windows(base_time + chrono::Duration::seconds(120));

        // Should close all 3 windows
        assert_eq!(closed.len(), 3);

        // Check that all windows are closed in order of end time

        assert_eq!(
            closed[0].window.start_time,
            base_time - chrono::Duration::seconds(20)
        );
        assert_eq!(
            closed[0].window.end_time,
            base_time + chrono::Duration::seconds(40)
        );
        match &closed[0].operation {
            WindowOperation::Close => {}
            _ => panic!("Expected Close operation"),
        }

        assert_eq!(
            closed[1].window.start_time,
            base_time - chrono::Duration::seconds(10)
        );
        assert_eq!(
            closed[1].window.end_time,
            base_time + chrono::Duration::seconds(50)
        );
        match &closed[1].operation {
            WindowOperation::Close => {}
            _ => panic!("Expected Close operation"),
        }

        assert_eq!(closed[2].window.start_time, base_time);
        assert_eq!(
            closed[2].window.end_time,
            base_time + chrono::Duration::seconds(60)
        );
        match &closed[2].operation {
            WindowOperation::Close => {}
            _ => panic!("Expected Close operation"),
        }

        // Verify all windows are closed
        {
            let active_windows = windower.active_windows.lock().unwrap();
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
            let mut active_windows = windower.active_windows.lock().unwrap();
            active_windows.insert(window1.clone());
            active_windows.insert(window2.clone());
            active_windows.insert(window3.clone());
        }

        // Verify all windows exist
        {
            let active_windows = windower.active_windows.lock().unwrap();
            assert_eq!(active_windows.len(), 3);
        }

        // Delete window1
        windower.delete_window(window1.clone());

        // Verify window1 is deleted
        {
            let active_windows = windower.active_windows.lock().unwrap();
            assert_eq!(active_windows.len(), 2);
            assert!(!active_windows.contains(&window1));
            assert!(active_windows.contains(&window2));
            assert!(active_windows.contains(&window3));
        }

        // Delete window2
        windower.delete_window(window2.clone());

        // Verify window2 is deleted
        {
            let active_windows = windower.active_windows.lock().unwrap();
            assert_eq!(active_windows.len(), 1);
            assert!(!active_windows.contains(&window1));
            assert!(!active_windows.contains(&window2));
            assert!(active_windows.contains(&window3));
        }

        // Delete window3
        windower.delete_window(window3.clone());

        // Verify window3 is deleted
        {
            let active_windows = windower.active_windows.lock().unwrap();
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
            let mut active_windows = windower.active_windows.lock().unwrap();
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

        // Delete window3 (the oldest)
        windower.delete_window(window3.clone());

        // Verify the oldest window end time is now window2
        assert_eq!(
            windower
                .oldest_window()
                .unwrap()
                .end_time
                .timestamp_millis(),
            (base_time + chrono::Duration::seconds(50)).timestamp_millis()
        );

        // Delete window2
        windower.delete_window(window2.clone());

        // Verify oldest window end time is now window1
        assert_eq!(
            windower
                .oldest_window()
                .unwrap()
                .end_time
                .timestamp_millis(),
            (base_time + chrono::Duration::seconds(60)).timestamp_millis()
        );

        // Delete window1
        windower.delete_window(window1.clone());
        assert_eq!(windower.oldest_window(), None);
    }

    #[test]
    fn test_assign_windows_with_small_slide() {
        // Create a sliding windower with 60s window length and 10s slide
        let windower = SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(10));

        // Create a test message with event time at 105s
        let msg = Message {
            event_time: Utc.timestamp_millis_opt(105000).unwrap(),
            ..Default::default()
        };

        // Pre-populate expected windows
        // For event time 105000ms (105s):
        // Window 1: [100000, 160000) - event falls in this window
        // Window 2: [90000, 150000) - event falls in this window
        // Window 3: [80000, 140000) - event falls in this window
        // Window 4: [70000, 130000) - event falls in this window
        // Window 5: [60000, 120000) - event falls in this window
        // Window 6: [50000, 110000) - event falls in this window
        let expected_windows = vec![
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
        let window_msgs = windower.assign_windows(msg.clone());

        // Verify results - should be assigned to exactly 6 windows
        assert_eq!(window_msgs.len(), expected_windows.len());

        // Check all windows match expected windows
        for (i, window_msg) in window_msgs.iter().enumerate() {
            // Check operation type
            match &window_msg.operation {
                WindowOperation::Open(open_msg) => {
                    assert_eq!(open_msg.event_time, msg.event_time);
                }
                _ => panic!("Expected Open operation"),
            }

            // Verify window matches expected window
            let window = &window_msg.window;
            assert_eq!(window.start_time, expected_windows[i].start_time);
            assert_eq!(window.end_time, expected_windows[i].end_time);
        }

        // Assign another message to the same window
        let msg2 = Message {
            event_time: Utc.timestamp_millis_opt(105000).unwrap(),
            ..Default::default()
        };

        let window_msgs_two = windower.assign_windows(msg2.clone());

        // Should also be assigned to 6 windows
        assert_eq!(window_msgs_two.len(), 6);

        for window_msg in &window_msgs_two {
            match &window_msg.operation {
                WindowOperation::Append(append_msg) => {
                    assert_eq!(append_msg.event_time, msg2.event_time);
                }
                _ => panic!("Expected Append operation"),
            }
        }
    }
}
