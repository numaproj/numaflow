use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};

use crate::message::Message;
use crate::reduce::pnf::aligned::windower::{
    AlignedWindowMessage, FixedWindowMessage, Window, WindowOperation, Windower,
};

#[derive(Debug, Clone)]
pub(crate) struct FixedWindower {
    /// Duration of each window
    window_length: Duration,
    /// Active windows sorted by end time
    active_windows: Arc<Mutex<BTreeMap<DateTime<Utc>, Window>>>,
}

impl FixedWindower {
    pub(crate) fn new(window_length: Duration) -> Self {
        Self {
            window_length,
            active_windows: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Creates a new window for the given message
    fn create_window(&self, msg: &Message) -> Window {
        // Truncate event time to window length
        let window_length_millis = self.window_length.as_millis() as i64;
        let event_time_millis = msg.event_time.timestamp_millis();
        let start_time_millis = (event_time_millis / window_length_millis) * window_length_millis;
        let end_time_millis = start_time_millis + window_length_millis;

        let start_time = Utc.timestamp_millis_opt(start_time_millis).unwrap();
        let end_time = Utc.timestamp_millis_opt(end_time_millis).unwrap();

        Window::new(start_time, end_time)
    }
}

impl Windower for FixedWindower {
    fn assign_windows(&self, msg: Message) -> Vec<AlignedWindowMessage> {
        let window = self.create_window(&msg);
        let mut result = Vec::new();

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
        let window_msg = AlignedWindowMessage::Fixed(FixedWindowMessage { operation, window });

        result.push(window_msg);
        result
    }

    fn close_windows(&self) -> Vec<AlignedWindowMessage> {
        let self_clone = self.clone();

        // For now, we're not implementing window closing based on watermarks
        // This would typically close windows that are past the watermark
        Vec::new()
    }

    fn delete_window(&self, window: Window) {
        let mut active_windows = self.active_windows.lock().unwrap();
        active_windows.remove(&window.end_time);
    }

    fn oldest_window_endtime(&self) -> DateTime<Utc> {
        let active_windows = self.active_windows.lock().unwrap();

        // Return the oldest window end time or a default if no windows exist
        active_windows
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| Utc.timestamp_millis_opt(-1).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_assign_windows() {
        // Create a fixed windower with 60s window length
        let windower = FixedWindower::new(Duration::from_secs(60));

        // Create a test message with event time at 60s
        let msg = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["test_key".to_string()]),
            tags: None,
            value: "test_value".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: Utc.timestamp_millis_opt(60000).unwrap(),
            watermark: None,
            id: Default::default(),
            headers: HashMap::new(),
            metadata: None,
        };

        // Assign windows
        let window_msgs = windower.assign_windows(msg.clone());

        // Verify results
        assert_eq!(window_msgs.len(), 1);

        match &window_msgs[0] {
            AlignedWindowMessage::Fixed(fixed_msg) => {
                // Check window boundaries
                assert_eq!(fixed_msg.window.start_time.timestamp_millis(), 60000);
                assert_eq!(fixed_msg.window.end_time.timestamp_millis(), 120000);

                // Check operation type
                match &fixed_msg.operation {
                    WindowOperation::Open(open_msg) => {
                        assert_eq!(open_msg.event_time, msg.event_time);
                    }
                    _ => panic!("Expected Open operation"),
                }
            }
            _ => panic!("Expected Fixed window message"),
        }

        // Assign another message to the same window
        let msg2 = Message {
            event_time: Utc.timestamp_millis_opt(90000).unwrap(),
            ..msg.clone()
        };

        let window_msgs2 = windower.assign_windows(msg2.clone());

        // Verify it's an Append operation
        match &window_msgs2[0] {
            AlignedWindowMessage::Fixed(fixed_msg) => match &fixed_msg.operation {
                WindowOperation::Append(append_msg) => {
                    assert_eq!(append_msg.event_time, msg2.event_time);
                }
                _ => panic!("Expected Append operation"),
            },
            _ => panic!("Expected Fixed window message"),
        }
    }

    #[test]
    fn test_delete_window() {
        // Create a fixed windower with 60s window length
        let windower = FixedWindower::new(Duration::from_secs(60));

        // Create a window
        let window = Window::new(
            Utc.timestamp_millis_opt(60000).unwrap(),
            Utc.timestamp_millis_opt(120000).unwrap(),
        );

        // Insert window manually
        {
            let mut active_windows = windower.active_windows.lock().unwrap();
            active_windows.insert(window.end_time, window.clone());
        }

        // Verify window exists
        {
            let active_windows = windower.active_windows.lock().unwrap();
            assert!(active_windows.contains_key(&window.end_time));
        }

        // Delete window
        windower.delete_window(window.clone());

        // Verify window was deleted
        {
            let active_windows = windower.active_windows.lock().unwrap();
            assert!(!active_windows.contains_key(&window.end_time));
        }
    }

    #[test]
    fn test_oldest_window_endtime() {
        // Create a fixed windower with 60s window length
        let windower = FixedWindower::new(Duration::from_secs(60));

        // Create windows
        let window1 = Window::new(
            Utc.timestamp_millis_opt(60000).unwrap(),
            Utc.timestamp_millis_opt(120000).unwrap(),
        );

        let window2 = Window::new(
            Utc.timestamp_millis_opt(120000).unwrap(),
            Utc.timestamp_millis_opt(180000).unwrap(),
        );

        // Insert windows manually
        {
            let mut active_windows = windower.active_windows.lock().unwrap();
            active_windows.insert(window1.end_time, window1.clone());
            active_windows.insert(window2.end_time, window2.clone());
        }

        // Verify oldest window end time
        assert_eq!(windower.oldest_window_endtime().timestamp_millis(), 120000);
    }
}
