use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::message::Message;
use crate::reduce::reducer::unaligned::windower::{
    SHARED_PNF_SLOT, UnalignedWindowMessage, UnalignedWindowOperation, Window,
};

/// Represents the state of an accumulator window, tracking message timestamps
#[derive(Debug, Clone)]
struct WindowState {
    /// The window this state belongs to
    window: Window,
    /// Sorted list of message timestamps, used for watermark calculation
    message_timestamps: Arc<RwLock<BTreeSet<DateTime<Utc>>>>,
    /// Last seen event time for this window
    last_seen_event_time: Arc<RwLock<DateTime<Utc>>>,
}

impl WindowState {
    fn new(window: Window) -> Self {
        Self {
            window,
            message_timestamps: Arc::new(RwLock::new(BTreeSet::new())),
            last_seen_event_time: Arc::new(RwLock::new(
                DateTime::from_timestamp_millis(0).unwrap(),
            )),
        }
    }

    /// Adds the event time to the message timestamps in sorted order
    fn append_timestamp(&self, event_time: DateTime<Utc>) {
        let mut timestamps = self.message_timestamps.write().expect("Poisoned lock");
        timestamps.insert(event_time);

        // Update last seen event time if this is newer
        let mut last_seen = self.last_seen_event_time.write().expect("Poisoned lock");
        if event_time > *last_seen {
            *last_seen = event_time;
        }
    }

    /// Deletes event times before the given end time
    fn delete_timestamps_before(&self, end_time: DateTime<Utc>) {
        let mut timestamps = self.message_timestamps.write().expect("Poisoned lock");

        // Remove all timestamps before end_time (keep timestamps >= end_time)
        timestamps.retain(|ts| *ts >= end_time);

        let mut last_seen = self.last_seen_event_time.write().expect("Poisoned lock");
        // Update last seen event time if needed
        if !timestamps.is_empty() {
            *last_seen = timestamps.iter().next().cloned().unwrap();
        } else {
            // set it to 0
            *last_seen = DateTime::from_timestamp_millis(0).unwrap();
        }
    }

    /// Gets the oldest timestamp in this window state
    fn oldest_timestamp(&self) -> Option<DateTime<Utc>> {
        let timestamps = self.message_timestamps.read().expect("Poisoned lock");
        timestamps.iter().next().cloned()
    }
}

/// AccumulatorWindowManager manages accumulator windows, which are similar to global windows
/// but with timeout-based expiration.
#[derive(Debug, Clone)]
pub(crate) struct AccumulatorWindowManager {
    /// Timeout duration after which inactive windows are closed
    timeout: Duration,
    /// Active windows mapped by combined key
    active_windows: Arc<RwLock<HashMap<String, WindowState>>>,
}

impl AccumulatorWindowManager {
    /// Creates a new AccumulatorWindowManager with the specified timeout
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            active_windows: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Combines keys into a single string for use as a map key
    fn combine_keys(keys: &[String]) -> String {
        keys.join(":")
    }

    /// Assigns windows to a message. For accumulator windows, each key gets exactly one window.
    pub(crate) fn assign_windows(&self, msg: Message) -> Vec<UnalignedWindowMessage> {
        let combined_key = Self::combine_keys(&msg.keys);
        let mut result = Vec::new();

        // Check if we already have a window for this key
        let mut active_windows = self.active_windows.write().expect("Poisoned lock");

        if let Some(window_state) = active_windows.get(&combined_key) {
            // Window exists, append message
            window_state.append_timestamp(msg.event_time);

            result.push(UnalignedWindowMessage {
                operation: UnalignedWindowOperation::Append {
                    message: msg.clone(),
                    window: window_state.window.clone(),
                },
                pnf_slot: SHARED_PNF_SLOT,
            });
        } else {
            // Create a new window for this key
            let window = Window::new(
                Utc::now(),
                Utc::now() + chrono::Duration::from_std(self.timeout).unwrap(),
                Arc::clone(&msg.keys),
            );

            let window_state = WindowState::new(window.clone());
            window_state.append_timestamp(msg.event_time);

            active_windows.insert(combined_key, window_state);

            result.push(UnalignedWindowMessage {
                operation: UnalignedWindowOperation::Open {
                    message: msg.clone(),
                    window,
                },
                pnf_slot: SHARED_PNF_SLOT,
            });
        }

        result
    }

    /// Closes windows that have been inactive for longer than the timeout
    pub(crate) fn close_windows(&self, current_time: DateTime<Utc>) -> Vec<UnalignedWindowMessage> {
        let mut result = Vec::new();
        let mut keys_to_delete = Vec::new();

        // Find windows to close
        let active_windows = self.active_windows.read().expect("Poisoned lock");

        for (key, window_state) in active_windows.iter() {
            let last_seen = *window_state
                .last_seen_event_time
                .read()
                .expect("Poisoned lock");

            // If the last event time plus timeout is before current time, close the window
            if current_time > last_seen + chrono::Duration::from_std(self.timeout).unwrap() {
                result.push(UnalignedWindowMessage {
                    operation: UnalignedWindowOperation::Close {
                        window: window_state.window.clone(),
                    },
                    pnf_slot: SHARED_PNF_SLOT,
                });
                keys_to_delete.push(key.clone());
            }
        }

        // Remove closed windows
        if !keys_to_delete.is_empty() {
            drop(active_windows); // Release read lock before acquiring write lock

            let mut active_windows = self.active_windows.write().expect("Poisoned lock");
            for key in keys_to_delete {
                active_windows.remove(&key);
            }
        }

        result
    }

    /// Deletes event times before the given window's end time for the given keyed window
    pub(crate) fn delete_window(&self, window: Window) {
        let combined_key = Self::combine_keys(&window.keys);

        let active_windows = self.active_windows.read().expect("Poisoned lock");
        if let Some(window_state) = active_windows.get(&combined_key) {
            window_state.delete_timestamps_before(window.end_time);
        }
    }

    /// Returns the oldest event time across all windows
    pub(crate) fn oldest_window_end_time(&self) -> Option<DateTime<Utc>> {
        let active_windows = self.active_windows.read().expect("Poisoned lock");

        let mut oldest_time = None;

        for window_state in active_windows.values() {
            if let Some(timestamp) = window_state.oldest_timestamp() {
                match oldest_time {
                    None => oldest_time = Some(timestamp),
                    Some(current_oldest) if timestamp < current_oldest => {
                        oldest_time = Some(timestamp)
                    }
                    _ => {}
                }
            }
        }

        oldest_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_assign_windows_new_key() {
        // Create accumulator windower with 60s timeout
        let windower = AccumulatorWindowManager::new(Duration::from_secs(60));

        // Create a test message
        let msg = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["test_key".to_string()]),
            tags: None,
            value: "test_value".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: Utc::now(),
            ..Default::default()
        };

        // Assign windows
        let window_msgs = windower.assign_windows(msg.clone());

        // Verify results - should be assigned to exactly 1 window with Open operation
        assert_eq!(window_msgs.len(), 1);
        if let UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Open { window, .. },
            ..
        } = &window_msgs[0]
        {
            assert_eq!(window.keys, msg.keys);
        } else {
            panic!("Expected Open message");
        }
    }

    #[test]
    fn test_assign_windows_existing_key() {
        // Create accumulator windower with 60s timeout
        let windower = AccumulatorWindowManager::new(Duration::from_secs(60));

        // Create a test message
        let msg = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["test_key".to_string()]),
            tags: None,
            value: "test_value".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: Utc::now(),
            ..Default::default()
        };

        // Assign windows for the first time
        windower.assign_windows(msg.clone());

        // Assign windows for the second time
        let window_msgs = windower.assign_windows(msg.clone());

        // Verify results - should be assigned to exactly 1 window with Append operation
        assert_eq!(window_msgs.len(), 1);
        if let UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Append { window, .. },
            ..
        } = &window_msgs[0]
        {
            assert_eq!(window.keys, msg.keys);
        } else {
            panic!("Expected Append message");
        }
    }

    #[test]
    fn test_close_windows() {
        // Create accumulator windower with 1s timeout
        let windower = AccumulatorWindowManager::new(Duration::from_secs(1));

        // Create a test message
        let msg = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["test_key".to_string()]),
            tags: None,
            value: "test_value".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: Utc::now() - chrono::Duration::seconds(2),
            ..Default::default()
        };

        // Assign windows
        windower.assign_windows(msg.clone());

        // Close windows with current time
        let closed = windower.close_windows(Utc::now());

        // Should close 1 window
        if let UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Close { window },
            ..
        } = &closed[0]
        {
            assert_eq!(window.keys, msg.keys);
        } else {
            panic!("Expected Close message");
        }

        // Verify window was removed
        assert!(windower.active_windows.read().unwrap().is_empty());
    }

    #[test]
    fn test_delete_closed_window() {
        // Create accumulator windower with 60s timeout
        let windower = AccumulatorWindowManager::new(Duration::from_secs(60));

        // Create test messages with different event times
        let msg1 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["test_key".to_string()]),
            tags: None,
            value: "test_value1".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: Utc::now() - chrono::Duration::seconds(30),
            ..Default::default()
        };

        let msg2 = Message {
            event_time: Utc::now() - chrono::Duration::seconds(10),
            ..msg1.clone()
        };

        // Assign both messages to windows
        windower.assign_windows(msg1.clone());
        windower.assign_windows(msg2.clone());

        // Get the window
        let window = {
            let active_windows = windower.active_windows.read().unwrap();
            let window_state = active_windows
                .get(&AccumulatorWindowManager::combine_keys(&msg1.keys))
                .unwrap();
            window_state.window.clone()
        };

        // Delete timestamps before msg2's event time
        windower.delete_window(Window {
            end_time: msg2.event_time,
            ..window
        });

        // Verify only msg2's timestamp remains
        let active_windows = windower.active_windows.read().unwrap();
        let window_state = active_windows
            .get(&AccumulatorWindowManager::combine_keys(&msg1.keys))
            .unwrap();
        let timestamps = window_state.message_timestamps.read().unwrap();

        assert_eq!(timestamps.len(), 1);
        assert_eq!(*timestamps.iter().next().unwrap(), msg2.event_time);
    }

    #[test]
    fn test_oldest_window_end_time() {
        // Create accumulator windower with 60s timeout
        let windower = AccumulatorWindowManager::new(Duration::from_secs(60));

        // Create test messages with different keys and event times
        let msg1 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".to_string()]),
            tags: None,
            value: "test_value1".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: Utc::now() - chrono::Duration::seconds(30),
            ..Default::default()
        };

        let msg2 = Message {
            keys: Arc::from(vec!["key2".to_string()]),
            event_time: Utc::now() - chrono::Duration::seconds(20),
            ..msg1.clone()
        };

        // Assign messages to windows
        windower.assign_windows(msg1.clone());
        windower.assign_windows(msg2.clone());

        // Verify the oldest window end time is msg1's event time
        let oldest_time = windower.oldest_window_end_time().unwrap();
        assert_eq!(oldest_time, msg1.event_time);
    }
}
