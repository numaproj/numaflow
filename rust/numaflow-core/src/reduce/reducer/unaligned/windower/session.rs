use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::message::Message;
use crate::reduce::reducer::unaligned::windower::{UnalignedWindowMessage, Window};
use chrono::{DateTime, Utc};

type WindowStore = Arc<RwLock<HashMap<String, BTreeMap<DateTime<Utc>, Window>>>>;

/// SessionWindowManager manages session windows.
#[derive(Debug, Clone)]
pub(crate) struct SessionWindowManager {
    /// Timeout duration after which inactive windows are closed
    timeout: Duration,
    /// All windows (both active and closed) mapped by combined key (joined keys)
    /// Windows are sorted by end time within each key
    all_windows: WindowStore,
}

impl SessionWindowManager {
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            all_windows: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Combines keys into a single string for use as a map key
    fn combine_keys(keys: &[String]) -> String {
        keys.join(":")
    }

    /// Assigns windows to a message based on session window logic
    pub(crate) fn assign_windows(&self, msg: Message) -> Vec<UnalignedWindowMessage> {
        let combined_key = Self::combine_keys(&msg.keys);
        let mut all_windows = self.all_windows.write().expect("Poisoned lock");

        let new_window = Window::new(
            msg.event_time,
            msg.event_time + chrono::Duration::from_std(self.timeout).unwrap(),
            Arc::clone(&msg.keys),
        );

        let window_map = all_windows.entry(combined_key).or_default();

        match Self::find_window_to_merge(window_map, &new_window) {
            Some(existing_window) => {
                let needs_expansion = new_window.start_time < existing_window.start_time
                    || new_window.end_time > existing_window.end_time;

                if needs_expansion {
                    let old_window = existing_window.clone();
                    window_map.remove(&old_window.end_time);

                    let expanded_window = Window::new(
                        new_window.start_time.min(old_window.start_time),
                        new_window.end_time.max(old_window.end_time),
                        Arc::clone(&msg.keys),
                    );

                    window_map.insert(expanded_window.end_time, expanded_window.clone());

                    vec![UnalignedWindowMessage::Expand {
                        message: msg,
                        windows: vec![old_window, expanded_window],
                    }]
                } else {
                    vec![UnalignedWindowMessage::Append {
                        message: msg,
                        window: existing_window.clone(),
                    }]
                }
            }
            None => {
                window_map.insert(new_window.end_time, new_window.clone());
                vec![UnalignedWindowMessage::Open {
                    message: msg,
                    window: new_window,
                }]
            }
        }
    }

    /// Find a window that can be merged with the given window
    fn find_window_to_merge<'a>(
        window_map: &'a BTreeMap<DateTime<Utc>, Window>,
        window: &Window,
    ) -> Option<&'a Window> {
        window_map.values().find(|existing_window| {
            // Windows overlap if they intersect
            window.start_time <= existing_window.end_time
                && existing_window.start_time <= window.end_time
        })
    }

    /// Closes windows that have been inactive for longer than the timeout
    pub(crate) fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<UnalignedWindowMessage> {
        let mut all_windows = self.all_windows.write().expect("Poisoned lock");

        // Extract and remove expired windows
        let closed_windows_by_key = Self::extract_expired_windows(&mut all_windows, watermark);

        // Process each key's closed windows
        closed_windows_by_key
            .into_iter()
            .flat_map(|(key, windows)| {
                Self::process_closed_windows(&mut all_windows, &key, windows)
            })
            .collect()
    }

    /// Extract expired windows from the window map
    fn extract_expired_windows(
        all_windows: &mut HashMap<String, BTreeMap<DateTime<Utc>, Window>>,
        watermark: DateTime<Utc>,
    ) -> HashMap<String, Vec<Window>> {
        let mut closed_windows_by_key = HashMap::new();

        for (key, window_map) in all_windows.iter_mut() {
            let expired_windows: Vec<_> = window_map
                .iter()
                .filter(|(end_time, _)| **end_time <= watermark)
                .map(|(end_time, window)| (*end_time, window.clone()))
                .collect();

            if !expired_windows.is_empty() {
                for (end_time, _) in &expired_windows {
                    window_map.remove(end_time);
                }

                closed_windows_by_key.insert(
                    key.clone(),
                    expired_windows
                        .into_iter()
                        .map(|(_, window)| window)
                        .collect(),
                );
            }
        }

        // Remove empty keys
        all_windows.retain(|_, window_map| !window_map.is_empty());
        closed_windows_by_key
    }

    /// Process closed windows for a specific key
    fn process_closed_windows(
        all_windows: &mut HashMap<String, BTreeMap<DateTime<Utc>, Window>>,
        key: &str,
        closed_windows: Vec<Window>,
    ) -> Vec<UnalignedWindowMessage> {
        Self::windows_that_can_be_merged(&closed_windows)
            .into_iter()
            .filter_map(|group| Self::process_window_group(all_windows, key, group))
            .collect()
    }

    /// Process a group of windows that can be merged
    fn process_window_group(
        all_windows: &mut HashMap<String, BTreeMap<DateTime<Utc>, Window>>,
        key: &str,
        group: Vec<Window>,
    ) -> Option<UnalignedWindowMessage> {
        if group.is_empty() {
            return None;
        }

        let window_to_close = Self::merge_windows(&group);

        // Try to merge with active windows
        match Self::try_merge_with_active(all_windows, key, &window_to_close) {
            Some((old_active, new_merged)) => Some(UnalignedWindowMessage::Merge {
                windows: vec![window_to_close, old_active, new_merged],
            }),
            None => Some(UnalignedWindowMessage::Close(window_to_close)),
        }
    }

    /// Try to merge a window with active windows
    fn try_merge_with_active(
        all_windows: &mut HashMap<String, BTreeMap<DateTime<Utc>, Window>>,
        key: &str,
        window: &Window,
    ) -> Option<(Window, Window)> {
        let window_map = all_windows.get_mut(key)?;
        let active_window = Self::find_window_to_merge(window_map, window)?.clone();

        window_map.remove(&active_window.end_time);

        let new_merged = Window::new(
            window.start_time.min(active_window.start_time),
            window.end_time.max(active_window.end_time),
            Arc::clone(&window.keys),
        );

        window_map.insert(new_merged.end_time, new_merged.clone());
        Some((active_window, new_merged))
    }

    /// Merge multiple windows into a single window
    fn merge_windows(windows: &[Window]) -> Window {
        let first = &windows[0];

        let (start_time, end_time) = windows.iter().fold(
            (first.start_time, first.end_time),
            |(min_start, max_end), window| {
                (
                    min_start.min(window.start_time),
                    max_end.max(window.end_time),
                )
            },
        );

        Window::new(start_time, end_time, Arc::clone(&first.keys))
    }

    /// Groups windows that can be merged together
    fn windows_that_can_be_merged(windows: &[Window]) -> Vec<Vec<Window>> {
        if windows.is_empty() {
            return Vec::new();
        }

        // Sort windows by end time (descending)
        let mut sorted_windows = windows.to_vec();
        sorted_windows.sort_by(|a, b| b.end_time.cmp(&a.end_time));

        let mut merged_groups = Vec::new();
        let mut i = 0;

        while i < sorted_windows.len() {
            let mut merged_group = vec![sorted_windows[i].clone()];
            let mut last_window = sorted_windows[i].clone();

            i += 1;

            while i < sorted_windows.len() && sorted_windows[i].end_time >= last_window.start_time {
                merged_group.push(sorted_windows[i].clone());

                // Update the last window to include this window
                if sorted_windows[i].start_time < last_window.start_time {
                    last_window.start_time = sorted_windows[i].start_time;
                }

                i += 1;
            }

            merged_groups.push(merged_group);
        }

        merged_groups
    }

    /// Deletes a window from the window list
    pub(crate) fn delete_window(&self, window: Window) {
        let mut all_windows = self.all_windows.write().expect("Poisoned lock");
        let combined_key = Self::combine_keys(&window.keys);

        if let Some(window_map) = all_windows.get_mut(&combined_key) {
            window_map.remove(&window.end_time);

            // Remove the key if no windows left
            if window_map.is_empty() {
                all_windows.remove(&combined_key);
            }
        }
    }

    /// Returns the end time of the oldest window
    pub(crate) fn oldest_window_end_time(&self) -> Option<DateTime<Utc>> {
        let all_windows = self.all_windows.read().expect("Poisoned lock");
        let mut min_end_time = None;

        for window_map in all_windows.values() {
            if let Some((end_time, _)) = window_map.iter().next() {
                if min_end_time.is_none() || *end_time < min_end_time.unwrap() {
                    min_end_time = Some(*end_time);
                }
            }
        }

        min_end_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_assign_windows_new_key() {
        // Create session windower with 60s timeout
        let windower = SessionWindowManager::new(Duration::from_secs(60));

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
        if let UnalignedWindowMessage::Open { window, .. } = &window_msgs[0] {
            assert_eq!(window.keys, msg.keys);
        } else {
            panic!("Expected Open message");
        }
    }

    #[test]
    fn test_assign_windows_existing_key() {
        // Create session windower with 60s timeout
        let windower = SessionWindowManager::new(Duration::from_secs(60));
        let now = Utc::now();

        // Create first message
        let msg1 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["test_key".to_string()]),
            tags: None,
            value: "test_value1".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: now,
            ..Default::default()
        };

        // Assign first message to window - creates window [now, now+60s]
        windower.assign_windows(msg1);

        // Create second message with event time that creates a window completely within the first (should append)
        let msg2 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["test_key".to_string()]),
            tags: None,
            value: "test_value2".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: now + chrono::Duration::seconds(30), // Creates window [now+30, now+90] which extends beyond [now, now+60]
            ..Default::default()
        };

        // Assign second message to window
        let window_msgs = windower.assign_windows(msg2.clone());

        // Verify results - should be assigned to exactly 1 window with Expand operation (not Append)
        // because the new window extends the existing window
        assert_eq!(window_msgs.len(), 1);
        if let UnalignedWindowMessage::Expand { windows, .. } = &window_msgs[0] {
            assert_eq!(windows.len(), 2); // old window and new expanded window
        } else {
            panic!("Expected Expand message, got {:?}", window_msgs[0]);
        }
    }

    #[test]
    fn test_assign_windows_append() {
        // Create session windower with 60s timeout
        let windower = SessionWindowManager::new(Duration::from_secs(60));
        let now = Utc::now();

        // Create first message
        let msg1 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["test_key".to_string()]),
            tags: None,
            value: "test_value1".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: now,
            ..Default::default()
        };

        // Assign first message to window - creates window [now, now+60s]
        windower.assign_windows(msg1);

        // Create second message with event time that creates a window completely within the first (should append)
        let msg2 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["test_key".to_string()]),
            tags: None,
            value: "test_value2".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: now + chrono::Duration::seconds(10), // Creates window [now+10, now+70] but we need it to be contained
            ..Default::default()
        };

        // Assign second message to window
        let window_msgs = windower.assign_windows(msg2.clone());

        // Since [now+10, now+70] extends beyond [now, now+60], this should be an Expand operation
        assert_eq!(window_msgs.len(), 1);
        if let UnalignedWindowMessage::Expand { windows, .. } = &window_msgs[0] {
            assert_eq!(windows.len(), 2); // old window and new expanded window
        } else {
            panic!("Expected Expand message, got {:?}", window_msgs[0]);
        }
    }

    #[test]
    fn test_close_windows() {
        // Create session windower with 1s timeout
        let windower = SessionWindowManager::new(Duration::from_secs(1));
        let now = Utc::now();

        // Create a test message
        let msg = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["test_key".to_string()]),
            tags: None,
            value: "test_value".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: now - chrono::Duration::seconds(2),
            ..Default::default()
        };

        // Assign windows
        windower.assign_windows(msg.clone());

        // Close windows with current time
        let closed = windower.close_windows(now);

        // Should close 1 window
        assert_eq!(closed.len(), 1);
        if let UnalignedWindowMessage::Close(window) = &closed[0] {
            assert_eq!(window.keys, msg.keys);
        } else {
            panic!("Expected Close message");
        }
    }

    #[test]
    fn test_windows_that_can_be_merged() {
        // Create session windower with 60s timeout
        let windower = SessionWindowManager::new(Duration::from_secs(60));
        let now = Utc::now();

        // Create overlapping windows
        let window1 = Window::new(
            now,
            now + chrono::Duration::seconds(10),
            Arc::from(vec!["test_key".to_string()]),
        );
        let window2 = Window::new(
            now + chrono::Duration::seconds(5),
            now + chrono::Duration::seconds(15),
            Arc::from(vec!["test_key".to_string()]),
        );
        let window3 = Window::new(
            now + chrono::Duration::seconds(20),
            now + chrono::Duration::seconds(30),
            Arc::from(vec!["test_key".to_string()]),
        );

        // Test merging
        let merged_groups = SessionWindowManager::windows_that_can_be_merged(&[
            window1.clone(),
            window2.clone(),
            window3.clone(),
        ]);

        // Should have 2 groups: [window3] and [window2, window1]
        // (sorted by end time descending, so window3 comes first)
        assert_eq!(merged_groups.len(), 2);

        // First group should have 1 window (window3)
        assert_eq!(merged_groups[0].len(), 1);

        // Second group should have 2 windows (window2 and window1)
        assert_eq!(merged_groups[1].len(), 2);
    }
}
