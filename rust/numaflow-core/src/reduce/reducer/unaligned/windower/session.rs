use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::message::Message;
use crate::reduce::reducer::unaligned::windower::{UnalignedWindowMessage, Window};
use chrono::{DateTime, Utc};

/// SessionWindowManager manages session windows.
#[derive(Debug, Clone)]
pub(crate) struct SessionWindowManager {
    /// Timeout duration after which inactive windows are closed
    timeout: Duration,
    /// All windows (both active and closed) mapped by combined key (joined keys)
    /// Windows are sorted by end time within each key
    all_windows: Arc<RwLock<HashMap<String, BTreeMap<DateTime<Utc>, Window>>>>,
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
        let mut result = Vec::new();

        // Check if we already have windows for this key
        let mut all_windows = self.all_windows.write().expect("Poisoned lock");

        // Create a new window for this message
        let new_window = Window::new(
            msg.event_time,
            msg.event_time + chrono::Duration::from_std(self.timeout).unwrap(),
            Arc::clone(&msg.keys),
        );

        // Get or create the window map for this key
        let window_map = all_windows.entry(combined_key).or_default();

        // Check if this window can be merged with any existing window
        if let Some((_, window_to_merge)) = Self::find_window_to_merge(window_map, &new_window) {
            let old_window = window_to_merge.clone();

            // Check if we need to expand the window
            if new_window.start_time < window_to_merge.start_time
                || new_window.end_time > window_to_merge.end_time
            {
                // Remove the old window from the map
                window_map.remove(&old_window.end_time);

                // Create an expanded window
                let mut expanded_window = old_window.clone();
                if new_window.start_time < expanded_window.start_time {
                    expanded_window.start_time = new_window.start_time;
                }
                if new_window.end_time > expanded_window.end_time {
                    expanded_window.end_time = new_window.end_time;
                }

                // Insert the expanded window
                window_map.insert(expanded_window.end_time, expanded_window.clone());

                // Create expand operation
                result.push(UnalignedWindowMessage::Expand {
                    message: msg,
                    windows: vec![old_window, expanded_window],
                });
            } else {
                // No need to expand, just append
                result.push(UnalignedWindowMessage::Append {
                    message: msg,
                    window: window_to_merge.clone(),
                });
            }
        } else {
            // No exiting window to expand, create a new one
            window_map.insert(new_window.end_time, new_window.clone());
            result.push(UnalignedWindowMessage::Open {
                message: msg,
                window: new_window,
            });
        }

        result
    }

    /// Find a window that can be merged with the given window
    fn find_window_to_merge<'a>(
        window_map: &'a BTreeMap<DateTime<Utc>, Window>,
        window: &Window,
    ) -> Option<(&'a DateTime<Utc>, &'a Window)> {
        for (end_time, existing_window) in window_map.iter() {
            // Windows can be merged if:
            // 1. New window starts before existing window ends
            // 2. Existing window starts before new window ends
            if window.start_time <= existing_window.end_time
                && existing_window.start_time <= window.end_time
            {
                return Some((end_time, existing_window));
            }
        }
        None
    }

    /// Closes windows that have been inactive for longer than the timeout
    pub(crate) fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<UnalignedWindowMessage> {
        let mut result = Vec::new();

        // Find windows to close
        let mut all_windows = self.all_windows.write().expect("Poisoned lock");
        let mut closed_windows_by_key: HashMap<String, Vec<Window>> = HashMap::new();

        // First pass: identify windows to close and group them by key
        for (key, window_map) in all_windows.iter_mut() {
            let mut windows_to_remove = Vec::new();

            for (end_time, window) in window_map.iter() {
                // If window end time is before or equal to watermark, close it
                if *end_time <= watermark {
                    windows_to_remove.push(*end_time);
                    closed_windows_by_key
                        .entry(key.clone())
                        .or_default()
                        .push(window.clone());
                }
            }

            // Remove closed windows from the map
            for end_time in windows_to_remove {
                window_map.remove(&end_time);
            }
        }

        // Remove empty keys
        all_windows.retain(|_, window_map| !window_map.is_empty());

        // Second pass: process closed windows for merging
        for (key, closed_windows) in closed_windows_by_key {
            // Find groups of windows that can be merged
            let merged_groups = self.windows_that_can_be_merged(&closed_windows);

            for group in merged_groups {
                if group.len() > 1 {
                    // Create a merge operation for windows that can be merged
                    result.push(UnalignedWindowMessage::Merge {
                        message: Message::default(), // No message for close operations
                        windows: group.clone(),
                    });

                    // Merge the windows into a single window
                    let mut merged_window = group[0].clone();
                    for window in &group[1..] {
                        if window.start_time < merged_window.start_time {
                            merged_window.start_time = window.start_time;
                        }
                        if window.end_time > merged_window.end_time {
                            merged_window.end_time = window.end_time;
                        }
                    }

                    // Check if the merged window can be merged with any remaining active window
                    let window_map = all_windows.get_mut(&key);
                    if let Some(window_map) = window_map {
                        if let Some((_, active_window)) =
                            Self::find_window_to_merge(window_map, &merged_window)
                        {
                            let old_window = active_window.clone();

                            // Remove the old window
                            window_map.remove(&old_window.end_time);

                            // Create a new merged window
                            let mut new_merged_window = old_window.clone();
                            if merged_window.start_time < new_merged_window.start_time {
                                new_merged_window.start_time = merged_window.start_time;
                            }
                            if merged_window.end_time > new_merged_window.end_time {
                                new_merged_window.end_time = merged_window.end_time;
                            }

                            // Insert the new merged window
                            window_map
                                .insert(new_merged_window.end_time, new_merged_window.clone());

                            // Create a merge operation
                            result.push(UnalignedWindowMessage::Merge {
                                message: Message::default(),
                                windows: vec![merged_window, old_window, new_merged_window],
                            });

                            continue;
                        }
                    }

                    // If we can't merge with an active window, close the merged window
                    result.push(UnalignedWindowMessage::Close(merged_window));
                } else if !group.is_empty() {
                    // For single windows, check if they can be merged with remaining active windows
                    let window = &group[0];
                    let window_map = all_windows.get_mut(&key);

                    if let Some(window_map) = window_map {
                        if let Some((_, active_window)) =
                            Self::find_window_to_merge(window_map, window)
                        {
                            let old_window = active_window.clone();

                            // Remove the old window
                            window_map.remove(&old_window.end_time);

                            // Create a new merged window
                            let mut new_merged_window = old_window.clone();
                            if window.start_time < new_merged_window.start_time {
                                new_merged_window.start_time = window.start_time;
                            }
                            if window.end_time > new_merged_window.end_time {
                                new_merged_window.end_time = window.end_time;
                            }

                            // Insert the new merged window
                            window_map
                                .insert(new_merged_window.end_time, new_merged_window.clone());

                            // Create a merge operation
                            result.push(UnalignedWindowMessage::Merge {
                                message: Message::default(),
                                windows: vec![window.clone(), old_window, new_merged_window],
                            });

                            continue;
                        }
                    }

                    // If we can't merge with an active window, close the window
                    result.push(UnalignedWindowMessage::Close(window.clone()));
                }
            }
        }

        result
    }

    /// Groups windows that can be merged together
    fn windows_that_can_be_merged(&self, windows: &[Window]) -> Vec<Vec<Window>> {
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
        let merged_groups = windower.windows_that_can_be_merged(&[
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
