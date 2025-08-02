//! # Session Window Manager
//!
//! SessionWindowManager manages [Session Window], a type of unaligned window where the window's
//! end time keeps moving until there is no data for a given time duration. Unlike fixed and sliding
//! windows, session windows are tracked at key level, and they don't have a fixed start and end time.
//! Multiple session windows share a common pnf slot, since windows are tracked at the key level it's
//! not optimal to create bidirectional streams with the SDK for every window, so we use a shared pnf
//! slot. Using the common slot we decide what operation to be performed on the keyed window. Below
//! are the different operations that can be performed on a session window. Unlike other window types,
//! we have concept of merging windows, since the windows are dynamic in nature we can end up creating
//! multiple windows for the same key which can be merged later.  We only have on single [WAL] for all
//! the windows and compaction is done based on the deleted windows.
//!
//! ## Different Session Window Operations
//!
//! - **Open**: Create a new window when a message arrives for a new key or after timeout
//! - **Append**: Add message to existing window when it falls within the current window bounds
//! - **Expand**: Extend window boundaries when a message extends beyond current start/end times
//! - **Merge**: Combine multiple windows when an out-of-order message bridges the gap
//! - **Close**: Finalize window when no messages arrive within the timeout duration
//!
//! [Session Window]: https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/windowing/session/
//! [WAL]: crate::reduce::wal

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::message::Message;
use crate::reduce::reducer::unaligned::windower;
use crate::reduce::reducer::unaligned::windower::{
    SHARED_PNF_SLOT, UnalignedWindowMessage, UnalignedWindowOperation, Window,
};
use chrono::{DateTime, Utc};

/// Active session windows for every key combination (combinedKey -> Sorted window set)
type ActiveWindowStore = Arc<RwLock<HashMap<String, BTreeSet<Window>>>>;

/// Closed session windows sorted by end time. We need to keep track of closed windows so that we can
/// find the oldest window computed and forwarded. The watermark is progressed based on the latest
/// closed window end time. The oldest window in the active_windows will be greater than the latest
/// closed window.
type ClosedWindowStore = Arc<RwLock<BTreeSet<Window>>>;

/// SessionWindowManager manages session windows.
#[derive(Debug, Clone)]
pub(crate) struct SessionWindowManager {
    /// Timeout duration after which inactive windows are closed.
    timeout: Duration,
    /// Active windows mapped by combined key (joined keys)
    /// Windows are sorted by end time within each key using Window's Ord implementation.
    active_windows: ActiveWindowStore,
    /// Closed windows sorted by end time. These windows have been closed but not yet garbage collected.
    closed_windows: ClosedWindowStore,
}

impl SessionWindowManager {
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            active_windows: Arc::new(RwLock::new(HashMap::new())),
            closed_windows: Arc::new(RwLock::new(BTreeSet::new())),
        }
    }

    /// Assigns windows to a message, we create a new window for the key with start time as the event time
    /// and end time as the event time + timeout.
    /// * If the start and end time of the existing key is same - append operation
    /// * If the start and end time can be expanded to accommodate the new window - expand operation
    /// * If the window is not present we will create a new window - open operation
    pub(crate) fn assign_windows(&self, msg: Message) -> Vec<UnalignedWindowMessage> {
        let combined_key = windower::combine_keys(&msg.keys);
        let event_time = msg.event_time;
        let keys = Arc::clone(&msg.keys);

        // truncate the event time to nearest millisecond, watermark precision is in milliseconds.
        let start_time_millis = event_time.timestamp_millis();
        let start_time = chrono::DateTime::from_timestamp_millis(start_time_millis).unwrap();

        // Calculate end time as start + timeout (in milliseconds)
        let timeout_millis = self.timeout.as_millis() as i64;
        let end_time =
            chrono::DateTime::from_timestamp_millis(start_time_millis + timeout_millis).unwrap();

        let new_window = Window::new(start_time, end_time, keys);

        let mut active_windows = self.active_windows.write().expect("Poisoned lock");
        let window_set = active_windows.entry(combined_key).or_default();

        if let Some(existing_window) = Self::find_window_to_merge(window_set, &new_window) {
            let expanded_window = Self::expand_window_if_needed(&new_window, &existing_window);

            if let Some(expanded_window) = expanded_window {
                window_set.remove(&existing_window);
                window_set.insert(expanded_window.clone());
                return vec![UnalignedWindowMessage {
                    operation: UnalignedWindowOperation::Expand {
                        message: msg,
                        windows: vec![existing_window, expanded_window],
                    },
                    pnf_slot: SHARED_PNF_SLOT,
                }];
            }

            return vec![UnalignedWindowMessage {
                operation: UnalignedWindowOperation::Append {
                    message: msg,
                    window: existing_window,
                },
                pnf_slot: SHARED_PNF_SLOT,
            }];
        }

        window_set.insert(new_window.clone());
        vec![UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Open {
                message: msg,
                window: new_window,
            },
            pnf_slot: SHARED_PNF_SLOT,
        }]
    }

    /// expands the window if the windows overlap.
    /// If windows overlap, always expand to accommodate both windows.
    /// If the new window is completely contained within existing boundaries, just append to existing window.
    /// Windows must overlap first (considering end time is exclusive) before they can be expanded.
    fn expand_window_if_needed(new_window: &Window, existing_window: &Window) -> Option<Window> {
        // First check if windows overlap (end time is exclusive, start time is inclusive)
        // [a,b) and [c,d) overlap if a < d and c < b
        let windows_overlap = new_window.start_time < existing_window.end_time
            && existing_window.start_time < new_window.end_time;

        if !windows_overlap {
            return None;
        }

        // Check if the new window is completely contained within the existing window
        // If so, this should be an append operation, not an expand operation
        let new_window_contained = new_window.start_time >= existing_window.start_time
            && new_window.end_time <= existing_window.end_time;

        if new_window_contained {
            return None; // No expansion needed, should append to existing window
        }

        // If windows overlap and expansion is needed, expand to accommodate both windows
        // Use min of start times and max of end times
        Some(Window::new(
            new_window.start_time.min(existing_window.start_time),
            new_window.end_time.max(existing_window.end_time),
            Arc::clone(&new_window.keys),
        ))
    }

    /// finds a window that can be merged with the given window.
    /// Windows overlap if they intersect, considering that end time is exclusive and start time is inclusive.
    /// For windows [a,b) and [c,d), they overlap if a < d and c < b.
    fn find_window_to_merge(window_set: &BTreeSet<Window>, window: &Window) -> Option<Window> {
        // TODO: this is not efficient, since its sorted we should be able to find the window in O(log n) time.
        window_set
            .iter()
            .find(|existing_window| {
                // Windows overlap if they intersect (end time is exclusive, start time is inclusive)
                // [a,b) and [c,d) overlap if a < d and c < b
                window.start_time < existing_window.end_time
                    && existing_window.start_time < window.end_time
            })
            .cloned()
    }

    /// Closes windows that have been inactive for longer than the timeout and will also merge windows
    /// that can be closed. The reason we have multiple windows that could be merged is because we
    /// might open multiple sessions for the same key if the messages are out of order and their
    /// window.end_time might grow overtime and overlap. At the time of close, we merge these windows
    /// those have overlapping end times.
    pub(crate) fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<UnalignedWindowMessage> {
        // Extract and remove expired windows from active windows
        let closed_windows_by_key = {
            let mut active_windows = self.active_windows.write().expect("Poisoned lock");
            Self::extract_expired_windows(&mut active_windows, watermark)
        };

        // Process each key's closed windows
        closed_windows_by_key
            .into_iter()
            .flat_map(|(key, windows)| self.process_closed_windows(&key, windows))
            .collect()
    }

    /// Extract expired windows from the window set. These are the windows that have not received any
    /// messages within the timeout duration (i.e, watermark > window.end_time where window.end_time
    /// includes the timeout).
    fn extract_expired_windows(
        all_windows: &mut HashMap<String, BTreeSet<Window>>,
        watermark: DateTime<Utc>,
    ) -> HashMap<String, Vec<Window>> {
        let mut closed_windows_by_key = HashMap::new();

        for (key, window_set) in all_windows.iter_mut() {
            // First collect expired windows to avoid borrowing issues
            let expired_windows: Vec<Window> = window_set
                .iter()
                .filter(|window| window.end_time <= watermark)
                .cloned()
                .collect();

            if !expired_windows.is_empty() {
                // Remove expired windows from the set
                for window in &expired_windows {
                    window_set.remove(window);
                }
                closed_windows_by_key.insert(key.clone(), expired_windows);
            }
        }

        // Remove the keys that have no active windows left
        all_windows.retain(|_, window_set| !window_set.is_empty());
        closed_windows_by_key
    }

    /// Process closed windows for a specific key. This function will merge the windows if possible
    /// and then close the windows that cannot be merged.
    fn process_closed_windows(
        &self,
        key: &str,
        closed_windows: Vec<Window>,
    ) -> Vec<UnalignedWindowMessage> {
        Self::windows_that_can_be_merged(&closed_windows)
            .into_iter()
            .filter_map(|group| self.process_close_window_group(key, group))
            .flatten()
            .collect()
    }

    /// Process multiple closing windows by merging them first, then attempting to merge with active windows
    fn process_close_window_group(
        &self,
        key: &str,
        closing_group: Vec<Window>,
    ) -> Option<Vec<UnalignedWindowMessage>> {
        if closing_group.is_empty() {
            return None;
        }

        let mut messages = Vec::new();
        let (merge_result, window) = if closing_group.len() == 1 {
            let window = closing_group.into_iter().next().unwrap();
            let mut active_windows = self.active_windows.write().expect("Poisoned lock");
            (
                Self::try_merge_with_active(&mut active_windows, key, &window),
                window,
            )
        } else {
            let (merged_window, initial_merge_msg) = Self::merge_windows(&closing_group);
            messages.push(initial_merge_msg);
            let mut active_windows = self.active_windows.write().expect("Poisoned lock");
            (
                Self::try_merge_with_active(&mut active_windows, key, &merged_window),
                merged_window,
            )
        };

        let final_message = match merge_result {
            Some((old_active, _new_merged)) => UnalignedWindowMessage {
                operation: UnalignedWindowOperation::Merge {
                    windows: vec![window, old_active],
                },
                pnf_slot: SHARED_PNF_SLOT,
            },
            None => {
                self.move_to_closed_windows(&window);
                UnalignedWindowMessage {
                    operation: UnalignedWindowOperation::Close { window },
                    pnf_slot: SHARED_PNF_SLOT,
                }
            }
        };

        messages.push(final_message);
        Some(messages)
    }

    /// Helper to move a window to the closed windows store
    fn move_to_closed_windows(&self, window: &Window) {
        self.closed_windows
            .write()
            .expect("Poisoned lock")
            .insert(window.clone());
    }

    /// Try to merge a window with active windows during the close operation.
    fn try_merge_with_active(
        active_windows: &mut HashMap<String, BTreeSet<Window>>,
        key: &str,
        window: &Window,
    ) -> Option<(Window, Window)> {
        let window_set = active_windows.get_mut(key)?;
        let active_window = Self::find_window_to_merge(window_set, window)?;

        window_set.remove(&active_window);

        let new_merged = Window::new(
            window.start_time.min(active_window.start_time),
            window.end_time.max(active_window.end_time),
            Arc::clone(&window.keys),
        );

        window_set.insert(new_merged.clone());
        Some((active_window, new_merged))
    }

    /// Merge multiple windows into a single window
    fn merge_windows(windows: &[Window]) -> (Window, UnalignedWindowMessage) {
        let first = windows.first().expect("should have first window");

        let (start_time, end_time) = windows.iter().fold(
            (first.start_time, first.end_time),
            |(min_start, max_end), window| {
                (
                    min_start.min(window.start_time),
                    max_end.max(window.end_time),
                )
            },
        );

        (
            Window::new(start_time, end_time, Arc::clone(&first.keys)),
            UnalignedWindowMessage {
                operation: UnalignedWindowOperation::Merge {
                    windows: windows.to_vec(),
                },
                pnf_slot: SHARED_PNF_SLOT,
            },
        )
    }

    /// Groups windows that can be merged together.
    /// This function takes a slice of windows (each window defined by a start and end time)
    /// and returns a slice of slices of windows that can be merged based on their overlapping times.
    /// A window can be merged with another if its end time is after the start time of the next window.
    ///
    /// For example, given the windows (75, 85), (60, 90), (80, 100) and (110, 120),
    /// the function returns Vec<Vec<Window>>{{(60, 90), (75, 85), (80, 100)}, {(110, 120)}}
    /// because the first three windows overlap and can be merged, while the last window stands alone.
    ///
    /// Note: The input windows are assumed to be sorted by end time in ascending order.
    fn windows_that_can_be_merged(windows: &[Window]) -> Vec<Vec<Window>> {
        // If there are no windows, return empty vec
        if windows.is_empty() {
            return Vec::new();
        }

        // Initialize an empty vec to hold slices of mergeable windows
        let mut merged_groups = Vec::new();

        let mut i = windows.len();
        // Reverse iterate over the windows because it is sorted by end-time in ascending order.
        while i > 0 {
            i -= 1;

            // Initialize a slice to hold the current window and any subsequent mergeable windows
            let mut merged_group = vec![windows.get(i).expect("should have window").clone()];

            // Set the last window to be the current window
            let mut last_window = windows.get(i).expect("should have window").clone();

            // Check if the end time of the last window is after the start time of the previous window
            // If it is that means they should be merged, add the previous window to the merged slice
            // and update the end time of the last window
            while i > 0
                && windows
                    .get(i - 1)
                    .expect("should have previous window")
                    .end_time
                    > last_window.start_time
            {
                i -= 1;
                merged_group.push(windows.get(i).expect("should have window").clone());

                // Merge the window into last_window to expand the range
                let current_window = windows.get(i).expect("should have window");
                if current_window.start_time < last_window.start_time {
                    last_window.start_time = current_window.start_time;
                }
                if current_window.end_time > last_window.end_time {
                    last_window.end_time = current_window.end_time;
                }
            }

            // Add the merged slice to the slice of all mergeable windows
            merged_groups.push(merged_group);
        }

        // reverse so that the smaller windows gets merged and closed first
        merged_groups.reverse();
        merged_groups
    }

    /// Deletes a window from the closed windows list after garbage collection
    pub(crate) fn delete_window(&self, window: Window) {
        // Remove the window from closed_windows
        self.closed_windows
            .write()
            .expect("Poisoned lock")
            .remove(&window);
    }

    /// Returns the end time of the oldest window among both active and closed windows
    pub(crate) fn oldest_window_end_time(&self) -> Option<DateTime<Utc>> {
        // Get the oldest window from closed_windows first, if closed_windows is empty, get the oldest
        // from active_windows
        // NOTE: closed windows will always have a lower end time than active_windows

        {
            let closed_windows = self.closed_windows.read().expect("Poisoned lock");
            if let Some(window) = closed_windows.iter().next() {
                return Some(window.end_time);
            }
        }

        let active_windows = self.active_windows.read().expect("Poisoned lock");
        active_windows
            .values()
            .filter_map(|windows| windows.iter().next())
            .map(|window| window.end_time)
            .min()
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
        if let UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Expand { windows, .. },
            ..
        } = &window_msgs[0]
        {
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
        if let UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Expand { windows, .. },
            ..
        } = &window_msgs[0]
        {
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
        if let UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Close { window },
            ..
        } = &closed[0]
        {
            assert_eq!(window.keys, msg.keys);
        } else {
            panic!("Expected Close message");
        }
    }

    #[test]
    fn test_windows_that_can_be_merged() {
        // Create session windower with 60s timeout
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

        // First group should have 2 windows (window2 and window1)
        assert_eq!(merged_groups[0].len(), 2);

        // Second group should  have 1 window (window3)
        assert_eq!(merged_groups[1].len(), 1);
    }

    #[test]
    fn test_closed_windows_tracking() {
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

        // Assign windows - this should create an active window
        windower.assign_windows(msg.clone());

        // Verify window is in active_windows
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 1);
            let closed_windows = windower.closed_windows.read().unwrap();
            assert_eq!(closed_windows.len(), 0);
        }

        // Close windows with current time - this should move window to closed_windows
        let closed = windower.close_windows(now);
        assert_eq!(closed.len(), 1);

        // Verify window is now in closed_windows and not in active_windows
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 0);
            let closed_windows = windower.closed_windows.read().unwrap();
            assert_eq!(closed_windows.len(), 1);
        }

        // Get the closed window for deletion
        let closed_window = if let UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Close { window },
            ..
        } = &closed[0]
        {
            window.clone()
        } else {
            panic!("Expected Close message");
        };

        // Delete the closed window - this should remove it from closed_windows
        windower.delete_window(closed_window);

        // Verify window is removed from closed_windows
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 0);
            let closed_windows = windower.closed_windows.read().unwrap();
            assert_eq!(closed_windows.len(), 0);
        }
    }

    #[test]
    fn test_oldest_window_end_time_with_closed_windows() {
        // Create session windower with 5s timeout
        let windower = SessionWindowManager::new(Duration::from_secs(5));
        let now = Utc::now();

        // Create first message (older) - window will be [now-10s, now-5s]
        let msg1 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key1".to_string()]),
            tags: None,
            value: "test_value1".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: now - chrono::Duration::seconds(10),
            ..Default::default()
        };

        // Create second message (newer) - window will be [now-5s, now]
        let msg2 = Message {
            typ: Default::default(),
            keys: Arc::from(vec!["key2".to_string()]),
            tags: None,
            value: "test_value2".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: now - chrono::Duration::seconds(5),
            ..Default::default()
        };

        // Assign both messages to create active windows
        windower.assign_windows(msg1.clone());
        windower.assign_windows(msg2.clone());

        // Close the older window by setting watermark to close only the first window
        // msg1 window end time: (now - 10s) + 5s = now - 5s
        // msg2 window end time: (now - 5s) + 5s = now
        // watermark: now - 3s should close msg1's window but not msg2's
        let watermark = now - chrono::Duration::seconds(3);
        let closed = windower.close_windows(watermark);
        assert_eq!(closed.len(), 1);

        // Now we should have one closed window (older) and one active window (newer)
        // The oldest window end time should be from the closed window
        let oldest_time = windower.oldest_window_end_time().unwrap();
        let expected_oldest =
            msg1.event_time + chrono::Duration::from_std(windower.timeout).unwrap();
        assert_eq!(
            oldest_time.timestamp_millis(),
            expected_oldest.timestamp_millis()
        );

        // Delete the closed window
        if let UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Close { window },
            ..
        } = &closed[0]
        {
            windower.delete_window(window.clone());
        }

        // Now the oldest window end time should be from the active window
        let oldest_time = windower.oldest_window_end_time().unwrap();
        let expected_oldest =
            msg2.event_time + chrono::Duration::from_std(windower.timeout).unwrap();
        assert_eq!(
            oldest_time.timestamp_millis(),
            expected_oldest.timestamp_millis()
        );
    }

    #[test]
    fn test_exclusive_end_time_no_overlap() {
        // Test that windows [5,10) and [10,15) are NOT considered overlapping
        let now = Utc::now();

        let window1 = Window::new(
            now + chrono::Duration::seconds(5),
            now + chrono::Duration::seconds(10),
            Arc::from(vec!["test_key".to_string()]),
        );
        let window2 = Window::new(
            now + chrono::Duration::seconds(10),
            now + chrono::Duration::seconds(15),
            Arc::from(vec!["test_key".to_string()]),
        );

        // Test find_window_to_merge
        let mut window_set = BTreeSet::new();
        window_set.insert(window1.clone());

        let result = SessionWindowManager::find_window_to_merge(&window_set, &window2);
        assert!(
            result.is_none(),
            "Windows [5,10) and [10,15) should NOT overlap"
        );

        // Test expand_window_if_needed
        let expansion = SessionWindowManager::expand_window_if_needed(&window2, &window1);
        assert!(
            expansion.is_none(),
            "Windows [5,10) and [10,15) should NOT be expandable"
        );
    }

    #[test]
    fn test_overlapping_windows_can_expand() {
        // Test that windows [5,11) and [10,15) CAN be expanded
        let now = Utc::now();

        let window1 = Window::new(
            now + chrono::Duration::seconds(5),
            now + chrono::Duration::seconds(11),
            Arc::from(vec!["test_key".to_string()]),
        );
        let window2 = Window::new(
            now + chrono::Duration::seconds(10),
            now + chrono::Duration::seconds(15),
            Arc::from(vec!["test_key".to_string()]),
        );

        // Test find_window_to_merge
        let mut window_set = BTreeSet::new();
        window_set.insert(window1.clone());

        let result = SessionWindowManager::find_window_to_merge(&window_set, &window2);
        assert!(
            result.is_some(),
            "Windows [5,11) and [10,15) should overlap"
        );

        // Test expand_window_if_needed - window2 extends beyond window1
        let expansion = SessionWindowManager::expand_window_if_needed(&window2, &window1);
        assert!(
            expansion.is_some(),
            "Windows [5,11) and [10,15) should be expandable"
        );

        let expanded = expansion.unwrap();
        assert_eq!(expanded.start_time, now + chrono::Duration::seconds(5));
        assert_eq!(expanded.end_time, now + chrono::Duration::seconds(15));
    }

    #[test]
    fn test_specific_contained_window_scenario() {
        // Test the specific scenario: existing window [10, 30), new window [20, 30)
        let now = Utc::now();

        let existing_window = Window::new(
            now + chrono::Duration::seconds(10),
            now + chrono::Duration::seconds(30),
            Arc::from(vec!["test_key".to_string()]),
        );
        let new_window = Window::new(
            now + chrono::Duration::seconds(20),
            now + chrono::Duration::seconds(30),
            Arc::from(vec!["test_key".to_string()]),
        );

        // Test that this should be an append operation (expand_window_if_needed returns None)
        let expansion =
            SessionWindowManager::expand_window_if_needed(&new_window, &existing_window);
        assert!(
            expansion.is_none(),
            "Window [20,30) should append to existing window [10,30), not expand"
        );

        // Test find_window_to_merge still finds the window for merging
        let mut window_set = BTreeSet::new();
        window_set.insert(existing_window.clone());

        let result = SessionWindowManager::find_window_to_merge(&window_set, &new_window);
        assert!(
            result.is_some(),
            "Windows should still be found for merging"
        );
        assert_eq!(result.unwrap(), existing_window);
    }

    #[test]
    fn test_comprehensive_window_scenarios() {
        let now = Utc::now();
        let key: Arc<[String]> = Arc::from(vec!["test_key".to_string()]);

        let existing_window =
            Window::new(now, now + chrono::Duration::seconds(100), Arc::clone(&key));

        // Scenario 1: New window contained in existing [20, 80) contained in [0, 100)
        let contained_window = Window::new(
            now + chrono::Duration::seconds(20),
            now + chrono::Duration::seconds(80),
            Arc::clone(&key),
        );

        let expansion =
            SessionWindowManager::expand_window_if_needed(&contained_window, &existing_window);
        assert!(
            expansion.is_none(),
            "Contained window should trigger append, not expand"
        );

        // Scenario 2: Existing window contained in new window [-10, 120) contains [0, 100)
        let larger_window = Window::new(
            now - chrono::Duration::seconds(10),
            now + chrono::Duration::seconds(120),
            Arc::clone(&key),
        );

        let expansion2 =
            SessionWindowManager::expand_window_if_needed(&larger_window, &existing_window);
        assert!(expansion2.is_some(), "Larger window should trigger expand");
        let expanded = expansion2.unwrap();
        assert_eq!(expanded.start_time, now - chrono::Duration::seconds(10));
        assert_eq!(expanded.end_time, now + chrono::Duration::seconds(120));

        // Scenario 3: Identical windows [0, 100) and [0, 100)
        let identical_window =
            Window::new(now, now + chrono::Duration::seconds(100), Arc::clone(&key));

        let expansion3 =
            SessionWindowManager::expand_window_if_needed(&identical_window, &existing_window);
        assert!(
            expansion3.is_none(),
            "Identical windows should trigger append, not expand"
        );

        // Scenario 4: Partial overlap requiring expansion [50, 150) overlaps [0, 100)
        let partial_overlap_window = Window::new(
            now + chrono::Duration::seconds(50),
            now + chrono::Duration::seconds(150),
            Arc::clone(&key),
        );

        let expansion4 = SessionWindowManager::expand_window_if_needed(
            &partial_overlap_window,
            &existing_window,
        );
        assert!(
            expansion4.is_some(),
            "Partial overlap should trigger expand"
        );
        let expanded2 = expansion4.unwrap();
        assert_eq!(expanded2.start_time, now);
        assert_eq!(expanded2.end_time, now + chrono::Duration::seconds(150));
    }

    #[test]
    fn test_assign_windows_different_cases() {
        // Test assign_windows flow for realistic session window behavior
        let windower = SessionWindowManager::new(Duration::from_secs(60));
        let now = Utc::now();
        let key: Arc<[String]> = Arc::from(vec!["test_key".to_string()]);

        // Scenario 1: Create initial window with first message
        let msg1 = Message {
            typ: Default::default(),
            keys: Arc::clone(&key),
            tags: None,
            value: "msg1".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: now,
            ..Default::default()
        };

        let result1 = windower.assign_windows(msg1);
        assert_eq!(result1.len(), 1);
        match &result1[0].operation {
            UnalignedWindowOperation::Open { window, .. } => {
                assert_eq!(window.start_time.timestamp_millis(), now.timestamp_millis());
                assert_eq!(
                    window.end_time.timestamp_millis(),
                    (now + chrono::Duration::seconds(60)).timestamp_millis()
                );
            }
            _ => panic!("Expected Open operation for first message"),
        }

        // Scenario 2: Test expansion - new message extends existing window
        let msg2 = Message {
            typ: Default::default(),
            keys: Arc::clone(&key),
            tags: None,
            value: "msg2".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: now + chrono::Duration::seconds(30), // Creates [now+30, now+90) which extends [now, now+60)
            ..Default::default()
        };

        let result2 = windower.assign_windows(msg2);
        assert_eq!(result2.len(), 1);
        match &result2[0].operation {
            UnalignedWindowOperation::Expand { windows, .. } => {
                assert_eq!(windows.len(), 2);
                let old_window = &windows[0];
                let new_window = &windows[1];
                assert_eq!(
                    old_window.start_time.timestamp_millis(),
                    now.timestamp_millis()
                );
                assert_eq!(
                    old_window.end_time.timestamp_millis(),
                    (now + chrono::Duration::seconds(60)).timestamp_millis()
                );
                assert_eq!(
                    new_window.start_time.timestamp_millis(),
                    now.timestamp_millis()
                );
                assert_eq!(
                    new_window.end_time.timestamp_millis(),
                    (now + chrono::Duration::seconds(90)).timestamp_millis()
                );
            }
            _ => panic!("Expected Expand operation, got {:?}", result2[0].operation),
        }

        // Scenario 3: Test expansion in the other direction - message before existing window
        let msg3 = Message {
            typ: Default::default(),
            keys: Arc::clone(&key),
            tags: None,
            value: "msg3".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: now - chrono::Duration::seconds(10), // Creates [now-10, now+50) which extends [now, now+90)
            ..Default::default()
        };

        let result3 = windower.assign_windows(msg3);
        assert_eq!(result3.len(), 1);
        match &result3[0].operation {
            UnalignedWindowOperation::Expand { windows, .. } => {
                assert_eq!(windows.len(), 2);
                let old_window = &windows[0];
                let new_window = &windows[1];
                assert_eq!(
                    old_window.start_time.timestamp_millis(),
                    now.timestamp_millis()
                );
                assert_eq!(
                    old_window.end_time.timestamp_millis(),
                    (now + chrono::Duration::seconds(90)).timestamp_millis()
                );
                assert_eq!(
                    new_window.start_time.timestamp_millis(),
                    (now - chrono::Duration::seconds(10)).timestamp_millis()
                );
                assert_eq!(
                    new_window.end_time.timestamp_millis(),
                    (now + chrono::Duration::seconds(90)).timestamp_millis()
                );
            }
            _ => panic!("Expected Expand operation, got {:?}", result3[0].operation),
        }

        // Scenario 4: Test with a new key to create a separate window
        let key2: Arc<[String]> = Arc::from(vec!["different_key".to_string()]);
        let msg4 = Message {
            typ: Default::default(),
            keys: Arc::clone(&key2),
            tags: None,
            value: "msg4".as_bytes().to_vec().into(),
            offset: Default::default(),
            event_time: now + chrono::Duration::seconds(20),
            ..Default::default()
        };

        let result4 = windower.assign_windows(msg4);
        assert_eq!(result4.len(), 1);
        match &result4[0].operation {
            UnalignedWindowOperation::Open { window, .. } => {
                assert_eq!(
                    window.start_time.timestamp_millis(),
                    (now + chrono::Duration::seconds(20)).timestamp_millis()
                );
                assert_eq!(
                    window.end_time.timestamp_millis(),
                    (now + chrono::Duration::seconds(80)).timestamp_millis()
                );
                assert_eq!(window.keys, key2);
            }
            _ => panic!("Expected Open operation for different key"),
        }
    }
}
