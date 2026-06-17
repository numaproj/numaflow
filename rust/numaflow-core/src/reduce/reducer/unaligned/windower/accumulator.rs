//! # Accumulator Window Manager
//!
//! AccumulatorWindowManager manages [Accumulator Windows], is very similar to [Session Windows], the
//! windows are tracked at key level and for every key combination we will have a new global window,
//! The window is created when the first message arrives for the key, the window state is cleared when
//! there are no messages for a timeout duration. We track the timestamps of the messages in the window
//! and use that to calculate the lowest watermark. When we get the response from the SDK for a keyed
//! window, all the messages with event time less than the response watermark will be deleted from the
//! window. Once the timestamps are empty for a window and if it's idle for the timeout duration, the
//! window will be closed. Similar to session windows we use a shared pnf slot because windows are
//! tracked at the key level. Same pnf slot will be used for all the windows, using different window
//! operations we decide what operation to be performed on the keyed window. We only have on single
//! [WAL] for all the windows and compaction is done based on the deleted windows.
//!
//!
//! ## Window Operations
//!
//! - **Open**: Create a new accumulator window when first message arrives for a key (Global window)
//! - **Append**: Add subsequent messages to the existing window for the same key
//! - **Close**: Close the window when there are no messages for a timeout duration and clear the window
//!   state.
//!
//! [Accumulator Windows]: https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/windowing/accumulator/
//! [WAL]: crate::reduce::wal
//! [Session Windows]: crate::reduce::reducer::unaligned::windower::session

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::message::Message;
use crate::reduce::reducer::unaligned::windower::{
    SHARED_PNF_SLOT, UnalignedWindowMessage, UnalignedWindowOperation, Window, combine_keys,
};
use chrono::{DateTime, Utc};

/// Represents the key global state of an accumulator window.
#[derive(Debug, Clone)]
struct WindowState {
    /// The window this state belongs to and this Window is a per key Global Window from event-time
    /// of the first message to +oo (end time is [`DateTime::<Utc>::MAX_UTC`]).
    window: Window,
    /// Sorted list of all the message timestamps for this particular window till the chunk of data is
    /// written to ISB (events for a chunk is done because the watermark has progressed as SDK has processed/sorted
    /// it). We need to store all the timestamps because this is a global state for the keyed window
    /// and there will be timestamps after the WM which has to be retained. This WindowState is not closed
    /// but only a part of it is truncated when watermark progresses.
    /// Watermark is published using this message_timestamps, and the published watermark will be
    /// <= the oldest timestamp in this list after the truncation.
    message_timestamps: Arc<RwLock<BTreeSet<DateTime<Utc>>>>,
    /// maxEventTime is the maximum (latest) event time we have ever seen for this window. This cannot
    /// be messageTimestamps\[last] because that list will be truncated as the watermark progresses, so
    /// we track it separately and it never decreases. Since WM is global (across keys) we need to
    /// trigger a close window to the UDF once the timeout has passed (WM > max-event-time + timeout),
    /// and the close request carries (max-event-time + timeout) as the window end time.
    max_event_time: Arc<RwLock<DateTime<Utc>>>,
}

impl WindowState {
    fn new(window: Window) -> Self {
        Self {
            window,
            message_timestamps: Arc::new(RwLock::new(BTreeSet::new())),
            max_event_time: Arc::new(RwLock::new(DateTime::from_timestamp_millis(0).unwrap())),
        }
    }

    /// Adds the event time to the message timestamps in sorted order.
    fn append_timestamp(&self, event_time: DateTime<Utc>) {
        {
            let mut timestamps = self.message_timestamps.write().expect("Poisoned lock");
            timestamps.insert(event_time);
        }

        // Update max event time if this is newer. This is monotonic and never decreases.
        let mut max_event_time = self.max_event_time.write().expect("Poisoned lock");
        if event_time > *max_event_time {
            *max_event_time = event_time;
        }
    }

    /// Deletes event times before the given end time. Unlike [`Self::append_timestamp`], this does
    /// not touch [`Self::max_event_time`] since that has to reflect the latest event time ever seen
    /// (used to compute the close request's window end time) and must not be lowered by truncation.
    /// Returns true if there are no timestamps left after the truncation.
    fn delete_timestamps_before(&self, end_time: DateTime<Utc>) -> bool {
        let mut timestamps = self
            .message_timestamps
            .write()
            .expect("Failed to acquire write lock on message_timestamps");

        // Retain timestamps greater than or equal to the end time
        timestamps.retain(|&ts| ts >= end_time);
        timestamps.is_empty()
    }

    /// Gets the oldest timestamp in this window state.
    fn oldest_timestamp(&self) -> Option<DateTime<Utc>> {
        let timestamps = self.message_timestamps.read().expect("Poisoned lock");
        timestamps.iter().next().cloned()
    }

    /// Returns the maximum event time ever seen for this window.
    fn max_event_time(&self) -> DateTime<Utc> {
        *self.max_event_time.read().expect("Poisoned lock")
    }
}

/// AccumulatorWindowManager manages accumulator [WindowState], which are similar to global windows
/// but with timeout-based expiration.
#[derive(Debug, Clone)]
pub(crate) struct AccumulatorWindowManager {
    /// Timeout duration after which inactive windows are closed.
    timeout: Duration,
    /// Active windows mapped by combined key.
    active_windows: Arc<RwLock<HashMap<String, WindowState>>>,
    /// Closed windows mapped by combined key. These windows have been closed (sent to the SDK) but
    /// still have message timestamps that are pending GC. We keep them around so that the oldest
    /// event time used for watermark propagation also accounts for closed-but-not-yet-GC'd windows.
    /// An entry is removed once its timestamps are fully drained (see [`Self::delete_window`]).
    closed_windows: Arc<RwLock<HashMap<String, WindowState>>>,
}

impl AccumulatorWindowManager {
    /// Creates a new AccumulatorWindowManager with the specified timeout.
    pub(crate) fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            active_windows: Arc::new(RwLock::new(HashMap::new())),
            closed_windows: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Assigns windows to a message. For accumulator windows, each key gets exactly one window.
    pub(crate) fn assign_windows(&self, msg: Message) -> Vec<UnalignedWindowMessage> {
        let combined_key = combine_keys(&msg.keys);
        let event_time = msg.event_time;
        let keys = Arc::clone(&msg.keys);

        let mut active_windows = self.active_windows.write().expect("Poisoned lock");
        // Check if a window already exists for the key, if exits we can do append else we will have
        // to create a new window for the key
        if let Some(window_state) = active_windows.get(&combined_key) {
            window_state.append_timestamp(event_time);
            return vec![UnalignedWindowMessage {
                operation: UnalignedWindowOperation::Append {
                    message: msg,
                    window: window_state.window.clone(),
                },
                pnf_slot: SHARED_PNF_SLOT,
            }];
        }

        // Create a new window for the key. The window is global per key, so we use the event time of
        // the first message as the start time and the max possible time as the end time. The actual
        // end time (max-event-time + timeout) is computed lazily when the close request is sent.
        let window = Window::new(event_time, DateTime::<Utc>::MAX_UTC, keys);

        let window_state = WindowState::new(window.clone());
        window_state.append_timestamp(event_time);

        active_windows.insert(combined_key, window_state);

        vec![UnalignedWindowMessage {
            operation: UnalignedWindowOperation::Open {
                message: msg,
                window,
            },
            pnf_slot: SHARED_PNF_SLOT,
        }]
    }

    /// Closes windows that have been inactive for longer than the timeout. This function is called
    /// when the watermark has progressed, and we need to close the windows that are inactive for
    /// longer than the timeout.
    pub(crate) fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<UnalignedWindowMessage> {
        let timeout = chrono::Duration::from_std(self.timeout).expect("valid timeout duration");

        // Extract windows that have been inactive for longer than the timeout from the active set.
        let expired: Vec<(String, WindowState)> = {
            let mut active_windows = self.active_windows.write().expect("Poisoned lock");
            active_windows
                .extract_if(|_, window_state| watermark > window_state.max_event_time() + timeout)
                .collect()
        };

        if expired.is_empty() {
            return vec![];
        }

        let mut closed_windows = self.closed_windows.write().expect("Poisoned lock");
        let mut messages = Vec::with_capacity(expired.len());

        for (combined_key, window_state) in expired {
            // The close request carries the actual window end time, which is max-event-time + timeout
            // (the stored window has MAX_UTC as the end time since it is a global window).
            let close_window = Window::new(
                window_state.window.start_time,
                window_state.max_event_time() + timeout,
                Arc::clone(&window_state.window.keys),
            );

            messages.push(UnalignedWindowMessage {
                operation: UnalignedWindowOperation::Close {
                    window: close_window,
                },
                pnf_slot: SHARED_PNF_SLOT,
            });

            // Move the closed window to the closed set so that it is still accounted for in
            // watermark propagation until its timestamps are GC'd.
            closed_windows.insert(combined_key, window_state);
        }

        messages
    }

    /// Deletes all the timestamps before the given end time for the window. The window can either be
    /// still active or already closed (closed but not yet GC'd). For a closed window, once all of its
    /// timestamps are drained, its entry is removed from the closed windows map.
    pub(crate) fn delete_window(&self, window: Window) {
        let combined_key = combine_keys(&window.keys);

        // First try the active windows. Active windows are never removed here; they are only
        // truncated and removed when they are closed because of inactivity.
        {
            let active_windows = self.active_windows.read().expect("Poisoned lock");
            if let Some(window_state) = active_windows.get(&combined_key) {
                window_state.delete_timestamps_before(window.end_time);
                return;
            }
        }

        // Otherwise the window must be in the closed set. Truncate its timestamps and drop the entry
        // once it is fully drained so that it no longer participates in watermark propagation.
        let mut closed_windows = self.closed_windows.write().expect("Poisoned lock");
        if let Some(window_state) = closed_windows.get(&combined_key) {
            let drained = window_state.delete_timestamps_before(window.end_time);
            if drained {
                closed_windows.remove(&combined_key);
            }
        }
    }

    /// Returns the oldest event time across all windows, both active and closed-but-not-yet-GC'd.
    /// This is used for watermark propagation so that the watermark does not advance past data that
    /// is still pending in a closed window.
    pub(crate) fn oldest_window_end_time(&self) -> Option<DateTime<Utc>> {
        let active_windows = self.active_windows.read().expect("Poisoned lock");
        let closed_windows = self.closed_windows.read().expect("Poisoned lock");

        active_windows
            .values()
            .chain(closed_windows.values())
            .filter_map(|window_state| window_state.oldest_timestamp())
            .min()
    }

    /// Returns the number of currently active windows (unique key slots).
    pub(crate) fn active_window_count(&self) -> usize {
        self.active_windows.read().expect("Poisoned lock").len()
    }

    /// Returns the number of closed windows that are awaiting GC.
    pub(crate) fn closed_window_count(&self) -> usize {
        self.closed_windows.read().expect("Poisoned lock").len()
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
        } = window_msgs.first().expect("Expected window message")
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
        } = window_msgs.first().expect("Expected window message")
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
        } = closed.first().expect("Expected closed window message")
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
            let window_state = active_windows.get(&combine_keys(&msg1.keys)).unwrap();
            window_state.window.clone()
        };

        // Delete timestamps before msg2's event time
        windower.delete_window(Window {
            end_time: msg2.event_time,
            ..window
        });

        // Verify only msg2's timestamp remains
        let active_windows = windower.active_windows.read().unwrap();
        let window_state = active_windows.get(&combine_keys(&msg1.keys)).unwrap();
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
