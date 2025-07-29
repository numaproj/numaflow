//! Implementation of [Sliding Window] requires a bit more bookkeeping than [Fixed Window],
//! because a single event can belong to multiple windows, but we only have a single [WAL] (we do not
//! have per window WALs). An example of sliding window with length 5s and slide 1s, and an event
//! time at 100 will belong to 5 windows `[95, 100)`, `[96, 101)`, `[97, 102)`, `[98, 103)`, `[99, 104)`.
//! This means that the element has to be to sent to multiple windows. To understand the implementation,
//! we need to look in how we handle window creation in different scenarios (startup, normal operation,
//! restart) but the code is oblivious to the scenario.
//!
//! ## Startup
//! During pipeline startup is the very first time a pipeline is started. It has not materialized any
//! windows yet and just started reading data from the ISB. When we read an element from the ISB with event-time
//! of `t1`, we need to create all windows that can contain `t1`. For example, if the window length is 30s
//! and slide is 10s, and the event-time is 100, we need to create windows `[100, 130)`, `[90, 120)`,
//! and `[80, 110)`.
//!
//! ## Normal Operation
//! During normal operation, when we read an element from the ISB, we need to create all windows that
//! can contain the event-time of the message. For example, if the window length is 30s and slide is
//! 10s, and the event-time is 60, we need to create windows `[60, 90)` but we can assume that
//! the windows `[50, 80)`, and `[40, 70)` has already been created when elements with event-time
//! 90 and 80 were received.
//! Out-of-order can cause the assumption to fail, so we make sure that the previous windows are created
//! if they do not exist. This can happen when we have watermark withholding, and we receive out of order
//! messages.
//!
//! ## Restart
//! When a pipeline is restarted, we will replay from the WAL to restore the state. Restart is a bit
//! tricky because we need to make sure that we do not create duplicate windows. To do that we keep
//! track of the min start time of the active windows. Any window with start time less than this value
//! should not be created again.
//! Let me explain this with an example. Let's say we have a window length of 30s and slide of 10s.
//! and we have already processed elements with event-time 100 and 110 and the watermark has advanced to 110.
//! So we have created windows `[100, 130)`, `[90, 120)`, `[80, 110)`, `[110, 140)`, `[100, 130)`, now
//! let's assume that the window `[80, 110)` is closed and deleted (WM is 110). Now when we restart, we will
//! replay the elements with event-time 100, etc. The reason for replaying element with event-time 100
//! even if the watermark has progressed to 110, is that there are widows with start time less that the
//! watermark. E.g., window`[100, 130)` is still open and data has to be sent to this window. This
//! is the reason why we need the min start time of the active windows.
//! Now to track the state of active windows across restarted, we write this information to a file on
//! shutdown and read it on startup. In case of SIGKILL, the state file will not be created, so
//! we will recreate the closed windows and there will be duplicates causing at-least-once
//! behavior and not a data-loss scenario.
//!
//! [Sliding Window]: (https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/windowing/sliding/)
//! [Fixed Window]: crate::reduce::reducer::aligned::windower::fixed
//! [WAL]: crate::reduce::wal

use std::collections::BTreeSet;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock, atomic::AtomicI64};
use std::time::Duration;

use crate::message::Message;
use crate::reduce::error::{Error, ReduceResult};
use crate::reduce::reducer::aligned::windower::{
    AlignedWindowMessage, AlignedWindowOperation, Window, truncate_to_duration, window_pnf_slot,
};
use crate::shared::grpc::utc_from_timestamp;
use chrono::{DateTime, TimeZone, Utc};
use numaflow_pb::objects::wal::Window as ProtoWindow;
use numaflow_pb::objects::wal::WindowManagerState;
use prost::Message as ProtoMessage;
use tracing::{error, info};

/// Sliding window manager is responsible for managing the sliding windows. It saves the state of the
/// active windows to a file on shutdown and loads it on startup to avoid duplicate processing.
#[derive(Debug, Clone)]
pub(crate) struct SlidingWindowManager {
    /// Duration of each window.
    window_length: Duration,
    /// Slide duration.
    slide: Duration,
    /// Active windows sorted by end time. The state is saved to a file on shutdown and loaded on
    /// startup.
    /// NOTE: During replay we will assume that these windows are opened when we get the first message.
    /// Since we do not differentiate between the first message and subsequent messages, we does an
    /// "append" and in the "append" we "open" the stream if stream doesn't exist.
    /// TODO: perhaps we can differentiate between replayed window vs new window during normal operation.
    active_windows: Arc<RwLock<BTreeSet<Window>>>,
    /// Closed windows sorted by end time. We need to keep track of closed windows so that we can
    /// find the oldest window computed and forwarded. The watermark is progressed based on the latest
    /// closed window end time. The oldest window in the active_window will be greater than the latest
    /// closed window.
    closed_windows: Arc<RwLock<BTreeSet<Window>>>,
    /// Optional path to save/load window state.
    active_window_state_file: Option<PathBuf>,
    /// max end time of the deleted windows, used to avoid creating
    /// windows twice during replay.
    max_deleted_window_end_time: Arc<AtomicI64>,
}

impl SlidingWindowManager {
    /// Creates a new SlidingWindowManager and repopulates the active windows from the state file if
    /// it exists.
    pub(crate) fn new(
        window_length: Duration,
        slide: Duration,
        state_file_path: Option<PathBuf>,
    ) -> Self {
        let manager = Self {
            window_length,
            slide,
            active_windows: Arc::new(RwLock::new(BTreeSet::new())),
            closed_windows: Arc::new(RwLock::new(BTreeSet::new())),
            max_deleted_window_end_time: Arc::new(AtomicI64::new(-1)),
            active_window_state_file: state_file_path,
        };

        // If state file path is provided, try to load state from file
        if let Some(path) = &manager.active_window_state_file
            && let Err(e) = manager.load_state(path)
        {
            error!("Failed to load window state from file: {}", e);
        }

        manager
    }

    /// Creates windows for the given message
    fn create_windows(&self, msg: &Message) -> Vec<Window> {
        let mut windows = Vec::new();
        let window_length_millis = self.window_length.as_millis() as i64;
        let slide_millis = self.slide.as_millis() as i64;
        let event_time_millis = msg.event_time.timestamp_millis();

        // Calculate the start time of the largest window that contains the event
        // use the highest integer multiple of slide length which is less than the eventTime
        // as the start time for the window. For example, if the eventTime is 810 and slide
        // length is 70, use 770 as the startTime of the window. In that way, we can guarantee
        // consistency while assigning the messages to the windows.
        let start_time_millis = truncate_to_duration(event_time_millis, slide_millis);
        let mut current_start = start_time_millis;
        let mut current_end = current_start + window_length_millis;

        // Create all windows that contain this event for example if the event time of the message
        // is 25 with length 5s and slide 1s we will have [21, 26), [22, 27), [23, 28), [24, 29), [25, 30)
        // windows.

        // startTime and endTime will be the largest timestamp window for the given eventTime,
        // using that we can create other windows by subtracting the slide length
        while current_start <= event_time_millis && event_time_millis < current_end {
            let start_time = Utc.timestamp_millis_opt(current_start).unwrap();
            let end_time = Utc.timestamp_millis_opt(current_end).unwrap();

            windows.push(Window::new(start_time, end_time));

            // Move to the previous window
            current_start -= slide_millis;
            current_end -= slide_millis;
        }

        windows
    }

    /// Closes any windows that can be closed because the Watermark has advanced beyond the window
    pub(crate) fn close_windows(&self, watermark: DateTime<Utc>) -> Vec<AlignedWindowMessage> {
        let mut result = Vec::new();

        let windows_to_close = {
            let mut active_windows = self
                .active_windows
                .write()
                .expect("Poisoned lock for active_windows");

            let mut windows_to_close = Vec::new();
            active_windows.retain(|window| {
                if window.end_time <= watermark {
                    windows_to_close.push(window.clone());
                    false
                } else {
                    true
                }
            });

            windows_to_close
        };

        // add the windows to closed_windows
        {
            let mut closed_windows = self
                .closed_windows
                .write()
                .expect("Poisoned lock for closed_windows");
            for window in &windows_to_close {
                closed_windows.insert(window.clone());
            }
        }

        for window in windows_to_close {
            result.push(AlignedWindowMessage {
                pnf_slot: window_pnf_slot(&window),
                operation: AlignedWindowOperation::Close { window },
            });
        }

        result
    }

    /// Deletes a window after it is closed and GC is done.
    pub(crate) fn gc_window(&self, window: Window) {
        // Update max_deleted_window_end_time if this window's end time is greater
        self.max_deleted_window_end_time
            .fetch_max(window.end_time.timestamp_millis(), Ordering::Relaxed);

        // Remove the window from closed_windows
        self.closed_windows
            .write()
            .expect("Poisoned lock for closed_windows")
            .remove(&window);
    }

    /// Returns the oldest window yet to be completed. This will be the lowest Watermark in the Vertex.
    pub(crate) fn oldest_window(&self) -> Option<Window> {
        // get the oldest window from closed_windows, if closed_windows is empty, get the oldest
        // from active_windows
        // NOTE: closed windows will always have a lower end time than active_windows
        {
            let closed_windows = self
                .closed_windows
                .read()
                .expect("Poisoned lock for closed_windows");
            if let Some(window) = closed_windows.iter().next() {
                return Some(window.clone());
            }
        }

        self.active_windows
            .read()
            .expect("Poisoned lock for active_windows")
            .iter()
            .next()
            .cloned()
    }

    /// Helper method to format sorted window information for logging.
    fn format_windows_for_log(windows: &[ProtoWindow]) -> String {
        let formatted_windows: String = windows
            .iter()
            .map(|window| {
                let start_time = utc_from_timestamp(window.start_time.unwrap()).timestamp_millis();
                let end_time = utc_from_timestamp(window.end_time.unwrap()).timestamp_millis();
                format!("[{start_time} - {end_time}]")
            })
            .collect::<Vec<_>>()
            .join(", ");

        format!("{} windows: {}", windows.len(), formatted_windows)
    }

    /// Saves the current window state to the specified file.
    pub(crate) fn save_state(&self) -> ReduceResult<()> {
        let path = match &self.active_window_state_file {
            Some(path) => path,
            None => return Ok(()),
        };

        let all_windows: Vec<ProtoWindow> = {
            let active_windows = self
                .active_windows
                .read()
                .expect("Poisoned lock for active_windows");
            let closed_windows = self
                .closed_windows
                .read()
                .expect("Poisoned lock for closed_windows");

            closed_windows
                .iter()
                .chain(active_windows.iter())
                .map(Into::into)
                .collect()
        };

        let state = WindowManagerState {
            windows: all_windows,
            max_deleted_window_end_time: self.max_deleted_window_end_time.load(Ordering::Relaxed),
        };

        info!(
            path = %path.display(),
            "Saving window state: {}",
            Self::format_windows_for_log(&state.windows)
        );

        let mut buf = Vec::new();
        state
            .encode(&mut buf)
            .map_err(|e| Error::Other(e.to_string()))?;

        File::create(path)
            .and_then(|mut file| file.write_all(&buf))
            .map_err(|e| Error::Other(e.to_string()))?;

        Ok(())
    }

    /// Loads window state from the specified file
    fn load_state(&self, path: &PathBuf) -> ReduceResult<()> {
        if !path.exists() {
            info!(path = %path.display(), "Window state file does not exist");
            return Ok(());
        }

        let buf = fs::read(path).map_err(|e| Error::Other(e.to_string()))?;

        let state =
            WindowManagerState::decode(&buf[..]).map_err(|e| Error::Other(e.to_string()))?;

        self.active_windows
            .write()
            .expect("Poisoned lock for active_windows")
            .extend(state.windows.iter().map(Into::into));

        self.max_deleted_window_end_time
            .store(state.max_deleted_window_end_time, Ordering::Relaxed);

        info!(
            "Loaded window state: {}",
            Self::format_windows_for_log(&state.windows)
        );

        // remove the state file, else we will load the state file on next restart in case of crash (SIGKILL)
        fs::remove_file(path).map_err(|e| Error::Other(e.to_string()))?;
        info!("Removed window state file: {}", path.display());

        Ok(())
    }

    /// Assigns windows to a message. It first figures all the windows that the message belongs to and
    /// then checks if the window already exists. If it does, it appends the message to the window,
    /// else it creates a new window and adds the message to it.
    pub(crate) fn assign_windows(&self, msg: Message) -> Vec<AlignedWindowMessage> {
        let windows = self.create_windows(&msg);
        let mut result = Vec::new();

        // Check if windows already exist and create appropriate operations
        let mut active_windows = self.active_windows.write().unwrap();

        for window in &windows {
            if active_windows.contains(window) {
                // Window exists, append message
                result.push(AlignedWindowMessage {
                    operation: AlignedWindowOperation::Append {
                        message: msg.clone(),
                        window: window.clone(),
                    },
                    pnf_slot: window_pnf_slot(window),
                });
            } else if window.end_time.timestamp_millis()
                > self.max_deleted_window_end_time.load(Ordering::Relaxed)
            {
                // we cannot use active_window because that could be misaligned due to out-of-order
                // messages.
                // only create the window if the end time is greater than the max deleted window end
                // time, during replay we might get the same message again and again, so we need to
                // avoid creating the materialized window again.
                active_windows.insert(window.clone());
                result.push(AlignedWindowMessage {
                    operation: AlignedWindowOperation::Open {
                        message: msg.clone(),
                        window: window.clone(),
                    },
                    pnf_slot: window_pnf_slot(window),
                });
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_assign_windows_length_divisible_by_slide() {
        // Create a sliding windower with 60s window length and 20s slide
        let windower =
            SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(20), None);

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
            ..Default::default()
        };

        // Assign windows
        let window_msgs = windower.assign_windows(msg.clone());

        // Verify results - should be assigned to exactly 3 windows
        assert_eq!(window_msgs.len(), 3);

        // Check first window: [60000, 120000)
        match &window_msgs[0].operation {
            AlignedWindowOperation::Open { window, .. } => {
                assert_eq!(window.start_time.timestamp_millis(), 60000);
                assert_eq!(window.end_time.timestamp_millis(), 120000);
            }
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

        // Verify results - should be assigned to exactly 3 windows
        assert_eq!(window_msgs2.len(), 3);

        // All operations should be Append
        for window_msg in &window_msgs2 {
            match &window_msg.operation {
                AlignedWindowOperation::Append { .. } => {}
                _ => panic!("Expected Append operation"),
            }
        }
    }

    #[tokio::test]
    async fn test_assign_windows_length_not_divisible_by_slide() {
        // Create a sliding windower with 60s window length and 40s slide
        let windower =
            SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(40), None);

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
            ..Default::default()
        };

        // Assign windows
        let window_msgs = windower.assign_windows(msg.clone());

        // Verify results - should be assigned to exactly 2 windows
        assert_eq!(window_msgs.len(), 2);

        match &window_msgs[0].operation {
            AlignedWindowOperation::Open { window, .. } => {
                assert_eq!(window.start_time.timestamp(), 600);
                assert_eq!(window.end_time.timestamp(), 660);
            }
            _ => panic!("Expected Open operation"),
        }

        // Check second window: [560, 620)
        match &window_msgs[1].operation {
            AlignedWindowOperation::Open { window, .. } => {
                assert_eq!(window.start_time.timestamp(), 560);
                assert_eq!(window.end_time.timestamp(), 620);
            }
            _ => panic!("Expected Open operation"),
        }

        // Create and insert the second window manually to test append behavior
        let window2 = Window::new(
            Utc.timestamp_opt(560, 0).unwrap(),
            Utc.timestamp_opt(620, 0).unwrap(),
        );
        {
            let mut active_windows = windower.active_windows.write().unwrap();
            active_windows.insert(window2.clone());
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
                AlignedWindowOperation::Append { .. } => {}
                _ => panic!("Expected Append operation"),
            }
        }
    }

    #[test]
    fn test_close_windows() {
        // Base time: 60000ms (60s)
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

        // Create a sliding windower with 60s window length and 10s slide
        let windower =
            SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(10), None);

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
            let mut active_windows = windower.active_windows.write().unwrap();
            active_windows.insert(window1.clone());
            active_windows.insert(window2.clone());
            active_windows.insert(window3.clone());
        }

        // Close windows with watermark at base_time + 120s
        let closed = windower.close_windows(base_time + chrono::Duration::seconds(120));

        // Should close all 3 windows
        assert_eq!(closed.len(), 3);

        // Check that all windows are closed in order of end time
        match &closed[0].operation {
            AlignedWindowOperation::Close { window } => {
                assert_eq!(window.start_time, base_time - chrono::Duration::seconds(20));
                assert_eq!(window.end_time, base_time + chrono::Duration::seconds(40));
            }
            _ => panic!("Expected Close operation"),
        }

        match &closed[1].operation {
            AlignedWindowOperation::Close { window } => {
                assert_eq!(window.start_time, base_time - chrono::Duration::seconds(10));
                assert_eq!(window.end_time, base_time + chrono::Duration::seconds(50));
            }
            _ => panic!("Expected Close operation"),
        }

        match &closed[2].operation {
            AlignedWindowOperation::Close { window } => {
                assert_eq!(window.start_time, base_time);
                assert_eq!(window.end_time, base_time + chrono::Duration::seconds(60));
            }
            _ => panic!("Expected Close operation"),
        }

        // Verify all windows are closed
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 0);
        }
    }

    #[test]
    fn test_delete_window() {
        // Base time: 60000ms (60s)
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

        // Create a sliding windower with 60s window length and 10s slide
        let windower =
            SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(10), None);

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
            let mut active_windows = windower.active_windows.write().unwrap();
            active_windows.insert(window1.clone());
            active_windows.insert(window2.clone());
            active_windows.insert(window3.clone());
        }

        // Verify all windows exist
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 3);
        }

        // Close window1
        windower.close_windows(base_time + chrono::Duration::seconds(60));

        // Delete window1
        windower.gc_window(window1.clone());

        // Verify window1 is deleted
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 0);
            let closed_windows = windower.closed_windows.read().unwrap();
            assert_eq!(closed_windows.len(), 2);
            assert!(!closed_windows.contains(&window1));
            assert!(closed_windows.contains(&window2));
            assert!(closed_windows.contains(&window3));
        }

        // Delete window2
        windower.gc_window(window2.clone());

        // Verify window2 is deleted
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 0);
            assert!(!active_windows.contains(&window1));
            assert!(!active_windows.contains(&window2));
            assert!(!active_windows.contains(&window3));
        }

        // Delete window3
        windower.gc_window(window3.clone());

        // Verify window3 is deleted
        {
            let active_windows = windower.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 0);
        }
    }

    #[test]
    fn test_oldest_window_endtime() {
        // Base time: 60000ms (60s)
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

        // Create a sliding windower with 60s window length and 10s slide
        let windower =
            SlidingWindowManager::new(Duration::from_secs(60), Duration::from_secs(10), None);

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
            let mut active_windows = windower.active_windows.write().unwrap();
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

        // Close window3
        windower.close_windows(base_time + chrono::Duration::seconds(40));

        // Delete window3 (the oldest)
        windower.gc_window(window3.clone());

        // Verify the oldest window end time is now window2
        assert_eq!(
            windower
                .oldest_window()
                .unwrap()
                .end_time
                .timestamp_millis(),
            (base_time + chrono::Duration::seconds(50)).timestamp_millis()
        );

        // Close window2
        windower.close_windows(base_time + chrono::Duration::seconds(50));

        // Delete window2
        windower.gc_window(window2.clone());

        // Verify oldest window end time is now window1
        assert_eq!(
            windower
                .oldest_window()
                .unwrap()
                .end_time
                .timestamp_millis(),
            (base_time + chrono::Duration::seconds(60)).timestamp_millis()
        );

        // Close window1
        windower.close_windows(base_time + chrono::Duration::seconds(60));

        // Delete window1
        windower.gc_window(window1.clone());
        assert_eq!(windower.oldest_window(), None);
    }

    #[test]
    fn test_assign_windows_with_small_slide() {
        // prepopulate active windows
        let active_windows = [
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

        // Create a sliding windower with 60s window length and 10s slide
        let windower = SlidingWindowManager {
            window_length: Duration::from_secs(60),
            slide: Duration::from_secs(10),
            active_windows: Arc::new(RwLock::new(BTreeSet::from_iter(
                active_windows.iter().cloned(),
            ))),
            closed_windows: Arc::new(RwLock::new(BTreeSet::new())),
            active_window_state_file: None,
            max_deleted_window_end_time: Arc::new(Default::default()),
        };

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
        let expected_windows = [
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

        // Should also be assigned to 6 windows (5 are already open, 1 is new)
        assert_eq!(window_msgs.len(), expected_windows.len());

        // Check all windows match expected windows
        for (i, window_msg) in window_msgs.iter().enumerate() {
            // Check operation type and extract window
            let window = match &window_msg.operation {
                AlignedWindowOperation::Open {
                    message: open_msg,
                    window,
                } => {
                    assert_eq!(open_msg.event_time, msg.event_time);
                    assert_eq!(window.start_time, Utc.timestamp_millis_opt(100000).unwrap());
                    assert_eq!(window.end_time, Utc.timestamp_millis_opt(160000).unwrap());
                    window
                }
                AlignedWindowOperation::Append {
                    message: append_msg,
                    window,
                } => {
                    assert_eq!(append_msg.event_time, msg.event_time);
                    window
                }
                AlignedWindowOperation::Close { .. } => {
                    panic!("Expected Open or Append operation");
                }
            };

            // Verify window matches expected window
            assert_eq!(window.start_time, expected_windows[i].start_time);
            assert_eq!(window.end_time, expected_windows[i].end_time);
        }

        // Assign another message to the same window
        let msg2 = Message {
            event_time: Utc.timestamp_millis_opt(105000).unwrap(),
            ..Default::default()
        };

        let window_msgs_two = windower.assign_windows(msg2.clone());

        // Should also be assigned to 6 windows, all append
        assert_eq!(window_msgs_two.len(), 6);

        for window_msg in &window_msgs_two {
            // largest window should be open, rest are append
            match &window_msg.operation {
                AlignedWindowOperation::Append {
                    message: append_msg,
                    ..
                } => {
                    assert_eq!(append_msg.event_time, msg2.event_time);
                }
                AlignedWindowOperation::Close { .. } | AlignedWindowOperation::Open { .. } => {
                    panic!("Expected Append operation");
                }
            }
        }
    }

    #[test]
    fn test_save_and_load_state() {
        use tempfile::tempdir;

        // Create a temporary directory for the test
        let temp_dir = tempdir().unwrap();
        let state_file_path = temp_dir.path().join("window_state.bin");

        // Create a sliding windower with 60s window length and 10s slide
        let windower = SlidingWindowManager::new(
            Duration::from_secs(60),
            Duration::from_secs(10),
            Some(state_file_path.clone()),
        );

        // Base time: 60000ms (60s)
        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

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
            let mut active_windows = windower.active_windows.write().unwrap();
            active_windows.insert(window1.clone());
            active_windows.insert(window2.clone());
            active_windows.insert(window3.clone());
        }

        // Manually save state
        windower.save_state().unwrap();

        // Verify file exists
        assert!(state_file_path.exists());

        // Create a new windower with the same state file
        let windower2 = SlidingWindowManager::new(
            Duration::from_secs(60),
            Duration::from_secs(10),
            Some(state_file_path.clone()),
        );

        // Verify windows were loaded
        {
            let active_windows = windower2.active_windows.read().unwrap();
            assert_eq!(active_windows.len(), 3);
            assert!(active_windows.contains(&window1));
            assert!(active_windows.contains(&window2));
            assert!(active_windows.contains(&window3));
        }

        // Clean up
        temp_dir.close().unwrap();
    }
}
