//! Persistence for sliding aligned window manager state across restarts.

use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use crate::reduce::error::{Error, ReduceResult};
use crate::reduce::reducer::aligned::windower::{
    Window, wal_window_from_window, window_from_wal_window,
};
use crate::shared::grpc::utc_from_timestamp;
use numaflow_pb::objects::wal::Window as ProtoWindow;
use numaflow_pb::objects::wal::WindowManagerState;
use numaflow_udf_client::SlidingWindowSnapshot;
use prost::Message as ProtoMessage;
use tracing::info;

/// Stores sliding window manager state to disk on shutdown and loads it on startup.
#[derive(Debug, Clone)]
pub(crate) struct SlidingWindowStateStore {
    path: PathBuf,
}

impl SlidingWindowStateStore {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
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

    /// Loads window state from the configured file, if present.
    pub(crate) fn load(&self) -> ReduceResult<Option<SlidingWindowSnapshot>> {
        let path = &self.path;
        if !path.exists() {
            info!(path = %path.display(), "Window state file does not exist");
            return Ok(None);
        }

        let buf = fs::read(path).map_err(|e| Error::Other(e.to_string()))?;

        let state =
            WindowManagerState::decode(&buf[..]).map_err(|e| Error::Other(e.to_string()))?;

        let windows: Vec<Window> = state.windows.iter().map(window_from_wal_window).collect();

        info!(
            "Loaded window state: {}",
            Self::format_windows_for_log(&state.windows)
        );

        // remove the state file, else we will load the state file on next restart in case of crash (SIGKILL)
        fs::remove_file(path).map_err(|e| Error::Other(e.to_string()))?;
        info!("Removed window state file: {}", path.display());

        Ok(Some(SlidingWindowSnapshot {
            windows,
            max_deleted_window_end_time: state.max_deleted_window_end_time,
        }))
    }

    /// Saves the snapshot to the configured file.
    pub(crate) fn save(&self, snapshot: &SlidingWindowSnapshot) -> ReduceResult<()> {
        let path = &self.path;

        let proto_windows: Vec<ProtoWindow> = snapshot
            .windows
            .iter()
            .map(wal_window_from_window)
            .collect();

        let state = WindowManagerState {
            windows: proto_windows.clone(),
            max_deleted_window_end_time: snapshot.max_deleted_window_end_time,
        };

        info!(
            path = %path.display(),
            "Saving window state: {}",
            Self::format_windows_for_log(&proto_windows)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use numaflow_udf_client::SlidingWindowManager;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_save_and_load_state() {
        let temp_dir = tempdir().unwrap();
        let state_file_path = temp_dir.path().join("window_state.bin");
        let store = SlidingWindowStateStore::new(state_file_path.clone());

        let base_time = Utc.timestamp_millis_opt(60000).unwrap();

        let window1 = Window::new(base_time, base_time + chrono::Duration::seconds(60));
        let window2 = Window::new(
            base_time - chrono::Duration::seconds(10),
            base_time + chrono::Duration::seconds(50),
        );
        let window3 = Window::new(
            base_time - chrono::Duration::seconds(20),
            base_time + chrono::Duration::seconds(40),
        );

        let snapshot = SlidingWindowSnapshot {
            windows: vec![window1.clone(), window2.clone(), window3.clone()],
            max_deleted_window_end_time: -1,
        };

        store.save(&snapshot).expect("save state");

        assert!(state_file_path.exists());

        let loaded = store.load().expect("load state").expect("snapshot");
        let windower2 = SlidingWindowManager::from_snapshot(
            Duration::from_secs(60),
            Duration::from_secs(10),
            loaded,
        );

        assert_eq!(windower2.active_window_count(), 3);

        temp_dir.close().unwrap();
    }
}
