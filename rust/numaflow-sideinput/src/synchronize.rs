//! The synchronizer continually monitors and synchronizes side input values from a key-value store
//! to the local filesystem, making them available to pipeline vertices.

use crate::error::{Error, Result};
use crate::synchronize::persistence::update_side_input_file;
use async_nats::jetstream;
use async_nats::jetstream::Context;
use async_nats::jetstream::kv::Operation;
use std::collections::HashSet;
use std::path;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{info, trace, warn};

/// Persistence to Local File Store related functions.
mod persistence;

/// Synchronizes the side input values from the ISB to the local file system.
pub(crate) struct SideInputSynchronizer {
    /// The name of the side input store. The bucket where each side-input is stored.
    side_input_store: &'static str,
    /// The list of side inputs to synchronize.
    side_inputs: Vec<&'static str>,
    /// The path where the side input files are mounted on the container.
    mount_path: &'static str,
    js_ctx: Context,
    /// If true, the synchronizer will only process the initial values and then stops.
    run_once: bool,
    cancellation_token: CancellationToken,
}

impl SideInputSynchronizer {
    pub(crate) fn new(
        side_input_store: &'static str,
        side_inputs: Vec<&'static str>,
        mount_path: &'static str,
        js_ctx: Context,
        run_once: bool,
        cancellation_token: CancellationToken,
    ) -> Self {
        SideInputSynchronizer {
            side_input_store,
            side_inputs,
            mount_path,
            js_ctx,
            run_once,
            cancellation_token,
        }
    }

    pub(crate) async fn synchronize(self) -> Result<()> {
        // TODO: move create_js_context outside of pipeline

        let bucket = self
            .js_ctx
            .get_key_value(self.side_input_store)
            .await
            .map_err(|e| {
                Error::SideInput(format!(
                    "Failed to get kv bucket {}: {}",
                    self.side_input_store, e
                ))
            })?;

        let bucket_watcher =
            numaflow_shared::isb::jetstream::JetstreamWatcher::new(bucket.clone()).await?;

        self.run(bucket, bucket_watcher).await;

        Ok(())
    }

    /// Monitors the bucket for changes and updates the side input files accordingly. If
    /// `SideInputSynchronizer.run_once` is true, it will only process the initial values and then return.
    async fn run(
        self,
        bucket: jetstream::kv::Store,
        mut bucket_watcher: numaflow_shared::isb::jetstream::JetstreamWatcher,
    ) {
        let mut seen_keys: Option<HashSet<String>> = None;
        if self.run_once {
            info!("Running side input synchronizer once for initialization");
            seen_keys = Some(HashSet::new());
        }

        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    info!("Cancellation token triggered. Stopping side input synchronizer.");
                    break;
                }
                kv = bucket_watcher.next() => {

                    // this watcher can never return None
                    let Some(kv) = kv else {
                        warn!("This should never happen, watcher should never return None");
                        continue;
                    };

                    match kv.operation {
                        Operation::Put => {
                            trace!(operation=?kv.operation, key=?kv.key, "Processing operation");
                            // check if the key is one of the side inputs
                            if !self.side_inputs.contains(&kv.key.as_str()) {
                                continue;
                            }

                            // if run_once is true, we only process the initial values and then return
                            if let Some(seen_keys) = &mut seen_keys {
                                let k = kv.key.as_str();
                                seen_keys.insert(k.to_string());
                                info!(?self.side_inputs, ?seen_keys, "Synchronizing side inputs");
                            }

                            let value = bucket.get(&kv.key).await.unwrap().unwrap();

                            let mount_path = path::Path::new(self.mount_path).join(&kv.key);
                            update_side_input_file(mount_path, &value).unwrap();
                        }
                        Operation::Delete | Operation::Purge => {
                            trace!(operation=?kv.operation, "Skipping operation");
                        }
                    }
                }
            }

            // if run_once is true, we only synchronizes once and quit
            if self.run_once && Self::got_all_sideinputs(&self, seen_keys.as_ref().unwrap()) {
                info!(side_inputs=?seen_keys, "one time synchronization completed.");
                return;
            }
        }
    }

    fn got_all_sideinputs(&self, seen_keys: &HashSet<String>) -> bool {
        for side_input in &self.side_inputs {
            if !seen_keys.contains(&(*side_input).to_string()) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_js_context;
    use numaflow_shared::isb::jetstream::config::ClientConfig;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    /// Test the basic construction of SideInputSynchronizer
    /// This test doesn't require NATS to be running
    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_side_input_synchronizer_new() {
        let temp_dir = TempDir::new().unwrap();
        let mount_path = Box::leak(
            temp_dir
                .path()
                .to_string_lossy()
                .into_owned()
                .into_boxed_str(),
        );

        let config = create_js_context(ClientConfig::default()).await.unwrap();
        let cancellation_token = CancellationToken::new();
        let synchronizer = SideInputSynchronizer::new(
            "test-store",
            vec!["input1", "input2"],
            mount_path,
            config,
            false,
            cancellation_token,
        );

        assert_eq!(synchronizer.side_input_store, "test-store");
        assert_eq!(synchronizer.side_inputs, vec!["input1", "input2"]);
        assert_eq!(synchronizer.mount_path, mount_path);
    }

    /// Integration test for SideInputSynchronizer
    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_side_input_synchronizer_integration() {
        // Connect to NATS server (assumes NATS is running on localhost:4222)
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        // Create a test KV store
        let store_name = "test-side-input-store";
        let _ = js_context.delete_key_value(store_name).await; // Clean up if exists

        let kv_store = js_context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: store_name.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Put some test data in the KV store
        kv_store.put("input1", "test-value-1".into()).await.unwrap();
        kv_store.put("input2", "test-value-2".into()).await.unwrap();
        kv_store
            .put("other-input", "other-value".into())
            .await
            .unwrap(); // Should be ignored

        // Use a temporary directory for testing
        let temp_dir = TempDir::new().unwrap();
        let mount_path = Box::leak(
            temp_dir
                .path()
                .to_string_lossy()
                .into_owned()
                .into_boxed_str(),
        );

        let cancellation_token = CancellationToken::new();
        let synchronizer = SideInputSynchronizer::new(
            "test-side-input-store",
            vec!["input1", "input2"],
            mount_path,
            js_context.clone(),
            false,
            cancellation_token.clone(),
        );

        // Start synchronization in a background task
        let sync_task = tokio::spawn(async move { synchronizer.synchronize().await });

        // Give some time for synchronization to process initial values
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Put additional values to test real-time synchronization
        kv_store
            .put("input1", "updated-value-1".into())
            .await
            .unwrap();

        // Give some time for the update to be processed
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Clean up
        cancellation_token.cancel();
        let _ = sync_task.await;
        let _ = js_context.delete_key_value(store_name).await;
    }

    /// Test error handling when KV store doesn't exist
    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_side_input_synchronizer_error_handling() {
        let temp_dir = TempDir::new().unwrap();
        let mount_path = Box::leak(
            temp_dir
                .path()
                .to_string_lossy()
                .into_owned()
                .into_boxed_str(),
        );

        let config = ClientConfig {
            url: "localhost:4222".to_string(),
            ..Default::default()
        };

        let cancellation_token = CancellationToken::new();
        let synchronizer = SideInputSynchronizer::new(
            "non-existent-store",
            vec!["input1"],
            mount_path,
            create_js_context(config).await.unwrap(),
            false,
            cancellation_token,
        );

        // This should fail because the store doesn't exist
        let result = synchronizer.synchronize().await;
        assert!(result.is_err());

        if let Err(Error::SideInput(msg)) = result {
            assert!(msg.contains("Failed to get kv bucket"));
        } else {
            panic!("Expected SideInput error");
        }
    }

    /// Test that side input filtering works correctly
    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_side_input_filtering() {
        // Connect to NATS server
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        // Create a test KV store
        let store_name = "test-filtering-store";
        let _ = js_context.delete_key_value(store_name).await; // Clean up if exists

        let kv_store = js_context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: store_name.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Use a temporary directory for testing
        let temp_dir = TempDir::new().unwrap();
        let mount_path = Box::leak(
            temp_dir
                .path()
                .to_string_lossy()
                .into_owned()
                .into_boxed_str(),
        );

        // Only include "allowed-input" in side_inputs
        let cancellation_token = CancellationToken::new();
        let synchronizer = SideInputSynchronizer::new(
            store_name,
            vec!["allowed-input"],
            mount_path,
            js_context.clone(),
            false,
            cancellation_token.clone(),
        );

        // Start synchronization in background
        let sync_task = tokio::spawn(async move { synchronizer.synchronize().await });

        // Give some time for synchronizer to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Put values for both allowed and disallowed inputs
        kv_store
            .put("allowed-input", "allowed-value".into())
            .await
            .unwrap();
        kv_store
            .put("disallowed-input", "disallowed-value".into())
            .await
            .unwrap();

        // Give some time for processing
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Clean up
        cancellation_token.cancel();
        let _ = sync_task.await;
        let _ = js_context.delete_key_value(store_name).await;
    }

    /// Test run_once functionality - synchronizer should process initial values and then stop
    ///
    /// This test verifies that when run_once=true:
    /// 1. The synchronizer processes all initial side input values
    /// 2. The synchronizer stops after processing all expected side inputs
    /// 3. The synchronizer does not continue monitoring for new changes
    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_side_input_synchronizer_run_once() {
        // Connect to NATS server
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        // Create a test KV store
        let store_name = "test-run-once-store";
        let _ = js_context.delete_key_value(store_name).await; // Clean up if exists

        let kv_store = js_context
            .create_key_value(jetstream::kv::Config {
                bucket: store_name.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Put initial test data in the KV store before starting synchronizer
        kv_store
            .put("input1", "initial-value-1".into())
            .await
            .unwrap();
        kv_store
            .put("input2", "initial-value-2".into())
            .await
            .unwrap();
        kv_store
            .put("other-input", "other-value".into())
            .await
            .unwrap(); // Should be ignored

        // Use a temporary directory for testing
        let temp_dir = TempDir::new().unwrap();
        let mount_path = Box::leak(
            temp_dir
                .path()
                .to_string_lossy()
                .into_owned()
                .into_boxed_str(),
        );

        let cancellation_token = CancellationToken::new();
        let synchronizer = SideInputSynchronizer::new(
            store_name,
            vec!["input1", "input2"],
            mount_path,
            js_context.clone(),
            true, // run_once = true
            cancellation_token.clone(),
        );

        // Start synchronization - this should complete after processing initial values
        let sync_task = tokio::spawn(async move { synchronizer.synchronize().await });

        // Give some time for synchronization to process initial values and complete
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // The task should have completed by now since run_once=true
        // We'll check if it's finished by trying to join with a short timeout
        let task_result = tokio::time::timeout(Duration::from_millis(100), sync_task).await;

        match task_result {
            Ok(join_result) => {
                // Task completed as expected
                assert!(
                    join_result.is_ok(),
                    "Synchronization task should complete successfully"
                );
                if let Ok(sync_result) = join_result {
                    assert!(sync_result.is_ok(), "Synchronization should succeed");
                }
            }
            Err(_) => {
                // Task is still running, which means run_once didn't work properly
                panic!(
                    "Synchronization task should have completed when run_once=true, but it's still running"
                );
            }
        }

        // Clean up
        let _ = js_context.delete_key_value(store_name).await;
    }

    /// Test run_once functionality with partial side inputs
    ///
    /// This test verifies that when run_once=true and only some side inputs are available:
    /// 1. The synchronizer processes available side input values
    /// 2. The synchronizer waits for missing side inputs before stopping
    /// 3. The synchronizer stops after all expected side inputs are received
    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_side_input_synchronizer_run_once_partial() {
        // Connect to NATS server
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        // Create a test KV store
        let store_name = "test-run-once-partial-store";
        let _ = js_context.delete_key_value(store_name).await; // Clean up if exists

        let kv_store = js_context
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: store_name.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Put only one of the expected side inputs initially
        kv_store
            .put("input1", "initial-value-1".into())
            .await
            .unwrap();
        // input2 is missing initially

        // Use a temporary directory for testing
        let temp_dir = TempDir::new().unwrap();
        let mount_path = Box::leak(
            temp_dir
                .path()
                .to_string_lossy()
                .into_owned()
                .into_boxed_str(),
        );

        let cancellation_token = CancellationToken::new();
        let synchronizer = SideInputSynchronizer::new(
            store_name,
            vec!["input1", "input2"],
            mount_path,
            js_context.clone(),
            true, // run_once = true
            cancellation_token.clone(),
        );

        // Start synchronization
        let sync_task = tokio::spawn(async move { synchronizer.synchronize().await });

        // Give some time for synchronization to process the first input
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Task should still be running since input2 is missing
        assert!(
            !sync_task.is_finished(),
            "Task should still be running waiting for input2"
        );

        // Now add the missing side input
        kv_store
            .put("input2", "initial-value-2".into())
            .await
            .unwrap();

        // Give some time for synchronization to process the second input and complete
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // The task should have completed by now
        let task_result = tokio::time::timeout(Duration::from_millis(100), sync_task).await;

        match task_result {
            Ok(join_result) => {
                // Task completed as expected
                assert!(
                    join_result.is_ok(),
                    "Synchronization task should complete successfully"
                );
                if let Ok(sync_result) = join_result {
                    assert!(sync_result.is_ok(), "Synchronization should succeed");
                }
            }
            Err(_) => {
                // Task is still running, which shouldn't happen
                panic!(
                    "Synchronization task should have completed after receiving all side inputs"
                );
            }
        }

        // Clean up
        let _ = js_context.delete_key_value(store_name).await;
    }
}
