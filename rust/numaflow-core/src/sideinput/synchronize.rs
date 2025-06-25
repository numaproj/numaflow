//! The synchronizer continually monitors and synchronizes side input values from a key-value store
//! to the local filesystem, making them available to pipeline vertices.

use crate::config::pipeline::isb;
use crate::error::{Error, Result};
use crate::pipeline::create_js_context;
use crate::sideinput::update_side_input_file;
use async_nats::jetstream::kv::{Operation, Watch};
use backoff::retry::Retry;
use backoff::strategy::fixed;
use std::collections::HashSet;
use std::path;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, trace, warn};

/// Synchronizes the side input values from the ISB to the local file system.
pub(crate) struct SideInputSynchronizer {
    side_input_store: &'static str,
    side_inputs: Vec<&'static str>,
    /// The path where the side input files are mounted on the container.
    mount_path: &'static str,
    js_client_config: isb::jetstream::ClientConfig,
    /// If true, the synchronizer will only process the initial values and then stops.
    run_once: bool,
    cancellation_token: CancellationToken,
}

impl SideInputSynchronizer {
    pub(crate) fn new(
        side_input_store: &'static str,
        side_inputs: Vec<&'static str>,
        mount_path: &'static str,
        js_client_config: isb::jetstream::ClientConfig,
        run_once: bool,
        cancellation_token: CancellationToken,
    ) -> Self {
        SideInputSynchronizer {
            side_input_store,
            side_inputs,
            mount_path,
            js_client_config,
            run_once,
            cancellation_token,
        }
    }

    pub(crate) async fn synchronize(self) -> Result<()> {
        // TODO: move create_js_context outside of pipeline
        let js_context = create_js_context(self.js_client_config.clone()).await?;

        let bucket = js_context
            .get_key_value(self.side_input_store)
            .await
            .map_err(|e| {
                Error::SideInput(format!(
                    "Failed to get kv bucket {}: {}",
                    self.side_input_store, e
                ))
            })?;

        self.run(bucket).await;

        Ok(())
    }

    /// Monitors the bucket for changes and updates the side input files accordingly. If
    /// `SideInputSynchronizer.run_once` is true, it will only process the initial values and then return.
    async fn run(self, bucket: async_nats::jetstream::kv::Store) {
        let mut bucket_watcher = create_watcher(bucket.clone()).await;

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
                val = bucket_watcher.next() => {
                    let Some(val) = val else {
                        warn!("SideInput watcher stopped, recreating watcher");
                        bucket_watcher = create_watcher(bucket.clone()).await;
                        continue;
                    };

                    let kv = match val {
                        Ok(kv) => kv,
                        Err(e) => {
                            warn!(error = ?e, "Failed to get next kv entry, recreating watcher");
                            bucket_watcher = create_watcher(bucket.clone()).await;
                            continue;
                        }
                    };

                    match kv.operation {
                        Operation::Put => {
                            // check if the key is one of the side inputs
                            if !self.side_inputs.contains(&kv.key.as_str()) {
                                continue;
                            }

                            // if run_once is true, we only process the initial values and then return
                            if let Some(seen_keys) = &mut seen_keys {
                                let k = kv.key.as_str();
                                if !seen_keys.insert(k.to_string()) {
                                    continue;
                                }
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

/// creates a watcher for the given bucket, will retry infinitely until it succeeds
async fn create_watcher(bucket: async_nats::jetstream::kv::Store) -> Watch {
    const RECONNECT_INTERVAL: u64 = 1000;
    // infinite retry
    let interval = fixed::Interval::from_millis(RECONNECT_INTERVAL).take(usize::MAX);

    Retry::retry(
        interval,
        async || match bucket.watch_all().await {
            Ok(w) => Ok(w),
            Err(e) => {
                error!(?e, "Failed to create watcher");
                Err(Error::SideInput(format!("Failed to create watcher: {}", e)))
            }
        },
        |_: &Error| true,
    )
    .await
    .expect("Failed to create ot watcher")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::pipeline::isb::jetstream::ClientConfig;
    use async_nats::jetstream;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    /// Test the basic construction of SideInputSynchronizer
    /// This test doesn't require NATS to be running
    #[test]
    fn test_side_input_synchronizer_new() {
        let config = ClientConfig::default();
        let cancellation_token = CancellationToken::new();
        let synchronizer = SideInputSynchronizer::new(
            "test-store",
            vec!["input1", "input2"],
            "/tmp/test",
            config,
            false,
            cancellation_token,
        );

        assert_eq!(synchronizer.side_input_store, "test-store");
        assert_eq!(synchronizer.side_inputs, vec!["input1", "input2"]);
        assert_eq!(synchronizer.mount_path, "/tmp/test");
    }

    /// Test that the default ClientConfig has expected values
    #[test]
    fn test_client_config_default() {
        let config = ClientConfig::default();
        assert_eq!(config.url, "localhost:4222");
        assert_eq!(config.user, None);
        assert_eq!(config.password, None);
    }

    /// Test SideInputSynchronizer with custom ClientConfig
    #[test]
    fn test_side_input_synchronizer_with_custom_config() {
        let config = ClientConfig {
            url: "nats://custom-server:4222".to_string(),
            user: Some("testuser".to_string()),
            password: Some("testpass".to_string()),
        };
        let cancellation_token = CancellationToken::new();

        let synchronizer = SideInputSynchronizer::new(
            "custom-store",
            vec!["custom-input"],
            "/custom/path",
            config.clone(),
            false,
            cancellation_token,
        );

        assert_eq!(synchronizer.side_input_store, "custom-store");
        assert_eq!(synchronizer.side_inputs, vec!["custom-input"]);
        assert_eq!(synchronizer.mount_path, "/custom/path");
        assert_eq!(
            synchronizer.js_client_config.url,
            "nats://custom-server:4222"
        );
        assert_eq!(
            synchronizer.js_client_config.user,
            Some("testuser".to_string())
        );
        assert_eq!(
            synchronizer.js_client_config.password,
            Some("testpass".to_string())
        );
    }

    /// Test SideInputSynchronizer with empty side inputs list
    #[test]
    fn test_side_input_synchronizer_empty_inputs() {
        let config = ClientConfig::default();
        let cancellation_token = CancellationToken::new();
        let synchronizer = SideInputSynchronizer::new(
            "empty-store",
            vec![],
            "/tmp/empty",
            config,
            false,
            cancellation_token,
        );

        assert_eq!(synchronizer.side_input_store, "empty-store");
        assert_eq!(synchronizer.side_inputs, Vec::<&str>::new());
        assert_eq!(synchronizer.mount_path, "/tmp/empty");
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

        let config = ClientConfig {
            url: "localhost:4222".to_string(),
            user: None,
            password: None,
        };

        // Use a static mount path for testing
        let cancellation_token = CancellationToken::new();
        let synchronizer = SideInputSynchronizer::new(
            "test-side-input-store",
            vec!["input1", "input2"],
            "/tmp/test-side-input",
            config,
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
        let config = ClientConfig {
            url: "localhost:4222".to_string(),
            user: None,
            password: None,
        };

        let cancellation_token = CancellationToken::new();
        let synchronizer = SideInputSynchronizer::new(
            "non-existent-store",
            vec!["input1"],
            "/tmp/test",
            config,
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

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_create_watcher() {
        // Connect to NATS server
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        // Create a test KV store
        let store_name = "test-watcher-store";
        let _ = js_context.delete_key_value(store_name).await; // Clean up if exists

        let kv_store = js_context
            .create_key_value(jetstream::kv::Config {
                bucket: store_name.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Test creating a watcher
        let _watcher = create_watcher(kv_store.clone()).await;

        // Clean up
        let _ = js_context.delete_key_value(store_name).await;
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

        let config = ClientConfig {
            url: "localhost:4222".to_string(),
            user: None,
            password: None,
        };

        // Only include "allowed-input" in side_inputs
        let cancellation_token = CancellationToken::new();
        let synchronizer = SideInputSynchronizer::new(
            store_name,
            vec!["allowed-input"],
            "/tmp/test-filtering",
            config,
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
}
