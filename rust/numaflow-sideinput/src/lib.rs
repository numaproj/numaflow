//! [SideInput] is a feature that allows users to access slow updated data or configuration without
//! needing to retrieve it during each message processing.
//!
//! [SideInput]: https://numaflow.numaproj.io/user-guide/reference/side-inputs/

use crate::error::Result;
use crate::manager::SideInputTrigger;
use async_nats::jetstream::Context;
use numaflow_shared::isb::jetstream::config::ClientConfig;
use numaflow_shared::isb::jetstream::create_js_context;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

mod error;

/// Configurations for side-input from the environment. Only side-input manager needs this, most of
/// the configurations for side-input are from command line.
mod config;

/// Runs the user-defined side-input generator at specified intervals to create the side-input values.
mod manager;

/// Synchronizes the side input values from the ISB to the local file system of the vertex by watching
/// the side input store for changes.
mod synchronize;

pub enum SideInputMode {
    Manager {
        /// The ISB bucket where the side-input values are stored.
        side_input_store: &'static str,
        /// Server Info file
        server_info_path: &'static str,
    },
    Synchronizer {
        /// The list of side input names to synchronize.
        side_inputs: Vec<&'static str>,
        /// The ISB bucket where the side-input values are stored.
        side_input_store: &'static str,
        /// The path where the side input files are mounted on the container.
        mount_path: &'static str,
        /// If true, the synchronizer will only process the initial values and then stops, making
        /// it behavior similar to the old side-input initializer.
        run_once: bool,
    },
}

/// build the side-input bucket name from the store name.
fn get_bucket_name(side_input_store: &str) -> &'static str {
    Box::leak(format!("{side_input_store}_SIDE_INPUTS").into_boxed_str())
}

/// Runs the side-input system in the specified mode.
pub async fn run(
    mode: SideInputMode,
    uds_path: std::path::PathBuf,
    env_vars: HashMap<String, String>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    match mode {
        SideInputMode::Manager {
            side_input_store,
            server_info_path,
        } => {
            start_manager(
                get_bucket_name(side_input_store),
                uds_path,
                env_vars,
                server_info_path,
                cancellation_token,
            )
            .await
        }
        SideInputMode::Synchronizer {
            side_inputs,
            side_input_store,
            mount_path,
            run_once,
        } => {
            start_synchronizer(
                side_inputs,
                get_bucket_name(side_input_store),
                mount_path,
                run_once,
                env_vars,
                cancellation_token,
            )
            .await
        }
    }
}

async fn start_manager(
    side_input_store: &'static str,
    uds_path: std::path::PathBuf,
    env_vars: HashMap<String, String>,
    server_info_path: &'static str,
    cancellation_token: CancellationToken,
) -> Result<()> {
    let trigger = config::SideInputTriggerConfig::load(env_vars.clone());

    let client = manager::client::UserDefinedSideInputClient::new(
        uds_path,
        server_info_path.into(),
        cancellation_token.clone(),
    )
    .await?;

    let side_input_trigger = SideInputTrigger::new(trigger.schedule, trigger.timezone)?;

    manager::SideInputManager::new(side_input_store, trigger.name, client, cancellation_token)
        .run(ClientConfig::load(env_vars)?, side_input_trigger)
        .await
}

async fn start_synchronizer(
    side_inputs: Vec<&'static str>,
    side_input_store: &'static str,
    mount_path: &'static str,
    run_once: bool,
    env_vars: HashMap<String, String>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    let js_ctx = build_js_context(env_vars).await?;

    let synchronizer = synchronize::SideInputSynchronizer::new(
        side_input_store,
        side_inputs,
        mount_path,
        js_ctx,
        run_once,
        cancellation_token,
    );

    synchronizer.synchronize().await
}

async fn build_js_context(env_vars: HashMap<String, String>) -> Result<Context> {
    let client = ClientConfig::load(env_vars)?;
    create_js_context(client).await.map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::synchronize::SideInputSynchronizer;
    use async_nats::jetstream;
    use base64::Engine;
    use numaflow::sideinput;
    use numaflow::sideinput::SideInputer;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;

    struct SideInputHandler {
        counter: Arc<AtomicU32>,
    }

    impl SideInputHandler {
        fn new() -> Self {
            Self {
                counter: Arc::new(AtomicU32::new(0)),
            }
        }
    }

    #[tonic::async_trait]
    impl SideInputer for SideInputHandler {
        async fn retrieve_sideinput(&self) -> Option<Vec<u8>> {
            let count = self.counter.fetch_add(1, Ordering::SeqCst);
            Some(format!("test-data-{count}").into_bytes())
        }
    }

    /// Test error scenarios in the end-to-end flow
    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_end_to_end_error_scenarios() -> Result<()> {
        // Test synchronizer with non-existent KV store
        let tmp_dir = TempDir::new().unwrap();
        let mount_path = Box::leak(
            tmp_dir
                .path()
                .to_string_lossy()
                .into_owned()
                .into_boxed_str(),
        );

        let js_ctx = create_js_context(ClientConfig {
            url: "localhost:4222".to_string(),
            user: None,
            password: None,
            ..Default::default()
        })
        .await?;

        let sync_cancel = CancellationToken::new();
        let synchronizer = SideInputSynchronizer::new(
            "non-existent-bucket",
            vec!["test-input"],
            mount_path,
            js_ctx,
            false,
            sync_cancel,
        );

        let result = synchronizer.synchronize().await;
        assert!(result.is_err(), "Should fail with non-existent bucket");

        if let Err(crate::error::Error::SideInput(msg)) = result {
            assert!(msg.contains("Failed to get kv bucket"));
        } else {
            panic!("Expected SideInput error");
        }

        Ok(())
    }

    /// Test both Manager and Synchronizer modes with the run function
    /// This test verifies that:
    /// 1. The Manager mode can generate side-input data and store it in the KV store
    /// 2. The Synchronizer mode can read from the KV store and write to local files
    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_manager_and_synchronizer_modes_with_run_function() -> Result<()> {
        // Setup temporary directories and files
        let tmp_dir = TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sideinput.sock");
        let server_info_file = tmp_dir.path().join("sideinput-server-info");

        // Setup side-input server
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();

        // Start the side-input server
        let server_handle = tokio::spawn(async move {
            sideinput::Server::new(SideInputHandler::new())
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("server failed");
        });

        // Wait for server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Setup NATS connection and KV store
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        let store_name = "test-manager-run-once-mode-store";
        let bucket_name = get_bucket_name(store_name);
        let _ = js_context.delete_key_value(bucket_name).await; // Clean up if exists

        let kv_store = js_context
            .create_key_value(jetstream::kv::Config {
                bucket: bucket_name.to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Prepare environment variables for Manager mode
        let side_input_spec = numaflow_models::models::SideInput {
            container: Box::new(numaflow_models::models::Container {
                args: None,
                command: None,
                env: None,
                env_from: None,
                image: None,
                image_pull_policy: None,
                liveness_probe: None,
                ports: None,
                readiness_probe: None,
                resources: None,
                security_context: None,
                volume_mounts: None,
            }),
            name: "test-side-input-run-once".to_string(),
            trigger: Box::from(numaflow_models::models::SideInputTrigger {
                schedule: "* * * * * *".to_string(), // Every second
                timezone: Some("UTC".to_string()),
            }),
            volumes: None,
        };

        let spec_json = serde_json::to_string(&side_input_spec).unwrap();
        let encoded_spec = base64::prelude::BASE64_STANDARD.encode(spec_json);

        let mut env_vars = HashMap::new();
        env_vars.insert("NUMAFLOW_SIDE_INPUT_OBJECT".to_string(), encoded_spec);
        env_vars.insert(
            "NUMAFLOW_ISBSVC_JETSTREAM_URL".to_string(),
            "localhost:4222".to_string(),
        );

        // Set socket file path in environment

        let cancel_token = CancellationToken::new();
        let server_info_path = Box::leak(
            server_info_file
                .to_string_lossy()
                .into_owned()
                .into_boxed_str(),
        );
        let mode = SideInputMode::Manager {
            side_input_store: store_name,
            server_info_path,
        };

        // Start the manager in a background task
        let manager_cancel = cancel_token.clone();
        let sock_file_clone = sock_file.clone();
        let env_vars_clone = env_vars.clone();
        let manager_handle = tokio::spawn(async move {
            run(mode, sock_file_clone, env_vars_clone, manager_cancel)
                .await
                .unwrap();
        });

        // Give manager time to do initial generation
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Verify that data was stored in the KV store
        let stored_value = kv_store.get("test-side-input-run-once").await.unwrap();
        assert!(
            stored_value.is_some(),
            "Side input should be stored in KV store"
        );

        let value = stored_value.unwrap();
        let value_str = String::from_utf8(value.to_vec()).unwrap();
        assert!(
            value_str.starts_with("test-data-"),
            "Stored value should contain test data, got: {value_str}",
        );

        // Test Synchronizer mode - read from KV store and write to files
        let sync_tmp_dir = TempDir::new().unwrap();
        let mount_path = Box::leak(
            sync_tmp_dir
                .path()
                .to_string_lossy()
                .into_owned()
                .into_boxed_str(),
        );

        let sync_cancel_token = CancellationToken::new();
        let sync_mode = SideInputMode::Synchronizer {
            side_inputs: vec!["test-side-input-run-once"],
            side_input_store: store_name,
            mount_path,
            run_once: true, // Run once to process initial values and stop
        };

        // Start the synchronizer in a background task
        let sync_handle = tokio::spawn(async move {
            run(sync_mode, sock_file, env_vars, sync_cancel_token)
                .await
                .unwrap();
        });

        // Give synchronizer time to process and complete (run_once=true should make it finish)
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Verify that the synchronizer task completed (since run_once=true)
        let sync_result = tokio::time::timeout(Duration::from_millis(100), sync_handle).await;
        assert!(
            sync_result.is_ok(),
            "Synchronizer should complete when run_once=true"
        );

        // Verify that the side input file was created in the mount path
        let side_input_file_path = sync_tmp_dir.path().join("test-side-input-run-once");
        assert!(
            side_input_file_path.exists(),
            "Side input file should be created at mount path"
        );

        // Verify the content of the side input file matches what was stored in KV
        let file_content = std::fs::read_to_string(&side_input_file_path).unwrap();
        assert!(
            file_content.starts_with("test-data-"),
            "File content should match KV store content, got: {file_content}",
        );

        // Cleanup
        cancel_token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), manager_handle).await;

        // Shutdown server
        shutdown_tx.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

        // Cleanup KV store
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);
        let _ = js_context.delete_key_value(bucket_name).await;

        Ok(())
    }
}
