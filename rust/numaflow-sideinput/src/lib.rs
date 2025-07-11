//! [SideInput] is a feature that allows users to access slow updated data or configuration without
//! needing to retrieve it during each message processing.
//!
//! [SideInput]: https://numaflow.numaproj.io/user-guide/reference/side-inputs/

#![allow(dead_code)]

use crate::error::{Error, Result};
use crate::manager::SideInputTrigger;
use async_nats::jetstream::Context;
use async_nats::{ConnectOptions, jetstream};
use config::isb;
use std::time::Duration;
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
        /// The name of the side input.
        side_input: &'static str,
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

/// Runs the side-input system in the specified mode.
pub async fn run(mode: SideInputMode, cancellation_token: CancellationToken) -> Result<()> {
    match mode {
        SideInputMode::Manager {
            side_input_store,
            side_input,
        } => start_manager(side_input_store, side_input, cancellation_token).await,
        SideInputMode::Synchronizer {
            side_inputs,
            side_input_store,
            mount_path,
            run_once,
        } => {
            start_synchronizer(
                side_inputs,
                side_input_store,
                mount_path,
                cancellation_token,
                run_once,
            )
            .await
        }
    }
}

async fn start_manager(
    side_input_store: &'static str,
    side_input: &'static str,
    cancellation_token: CancellationToken,
) -> Result<()> {
    let trigger = config::SideInputTriggerConfig::load(std::env::vars().collect());

    let client = manager::client::UserDefinedSideInputClient::new(
        std::env::var("NUMAFLOW_UDS_PATH")
            .expect("NUMAFLOW_UDS_PATH is not set")
            .into(),
    )
    .await?;

    let side_input_trigger = SideInputTrigger::new(trigger.schedule, trigger.timezone)?;

    manager::SideInputManager::new(side_input_store, side_input, client, cancellation_token)
        .run(
            isb::ClientConfig::load(std::env::vars())?,
            side_input_trigger,
        )
        .await
}

async fn start_synchronizer(
    side_inputs: Vec<&'static str>,
    side_input_store: &'static str,
    mount_path: &'static str,
    cancellation_token: CancellationToken,
    run_once: bool,
) -> Result<()> {
    let js_ctx = build_js_context().await?;

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

async fn build_js_context() -> Result<Context> {
    let client = isb::ClientConfig::load(std::env::vars())?;
    create_js_context(client).await
}

async fn create_js_context(config: isb::ClientConfig) -> Result<Context> {
    // TODO: make these configurable. today this is hardcoded on Golang code too.
    let mut opts = ConnectOptions::new()
        .max_reconnects(None) // unlimited reconnects
        .ping_interval(Duration::from_secs(3))
        .retry_on_initial_connect();

    if let (Some(user), Some(password)) = (config.user, config.password) {
        opts = opts.user_and_password(user, password);
    }

    let js_client = async_nats::connect_with_options(&config.url, opts)
        .await
        .map_err(|e| Error::Connection(e.to_string()))?;

    Ok(jetstream::new(js_client))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration Tests - These test the run function behavior and error handling

    /// Integration test for the run function with cancellation
    /// This test verifies that the run function properly handles cancellation tokens
    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_run_function_cancellation() {
        // Test Manager mode cancellation
        let cancellation_token = CancellationToken::new();
        cancellation_token.cancel(); // Cancel immediately

        let manager_result = run(
            SideInputMode::Manager {
                side_input_store: "test-store",
                side_input: "test-input",
            },
            cancellation_token.clone(),
        )
        .await;

        // Should fail due to missing environment variables or cancellation
        assert!(manager_result.is_err());

        // Test Synchronizer mode cancellation
        let sync_result = run(
            SideInputMode::Synchronizer {
                side_inputs: vec!["input1"],
                side_input_store: "test-store",
                mount_path: "/tmp/test",
                run_once: true,
            },
            cancellation_token,
        )
        .await;

        // Should fail due to missing environment variables or cancellation
        assert!(sync_result.is_err());
    }
}
