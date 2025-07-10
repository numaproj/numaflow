//! [SideInput] is a feature that allows users to access slow updated data or configuration without
//! needing to retrieve it during each message processing.
//!
//! [SideInput]: https://numaflow.numaproj.io/user-guide/reference/side-inputs/

#![allow(dead_code)]

use crate::error::{Error, Result};
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
    },
    Initializer {
        /// The list of side input names to initialize.
        side_inputs: Vec<&'static str>,
        /// The ISB bucket where the side-input values are stored.
        side_input_store: &'static str,
    },
}

pub async fn run(mode: SideInputMode, cancellation_token: CancellationToken) -> Result<()> {
    unimplemented!()
}

async fn start_initializer(
    side_inputs: Vec<&'static str>,
    side_input_store: &'static str,
    cancellation_token: CancellationToken,
) -> Result<()> {
    unimplemented!()
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
