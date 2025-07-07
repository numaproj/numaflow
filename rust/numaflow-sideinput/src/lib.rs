//! [SideInput] is a feature that allows users to access slow updated data or configuration without
//! needing to retrieve it during each message processing.
//!
//! [SideInput]: https://numaflow.numaproj.io/user-guide/reference/side-inputs/

#![allow(dead_code)]

use crate::error::Error;
use async_nats::jetstream::Context;
use async_nats::{ConnectOptions, jetstream};
use config::isb;
use std::time::Duration;

mod error;

/// Configurations for side-input from the environment. Only side-input manager needs this, most of
/// the configurations for side-input are from command line.
mod config;

/// Runs the user-defined side-input generator at specified intervals to create the side-input values.
mod manager;

/// Synchronizes the side input values from the ISB to the local file system of the vertex by watching
/// the side input store for changes.
mod synchronize;

async fn create_js_context(config: isb::ClientConfig) -> error::Result<Context> {
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
