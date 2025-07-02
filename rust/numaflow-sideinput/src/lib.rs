//! [SideInput] is a feature that allows users to access slow updated data or configuration without
//! needing to retrieve it during each message processing.
//!
//! [SideInput]: https://numaflow.numaproj.io/user-guide/reference/side-inputs/

#![allow(dead_code)]

use crate::error::Error;
use async_nats::jetstream::Context;
use async_nats::{ConnectOptions, jetstream};
use std::time::Duration;

mod error;

/// Synchronizes the side input values from the ISB to the local file system of the vertex by watching
/// the side input store for changes.
mod synchronize;

pub(crate) mod isb {
    const DEFAULT_URL: &str = "localhost:4222";
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct ClientConfig {
        pub url: String,
        pub user: Option<String>,
        pub password: Option<String>,
    }

    impl Default for ClientConfig {
        fn default() -> Self {
            ClientConfig {
                url: DEFAULT_URL.to_string(),
                user: None,
                password: None,
            }
        }
    }
}

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
