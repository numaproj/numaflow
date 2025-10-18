use std::time::Duration;

use crate::config::pipeline;
use crate::config::pipeline::PipelineConfig;
use crate::tracker::Tracker;
use crate::{Result, error};
use async_nats::jetstream::Context;
use async_nats::{ConnectOptions, jetstream};
use tokio_util::sync::CancellationToken;

pub(crate) mod forwarder;
pub(crate) mod isb;

/// PipelineContext contains the common context for all the forwarders.
pub(crate) struct PipelineContext<'a> {
    pub(crate) cln_token: CancellationToken,
    pub(crate) js_context: &'a Context,
    pub(crate) config: &'a PipelineConfig,
    pub(crate) tracker: Tracker,
}

/// Creates a jetstream context based on the provided configuration
pub(crate) async fn create_js_context(
    config: pipeline::isb::jetstream::ClientConfig,
) -> Result<Context> {
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
        .map_err(|e| error::Error::Connection(e.to_string()))?;

    Ok(jetstream::new(js_client))
}
