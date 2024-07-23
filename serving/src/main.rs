use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub use config::config;

use crate::pipeline::pipeline_spec;
use crate::{app::start_main_server, metrics::start_metrics_server};

pub use self::error::{Error, Result};

mod app;
mod config;
mod consts;
mod error;
mod metrics;
mod pipeline;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                // axum logs rejections from built-in extractors with the `axum::rejection`
                // target, at `TRACE` level. `axum::rejection=trace` enables showing those events
                .unwrap_or_else(|_| "info,numaserve=debug,axum::rejection=trace".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_ansi(false))
        .init();

    info!(config = ?config(), pipeline_spec = ? pipeline_spec(), "Starting server with config and pipeline spec");

    let metrics_server_handle = tokio::spawn(start_metrics_server((
        "0.0.0.0",
        config().metrics_server_listen_port,
    )));
    let app_server_handle = tokio::spawn(start_main_server(("0.0.0.0", config().app_listen_port)));

    // TODO: is try_join the best? we need to short-circuit at the first failure
    let servers = tokio::try_join!(flatten(app_server_handle), flatten(metrics_server_handle));

    if let Err(e) = servers {
        error!(error=?e, "Failed to run the servers");
        std::process::exit(1)
    }
}

async fn flatten<T>(handle: tokio::task::JoinHandle<Result<T>>) -> Result<T> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(Error::Other(format!("Spawning the server: {err:?}"))),
    }
}
