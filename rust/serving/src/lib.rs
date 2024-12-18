use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use axum_server::tls_rustls::RustlsConfig;
use tracing::info;

pub use self::error::{Error, Result};
use self::pipeline::PipelineDCG;
use crate::app::start_main_server;
use crate::config::generate_certs;
use crate::metrics::start_https_metrics_server;

mod app;

mod config;
pub use config::Settings;

mod consts;
mod error;
mod metrics;
mod pipeline;

const ENV_MIN_PIPELINE_SPEC: &str = "NUMAFLOW_SERVING_MIN_PIPELINE_SPEC";

pub async fn serve(
    settings: Arc<Settings>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let (cert, key) = generate_certs()?;

    let tls_config = RustlsConfig::from_pem(cert.pem().into(), key.serialize_pem().into())
        .await
        .map_err(|e| format!("Failed to create tls config {:?}", e))?;

    // TODO: Move all env variables into one place. Some env variables are loaded when Settings is initialized
    let pipeline_spec: PipelineDCG = env::var(ENV_MIN_PIPELINE_SPEC)
        .map_err(|_| {
            format!("Pipeline spec is not set using environment variable {ENV_MIN_PIPELINE_SPEC}")
        })?
        .parse()
        .map_err(|e| {
            format!(
                "Parsing pipeline spec: {}: error={e:?}",
                env::var(ENV_MIN_PIPELINE_SPEC).unwrap()
            )
        })?;

    info!(config = ?settings, ?pipeline_spec, "Starting server with config and pipeline spec");

    // Start the metrics server, which serves the prometheus metrics.
    let metrics_addr: SocketAddr =
        format!("0.0.0.0:{}", &settings.metrics_server_listen_port).parse()?;

    let metrics_server_handle =
        tokio::spawn(start_https_metrics_server(metrics_addr, tls_config.clone()));

    // Start the main server, which serves the application.
    let app_server_handle = tokio::spawn(start_main_server(settings, tls_config, pipeline_spec));

    // TODO: is try_join the best? we need to short-circuit at the first failure
    tokio::try_join!(flatten(app_server_handle), flatten(metrics_server_handle))?;

    Ok(())
}

async fn flatten<T>(handle: tokio::task::JoinHandle<Result<T>>) -> Result<T> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(Error::Other(format!("Spawning the server: {err:?}"))),
    }
}
