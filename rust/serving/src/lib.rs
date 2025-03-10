use std::net::SocketAddr;
use std::sync::Arc;

use axum_server::tls_rustls::RustlsConfig;
use tokio::sync::mpsc;
use tracing::info;

pub use self::error::{Error, Result};
use crate::app::start_main_server;
use crate::config::generate_certs;
use crate::metrics::start_https_metrics_server;
use app::orchestrator::OrchestratorState;

mod app;

mod config;
pub use {config::Settings, config::DEFAULT_CALLBACK_URL_HEADER_KEY, config::DEFAULT_ID_HEADER};

mod error;
mod metrics;
mod pipeline;

pub mod source;
use source::MessageWrapper;
pub use source::{Message, ServingSource};

use crate::app::store::cbstore::CallbackStore;
use crate::app::store::datastore::DataStore;

///
pub mod callback;

#[derive(Clone)]
pub(crate) struct AppState<T, U> {
    pub(crate) message: mpsc::Sender<MessageWrapper>,
    pub(crate) settings: Arc<Settings>,
    pub(crate) orchestrator_state: OrchestratorState<T, U>,
}

pub(crate) async fn serve<T, U>(
    app: AppState<T, U>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>
where
    T: Clone + Send + Sync + DataStore + 'static,
    U: Clone + Send + Sync + CallbackStore + 'static,
{
    let (cert, key) = generate_certs()?;

    let tls_config = RustlsConfig::from_pem(cert.pem().into(), key.serialize_pem().into())
        .await
        .map_err(|e| format!("Failed to create tls config {:?}", e))?;

    info!(config = ?app.settings, "Starting server with config and pipeline spec");

    // Start the metrics server, which serves the prometheus metrics.
    let metrics_addr: SocketAddr =
        format!("0.0.0.0:{}", &app.settings.metrics_server_listen_port).parse()?;

    let metrics_server_handle =
        tokio::spawn(start_https_metrics_server(metrics_addr, tls_config.clone()));

    // Start the main server, which serves the application.
    let app_server_handle = tokio::spawn(start_main_server(app, tls_config));

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
