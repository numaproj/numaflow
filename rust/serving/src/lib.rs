use async_nats::jetstream::Context;
use async_nats::{jetstream, ConnectOptions};
use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

pub use self::error::{Error, Result};
use crate::app::orchestrator::OrchestratorState as CallbackState;
use crate::app::start_main_server;
use crate::app::store::cbstore::jetstreamstore::JetStreamCallbackStore;
use crate::app::store::datastore::jetstream::JetStreamDataStore;
use crate::app::store::datastore::user_defined::UserDefinedStore;
use crate::app::tracker::MessageGraph;
use crate::config::generate_certs;
use crate::config::StoreType;
use crate::metrics::start_https_metrics_server;
use app::orchestrator::OrchestratorState;

mod app;

mod config;
pub use {
    config::Settings, config::DEFAULT_CALLBACK_URL_HEADER_KEY, config::DEFAULT_ID_HEADER,
    config::ENV_MIN_PIPELINE_SPEC,
};

mod error;
mod metrics;
mod pipeline;

use crate::app::store::cbstore::CallbackStore;
use crate::app::store::datastore::DataStore;

pub mod callback;

#[derive(Clone)]
pub(crate) struct AppState<T, U> {
    pub(crate) js_context: Context,
    pub(crate) settings: Arc<Settings>,
    pub(crate) orchestrator_state: OrchestratorState<T, U>,
}

/// Sets up and starts the main application server and the metrics server
/// using the provided TLS configuration. It ensures both servers run concurrently and
/// handles any errors that may occur during their execution.
async fn serve<T, U>(
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

pub async fn run(config: Settings) -> Result<()> {
    let mut opts = ConnectOptions::new()
        .max_reconnects(None) // unlimited reconnects
        .ping_interval(Duration::from_secs(3))
        .retry_on_initial_connect();

    if let Some((user, password)) = config.nats_basic_auth.as_ref().cloned() {
        opts = opts.user_and_password(user, password);
    }

    let js_client = async_nats::connect_with_options(&config.jetstream_url, opts)
        .await
        .map_err(|e| Error::Connection(e.to_string()))?;

    let js_context = jetstream::new(js_client);
    start(js_context, Arc::new(config))
        .await
        .map_err(|e| Error::Source(e.to_string()))?;

    Ok(())
}

/// Starts the serving code after setting up the callback store, message DCG processor, and data store.
pub async fn start(js_context: Context, settings: Arc<Settings>) -> Result<()> {
    // create a callback store for tracking
    let callback_store =
        JetStreamCallbackStore::new(js_context.clone(), &settings.js_callback_store).await?;

    // Create the message graph from the pipeline spec and the redis store
    let msg_graph = MessageGraph::from_pipeline(&settings.pipeline_spec).map_err(|e| {
        Error::InitError(format!(
            "Creating message graph from pipeline spec: {:?}",
            e
        ))
    })?;

    // Create a store (builtin or user-defined) to store the callbacks and the custom responses
    match &settings.store_type {
        StoreType::Nats => {
            let nats_store =
                JetStreamDataStore::new(js_context.clone(), &settings.js_callback_store).await?;
            let callback_state = CallbackState::new(msg_graph, nats_store, callback_store).await?;
            let app = AppState {
                js_context,
                settings,
                orchestrator_state: callback_state,
            };
            serve(app).await.map_err(|e| Error::Store(e.to_string()))?
        }
        StoreType::UserDefined(ud_config) => {
            let ud_store = UserDefinedStore::new(ud_config.clone()).await?;
            let callback_state = CallbackState::new(msg_graph, ud_store, callback_store).await?;
            let app = AppState {
                js_context,
                settings,
                orchestrator_state: callback_state,
            };
            serve(app).await.map_err(|e| Error::Store(e.to_string()))?
        }
    }

    Ok(())
}
