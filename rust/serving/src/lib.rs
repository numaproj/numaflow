use std::net::SocketAddr;
use std::sync::Arc;

use async_nats::jetstream::Context;
use axum_server::tls_rustls::RustlsConfig;
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

///
pub mod callback;

#[derive(Clone)]
pub(crate) struct AppState<T, U> {
    pub(crate) js_context: Context,
    pub(crate) settings: Arc<Settings>,
    pub(crate) orchestrator_state: OrchestratorState<T, U>,
}

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

    // Create a redis store to store the callbacks and the custom responses
    match &settings.store_type {
        StoreType::Nats => {
            let nats_store =
                JetStreamDataStore::new(js_context.clone(), &settings.js_callback_store).await?;
            let callback_state = CallbackState::new(msg_graph, nats_store, callback_store).await?;
            let app = crate::AppState {
                js_context,
                settings,
                orchestrator_state: callback_state,
            };
            crate::serve(app).await.unwrap();
        }
        StoreType::UserDefined(ud_config) => {
            let ud_store = UserDefinedStore::new(ud_config.clone()).await?;
            let callback_state = CallbackState::new(msg_graph, ud_store, callback_store).await?;
            let app = crate::AppState {
                js_context,
                settings,
                orchestrator_state: callback_state,
            };
            crate::serve(app).await.unwrap();
        }
    }
    Ok(())
}
