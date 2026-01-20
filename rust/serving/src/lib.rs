use async_nats::jetstream::Context;
use async_nats::{ConnectOptions, jetstream};
use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub use self::error::{Error, Result};
use crate::app::orchestrator::OrchestratorState as CallbackState;
use crate::app::store::cbstore::jetstream_store::JetStreamCallbackStore;
use crate::app::store::datastore::jetstream::JetStreamDataStore;
use crate::app::store::datastore::user_defined::UserDefinedStore;
use crate::app::tracker::MessageGraph;
use crate::app::{start_main_server_http, start_main_server_https};
use crate::config::StoreType;
use crate::config::generate_certs;
use crate::metrics::start_https_metrics_server;
use app::orchestrator::OrchestratorState;

mod app;

mod config;
pub use {
    config::DEFAULT_CALLBACK_URL_HEADER_KEY, config::DEFAULT_ID_HEADER,
    config::DEFAULT_POD_HASH_KEY, config::ENV_MIN_PIPELINE_SPEC, config::Settings,
};

mod error;
mod metrics;
mod pipeline;

use crate::app::store::cbstore::CallbackStore;
use crate::app::store::datastore::DataStore;
use crate::app::store::status::StatusTracker;

pub mod callback;

#[derive(Clone)]
pub(crate) struct AppState<T, U> {
    pub(crate) js_context: Context,
    pub(crate) settings: Arc<Settings>,
    pub(crate) orchestrator_state: OrchestratorState<T, U>,
    pub(crate) cancellation_token: CancellationToken,
}

/// Sets up and starts the main application server and the metrics server
/// using the provided TLS configuration. It ensures both servers run concurrently and
/// handles any errors that may occur during their execution.
async fn serve<T, U>(
    app: AppState<T, U>,
    cln_token: CancellationToken,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>
where
    T: Clone + Send + Sync + DataStore + 'static,
    U: Clone + Send + Sync + CallbackStore + 'static,
{
    let (cert, key) = generate_certs()?;

    let tls_config = RustlsConfig::from_pem(cert.pem().into(), key.serialize_pem().into())
        .await
        .map_err(|e| format!("Failed to create tls config {e:?}"))?;

    info!(config = ?app.settings, "Starting server with config and pipeline spec");

    // Start the metrics server, which serves the prometheus metrics.
    let metrics_addr: SocketAddr =
        format!("0.0.0.0:{}", &app.settings.metrics_server_listen_port).parse()?;

    let metrics_server_handle = tokio::spawn(start_https_metrics_server(
        metrics_addr,
        tls_config.clone(),
        cln_token.clone(),
    ));

    // Start the main server, which serves the application.
    let app_server_https_handle = tokio::spawn(start_main_server_https(
        app.clone(),
        tls_config,
        cln_token.clone(),
    ));

    let app_server_http_handle = tokio::spawn(start_main_server_http(app, cln_token));

    // TODO: is try_join the best? we need to short-circuit at the first failure
    tokio::try_join!(
        flatten(app_server_https_handle),
        flatten(app_server_http_handle),
        flatten(metrics_server_handle)
    )?;

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
async fn start(js_context: Context, settings: Arc<Settings>) -> Result<()> {
    let cancel_token = CancellationToken::new();

    // create a callback store for tracking
    let callback_store = JetStreamCallbackStore::new(
        js_context.clone(),
        settings.pod_hash,
        settings.js_callback_store,
        cancel_token.clone(),
    )
    .await?;

    // Create the message graph from the pipeline spec and the redis store
    let msg_graph = MessageGraph::from_pipeline(&settings.pipeline_spec).map_err(|e| {
        Error::InitError(format!("Creating message graph from pipeline spec: {e:?}"))
    })?;

    // Create a store (builtin or user-defined) to store the callbacks and the custom responses
    match &settings.store_type {
        StoreType::Nats => {
            let nats_store = JetStreamDataStore::new(
                js_context.clone(),
                settings.js_response_store,
                settings.pod_hash,
                cancel_token.clone(),
            )
            .await?;
            let status_tracker = StatusTracker::new(
                js_context.clone(),
                settings.js_status_store,
                settings.pod_hash,
                Some(settings.js_response_store.to_string()),
            )
            .await?;
            let callback_state =
                CallbackState::new(msg_graph, nats_store, callback_store, status_tracker).await?;
            let app = AppState {
                js_context,
                settings,
                orchestrator_state: callback_state,
                cancellation_token: cancel_token.clone(),
            };
            info!("Starting app");
            serve(app, cancel_token.clone())
                .await
                .map_err(|e| Error::Store(e.to_string()))?
        }
        StoreType::UserDefined(ud_config) => {
            let status_tracker = StatusTracker::new(
                js_context.clone(),
                settings.js_status_store,
                settings.pod_hash,
                None,
            )
            .await?;
            let ud_store = UserDefinedStore::new(ud_config.clone()).await?;
            let callback_state =
                CallbackState::new(msg_graph, ud_store, callback_store, status_tracker).await?;
            let app = AppState {
                js_context,
                settings,
                orchestrator_state: callback_state,
                cancellation_token: cancel_token.clone(),
            };
            serve(app, cancel_token.clone())
                .await
                .map_err(|e| Error::Store(e.to_string()))?
        }
    }
    info!("Done app");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::Settings;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_run() {
        use std::time::Duration;

        use async_nats::jetstream;
        use http::StatusCode;

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let env_vars = [
            ("NUMAFLOW_POD", "serving-serve-cbdf"),
            (
                "NUMAFLOW_SERVING_CALLBACK_STORE",
                "serving-test-run-kv-store",
            ),
            (
                "NUMAFLOW_SERVING_RESPONSE_STORE",
                "serving-test-run-kv-store",
            ),
            ("NUMAFLOW_SERVING_STATUS_STORE", "serving-test-run-kv-store"),
            (
                "NUMAFLOW_SERVING_SPEC",
                "eyJhdXRoIjpudWxsLCJzZXJ2aWNlIjp0cnVlLCJtc2dJREhlYWRlcktleSI6IlgtTnVtYWZsb3ctSWQiLCJwb3J0cyI6eyJodHRwcyI6MzI0NDMsImh0dHAiOjMyNDQ1fX0=",
            ),
            (
                "NUMAFLOW_SERVING_MIN_PIPELINE_SPEC",
                "eyJ2ZXJ0aWNlcyI6W3sibmFtZSI6ImluIiwic291cmNlIjp7ImpldHN0cmVhbSI6eyJ1cmwiOiJuYXRzOi8vbG9jYWxob3N0OjQyMjIiLCJzdHJlYW0iOiJzZXJ2aW5nLXNvdXJjZS1zaW1wbGUtcGlwZWxpbmUiLCJ0bHMiOm51bGwsImF1dGgiOnsiYmFzaWMiOnsidXNlciI6eyJuYW1lIjoiaXNic3ZjLWRlZmF1bHQtanMtY2xpZW50LWF1dGgiLCJrZXkiOiJjbGllbnQtYXV0aC11c2VyIn0sInBhc3N3b3JkIjp7Im5hbWUiOiJpc2JzdmMtZGVmYXVsdC1qcy1jbGllbnQtYXV0aCIsImtleSI6ImNsaWVudC1hdXRoLXBhc3N3b3JkIn19fX19LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJlbnYiOlt7Im5hbWUiOiJOVU1BRkxPV19DQUxMQkFDS19FTkFCTEVEIiwidmFsdWUiOiJ0cnVlIn0seyJuYW1lIjoiTlVNQUZMT1dfU0VSVklOR19TT1VSQ0VfU0VUVElOR1MiLCJ2YWx1ZSI6ImV5SmhkWFJvSWpwdWRXeHNMQ0p6WlhKMmFXTmxJanAwY25WbExDSnRjMmRKUkVobFlXUmxja3RsZVNJNklsZ3RUblZ0WVdac2IzY3RTV1FpZlE9PSJ9LHsibmFtZSI6Ik5VTUFGTE9XX1NFUlZJTkdfS1ZfU1RPUkUiLCJ2YWx1ZSI6InNlcnZpbmctc3RvcmUtc2ltcGxlLXBpcGVsaW5lX1NFUlZJTkdfS1ZfU1RPUkUifV19LCJzY2FsZSI6eyJtaW4iOjEsIm1heCI6MX0sImluaXRDb250YWluZXJzIjpbeyJuYW1lIjoidmFsaWRhdGUtc3RyZWFtLWluaXQiLCJpbWFnZSI6InF1YXkuaW8vbnVtYXByb2ovbnVtYWZsb3c6ODA4MkU2NTYtQTAxOS00QjE1LUE2ODQtNTg2RkM3RDJDQTFGIiwiYXJncyI6WyJpc2JzdmMtdmFsaWRhdGUiLCItLWlzYnN2Yy10eXBlPWpldHN0cmVhbSIsIi0tYnVmZmVycz1zZXJ2aW5nLXNvdXJjZS1zaW1wbGUtcGlwZWxpbmUiXSwiZW52IjpbeyJuYW1lIjoiTlVNQUZMT1dfUElQRUxJTkVfTkFNRSIsInZhbHVlIjoicy1zaW1wbGUtcGlwZWxpbmUifSx7Im5hbWUiOiJHT0RFQlVHIn0seyJuYW1lIjoiTlVNQUZMT1dfSVNCU1ZDX0NPTkZJRyIsInZhbHVlIjoiZXlKcVpYUnpkSEpsWVcwaU9uc2lkWEpzSWpvaWJtRjBjem92TDJselluTjJZeTFrWldaaGRXeDBMV3B6TFhOMll5NWtaV1poZFd4MExuTjJZem8wTWpJeUlpd2lZWFYwYUNJNmV5SmlZWE5wWXlJNmV5SjFjMlZ5SWpwN0ltNWhiV1VpT2lKcGMySnpkbU10WkdWbVlYVnNkQzFxY3kxamJHbGxiblF0WVhWMGFDSXNJbXRsZVNJNkltTnNhV1Z1ZEMxaGRYUm9MWFZ6WlhJaWZTd2ljR0Z6YzNkdmNtUWlPbnNpYm1GdFpTSTZJbWx6WW5OMll5MWtaV1poZFd4MExXcHpMV05zYVdWdWRDMWhkWFJvSWl3aWEyVjVJam9pWTJ4cFpXNTBMV0YxZEdndGNHRnpjM2R2Y21RaWZYMTlMQ0p6ZEhKbFlXMURiMjVtYVdjaU9pSmpiMjV6ZFcxbGNqcGNiaUFnWVdOcmQyRnBkRG9nTmpCelhHNGdJRzFoZUdGamEzQmxibVJwYm1jNklESTFNREF3WEc1dmRHSjFZMnRsZERwY2JpQWdhR2x6ZEc5eWVUb2dNVnh1SUNCdFlYaGllWFJsY3pvZ01GeHVJQ0J0WVhoMllXeDFaWE5wZW1VNklEQmNiaUFnY21Wd2JHbGpZWE02SUROY2JpQWdjM1J2Y21GblpUb2dNRnh1SUNCMGRHdzZJRE5vWEc1d2NtOWpZblZqYTJWME9seHVJQ0JvYVhOMGIzSjVPaUF4WEc0Z0lHMWhlR0o1ZEdWek9pQXdYRzRnSUcxaGVIWmhiSFZsYzJsNlpUb2dNRnh1SUNCeVpYQnNhV05oY3pvZ00xeHVJQ0J6ZEc5eVlXZGxPaUF3WEc0Z0lIUjBiRG9nTnpKb1hHNXpkSEpsWVcwNlhHNGdJR1IxY0d4cFkyRjBaWE02SURZd2MxeHVJQ0J0WVhoaFoyVTZJRGN5YUZ4dUlDQnRZWGhpZVhSbGN6b2dMVEZjYmlBZ2JXRjRiWE5uY3pvZ01UQXdNREF3WEc0Z0lISmxjR3hwWTJGek9pQXpYRzRnSUhKbGRHVnVkR2x2YmpvZ01GeHVJQ0J6ZEc5eVlXZGxPaUF3WEc0aWZYMD0ifSx7Im5hbWUiOiJOVU1BRkxPV19JU0JTVkNfSkVUU1RSRUFNX1VSTCIsInZhbHVlIjoibmF0czovL2lzYnN2Yy1kZWZhdWx0LWpzLXN2Yy5kZWZhdWx0LnN2Yzo0MjIyIn0seyJuYW1lIjoiTlVNQUZMT1dfSVNCU1ZDX0pFVFNUUkVBTV9UTFNfRU5BQkxFRCIsInZhbHVlIjoiZmFsc2UifSx7Im5hbWUiOiJOVU1BRkxPV19JU0JTVkNfSkVUU1RSRUFNX1VTRVIiLCJ2YWx1ZUZyb20iOnsic2VjcmV0S2V5UmVmIjp7Im5hbWUiOiJpc2JzdmMtZGVmYXVsdC1qcy1jbGllbnQtYXV0aCIsImtleSI6ImNsaWVudC1hdXRoLXVzZXIifX19LHsibmFtZSI6Ik5VTUFGTE9XX0lTQlNWQ19KRVRTVFJFQU1fUEFTU1dPUkQiLCJ2YWx1ZUZyb20iOnsic2VjcmV0S2V5UmVmIjp7Im5hbWUiOiJpc2JzdmMtZGVmYXVsdC1qcy1jbGllbnQtYXV0aCIsImtleSI6ImNsaWVudC1hdXRoLXBhc3N3b3JkIn19fV0sInJlc291cmNlcyI6eyJyZXF1ZXN0cyI6eyJjcHUiOiIxMDBtIiwibWVtb3J5IjoiMTI4TWkifX0sImltYWdlUHVsbFBvbGljeSI6IklmTm90UHJlc2VudCJ9XSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19fSx7Im5hbWUiOiJjYXQiLCJ1ZGYiOnsiY29udGFpbmVyIjp7ImltYWdlIjoicXVheS5pby9udW1haW8vbnVtYWZsb3ctZ28vbWFwLWZvcndhcmQtbWVzc2FnZTpzdGFibGUiLCJyZXNvdXJjZXMiOnt9fSwiYnVpbHRpbiI6bnVsbCwiZ3JvdXBCeSI6bnVsbH0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImVudiI6W3sibmFtZSI6Ik5VTUFGTE9XX0NBTExCQUNLX0VOQUJMRUQiLCJ2YWx1ZSI6InRydWUifSx7Im5hbWUiOiJOVU1BRkxPV19TRVJWSU5HX1NPVVJDRV9TRVRUSU5HUyIsInZhbHVlIjoiZXlKaGRYUm9JanB1ZFd4c0xDSnpaWEoyYVdObElqcDBjblZsTENKdGMyZEpSRWhsWVdSbGNrdGxlU0k2SWxndFRuVnRZV1pzYjNjdFNXUWlmUT09In0seyJuYW1lIjoiTlVNQUZMT1dfU0VSVklOR19LVl9TVE9SRSIsInZhbHVlIjoic2VydmluZy1zdG9yZS1zaW1wbGUtcGlwZWxpbmVfU0VSVklOR19LVl9TVE9SRSJ9XX0sInNjYWxlIjp7Im1pbiI6MSwibWF4IjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19fSx7Im5hbWUiOiJvdXQiLCJzaW5rIjp7InVkc2luayI6eyJjb250YWluZXIiOnsiaW1hZ2UiOiJxdWF5LmlvL251bWFpby9udW1hZmxvdy1nby9zaW5rLXNlcnZlOnN0YWJsZSIsInJlc291cmNlcyI6e319fSwicmV0cnlTdHJhdGVneSI6e319LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJlbnYiOlt7Im5hbWUiOiJOVU1BRkxPV19DQUxMQkFDS19FTkFCTEVEIiwidmFsdWUiOiJ0cnVlIn0seyJuYW1lIjoiTlVNQUZMT1dfU0VSVklOR19TT1VSQ0VfU0VUVElOR1MiLCJ2YWx1ZSI6ImV5SmhkWFJvSWpwdWRXeHNMQ0p6WlhKMmFXTmxJanAwY25WbExDSnRjMmRKUkVobFlXUmxja3RsZVNJNklsZ3RUblZ0WVdac2IzY3RTV1FpZlE9PSJ9LHsibmFtZSI6Ik5VTUFGTE9XX1NFUlZJTkdfS1ZfU1RPUkUiLCJ2YWx1ZSI6InNlcnZpbmctc3RvcmUtc2ltcGxlLXBpcGVsaW5lX1NFUlZJTkdfS1ZfU1RPUkUifV19LCJzY2FsZSI6eyJtaW4iOjEsIm1heCI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX1dLCJlZGdlcyI6W3siZnJvbSI6ImluIiwidG8iOiJjYXQiLCJjb25kaXRpb25zIjpudWxsfSx7ImZyb20iOiJjYXQiLCJ0byI6Im91dCIsImNvbmRpdGlvbnMiOm51bGx9XSwibGlmZWN5Y2xlIjp7fSwid2F0ZXJtYXJrIjp7fX0=",
            ),
        ];
        let vars: HashMap<String, String> = env_vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let cfg: Settings = vars.try_into().unwrap();

        let store_name = "serving-test-run-kv-store";
        let message_stream_name = "serving-source-simple-pipeline";
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let _ = context.delete_key_value(store_name).await;
        let _ = context.delete_stream(message_stream_name).await;

        let _ = context
            .create_key_value(jetstream::kv::Config {
                bucket: store_name.to_string(),
                history: 5,
                ..Default::default()
            })
            .await
            .unwrap();

        context
            .create_stream(jetstream::stream::Config {
                name: message_stream_name.into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let server_task = tokio::spawn(async move {
            super::run(cfg).await.unwrap();
        });
        // Wait for the server to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        let response = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap()
            .get("https://localhost:32443/health")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap()
            .get("http://localhost:32445/health")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        server_task.abort();
    }
}
