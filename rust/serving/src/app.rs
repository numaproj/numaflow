use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::Context;
use axum::extract::{MatchedPath, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use axum::{body::Body, http::Request, middleware, response::IntoResponse, routing::get, Router};
use axum_server::tls_rustls::RustlsConfig;
use axum_server::Handle;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use tokio::signal;
use tower::ServiceBuilder;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use tracing::{info, info_span, Level};
use uuid::Uuid;

use self::{
    callback::callback_handler, direct_proxy::direct_proxy, jetstream_proxy::jetstream_proxy,
    message_path::get_message_path,
};
use crate::app::callback::store::Store;
use crate::app::tracker::MessageGraph;
use crate::config::JetStreamConfig;
use crate::pipeline::PipelineDCG;
use crate::Error::InitError;
use crate::Settings;
use crate::{app::callback::state::State as CallbackState, metrics::capture_metrics};

/// manage callbacks
pub(crate) mod callback;
/// simple direct reverse-proxy
mod direct_proxy;
/// write the incoming messages to jetstream
mod jetstream_proxy;
/// Return message path in response to UI requests
mod message_path; // TODO: merge message_path and tracker
mod response;
mod tracker;

/// Everything for numaserve starts here. The routing, middlewares, proxying, etc.
// TODO
// - [ ] implement an proxy and pass in UUID in the header if not present
// - [ ] outer fallback for /v1/direct

/// Start the main application Router and the axum server.
pub(crate) async fn start_main_server(
    settings: Arc<Settings>,
    tls_config: RustlsConfig,
    pipeline_spec: PipelineDCG,
) -> crate::Result<()> {
    let app_addr: SocketAddr = format!("0.0.0.0:{}", &settings.app_listen_port)
        .parse()
        .map_err(|e| InitError(format!("{e:?}")))?;

    let tid_header = settings.tid_header.clone();
    let layers = ServiceBuilder::new()
        // Add tracing to all requests
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(move |req: &Request<Body>| {
                    let tid = req
                        .headers()
                        .get(&tid_header)
                        .and_then(|v| v.to_str().ok())
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| Uuid::new_v4().to_string());

                    let matched_path = req
                        .extensions()
                        .get::<MatchedPath>()
                        .map(MatchedPath::as_str);

                    info_span!("request", tid, method=?req.method(), matched_path)
                })
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        // capture metrics for all requests
        .layer(middleware::from_fn(capture_metrics))
        .layer(
            // Graceful shutdown will wait for outstanding requests to complete. Add a timeout so
            // requests don't hang forever.
            TimeoutLayer::new(Duration::from_secs(settings.drain_timeout_secs)),
        )
        // Add auth middleware to all user facing routes
        .layer(middleware::from_fn_with_state(
            settings.api_auth_token.clone(),
            auth_middleware,
        ));

    // Create the message graph from the pipeline spec and the redis store
    let msg_graph = MessageGraph::from_pipeline(&pipeline_spec).map_err(|e| {
        InitError(format!(
            "Creating message graph from pipeline spec: {:?}",
            e
        ))
    })?;

    // Create a redis store to store the callbacks and the custom responses
    let redis_store =
        callback::store::redisstore::RedisConnection::new(settings.redis.clone()).await?;
    let state = CallbackState::new(msg_graph, redis_store).await?;

    let handle = Handle::new();
    // Spawn a task to gracefully shutdown server.
    tokio::spawn(graceful_shutdown(handle.clone()));

    // Create a Jetstream context
    let js_context = create_js_context(&settings.jetstream).await?;

    let router = setup_app(settings, js_context, state).await?.layer(layers);

    info!(?app_addr, "Starting application server");

    axum_server::bind_rustls(app_addr, tls_config)
        .handle(handle)
        .serve(router.into_make_service())
        .await
        .map_err(|e| InitError(format!("Starting web server for metrics: {}", e)))?;

    Ok(())
}

// Gracefully shutdown the server on receiving SIGINT or SIGTERM
// by sending a shutdown signal to the server using the handle.
async fn graceful_shutdown(handle: Handle) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("sending graceful shutdown signal");

    // Signal the server to shutdown using Handle.
    // TODO: make the duration configurable
    handle.graceful_shutdown(Some(Duration::from_secs(30)));
}

async fn create_js_context(js_config: &JetStreamConfig) -> crate::Result<Context> {
    // Connect to Jetstream with user and password if they are set
    let js_client = match js_config.auth.as_ref() {
        Some(auth) => {
            async_nats::connect_with_options(
                &js_config.url,
                async_nats::ConnectOptions::with_user_and_password(
                    auth.username.clone(),
                    auth.password.clone(),
                ),
            )
            .await
        }
        _ => async_nats::connect(&js_config.url).await,
    }
    .map_err(|e| {
        InitError(format!(
            "Connecting to jetstream server {}: {}",
            &js_config.url, e
        ))
    })?;
    Ok(jetstream::new(js_client))
}

const PUBLISH_ENDPOINTS: [&str; 3] = [
    "/v1/process/sync",
    "/v1/process/sync_serve",
    "/v1/process/async",
];

// auth middleware to do token based authentication for all user facing routes
// if auth is enabled.
async fn auth_middleware(
    State(api_auth_token): State<Option<String>>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let path = request.uri().path();

    // we only need to check for the presence of the auth token in the request headers for the publish endpoints
    if !PUBLISH_ENDPOINTS.contains(&path) {
        return next.run(request).await;
    }

    match api_auth_token {
        Some(token) => {
            // Check for the presence of the auth token in the request headers
            let auth_token = match request.headers().get("Authorization") {
                Some(token) => token,
                None => {
                    return Response::builder()
                        .status(401)
                        .body(Body::empty())
                        .expect("failed to build response")
                }
            };
            if auth_token.to_str().expect("auth token should be a string")
                != format!("Bearer {}", token)
            {
                Response::builder()
                    .status(401)
                    .body(Body::empty())
                    .expect("failed to build response")
            } else {
                next.run(request).await
            }
        }
        None => {
            // If the auth token is not set, we don't need to check for the presence of the auth token in the request headers
            next.run(request).await
        }
    }
}

#[derive(Clone)]
pub(crate) struct AppState<T> {
    pub(crate) settings: Arc<Settings>,
    pub(crate) callback_state: CallbackState<T>,
    pub(crate) context: Context,
}

async fn setup_app<T: Clone + Send + Sync + Store + 'static>(
    settings: Arc<Settings>,
    context: Context,
    callback_state: CallbackState<T>,
) -> crate::Result<Router> {
    let app_state = AppState {
        settings,
        callback_state: callback_state.clone(),
        context: context.clone(),
    };
    let parent = Router::new()
        .route("/health", get(health_check))
        .route("/livez", get(livez)) // Liveliness check
        .route("/readyz", get(readyz))
        .with_state(app_state.clone()); // Readiness check

    // a pool based client implementation for direct proxy, this client is cloneable.
    let client: direct_proxy::Client =
        hyper_util::client::legacy::Client::<(), ()>::builder(TokioExecutor::new())
            .build(HttpConnector::new());

    // let's nest each endpoint
    let app = parent
        .nest(
            "/v1/direct",
            direct_proxy(client, app_state.settings.upstream_addr.clone()),
        )
        .nest("/v1/process", routes(app_state).await?);

    Ok(app)
}

async fn health_check() -> impl IntoResponse {
    "ok"
}

async fn livez() -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

async fn readyz<T: Send + Sync + Clone + Store + 'static>(
    State(app): State<AppState<T>>,
) -> impl IntoResponse {
    if app.callback_state.clone().ready().await
        && app
            .context
            .get_stream(&app.settings.jetstream.stream)
            .await
            .is_ok()
    {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

async fn routes<T: Clone + Send + Sync + Store + 'static>(
    app_state: AppState<T>,
) -> crate::Result<Router> {
    let state = app_state.callback_state.clone();
    let jetstream_proxy = jetstream_proxy(app_state.clone()).await?;
    let callback_router = callback_handler(
        app_state.settings.tid_header.clone(),
        app_state.callback_state.clone(),
    );
    let message_path_handler = get_message_path(state);
    Ok(jetstream_proxy
        .merge(callback_router)
        .merge(message_path_handler))
}

#[cfg(test)]
mod tests {
    use async_nats::jetstream::stream;
    use axum::http::StatusCode;
    use tokio::time::{sleep, Duration};
    use tower::ServiceExt;

    use super::*;
    use crate::app::callback::store::memstore::InMemoryStore;
    use crate::config::generate_certs;

    const PIPELINE_SPEC_ENCODED: &str = "eyJ2ZXJ0aWNlcyI6W3sibmFtZSI6ImluIiwic291cmNlIjp7InNlcnZpbmciOnsiYXV0aCI6bnVsbCwic2VydmljZSI6dHJ1ZSwibXNnSURIZWFkZXJLZXkiOiJYLU51bWFmbG93LUlkIiwic3RvcmUiOnsidXJsIjoicmVkaXM6Ly9yZWRpczo2Mzc5In19fSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIiLCJlbnYiOlt7Im5hbWUiOiJSVVNUX0xPRyIsInZhbHVlIjoiZGVidWcifV19LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6InBsYW5uZXIiLCJ1ZGYiOnsiY29udGFpbmVyIjp7ImltYWdlIjoiYXNjaWk6MC4xIiwiYXJncyI6WyJwbGFubmVyIl0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sImJ1aWx0aW4iOm51bGwsImdyb3VwQnkiOm51bGx9LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6InRpZ2VyIiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsidGlnZXIiXSwicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwiYnVpbHRpbiI6bnVsbCwiZ3JvdXBCeSI6bnVsbH0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiZG9nIiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsiZG9nIl0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sImJ1aWx0aW4iOm51bGwsImdyb3VwQnkiOm51bGx9LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6ImVsZXBoYW50IiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsiZWxlcGhhbnQiXSwicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwiYnVpbHRpbiI6bnVsbCwiZ3JvdXBCeSI6bnVsbH0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiYXNjaWlhcnQiLCJ1ZGYiOnsiY29udGFpbmVyIjp7ImltYWdlIjoiYXNjaWk6MC4xIiwiYXJncyI6WyJhc2NpaWFydCJdLCJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJidWlsdGluIjpudWxsLCJncm91cEJ5IjpudWxsfSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwic2NhbGUiOnsibWluIjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19fSx7Im5hbWUiOiJzZXJ2ZS1zaW5rIiwic2luayI6eyJ1ZHNpbmsiOnsiY29udGFpbmVyIjp7ImltYWdlIjoic2VydmVzaW5rOjAuMSIsImVudiI6W3sibmFtZSI6Ik5VTUFGTE9XX0NBTExCQUNLX1VSTF9LRVkiLCJ2YWx1ZSI6IlgtTnVtYWZsb3ctQ2FsbGJhY2stVXJsIn0seyJuYW1lIjoiTlVNQUZMT1dfTVNHX0lEX0hFQURFUl9LRVkiLCJ2YWx1ZSI6IlgtTnVtYWZsb3ctSWQifV0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn19LCJyZXRyeVN0cmF0ZWd5Ijp7fX0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiZXJyb3Itc2luayIsInNpbmsiOnsidWRzaW5rIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6InNlcnZlc2luazowLjEiLCJlbnYiOlt7Im5hbWUiOiJOVU1BRkxPV19DQUxMQkFDS19VUkxfS0VZIiwidmFsdWUiOiJYLU51bWFmbG93LUNhbGxiYWNrLVVybCJ9LHsibmFtZSI6Ik5VTUFGTE9XX01TR19JRF9IRUFERVJfS0VZIiwidmFsdWUiOiJYLU51bWFmbG93LUlkIn1dLCJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9fSwicmV0cnlTdHJhdGVneSI6e319LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19XSwiZWRnZXMiOlt7ImZyb20iOiJpbiIsInRvIjoicGxhbm5lciIsImNvbmRpdGlvbnMiOm51bGx9LHsiZnJvbSI6InBsYW5uZXIiLCJ0byI6ImFzY2lpYXJ0IiwiY29uZGl0aW9ucyI6eyJ0YWdzIjp7Im9wZXJhdG9yIjoib3IiLCJ2YWx1ZXMiOlsiYXNjaWlhcnQiXX19fSx7ImZyb20iOiJwbGFubmVyIiwidG8iOiJ0aWdlciIsImNvbmRpdGlvbnMiOnsidGFncyI6eyJvcGVyYXRvciI6Im9yIiwidmFsdWVzIjpbInRpZ2VyIl19fX0seyJmcm9tIjoicGxhbm5lciIsInRvIjoiZG9nIiwiY29uZGl0aW9ucyI6eyJ0YWdzIjp7Im9wZXJhdG9yIjoib3IiLCJ2YWx1ZXMiOlsiZG9nIl19fX0seyJmcm9tIjoicGxhbm5lciIsInRvIjoiZWxlcGhhbnQiLCJjb25kaXRpb25zIjp7InRhZ3MiOnsib3BlcmF0b3IiOiJvciIsInZhbHVlcyI6WyJlbGVwaGFudCJdfX19LHsiZnJvbSI6InRpZ2VyIiwidG8iOiJzZXJ2ZS1zaW5rIiwiY29uZGl0aW9ucyI6bnVsbH0seyJmcm9tIjoiZG9nIiwidG8iOiJzZXJ2ZS1zaW5rIiwiY29uZGl0aW9ucyI6bnVsbH0seyJmcm9tIjoiZWxlcGhhbnQiLCJ0byI6InNlcnZlLXNpbmsiLCJjb25kaXRpb25zIjpudWxsfSx7ImZyb20iOiJhc2NpaWFydCIsInRvIjoic2VydmUtc2luayIsImNvbmRpdGlvbnMiOm51bGx9LHsiZnJvbSI6InBsYW5uZXIiLCJ0byI6ImVycm9yLXNpbmsiLCJjb25kaXRpb25zIjp7InRhZ3MiOnsib3BlcmF0b3IiOiJvciIsInZhbHVlcyI6WyJlcnJvciJdfX19XSwibGlmZWN5Y2xlIjp7fSwid2F0ZXJtYXJrIjp7fX0=";

    type Result<T> = core::result::Result<T, Error>;
    type Error = Box<dyn std::error::Error>;

    #[tokio::test]
    async fn test_start_main_server() -> Result<()> {
        let (cert, key) = generate_certs()?;

        let tls_config = RustlsConfig::from_pem(cert.pem().into(), key.serialize_pem().into())
            .await
            .unwrap();

        let settings = Arc::new(Settings {
            app_listen_port: 0,
            ..Settings::default()
        });

        let server = tokio::spawn(async move {
            let pipeline_spec = PIPELINE_SPEC_ENCODED.parse().unwrap();
            let result = start_main_server(settings, tls_config, pipeline_spec).await;
            assert!(result.is_ok())
        });

        // Give the server a little bit of time to start
        sleep(Duration::from_millis(50)).await;

        // Stop the server
        server.abort();
        Ok(())
    }

    #[cfg(feature = "all-tests")]
    #[tokio::test]
    async fn test_setup_app() -> Result<()> {
        let settings = Arc::new(Settings::default());
        let client = async_nats::connect(&settings.jetstream.url).await?;
        let context = jetstream::new(client);
        let stream_name = &settings.jetstream.stream;

        let stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await;

        assert!(stream.is_ok());

        let mem_store = InMemoryStore::new();
        let pipeline_spec = PIPELINE_SPEC_ENCODED.parse().unwrap();
        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec)?;

        let callback_state = CallbackState::new(msg_graph, mem_store).await?;

        let result = setup_app(settings, context, callback_state).await;
        assert!(result.is_ok());
        Ok(())
    }

    #[cfg(feature = "all-tests")]
    #[tokio::test]
    async fn test_livez() -> Result<()> {
        let settings = Arc::new(Settings::default());
        let client = async_nats::connect(&settings.jetstream.url).await?;
        let context = jetstream::new(client);
        let stream_name = &settings.jetstream.stream;

        let stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await;

        assert!(stream.is_ok());

        let mem_store = InMemoryStore::new();
        let pipeline_spec = PIPELINE_SPEC_ENCODED.parse().unwrap();
        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec)?;

        let callback_state = CallbackState::new(msg_graph, mem_store).await?;

        let result = setup_app(settings, context, callback_state).await;

        let request = Request::builder().uri("/livez").body(Body::empty())?;

        let response = result?.oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        Ok(())
    }

    #[cfg(feature = "all-tests")]
    #[tokio::test]
    async fn test_readyz() -> Result<()> {
        let settings = Arc::new(Settings::default());
        let client = async_nats::connect(&settings.jetstream.url).await?;
        let context = jetstream::new(client);
        let stream_name = &settings.jetstream.stream;

        let stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await;

        assert!(stream.is_ok());

        let mem_store = InMemoryStore::new();
        let pipeline_spec = PIPELINE_SPEC_ENCODED.parse().unwrap();
        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec)?;

        let callback_state = CallbackState::new(msg_graph, mem_store).await?;

        let result = setup_app(settings, context, callback_state).await;

        let request = Request::builder().uri("/readyz").body(Body::empty())?;

        let response = result.unwrap().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        Ok(())
    }

    #[tokio::test]
    async fn test_health_check() {
        let response = health_check().await;
        let response = response.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[cfg(feature = "all-tests")]
    #[tokio::test]
    async fn test_auth_middleware() -> Result<()> {
        let settings = Arc::new(Settings::default());
        let client = async_nats::connect(&settings.jetstream.url).await?;
        let context = jetstream::new(client);
        let stream_name = &settings.jetstream.stream;

        let stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await;

        assert!(stream.is_ok());

        let mem_store = InMemoryStore::new();
        let pipeline_spec = PIPELINE_SPEC_ENCODED.parse().unwrap();
        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec)?;
        let callback_state = CallbackState::new(msg_graph, mem_store).await?;

        let app_state = AppState {
            settings,
            callback_state,
            context,
        };

        let app = Router::new()
            .nest("/v1/process", routes(app_state).await.unwrap())
            .layer(middleware::from_fn_with_state(
                Some("test_token".to_owned()),
                auth_middleware,
            ));

        let res = app
            .oneshot(
                axum::extract::Request::builder()
                    .uri("/v1/process/sync")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await?;

        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        Ok(())
    }
}
