use std::net::SocketAddr;
use std::time::Duration;

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
use tower_http::classify::ServerErrorsFailureClass;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, info_span, Span};
use uuid::Uuid;

use self::{
    callback::callback_handler, direct_proxy::direct_proxy, jetstream_proxy::jetstream_proxy,
    message_path::get_message_path,
};
use crate::app::callback::store::Store;
use crate::metrics::capture_metrics;
use crate::AppState;
use crate::Error::InitError;

/// manage callbacks
pub(crate) mod callback;
/// simple direct reverse-proxy
mod direct_proxy;
/// write the incoming messages to jetstream
mod jetstream_proxy;
/// Return message path in response to UI requests
mod message_path; // TODO: merge message_path and tracker
mod response;
pub(crate) mod tracker;

/// Everything for numaserve starts here. The routing, middlewares, proxying, etc.
// TODO
// - [ ] implement an proxy and pass in UUID in the header if not present
// - [ ] outer fallback for /v1/direct

/// Start the main application Router and the axum server.
pub(crate) async fn start_main_server<T>(
    app: AppState<T>,
    tls_config: RustlsConfig,
) -> crate::Result<()>
where
    T: Clone + Send + Sync + Store + 'static,
{
    let app_addr: SocketAddr = format!("0.0.0.0:{}", &app.settings.app_listen_port)
        .parse()
        .map_err(|e| InitError(format!("{e:?}")))?;

    let handle = Handle::new();
    // Spawn a task to gracefully shutdown server.
    tokio::spawn(graceful_shutdown(handle.clone()));

    info!(?app_addr, "Starting application server");

    let router = router_with_auth(app).await?;

    axum_server::bind_rustls(app_addr, tls_config)
        .handle(handle)
        .serve(router.into_make_service())
        .await
        .map_err(|e| InitError(format!("Starting web server for metrics: {}", e)))?;

    Ok(())
}

pub(crate) async fn router_with_auth<T>(app: AppState<T>) -> crate::Result<Router>
where
    T: Clone + Send + Sync + Store + 'static,
{
    let layers = ServiceBuilder::new()
        // Add tracing to all requests
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(move |req: &Request<Body>| {
                    let req_path = req.uri().path();
                    if ["/metrics", "/readyz", "/livez", "/sidecar-livez"].contains(&req_path) {
                        // We don't need request ID for these endpoints
                        return info_span!("request", method=?req.method(), path=req_path);
                    }

                    // Generate a tid with good enough randomness and not too long
                    // Example of a UUID v7: 01951b72-d0f4-711e-baba-4efe03d9cb76
                    // We use the characters representing timestamp in milliseconds (without '-'), and last 5 characters for randomness.
                    let uuid = Uuid::now_v7().to_string();
                    let tid = format!("{}{}{}", &uuid[..8], &uuid[10..13], &uuid[uuid.len() - 5..]);

                    let matched_path = req
                        .extensions()
                        .get::<MatchedPath>()
                        .map(MatchedPath::as_str);

                    info_span!("request", tid, method=?req.method(), path=req_path, matched_path)
                })
                .on_response(
                    |response: &Response<Body>, latency: Duration, _span: &Span| {
                        if response.status().is_server_error() {
                            // 5xx responses will be logged at 'error' level in `on_failure`
                            return;
                        }
                        tracing::info!(status=?response.status(), ?latency)
                    },
                )
                .on_failure(
                    |error: ServerErrorsFailureClass, latency: Duration, _span: &Span| {
                        tracing::error!(?error, ?latency, "Server error");
                    },
                ),
        )
        // capture metrics for all requests
        .layer(middleware::from_fn(capture_metrics))
        .layer(
            // Graceful shutdown will wait for outstanding requests to complete. Add a timeout so
            // requests don't hang forever.
            TimeoutLayer::new(Duration::from_secs(app.settings.drain_timeout_secs)),
        )
        // Add auth middleware to all user facing routes
        .layer(middleware::from_fn_with_state(
            app.settings.api_auth_token.clone(),
            auth_middleware,
        ));
    Ok(setup_app(app).await?.layer(layers))
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

async fn setup_app<T: Clone + Send + Sync + Store + 'static>(
    app: AppState<T>,
) -> crate::Result<Router> {
    let parent = Router::new()
        .route("/health", get(health_check))
        .route("/livez", get(livez)) // Liveliness check
        .route("/readyz", get(readyz))
        .with_state(app.clone()); // Readiness check

    // a pool based client implementation for direct proxy, this client is cloneable.
    let client: direct_proxy::Client =
        hyper_util::client::legacy::Client::<(), ()>::builder(TokioExecutor::new())
            .build(HttpConnector::new());

    // let's nest each endpoint
    let app = parent
        .nest(
            "/v1/direct",
            direct_proxy(client, app.settings.upstream_addr.clone()),
        )
        .nest("/v1/process", routes(app).await?);

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
    if app.callback_state.clone().ready().await {
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
    use std::sync::Arc;

    use axum::http::StatusCode;
    use callback::state::State as CallbackState;
    use tokio::sync::mpsc;
    use tower::ServiceExt;
    use tracker::MessageGraph;

    use super::*;
    use crate::app::callback::store::memstore::InMemoryStore;
    use crate::Settings;

    const PIPELINE_SPEC_ENCODED: &str = "eyJ2ZXJ0aWNlcyI6W3sibmFtZSI6ImluIiwic291cmNlIjp7InNlcnZpbmciOnsiYXV0aCI6bnVsbCwic2VydmljZSI6dHJ1ZSwibXNnSURIZWFkZXJLZXkiOiJYLU51bWFmbG93LUlkIiwic3RvcmUiOnsidXJsIjoicmVkaXM6Ly9yZWRpczo2Mzc5In19fSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIiLCJlbnYiOlt7Im5hbWUiOiJSVVNUX0xPRyIsInZhbHVlIjoiZGVidWcifV19LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6InBsYW5uZXIiLCJ1ZGYiOnsiY29udGFpbmVyIjp7ImltYWdlIjoiYXNjaWk6MC4xIiwiYXJncyI6WyJwbGFubmVyIl0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sImJ1aWx0aW4iOm51bGwsImdyb3VwQnkiOm51bGx9LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6InRpZ2VyIiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsidGlnZXIiXSwicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwiYnVpbHRpbiI6bnVsbCwiZ3JvdXBCeSI6bnVsbH0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiZG9nIiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsiZG9nIl0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sImJ1aWx0aW4iOm51bGwsImdyb3VwQnkiOm51bGx9LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6ImVsZXBoYW50IiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsiZWxlcGhhbnQiXSwicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwiYnVpbHRpbiI6bnVsbCwiZ3JvdXBCeSI6bnVsbH0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiYXNjaWlhcnQiLCJ1ZGYiOnsiY29udGFpbmVyIjp7ImltYWdlIjoiYXNjaWk6MC4xIiwiYXJncyI6WyJhc2NpaWFydCJdLCJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJidWlsdGluIjpudWxsLCJncm91cEJ5IjpudWxsfSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwic2NhbGUiOnsibWluIjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19fSx7Im5hbWUiOiJzZXJ2ZS1zaW5rIiwic2luayI6eyJ1ZHNpbmsiOnsiY29udGFpbmVyIjp7ImltYWdlIjoic2VydmVzaW5rOjAuMSIsImVudiI6W3sibmFtZSI6Ik5VTUFGTE9XX0NBTExCQUNLX1VSTF9LRVkiLCJ2YWx1ZSI6IlgtTnVtYWZsb3ctQ2FsbGJhY2stVXJsIn0seyJuYW1lIjoiTlVNQUZMT1dfTVNHX0lEX0hFQURFUl9LRVkiLCJ2YWx1ZSI6IlgtTnVtYWZsb3ctSWQifV0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn19LCJyZXRyeVN0cmF0ZWd5Ijp7fX0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiZXJyb3Itc2luayIsInNpbmsiOnsidWRzaW5rIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6InNlcnZlc2luazowLjEiLCJlbnYiOlt7Im5hbWUiOiJOVU1BRkxPV19DQUxMQkFDS19VUkxfS0VZIiwidmFsdWUiOiJYLU51bWFmbG93LUNhbGxiYWNrLVVybCJ9LHsibmFtZSI6Ik5VTUFGTE9XX01TR19JRF9IRUFERVJfS0VZIiwidmFsdWUiOiJYLU51bWFmbG93LUlkIn1dLCJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9fSwicmV0cnlTdHJhdGVneSI6e319LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19XSwiZWRnZXMiOlt7ImZyb20iOiJpbiIsInRvIjoicGxhbm5lciIsImNvbmRpdGlvbnMiOm51bGx9LHsiZnJvbSI6InBsYW5uZXIiLCJ0byI6ImFzY2lpYXJ0IiwiY29uZGl0aW9ucyI6eyJ0YWdzIjp7Im9wZXJhdG9yIjoib3IiLCJ2YWx1ZXMiOlsiYXNjaWlhcnQiXX19fSx7ImZyb20iOiJwbGFubmVyIiwidG8iOiJ0aWdlciIsImNvbmRpdGlvbnMiOnsidGFncyI6eyJvcGVyYXRvciI6Im9yIiwidmFsdWVzIjpbInRpZ2VyIl19fX0seyJmcm9tIjoicGxhbm5lciIsInRvIjoiZG9nIiwiY29uZGl0aW9ucyI6eyJ0YWdzIjp7Im9wZXJhdG9yIjoib3IiLCJ2YWx1ZXMiOlsiZG9nIl19fX0seyJmcm9tIjoicGxhbm5lciIsInRvIjoiZWxlcGhhbnQiLCJjb25kaXRpb25zIjp7InRhZ3MiOnsib3BlcmF0b3IiOiJvciIsInZhbHVlcyI6WyJlbGVwaGFudCJdfX19LHsiZnJvbSI6InRpZ2VyIiwidG8iOiJzZXJ2ZS1zaW5rIiwiY29uZGl0aW9ucyI6bnVsbH0seyJmcm9tIjoiZG9nIiwidG8iOiJzZXJ2ZS1zaW5rIiwiY29uZGl0aW9ucyI6bnVsbH0seyJmcm9tIjoiZWxlcGhhbnQiLCJ0byI6InNlcnZlLXNpbmsiLCJjb25kaXRpb25zIjpudWxsfSx7ImZyb20iOiJhc2NpaWFydCIsInRvIjoic2VydmUtc2luayIsImNvbmRpdGlvbnMiOm51bGx9LHsiZnJvbSI6InBsYW5uZXIiLCJ0byI6ImVycm9yLXNpbmsiLCJjb25kaXRpb25zIjp7InRhZ3MiOnsib3BlcmF0b3IiOiJvciIsInZhbHVlcyI6WyJlcnJvciJdfX19XSwibGlmZWN5Y2xlIjp7fSwid2F0ZXJtYXJrIjp7fX0=";

    type Result<T> = core::result::Result<T, Error>;
    type Error = Box<dyn std::error::Error>;

    #[tokio::test]
    async fn test_setup_app() -> Result<()> {
        let settings = Arc::new(Settings::default());

        let mem_store = InMemoryStore::new();
        let pipeline_spec = PIPELINE_SPEC_ENCODED.parse().unwrap();
        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec)?;

        let callback_state = CallbackState::new(msg_graph, mem_store).await?;
        let (tx, _) = mpsc::channel(10);
        let app = AppState {
            message: tx,
            settings,
            callback_state,
        };

        let result = setup_app(app).await;
        assert!(result.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_health_check_endpoints() -> Result<()> {
        let settings = Arc::new(Settings::default());

        let mem_store = InMemoryStore::new();
        let msg_graph = MessageGraph::from_pipeline(&settings.pipeline_spec)?;
        let callback_state = CallbackState::new(msg_graph, mem_store).await?;

        let (messages_tx, _messages_rx) = mpsc::channel(10);
        let app = AppState {
            message: messages_tx,
            settings,
            callback_state,
        };

        let router = setup_app(app).await.unwrap();

        let request = Request::builder().uri("/livez").body(Body::empty())?;
        let response = router.clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let request = Request::builder().uri("/readyz").body(Body::empty())?;
        let response = router.clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let request = Request::builder().uri("/health").body(Body::empty())?;
        let response = router.clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::OK);
        Ok(())
    }

    #[tokio::test]
    async fn test_auth_middleware() -> Result<()> {
        let settings = Settings {
            api_auth_token: Some("test-token".into()),
            ..Default::default()
        };

        let mem_store = InMemoryStore::new();
        let pipeline_spec = PIPELINE_SPEC_ENCODED.parse().unwrap();
        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec)?;
        let callback_state = CallbackState::new(msg_graph, mem_store).await?;

        let (messages_tx, _messages_rx) = mpsc::channel(10);

        let app_state = AppState {
            message: messages_tx,
            settings: Arc::new(settings),
            callback_state,
        };

        let router = router_with_auth(app_state).await.unwrap();
        let res = router
            .oneshot(
                axum::extract::Request::builder()
                    .method("POST")
                    .uri("/v1/process/sync")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await?;

        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        Ok(())
    }
}
