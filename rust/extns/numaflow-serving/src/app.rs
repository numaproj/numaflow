use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{MatchedPath, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_server::tls_rustls::RustlsConfig;
use axum_server::Handle;
use bytes::Bytes;
use rcgen::{generate_simple_self_signed, Certificate, CertifiedKey, KeyPair};
use tokio::sync::{mpsc, oneshot};
use tower::ServiceBuilder;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use uuid::Uuid;

use crate::{Error, Message, MessageWrapper, Settings};
///
/// manage callbacks
pub(crate) mod callback;

mod response;

use crate::app::callback::state::State as CallbackState;

pub(crate) mod tracker;

use self::callback::store::Store;
use self::response::{ApiError, ServeResponse};

fn generate_certs() -> crate::Result<(Certificate, KeyPair)> {
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec!["localhost".into()])
        .map_err(|e| Error::InitError(format!("Failed to generate cert {:?}", e)))?;
    Ok((cert, key_pair))
}

pub(crate) async fn serve<T>(app: AppState<T>) -> crate::Result<()>
where
    T: Clone + Send + Sync + Store + 'static,
{
    let (cert, key) = generate_certs()?;

    let tls_config = RustlsConfig::from_pem(cert.pem().into(), key.serialize_pem().into())
        .await
        .map_err(|e| Error::InitError(format!("Failed to create tls config {:?}", e)))?;

    // TODO: Move all env variables into one place. Some env variables are loaded when Settings is initialized

    tracing::info!(config = ?app.settings, "Starting server with config and pipeline spec");
    // Start the main server, which serves the application.
    tokio::spawn(start_main_server(app, tls_config));

    Ok(())
}

#[derive(Clone)]
pub struct AppState<T> {
    pub message: mpsc::Sender<MessageWrapper>,
    pub settings: Arc<Settings>,
    pub callback_state: CallbackState<T>,
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

async fn start_main_server<T>(app: AppState<T>, tls_config: RustlsConfig) -> crate::Result<()>
where
    T: Clone + Send + Sync + Store + 'static,
{
    let app_addr: SocketAddr = format!("0.0.0.0:{}", &app.settings.app_listen_port)
        .parse()
        .map_err(|e| Error::InitError(format!("{e:?}")))?;

    let tid_header = app.settings.tid_header.clone();
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

                    tracing::info_span!("request", tid, method=?req.method(), matched_path)
                })
                .on_response(DefaultOnResponse::new().level(tracing::Level::INFO)),
        )
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

    let handle = Handle::new();

    let router = setup_app(app).await.layer(layers);

    tracing::info!(?app_addr, "Starting application server");

    axum_server::bind_rustls(app_addr, tls_config)
        .handle(handle)
        .serve(router.into_make_service())
        .await
        .map_err(|e| Error::InitError(format!("Starting web server for metrics: {}", e)))?;

    Ok(())
}

async fn setup_app<T: Clone + Send + Sync + Store + 'static>(state: AppState<T>) -> Router {
    let router = Router::new()
        .route("/health", get(health_check))
        .route("/livez", get(livez)) // Liveliness check
        .route("/readyz", get(readyz))
        .with_state(state.clone());

    let router = router.nest("/v1/process", routes(state.clone()));
    router
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

// extracts the ID from the headers, if not found, generates a new UUID
fn extract_id_from_headers(tid_header: &str, headers: &HeaderMap) -> String {
    headers.get(tid_header).map_or_else(
        || Uuid::new_v4().to_string(),
        |v| String::from_utf8_lossy(v.as_bytes()).to_string(),
    )
}

fn routes<T: Clone + Send + Sync + Store + 'static>(app_state: AppState<T>) -> Router {
    let jetstream_proxy = jetstream_proxy(app_state);
    jetstream_proxy
}

const CALLBACK_URL_KEY: &str = "X-Numaflow-Callback-Url";
const NUMAFLOW_RESP_ARRAY_LEN: &str = "Numaflow-Array-Len";
const NUMAFLOW_RESP_ARRAY_IDX_LEN: &str = "Numaflow-Array-Index-Len";

struct ProxyState<T> {
    tid_header: String,
    callback: CallbackState<T>,
    stream: String,
    callback_url: String,
    messages: mpsc::Sender<MessageWrapper>,
}

pub(crate) fn jetstream_proxy<T: Clone + Send + Sync + Store + 'static>(
    state: AppState<T>,
) -> Router {
    let proxy_state = Arc::new(ProxyState {
        tid_header: state.settings.tid_header.clone(),
        callback: state.callback_state.clone(),
        stream: state.settings.jetstream.stream.clone(),
        messages: state.message.clone(),
        callback_url: format!(
            "https://{}:{}/v1/process/callback",
            state.settings.host_ip, state.settings.app_listen_port
        ),
    });

    let router = Router::new()
        .route("/async", post(async_publish))
        .with_state(proxy_state);
    router
}

async fn async_publish<T: Send + Sync + Clone + Store>(
    State(proxy_state): State<Arc<ProxyState<T>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<ServeResponse>, ApiError> {
    let id = extract_id_from_headers(&proxy_state.tid_header, &headers);
    let mut msg_headers: HashMap<String, String> = HashMap::new();
    for (k, v) in headers.iter() {
        msg_headers.insert(
            k.to_string(),
            String::from_utf8_lossy(v.as_bytes()).to_string(),
        );
    }
    let (tx, rx) = oneshot::channel();
    let message = MessageWrapper {
        confirm_save: tx,
        message: Message {
            value: body,
            id: id.clone(),
            headers: msg_headers,
        },
    };
    proxy_state.messages.send(message).await.unwrap();
    rx.await.unwrap();

    Ok(Json(ServeResponse::new(
        "Successfully published message".to_string(),
        id,
        StatusCode::OK,
    )))
}
