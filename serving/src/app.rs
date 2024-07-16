use std::env;
use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::Context;
use axum::extract::{MatchedPath, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use axum::{body::Body, http::Request, middleware, response::IntoResponse, routing::get, Router};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use tokio::net::TcpListener;
use tokio::signal;
use tower::ServiceBuilder;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::{DefaultOnResponse, TraceLayer};
use tracing::{debug, info_span, Level};
use uuid::Uuid;

use self::{
    callback::callback_handler, direct_proxy::direct_proxy, jetstream_proxy::jetstream_proxy,
    message_path::get_message_path,
};
use crate::app::callback::store::Store;
use crate::app::tracker::MessageGraph;
use crate::pipeline::pipeline_spec;
use crate::{app::callback::state::State as CallbackState, config, metrics::capture_metrics};

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

const ENV_NUMAFLOW_SERVING_JETSTREAM_USER: &str = "NUMAFLOW_ISBSVC_JETSTREAM_USER";
const ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD: &str = "NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD";
const ENV_NUMAFLOW_SERVING_AUTH_TOKEN: &str = "NUMAFLOW_SERVING_AUTH_TOKEN";

/// Everything for numaserve starts here. The routing, middlewares, proxying, etc.
// TODO
// - [ ] implement an proxy and pass in UUID in the header if not present
// - [ ] outer fallback for /v1/direct

/// Start the main application Router and the axum server.
pub(crate) async fn start_main_server<A>(addr: A) -> crate::Result<()>
where
    A: tokio::net::ToSocketAddrs + std::fmt::Debug,
{
    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("Creating listener on {:?}: {}", addr, e))?;

    debug!(?addr, "App server started");

    let layers = ServiceBuilder::new()
        // Add tracing to all requests
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|req: &Request<Body>| {
                    let tid = req
                        .headers()
                        .get(&config().tid_header)
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
            TimeoutLayer::new(Duration::from_secs(config().drain_timeout_secs)),
        )
        // Add auth middleware to all user facing routes
        .layer(middleware::from_fn(auth_middleware));

    // Create the message graph from the pipeline spec and the redis store
    let msg_graph = MessageGraph::from_pipeline(pipeline_spec())
        .map_err(|e| format!("Creating message graph from pipeline spec: {:?}", e))?;
    let redis_store = callback::store::redisstore::RedisConnection::new(
        &config().redis.addr,
        config().redis.max_tasks,
    )
    .await?;
    let state = CallbackState::new(msg_graph, redis_store).await?;

    // Create a Jetstream context
    let js_context = create_js_context().await?;

    let router = setup_app(js_context, state).await?.layer(layers);
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| format!("Starting web server: {}", e))?;

    Ok(())
}

async fn create_js_context() -> crate::Result<Context> {
    // Check for user and password in the Jetstream configuration
    let js_config = &config().jetstream;

    // Connect to Jetstream with user and password if they are set
    let js_client = match (
        env::var(ENV_NUMAFLOW_SERVING_JETSTREAM_USER),
        env::var(ENV_NUMAFLOW_SERVING_JETSTREAM_PASSWORD),
    ) {
        (Ok(user), Ok(password)) => {
            async_nats::connect_with_options(
                &js_config.url,
                async_nats::ConnectOptions::with_user_and_password(user, password),
            )
            .await
        }
        _ => async_nats::connect(&js_config.url).await,
    }
    .map_err(|e| {
        format!(
            "Connecting to jetstream server {}: {}",
            &config().jetstream.url,
            e
        )
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
async fn auth_middleware(request: axum::extract::Request, next: Next) -> Response {
    let path = request.uri().path();

    // we only need to check for the presence of the auth token in the request headers for the publish endpoints
    if !PUBLISH_ENDPOINTS.contains(&path) {
        return next.run(request).await;
    }

    match env::var(ENV_NUMAFLOW_SERVING_AUTH_TOKEN) {
        Ok(token) => {
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
        Err(_) => {
            // If the auth token is not set, we don't need to check for the presence of the auth token in the request headers
            next.run(request).await
        }
    }
}

async fn setup_app<T: Clone + Send + Sync + Store + 'static>(
    context: Context,
    state: CallbackState<T>,
) -> crate::Result<Router> {
    let parent = Router::new()
        .route("/health", get(health_check))
        .route("/livez", get(livez)) // Liveliness check
        .route("/readyz", get(readyz))
        .with_state((state.clone(), context.clone())); // Readiness check

    // a pool based client implementation for direct proxy, this client is cloneable.
    let client: direct_proxy::Client =
        hyper_util::client::legacy::Client::<(), ()>::builder(TokioExecutor::new())
            .build(HttpConnector::new());

    // let's nest each endpoint
    let app = parent
        .nest("/v1/direct", direct_proxy(client))
        .nest("/v1/process", routes(context, state).await?);

    Ok(app)
}

async fn health_check() -> impl IntoResponse {
    "ok"
}

async fn livez<T: Send + Sync + Clone + Store + 'static>(
    State((_state, _context)): State<(CallbackState<T>, Context)>,
) -> impl IntoResponse {
    StatusCode::NO_CONTENT
}

async fn readyz<T: Send + Sync + Clone + Store + 'static>(
    State((mut state, context)): State<(CallbackState<T>, Context)>,
) -> impl IntoResponse {
    if state.ready().await && context.get_stream(&config().jetstream.stream).await.is_ok() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

async fn routes<T: Clone + Send + Sync + Store + 'static>(
    context: Context,
    state: CallbackState<T>,
) -> crate::Result<Router> {
    let jetstream_proxy = jetstream_proxy(context, state.clone()).await?;
    let callback_router = callback_handler(state.clone());
    let message_path_handler = get_message_path(state);
    Ok(jetstream_proxy
        .merge(callback_router)
        .merge(message_path_handler))
}

async fn shutdown_signal() {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::callback::store::memstore::InMemoryStore;
    use async_nats::jetstream::stream;
    use axum::http::StatusCode;
    use std::net::SocketAddr;
    use tokio::time::{sleep, Duration};
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_start_main_server() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let server = tokio::spawn(async move {
            let result = start_main_server(addr).await;
            assert!(result.is_ok())
        });

        // Give the server a little bit of time to start
        sleep(Duration::from_millis(50)).await;

        // Stop the server
        server.abort();
    }

    #[cfg(feature = "all-tests")]
    #[tokio::test]
    async fn test_setup_app() {
        let client = async_nats::connect(&config().jetstream.url).await.unwrap();
        let context = jetstream::new(client);
        let stream_name = &config().jetstream.stream;

        let stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await;

        assert!(stream.is_ok());

        let mem_store = InMemoryStore::new();
        let msg_graph = MessageGraph::from_pipeline(pipeline_spec()).unwrap();

        let callback_state = CallbackState::new(msg_graph, mem_store).await.unwrap();

        let result = setup_app(context, callback_state).await;
        assert!(result.is_ok());
    }

    #[cfg(feature = "all-tests")]
    #[tokio::test]
    async fn test_livez() {
        let client = async_nats::connect(&config().jetstream.url).await.unwrap();
        let context = jetstream::new(client);
        let stream_name = &config().jetstream.stream;

        let stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await;

        assert!(stream.is_ok());

        let mem_store = InMemoryStore::new();
        let msg_graph = MessageGraph::from_pipeline(pipeline_spec()).unwrap();

        let callback_state = CallbackState::new(msg_graph, mem_store).await.unwrap();

        let result = setup_app(context, callback_state).await;

        let request = Request::builder()
            .uri("/livez")
            .body(Body::empty())
            .unwrap();

        let response = result.unwrap().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[cfg(feature = "all-tests")]
    #[tokio::test]
    async fn test_readyz() {
        let client = async_nats::connect(&config().jetstream.url).await.unwrap();
        let context = jetstream::new(client);
        let stream_name = &config().jetstream.stream;

        let stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await;

        assert!(stream.is_ok());

        let mem_store = InMemoryStore::new();
        let msg_graph = MessageGraph::from_pipeline(pipeline_spec()).unwrap();

        let callback_state = CallbackState::new(msg_graph, mem_store).await.unwrap();

        let result = setup_app(context, callback_state).await;

        let request = Request::builder()
            .uri("/readyz")
            .body(Body::empty())
            .unwrap();

        let response = result.unwrap().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_health_check() {
        let response = health_check().await;
        let response = response.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[cfg(feature = "all-tests")]
    #[tokio::test]
    async fn test_auth_middleware() {
        let client = async_nats::connect(&config().jetstream.url).await.unwrap();
        let context = jetstream::new(client);
        let stream_name = &config().jetstream.stream;

        let stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await;

        assert!(stream.is_ok());

        let mem_store = InMemoryStore::new();
        let msg_graph = MessageGraph::from_pipeline(pipeline_spec()).unwrap();
        let callback_state = CallbackState::new(msg_graph, mem_store).await.unwrap();

        let app = Router::new()
            .nest(
                "/v1/process",
                routes(context, callback_state).await.unwrap(),
            )
            .layer(middleware::from_fn(auth_middleware));

        env::set_var(ENV_NUMAFLOW_SERVING_AUTH_TOKEN, "test_token");
        let res = app
            .oneshot(
                axum::extract::Request::builder()
                    .uri("/v1/process/sync")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        env::remove_var(ENV_NUMAFLOW_SERVING_AUTH_TOKEN);
    }
}
