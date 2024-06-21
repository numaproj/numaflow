use std::time::Duration;

use axum::{response::IntoResponse, routing::get, Router};
use axum_macros::debug_handler;
use tokio::{net::TcpListener, time::sleep};
use tower_http::trace::TraceLayer;
use tracing::{debug, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "simple_proxy=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let router = app();
    let router = router.layer(TraceLayer::new_for_http());

    let listener = TcpListener::bind("localhost:8888").await.unwrap();
    info!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, router).await.unwrap();
}

fn app() -> Router {
    Router::new()
        .route("/fast", get(|| async {}))
        .route(
            "/slow",
            get(|| async {
                debug!("sleeping");
                sleep(Duration::from_secs(1)).await
            }),
        )
        .route("/", get(root_handler))
}

#[debug_handler]
async fn root_handler() -> impl IntoResponse {
    "ok"
}

#[cfg(test)]
mod tests {

    // inspired from: https://github.com/tokio-rs/axum/blob/main/examples/testing/src/main.rs

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn fast() {
        let app = app();

        let request = Request::builder().uri("/fast").body(Body::empty()).unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn slow() {
        let app = app();

        let request = Request::builder().uri("/slow").body(Body::empty()).unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn root() {
        let app = app();

        let request = Request::builder().uri("/").body(Body::empty()).unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"ok");
    }
}
