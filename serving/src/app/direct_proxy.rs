use axum::{
    body::Body,
    extract::{Request, State},
    http::Uri,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use axum_macros::debug_handler;
use hyper_util::client::legacy::connect::HttpConnector;
use tracing::error;

use crate::app::response::ApiError;
use crate::config;

pub(crate) type Client = hyper_util::client::legacy::Client<HttpConnector, Body>;

/// A simple direct reverse-proxy that forwards the request returns the response as-is from the container.

// TODO
// - [ ] fallbacks for better errors https://docs.rs/axum/latest/axum/struct.Router.html#fallbacks
// - [ ] should wild captures `/*upstream` be captured using https://docs.rs/axum/latest/axum/extract/struct.Path.html?
// - [ ] propagate headers
#[derive(Clone, Debug)]
struct ProxyState {
    client: Client,
}

/// Router for direct proxy.
pub(crate) fn direct_proxy(client: Client) -> Router {
    let proxy_state = ProxyState { client };

    Router::new()
        // https://docs.rs/axum/latest/axum/struct.Router.html#wildcards
        .route("/*upstream", get(proxy))
        .with_state(proxy_state)
}

#[debug_handler]
async fn proxy(
    State(proxy_state): State<ProxyState>,
    mut request: Request,
) -> Result<Response, ApiError> {
    // This handler is registered with wildcard capture /*upstream. So the path here will never be empty.
    let path_query = request.uri().path_and_query().unwrap();

    let upstream_uri = format!("http://{}{}", &config().upstream_addr, path_query);
    *request.uri_mut() = Uri::try_from(&upstream_uri)
        .inspect_err(|e| error!(?e, upstream_uri, "Parsing URI for upstream"))
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    Ok(proxy_state
        .client
        .request(request)
        .await
        .inspect_err(|e| error!(?e, "Upstream returned error"))
        .map_err(|e| ApiError::InternalServerError(e.to_string()))?
        .into_response())
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use hyper_util::client::legacy::connect::HttpConnector;
    use hyper_util::rt::TokioExecutor;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    use crate::app::direct_proxy::direct_proxy;
    use crate::config;

    async fn start_server() {
        let addr = config().upstream_addr.to_string();
        let listener = TcpListener::bind(&addr).await.unwrap();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buffer = [0; 1024];
                    let _ = socket.read(&mut buffer).await.unwrap();
                    let request = String::from_utf8_lossy(&buffer[..]);
                    let response = if request.contains("/error") {
                        "HTTP/1.1 500 INTERNAL SERVER ERROR\r\n\
                        content-length: 0\r\n\
                    \r\n"
                    } else {
                        "HTTP/1.1 200 OK\r\n\
                        content-length: 0\r\n\
                    \r\n"
                    };
                    socket.write_all(response.as_bytes()).await.unwrap();
                });
            }
        });
    }

    #[tokio::test]
    async fn test_direct_proxy() {
        start_server().await;
        let client = hyper_util::client::legacy::Client::<(), ()>::builder(TokioExecutor::new())
            .build(HttpConnector::new());

        let app = direct_proxy(client);

        // Test valid request
        let res = Request::builder()
            .method("GET")
            .uri("/upstream")
            .header("test-header", "test_value")
            .body(Body::from("test_body"))
            .unwrap();

        let resp = app.clone().oneshot(res).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);

        // Test error request
        let res = Request::builder()
            .method("GET")
            .uri("/upstream/error")
            .header("test-header", "test_value")
            .body(Body::from("test_body"))
            .unwrap();

        let resp = app.clone().oneshot(res).await.unwrap();

        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
