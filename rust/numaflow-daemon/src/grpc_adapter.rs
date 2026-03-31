use axum::body::Body;
use axum::response::Response;
use http::Request;
use http_body_util::BodyExt;
use numaflow_pb::servers::mvtxdaemon::mono_vertex_daemon_service_server::MonoVertexDaemonServiceServer;
use std::task::{Context, Poll};
use tower::Service;

use crate::MvtxDaemonService;

/// GrpcAdapter implements tower::Service such that it can serve gRPC requests.
///
/// It serves as a fallback service for the Axum Router.
/// The axum router handles HTTP 1.1 requests like /api/v1/status using .route(). It's not able to recognize gRPC h2 requests.
/// gRPC h2 requests like /mvtxdaemon.MonoVertexDaemonService/GetMonoVertexMetrics are redirected to the GrpcAdapter.
#[derive(Clone)]
pub(crate) struct GrpcAdapter {
    inner: MonoVertexDaemonServiceServer<MvtxDaemonService>,
}

impl GrpcAdapter {
    pub(crate) fn new() -> Self {
        Self {
            inner: MonoVertexDaemonServiceServer::new(MvtxDaemonService),
        }
    }
}

impl Service<Request<Body>> for GrpcAdapter {
    type Response = Response<Body>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<Body>, std::convert::Infallible>>
                + Send,
        >,
    >;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let res = inner.call(req).await.map_err(|e| match e {})?;
            let (parts, body) = res.into_parts();
            let axum_body = Body::new(body.map_err(axum::Error::new));
            Ok(Response::from_parts(parts, axum_body))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;

    /// gRPC unary request: 1 byte compressed-flag (0) + 4 byte length (big-endian) + message.
    /// For Empty, message is 0 bytes, so body = [0, 0, 0, 0, 0].
    fn grpc_empty_body() -> Body {
        Body::from(vec![0u8, 0, 0, 0, 0])
    }

    #[tokio::test]
    async fn grpc_adapter_returns_ok_for_get_metrics_request() {
        let mut adapter = GrpcAdapter::new();
        let request = Request::builder()
            .method(Method::POST)
            .uri("/mvtxdaemon.MonoVertexDaemonService/GetMonoVertexMetrics")
            .header("content-type", "application/grpc")
            .body(grpc_empty_body())
            .unwrap();
        let result = adapter.call(request).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/grpc")
        );
    }
}
