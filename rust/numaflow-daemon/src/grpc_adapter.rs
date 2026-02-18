//! Wraps the Tonic gRPC service so its response type is compatible with Axum's `IntoResponse`.

use axum::body::Body;
use axum::response::Response;
use http::Request;
use http_body_util::BodyExt;
use numaflow_pb::servers::mvtxdaemon::mono_vertex_daemon_service_server::MonoVertexDaemonServiceServer;
use std::task::{Context, Poll};
use tower::Service;

use crate::MvtxDaemonService;

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
