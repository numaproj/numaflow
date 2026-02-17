//! Wraps the Tonic gRPC service so its response type is compatible with Axum's `IntoResponse`.

use axum::body::Body as AxumBody;
use axum::response::Response;
use http::Request;
use http_body_util::BodyExt;
use numaflow_pb::servers::mvtxdaemon::mono_vertex_daemon_service_server::MonoVertexDaemonServiceServer;
use std::task::{Context, Poll};
use tower::Service;

use crate::MvtxDaemonService;

/// Wraps the Tonic gRPC server so it can be used as an Axum fallback service
/// (response type implements `IntoResponse`).
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

impl Service<Request<AxumBody>> for GrpcAdapter {
    type Response = Response<AxumBody>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<AxumBody>, std::convert::Infallible>>
                + Send,
        >,
    >;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<AxumBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let res = inner.call(req).await.map_err(|e| match e {})?;
            let (parts, body) = res.into_parts();
            // Convert the Tonic body to Axum body by mapping frames
            let axum_body = AxumBody::new(body.map_err(axum::Error::new));
            Ok(Response::from_parts(parts, axum_body))
        })
    }
}
