use std::convert::Infallible;
use std::error::Error;

use bytes::Bytes;
use http::{Request as HttpRequest, Response as HttpResponse, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::task::JoinSet;
use tracing::warn;

use crate::MvtxDaemonService;

pub(crate) async fn run_http_server(
    mut http_rx: tokio::sync::mpsc::Receiver<
        tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    >,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let _svc_for_http = MvtxDaemonService;
    let mut conn_set = JoinSet::new();

    while let Some(stream) = http_rx.recv().await {
        conn_set.spawn(async move {
            let svc = service_fn(|req: HttpRequest<Incoming>| async move {
                let method = req.method().clone();
                let path = req.uri().path().to_string();

                let resp = match (method.as_str(), path.as_str()) {
                    ("GET", "/readyz" | "/livez") => HttpResponse::builder()
                        .status(StatusCode::NO_CONTENT)
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                    // TODO - add remaining endpoints.
                    _ => HttpResponse::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                };

                Ok::<HttpResponse<Full<Bytes>>, Infallible>(resp)
            });

            let io = TokioIo::new(stream);
            let _ = http1::Builder::new().serve_connection(io, svc).await;
        });
    }

    while let Some(res) = conn_set.join_next().await {
        if let Err(join_err) = res {
            warn!(error = %join_err, "HTTP connection task failed");
        }
    }

    Ok(())
}
