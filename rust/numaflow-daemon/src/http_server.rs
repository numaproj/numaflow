use crate::MvtxDaemonService;
use crate::TlsStreamReceiver;
use crate::error::{Error, Result};

use bytes::Bytes;
use http::{Request as HttpRequest, Response as HttpResponse, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use tokio::task::JoinSet;

pub(crate) async fn run_http_server(mut http_rx: TlsStreamReceiver) -> Result<()> {
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
        res.map_err(|join_error| {
            Error::Completion(format!(
                "Failed to complete one of the HTTP stream connections: {}",
                join_error
            ))
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use http::StatusCode;
    use http_body_util::{BodyExt, Empty};
    use hyper::client::conn::http1;
    use rcgen::{CertificateParams, KeyPair};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
    use rustls::{ClientConfig, RootCertStore, ServerConfig};
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;
    use tokio::time::{Duration, timeout};
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    #[tokio::test]
    async fn serves_readyz() -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (tls_acceptor, cert_der) = build_test_tls_acceptor()?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (tx, rx) = mpsc::channel(1);

        let http_handle = tokio::spawn(async move { run_http_server(rx).await });

        let server_task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await?;
            let tls_stream = tls_acceptor.accept(tcp).await?;
            tx.send(tls_stream)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });

        let connector = build_test_tls_connector(cert_der)?;
        let tcp = TcpStream::connect(addr).await?;
        let server_name = ServerName::try_from("localhost")?;
        let tls_stream = connector.connect(server_name, tcp).await?;

        let (mut sender, conn) = http1::handshake(hyper_util::rt::TokioIo::new(tls_stream)).await?;
        let conn_task = tokio::spawn(async move { conn.await });

        let ready_req = http::Request::builder()
            .method("GET")
            .uri("/readyz")
            .body(Empty::<Bytes>::new())?;
        let ready_resp = sender.send_request(ready_req).await?;
        assert_eq!(ready_resp.status(), StatusCode::NO_CONTENT);
        let _ = ready_resp.into_body().collect().await?;

        let not_found_req = http::Request::builder()
            .method("GET")
            .uri("/non-existing-path")
            .body(Empty::<Bytes>::new())?;
        let not_found_resp = sender.send_request(not_found_req).await?;
        assert_eq!(not_found_resp.status(), StatusCode::NOT_FOUND);
        let _ = not_found_resp.into_body().collect().await?;

        drop(sender);

        match timeout(Duration::from_secs(2), conn_task).await {
            Ok(res) => res??,
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Timed out waiting for HTTP client connection task to stop",
                )
                .into());
            }
        }

        match timeout(Duration::from_secs(2), server_task).await {
            Ok(res) => res??,
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Timed out waiting for HTTP server task to stop",
                )
                .into());
            }
        }

        match timeout(Duration::from_secs(2), http_handle).await {
            Ok(res) => res??,
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Timed out waiting for HTTP server to stop",
                )
                .into());
            }
        }

        Ok(())
    }

    fn build_test_tls_acceptor() -> std::result::Result<
        (TlsAcceptor, CertificateDer<'static>),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let params = CertificateParams::new(vec!["localhost".to_string()])?;
        let signing_key = KeyPair::generate()?;
        let cert = params.self_signed(&signing_key)?;
        let cert_der = cert.der().clone();
        let key_der = PrivatePkcs8KeyDer::from(signing_key.serialize_der());
        let key_der = PrivateKeyDer::from(key_der);

        let cfg = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)?;

        Ok((TlsAcceptor::from(Arc::new(cfg)), cert_der))
    }

    fn build_test_tls_connector(
        cert_der: CertificateDer<'static>,
    ) -> std::result::Result<TlsConnector, Box<dyn std::error::Error + Send + Sync>> {
        let mut root_store = RootCertStore::empty();
        root_store.add(cert_der)?;

        let cfg = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Ok(TlsConnector::from(Arc::new(cfg)))
    }
}
