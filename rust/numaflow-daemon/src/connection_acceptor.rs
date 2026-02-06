use crate::TlsStreamSender;
use crate::error::{Error, Result};

use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// ConnectionAcceptor accepts incoming connections and
/// dispatches them to the either gRPC or HTTP channels based on the connection type.
pub(crate) struct ConnectionAcceptor {
    /// TcpListener is used to accept incoming TCP connections.
    tcp_listener: TcpListener,
    /// TlsAcceptor is used to accept incoming TLS connections.
    tls_acceptor: TlsAcceptor,
    /// grpc_tx is used to send incoming gRPC connections to the gRPC channel.
    grpc_tx: TlsStreamSender,
    /// http_tx is used to send incoming HTTP connections to the HTTP channel.
    http_tx: TlsStreamSender,
    /// cln_token is used to signal a cancellation and trigger a graceful shutdown.
    cln_token: CancellationToken,
}

impl ConnectionAcceptor {
    pub(crate) fn new(
        tcp_listener: TcpListener,
        tls_acceptor: TlsAcceptor,
        grpc_tx: TlsStreamSender,
        http_tx: TlsStreamSender,
        cln_token: CancellationToken,
    ) -> Self {
        Self {
            tcp_listener,
            tls_acceptor,
            grpc_tx,
            http_tx,
            cln_token,
        }
    }

    pub(crate) async fn run(self) -> Result<()> {
        let ConnectionAcceptor {
            tcp_listener,
            tls_acceptor,
            grpc_tx,
            http_tx,
            cln_token,
        } = self;

        let mut conn_set = JoinSet::new();

        loop {
            tokio::select! {
                _ = cln_token.cancelled() => {
                    info!("Cancellation token triggered. Stop accepting new connections, close gRPC and HTTP stream senders");
                    drop(grpc_tx);
                    drop(http_tx);
                    break;
                }
                accept_res = tcp_listener.accept() => {
                    // Accept a connection.
                    let (tcp, peer_addr) = match accept_res {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(error = %e, "Failed to accept a TCP connection");
                            // Skip the failed connection as we don't want one failed connection to shutdown the entire system.
                            // Ideally this shouldn't happen.
                            // TODO - monitor this one during manual testing and revisit the decision.
                            continue;
                        }
                    };

                    // Handle the new connection.
                    let grpc_sender = grpc_tx.clone();
                    let http_sender = http_tx.clone();
                    let acceptor = tls_acceptor.clone();

                    // Start a new tokio task so that we don't block accepting other connections.
                    conn_set.spawn(async move {
                        let stream = match acceptor.accept(tcp).await {
                            Ok(s) => s,
                            Err(e) => {
                                warn!(peer_addr = %peer_addr, error = %e, "TLS handshake failed.");
                                // TLS handshake failed, skip handling this connection.
                                return;
                            }
                        };

                        let alpn = stream
                            .get_ref()
                            .1
                            .alpn_protocol()
                            .map(|p| String::from_utf8_lossy(p).into_owned());

                        match alpn.as_deref() {
                            Some("http/1.1") => {
                                // Send to the HTTP channel.
                                match http_sender.send(stream).await {
                                    Ok(_) => (),
                                    // Log the error and finish the task.
                                    // The stream will go out of scope and be dropped.
                                    Err(e) => warn!(error = %e, "Failed to send HTTP stream to the HTTP channel, skipping."),
                                }
                            }
                            Some("h2") => {
                                // Send to the gRPC channel.
                                match grpc_sender.send(stream).await {
                                    Ok(_) => (),
                                    Err(e) => warn!(error = %e, "Failed to send gRPC stream to the gRPC channel, skipping."),
                                }
                            }
                            _ => {
                                // Send to the HTTP channel by default.
                                // This is because most of the time, HTTP is used for communication.
                                // On Numaflow, if a client is sending a gRPC request, the h2 protocol is explicitly used.
                                match http_sender.send(stream).await {
                                    Ok(_) => (),
                                    Err(e) => warn!(error = %e, "Failed to send HTTP stream to the HTTP channel, skipping."),
                                }
                            }
                        }
                    });
                }
            }
        }

        while let Some(res) = conn_set.join_next().await {
            res.map_err(|join_error| {
                Error::Completion(format!(
                    "Failed to complete one of the TCP connection acceptor tasks: {}",
                    join_error
                ))
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rcgen::{CertificateParams, KeyPair};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
    use rustls::{ClientConfig, RootCertStore, ServerConfig};
    use std::sync::Arc;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;
    use tokio::time::{Duration as TokioDuration, timeout};
    use tokio_rustls::TlsConnector;

    #[tokio::test]
    async fn routes_h2_to_grpc_channel()
    -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (tls_acceptor, cert_der) = build_test_tls_acceptor()?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let (grpc_tx, mut grpc_rx) = mpsc::channel(1);
        let (http_tx, mut http_rx) = mpsc::channel(1);
        let cln_token = CancellationToken::new();

        let acceptor =
            ConnectionAcceptor::new(listener, tls_acceptor, grpc_tx, http_tx, cln_token.clone());

        let acceptor_handle = tokio::spawn(async move { acceptor.run().await });

        // Start a TCP stream connection with h2 alpn protocol.
        connect_with_alpn(addr, Some(b"h2"), cert_der).await?;

        // Verify that the stream is received on gRPC channel.
        match timeout(TokioDuration::from_secs(2), grpc_rx.recv()).await {
            Ok(Some(_stream)) => (),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "gRPC channel did not receive a stream",
                )
                .into());
            }
        };

        // Verify that the stream is not received on HTTP channel.
        match timeout(TokioDuration::from_millis(200), http_rx.recv()).await {
            Ok(Some(_)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Unexpected HTTP stream received for h2 connection",
                )
                .into());
            }
            _ => (),
        }

        cln_token.cancel();
        // Verify the graceful shutdown.
        acceptor_handle.await??;

        Ok(())
    }

    #[tokio::test]
    async fn routes_http11_to_http_channel()
    -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (tls_acceptor, cert_der) = build_test_tls_acceptor()?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let (grpc_tx, mut grpc_rx) = mpsc::channel(1);
        let (http_tx, mut http_rx) = mpsc::channel(1);
        let cln_token = CancellationToken::new();

        let acceptor =
            ConnectionAcceptor::new(listener, tls_acceptor, grpc_tx, http_tx, cln_token.clone());

        let acceptor_handle = tokio::spawn(async move { acceptor.run().await });

        // Start a TCP stream connection with http/1.1 alpn protocol.
        connect_with_alpn(addr, Some(b"http/1.1"), cert_der).await?;

        // Verify that the stream is received on HTTP channel.
        match timeout(TokioDuration::from_secs(2), http_rx.recv()).await {
            Ok(Some(_stream)) => (),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "HTTP channel did not receive a stream",
                )
                .into());
            }
        };

        // Verify that the stream is not received on gRPC channel.
        match timeout(TokioDuration::from_millis(200), grpc_rx.recv()).await {
            Ok(Some(_)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Unexpected gRPC stream received for http/1.1 connection",
                )
                .into());
            }
            _ => (),
        }

        cln_token.cancel();
        // Verify the graceful shutdown.
        acceptor_handle.await??;

        Ok(())
    }

    #[tokio::test]
    async fn routes_no_alpn_to_http_channel()
    -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (tls_acceptor, cert_der) = build_test_tls_acceptor()?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let (grpc_tx, mut grpc_rx) = mpsc::channel(1);
        let (http_tx, mut http_rx) = mpsc::channel(1);
        let cln_token = CancellationToken::new();

        let acceptor =
            ConnectionAcceptor::new(listener, tls_acceptor, grpc_tx, http_tx, cln_token.clone());

        let acceptor_handle = tokio::spawn(async move { acceptor.run().await });

        // Start a TCP stream connection with no ALPN protocol.
        connect_with_alpn(addr, None, cert_der).await?;

        // Verify the stream is received on HTTP channel.
        match timeout(TokioDuration::from_secs(2), http_rx.recv()).await {
            Ok(Some(_stream)) => (),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "HTTP channel did not receive a stream",
                )
                .into());
            }
        };

        // Verify the stream is not received on gRPC channel.
        match timeout(TokioDuration::from_millis(200), grpc_rx.recv()).await {
            Ok(Some(_)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Unexpected gRPC stream received on gRPC channel",
                )
                .into());
            }
            _ => (),
        }

        cln_token.cancel();
        // Verify the graceful shutdown.
        acceptor_handle.await??;

        Ok(())
    }

    fn build_test_tls_acceptor() -> std::result::Result<
        (TlsAcceptor, CertificateDer<'static>),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let params = CertificateParams::new(vec!["localhost".to_string()])?;
        let signing_key = KeyPair::generate()?;
        let cert = params.self_signed(&signing_key)?;
        let cert_der = cert.der().clone();
        let key_der = PrivatePkcs8KeyDer::from(signing_key.serialize_der());
        let key_der = PrivateKeyDer::from(key_der);

        let mut cfg = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)?;
        cfg.alpn_protocols = vec![b"http/1.1".to_vec(), b"h2".to_vec()];

        Ok((TlsAcceptor::from(Arc::new(cfg)), cert_der))
    }

    async fn connect_with_alpn(
        addr: std::net::SocketAddr,
        alpn: Option<&[u8]>,
        cert_der: CertificateDer<'static>,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut root_store = RootCertStore::empty();
        root_store.add(cert_der)?;

        let mut cfg = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        if let Some(alpn_bytes) = alpn {
            cfg.alpn_protocols = vec![alpn_bytes.to_vec()];
        }

        let connector = TlsConnector::from(Arc::new(cfg));
        let tcp = TcpStream::connect(addr).await?;
        let server_name = ServerName::try_from("localhost")?;
        let _tls = connector.connect(server_name, tcp).await?;
        Ok(())
    }
}
