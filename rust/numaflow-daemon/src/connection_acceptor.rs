use std::error::Error;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub(crate) async fn run_connection_acceptor(
    tcp_listener: TcpListener,
    tls_acceptor: TlsAcceptor,
    grpc_tx: mpsc::Sender<TlsStream<TcpStream>>,
    http_tx: mpsc::Sender<TlsStream<TcpStream>>,
    cln_token: CancellationToken,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut conn_set = JoinSet::new();

    loop {
        tokio::select! {
            _ = cln_token.cancelled() => {
                info!("Cancellation token triggered. Stopping accepting new connections, and closing gRPC and HTTP senders");
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
                        continue;
                    }
                };

                // Handle the new connection.
                // Start a new tokio task so that we don't block accepting other connections.
                let grpc_sender = grpc_tx.clone();
                let http_sender = http_tx.clone();
                let acceptor = tls_acceptor.clone();

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
        if let Err(join_err) = res {
            warn!(error = %join_err, "TCP connection task failed");
        }
    }

    Ok(())
}
