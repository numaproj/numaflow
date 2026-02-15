use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_rustls::server::TlsStream;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

type TlsStreamSender = mpsc::Sender<TlsStream<TcpStream>>;
type TlsStreamReceiver = mpsc::Receiver<TlsStream<TcpStream>>;

mod connection_acceptor;
mod error;
mod grpc_server;
mod http_server;
mod service;

use connection_acceptor::ConnectionAcceptor;
use error::Result;
use grpc_server::run_grpc_server;
use http_server::run_http_server;
pub(crate) use service::MvtxDaemonService;

#[cfg(test)]
mod timeout_checker;

/// MonoVertex Daemon Service
/// ========================
///
/// ```text
///        ┌──────────────────────────────────────────────┐
///        │                main async task               │
///        └───────────────────────┬──────────────────────┘
///                                │
///        ┌───────────────────────┼───────────────────────┐
///        │                       │                       │
///        ▼                       ▼                       ▼
/// ┌──────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐
/// │       Acceptor       │ │         gRPC         │ │         HTTP         │
/// │         Task         │ │        Worker        │ │        Worker        │
/// └───────────┬──────────┘ └───────────▲──────────┘ └───────────▲──────────┘
///             │ new connection         │                        │
///             ▼                        │                        │
/// ┌──────────────────────┐             │                        │
/// │    Connection Task   │             │                        │
/// └───────────┬──────────┘             │                        │
///             │                        │                        │
///             ▼                        │                        │
/// ┌──────────────────────┐             │                        │
/// │        Route         │             │                        │
/// └───────────┬──────────┘             │                        │
///             │                        │                        │
///      ┌──────┴──────┐                 │                        │
///      ▼             ▼                 │                        │
/// [ gRPC channel ] [ HTTP channel ]────┴────────────────────────┘
/// ```
///
/// Shutdown:
/// - Acceptor listens for cancellation
/// - Stops accepting new connections
/// - Channels close, workers drain and exit
pub async fn run_monovertex(mvtx_name: String, cln_token: CancellationToken) -> Result<()> {
    info!("MonoVertex name is {}", mvtx_name);

    // Create two channels, one serving gRPC requests, the other HTTP.
    // A buffer size of 500 should be sufficient to handle the expected request rate.
    let (grpc_tx, grpc_rx) = mpsc::channel(500);
    let (http_tx, http_rx) = mpsc::channel(500);

    // Use a join set to manage spawned tasks.
    let mut join_set = JoinSet::new();

    // Start a tokio task to accept tcp connections.
    let acceptor = ConnectionAcceptor::new(grpc_tx, http_tx, cln_token.clone())?;
    join_set.spawn(async move {
        if let Err(error) = acceptor.run().await {
            warn!(error = %error, "Connection acceptor failed");
        }
    });

    // Start a tokio task to serve gRPC requests.
    join_set.spawn(async move {
        if let Err(error) = run_grpc_server(grpc_rx, cln_token.clone()).await {
            warn!(error = %error, "gRPC server failed");
        }
    });

    // Start a tokio task to serve HTTP requests.
    join_set.spawn(async move {
        if let Err(error) = run_http_server(http_rx).await {
            warn!(error = %error, "HTTP server failed");
        }
    });

    while let Some(res) = join_set.join_next().await {
        if let Err(join_err) = res {
            warn!(error = %join_err, "Daemon task failed waiting for completion");
        }
    }

    Ok(())
}
