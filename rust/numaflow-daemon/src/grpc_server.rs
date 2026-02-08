use crate::MvtxDaemonService;
use crate::TlsStreamReceiver;
use crate::error::{Error, Result};

use numaflow_pb::servers::mvtxdaemon::mono_vertex_daemon_service_server::MonoVertexDaemonServiceServer;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

pub(crate) async fn run_grpc_server(
    grpc_rx: TlsStreamReceiver,
    cln_token: CancellationToken,
) -> Result<()> {
    let grpc_service = MonoVertexDaemonServiceServer::new(MvtxDaemonService);
    let incoming_stream = ReceiverStream::new(grpc_rx).map(Ok::<_, std::io::Error>);

    Server::builder()
        .add_service(grpc_service)
        .serve_with_incoming_shutdown(incoming_stream, cln_token.cancelled())
        .await
        .map_err(|e| Error::NotComplete(format!("Failed to terminate the gRPC server: {}", e)))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::panic;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    #[tokio::test]
    async fn stops_on_cancellation()
    -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (tx, rx) = mpsc::channel::<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>(1);
        let cln_token = CancellationToken::new();
        let cln_token_copy = cln_token.clone();

        let handle = tokio::spawn(async move { run_grpc_server(rx, cln_token_copy).await });

        cln_token.cancel();

        if let Err(e) = timeout(Duration::from_millis(20), handle).await {
            panic!("Failed waiting for the gRPC server to stop: {}", e);
        }
        let _ = tx;
        Ok(())
    }

    #[tokio::test]
    async fn long_running_if_not_cancelled()
    -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (tx, rx) = mpsc::channel::<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>(1);
        let cln_token = CancellationToken::new();
        let cln_token_copy = cln_token.clone();

        let handle = tokio::spawn(async move { run_grpc_server(rx, cln_token_copy).await });

        if (timeout(Duration::from_millis(20), handle).await).is_ok() {
            panic!("Unexpected termination of the gRPC server");
        }

        let _ = tx;
        Ok(())
    }
}
