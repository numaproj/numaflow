use std::error::Error;

use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use numaflow_pb::servers::mvtxdaemon::mono_vertex_daemon_service_server::MonoVertexDaemonServiceServer;

use crate::MvtxDaemonService;
use crate::TlsStreamReceiver;

pub(crate) async fn run_grpc_server(
    grpc_rx: TlsStreamReceiver,
    cln_token: CancellationToken,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let grpc_service = MonoVertexDaemonServiceServer::new(MvtxDaemonService);
    let incoming_stream = ReceiverStream::new(grpc_rx).map(Ok::<_, std::io::Error>);

    Server::builder()
        .add_service(grpc_service)
        .serve_with_incoming_shutdown(incoming_stream, cln_token.cancelled())
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::sync::mpsc;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn stops_on_cancellation() -> Result<(), Box<dyn Error + Send + Sync>> {
        let (_, rx) = mpsc::channel::<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>(1);
        let cln_token = CancellationToken::new();
        let cln_token_copy = cln_token.clone();

        let handle = tokio::spawn(async move { run_grpc_server(rx, cln_token_copy).await });

        cln_token.cancel();

        match timeout(Duration::from_secs(2), handle).await {
            Ok(res) => res??,
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Timed out waiting for gRPC server to stop",
                )
                .into());
            }
        }

        Ok(())
    }
}
