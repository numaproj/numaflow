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
