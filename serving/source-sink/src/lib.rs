use std::env;

use tokio::signal;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::info;

use crate::error::Error;
use crate::forwarder::Forwarder;
use crate::sink::SinkClient;
use crate::source::SourceClient;
use crate::transformer::TransformerClient;

///! SourcerSinker orchestrates data movement from the Source to the Sink via the optional SourceTransformer.
///! The infamous forward-a-chunk executes the following in an infinite loop:
///! - Read X messages from the source
///! - Invokes the SourceTransformer concurrently
///! - Calls the Sinker to write the batch to the Sink
///! - Send Acknowledgement back to the Source

/// TODO
/// - [ ] integrate with main
/// - [ ] add metrics and metrics-server
/// - [ ] integrate with trace!
/// - [ ] add code comment
/// - [ ] error handling using anyhow
/// - [ ] unit testing >= 85%
/// - [ ] local integration testing
pub use self::error::Result;

pub mod error;

pub mod metrics;

pub(crate) mod source;

pub(crate) mod sink;

pub(crate) mod transformer;

pub(crate) mod forwarder;

pub(crate) mod message;

pub(crate) mod shared;

const SOURCE_SOCKET: &str = "/var/run/numaflow/source.sock";
const SINK_SOCKET: &str = "/var/run/numaflow/sink.sock";
const TRANSFORMER_SOCKET: &str = "/var/run/numaflow/sourcetransform.sock";
const TIMEOUT_IN_MS: u32 = 1000;
const BATCH_SIZE: u64 = 500;

pub async fn run_forwarder(custom_shutdown_rx: Option<oneshot::Receiver<()>>) -> Result<()> {
    let mut source_client = SourceClient::connect(SOURCE_SOCKET.into()).await?;
    let mut sink_client = SinkClient::connect(SINK_SOCKET.into()).await?;
    let mut transformer_client = if env::var("NUMAFLOW_TRANSFORMER").is_ok() {
        Some(TransformerClient::connect(TRANSFORMER_SOCKET.into()).await?)
    } else {
        None
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // readiness check for all the servers
    wait_until_ready(
        &mut source_client,
        &mut sink_client,
        &mut transformer_client,
    )
    .await?;

    let mut forwarder = Forwarder::new(
        source_client,
        sink_client,
        transformer_client,
        TIMEOUT_IN_MS,
        BATCH_SIZE,
        shutdown_rx,
    )
    .await?;

    let forwarder_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        forwarder.run().await?;
        Ok(())
    });

    let shutdown_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        shutdown_signal(custom_shutdown_rx).await;
        shutdown_tx
            .send(())
            .map_err(|_| Error::ForwarderError("Failed to send shutdown signal".to_string()))?;
        Ok(())
    });

    let _ = tokio::try_join!(forwarder_handle, shutdown_handle)
        .map_err(|e| Error::ForwarderError(format!("{:?}", e)))?;

    info!("Forwarder stopped gracefully");
    Ok(())
}

async fn wait_until_ready(
    source_client: &mut SourceClient,
    sink_client: &mut SinkClient,
    transformer_client: &mut Option<TransformerClient>,
) -> Result<()> {
    loop {
        let source_ready = source_client.is_ready().await.is_ok();
        if !source_ready {
            info!("UDSource is not ready, waiting...");
        }

        let sink_ready = sink_client.is_ready().await.is_ok();
        if !sink_ready {
            info!("UDSink is not ready, waiting...");
        }

        let transformer_ready = if let Some(client) = transformer_client {
            let ready = client.is_ready().await.is_ok();
            if !ready {
                info!("UDTransformer is not ready, waiting...");
            }
            ready
        } else {
            true
        };

        if source_ready && sink_ready && transformer_ready {
            break;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    Ok(())
}

async fn shutdown_signal(shutdown_rx: Option<oneshot::Receiver<()>>) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    let custom_shutdown = async {
        if let Some(rx) = shutdown_rx {
            rx.await.ok();
        }
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
        _ = custom_shutdown => {},
    }
}

// #[cfg(test)]
// mod tests {
//     use numaflow::{sink, source};
//     use numaflow::source::{Message, Offset, SourceReadRequest};
//     use tokio::sync::mpsc::Sender;
// 
//     struct SimpleSource;
//     #[tonic::async_trait]
//     impl source::Sourcer for SimpleSource {
//         async fn read(&self, _: SourceReadRequest, _: Sender<Message>) {
//         }
// 
//         async fn ack(&self, _: Vec<Offset>) {
//         }
// 
//         async fn pending(&self) -> usize {
//             0
//         }
// 
//         async fn partitions(&self) -> Option<Vec<i32>> {
//             None
//         }
//     }
// 
//     struct SimpleSink;
// 
//     #[tonic::async_trait]
//     impl sink::Sinker for SimpleSink {
//         async fn sink(&self, input: tokio::sync::mpsc::Receiver<sink::SinkRequest>) -> Vec<sink::Response> {
//             vec![]
//         }
//     }
//     #[tokio::test]
//     async fn run_forwarder() {
//         let (src_shutdown_tx, src_shutdown_rx) = tokio::sync::oneshot::channel();
//         let tmp_dir = tempfile::TempDir::new().unwrap();
//         let sock_file = tmp_dir.path().join("source.sock");
// 
//         let server_socket = sock_file.clone();
//         let src_server_handle = tokio::spawn(async move {
//             let server_info_file = tmp_dir.path().join("source-server-info");
//             source::Server::new(SimpleSource)
//                 .with_socket_file(server_socket)
//                 .with_server_info_file(server_info_file)
//                 .start_with_shutdown(src_shutdown_rx)
//                 .await
//                 .unwrap();
//         });
// 
//         let (sink_shutdown_tx, sink_shutdown_rx) = tokio::sync::oneshot::channel();
//         let tmp_dir = tempfile::TempDir::new().unwrap();
//         let sock_file = tmp_dir.path().join("sink.sock");
// 
//         let server_socket = sock_file.clone();
//         let sink_server_handle = tokio::spawn(async move {
//             let server_info_file = tmp_dir.path().join("sink-server-info");
//             sink::Server::new(SimpleSink)
//                 .with_socket_file(server_socket)
//                 .with_server_info_file(server_info_file)
//                 .start_with_shutdown(sink_shutdown_rx)
//                 .await
//                 .unwrap();
//         });
// 
//         let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
// 
//         let result = super::run_forwarder(Some(shutdown_rx)).await;
//         println!("{:?}", result);
//         assert!(result.is_ok());
// 
//         // stop the source and sink servers
//         src_shutdown_tx.send(()).unwrap();
//         sink_shutdown_tx.send(()).unwrap();
// 
//         src_server_handle.await.unwrap();
//         sink_server_handle.await.unwrap();
//     }
// }