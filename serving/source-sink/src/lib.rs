use serde::Deserialize;
use std::time::Duration;
use std::{env, fs};
use tokio::signal;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{info, warn};

pub(crate) use crate::error::Error;
use crate::forwarder::Forwarder;
use crate::sink::{SinkClient, SinkConfig};
use crate::source::{SourceClient, SourceConfig};
use crate::transformer::{TransformerClient, TransformerConfig};

pub(crate) use self::error::Result;

/// SourcerSinker orchestrates data movement from the Source to the Sink via the optional SourceTransformer.
/// The forward-a-chunk executes the following in an infinite loop till a shutdown signal is received:
/// - Read X messages from the source
/// - Invokes the SourceTransformer concurrently
/// - Calls the Sinker to write the batch to the Sink
/// - Send Acknowledgement back to the Source
pub mod error;

pub mod metrics;

pub mod source;

pub mod sink;

pub mod transformer;

pub mod forwarder;

pub mod message;

pub mod server_info;

pub mod version;

pub(crate) mod shared;

const TIMEOUT_IN_MS: &str = "1000";
const BATCH_SIZE: &str = "500";

/// forwards a chunk of data from the source to the sink via an optional transformer.
/// It takes an optional custom_shutdown_rx for shutting down the forwarder, useful for testing.
pub async fn run_forwarder(
    source_config: SourceConfig,
    sink_config: SinkConfig,
    transformer_config: Option<TransformerConfig>,
    custom_shutdown_rx: Option<oneshot::Receiver<()>>,
) -> Result<()> {
    server_info::wait_for_server_info(&source_config.server_info_file)
        .await
        .map_err(|e| {
            warn!("Error waiting for source server info file: {:?}", e);
            Error::ForwarderError("Error waiting for server info file".to_string())
        })?;
    // TODO: get this from vertex object, controller will have to pass this
    let vertex_name = env::var("VERTEX_NAME").unwrap_or_else(|_| "vertex".to_string());
    let pipeline_name = env::var("PIPELINE_NAME").unwrap_or_else(|_| "pipeline".to_string());
    let replica = 0;

    let mut source_client = SourceClient::connect(source_config).await?;

    server_info::wait_for_server_info(&sink_config.server_info_file)
        .await
        .map_err(|e| {
            warn!("Error waiting for sink server info file: {:?}", e);
            Error::ForwarderError("Error waiting for server info file".to_string())
        })?;
    let mut sink_client = SinkClient::connect(sink_config).await?;

    let mut transformer_client = if let Some(config) = transformer_config {
        server_info::wait_for_server_info(&config.server_info_file)
            .await
            .map_err(|e| {
                warn!("Error waiting for transformer server info file: {:?}", e);
                Error::ForwarderError("Error waiting for server info file".to_string())
            })?;
        Some(TransformerClient::connect(config).await?)
    } else {
        None
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // readiness check for all the ud containers
    wait_until_ready(
        &mut source_client,
        &mut sink_client,
        &mut transformer_client,
    )
    .await?;

    // TODO get these from the vertex object
    let timeout_in_ms: u32 = env::var("TIMEOUT_IN_MS")
        .unwrap_or_else(|_| TIMEOUT_IN_MS.to_string())
        .parse()
        .expect("Invalid TIMEOUT_IN_MS");
    let batch_size: u64 = env::var("BATCH_SIZE")
        .unwrap_or_else(|_| BATCH_SIZE.to_string())
        .parse()
        .expect("Invalid BATCH_SIZE");

    // TODO: use builder pattern of options like TIMEOUT, BATCH_SIZE, etc?
    let mut forwarder = Forwarder::new(
        vertex_name,
        pipeline_name,
        replica,
        source_client,
        sink_client,
        transformer_client,
        timeout_in_ms,
        batch_size,
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

// async fn wait_for_server_info(file_path: &str) -> Result<()> {
//     loop {
//         if let Ok(metadata) = fs::metadata(file_path) {
//             if metadata.len() > 0 {
//                 return Ok(());
//             }
//         }
//         info!("Server info file {} is not ready, waiting...", file_path);
//         sleep(Duration::from_secs(1)).await;
//     }
// }

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

        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}

async fn shutdown_signal(shutdown_rx: Option<oneshot::Receiver<()>>) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        info!("Received Ctrl+C signal");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
        info!("Received terminate signal");
    };

    let custom_shutdown = async {
        if let Some(rx) = shutdown_rx {
            rx.await.ok();
        } else {
            // Create a watch channel that never sends
            let (_tx, mut rx) = tokio::sync::watch::channel(());
            rx.changed().await.ok();
        }
        info!("Received custom shutdown signal");
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
        _ = custom_shutdown => {},
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{sink, source};
    use tokio::sync::mpsc::Sender;

    use crate::sink::SinkConfig;
    use crate::source::SourceConfig;

    struct SimpleSource;
    #[tonic::async_trait]
    impl source::Sourcer for SimpleSource {
        async fn read(&self, _: SourceReadRequest, _: Sender<Message>) {}

        async fn ack(&self, _: Vec<Offset>) {}

        async fn pending(&self) -> usize {
            0
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            None
        }
    }

    struct SimpleSink;

    #[tonic::async_trait]
    impl sink::Sinker for SimpleSink {
        async fn sink(
            &self,
            _input: tokio::sync::mpsc::Receiver<sink::SinkRequest>,
        ) -> Vec<sink::Response> {
            vec![]
        }
    }
    #[tokio::test]
    async fn run_forwarder() {
        let (src_shutdown_tx, src_shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let src_sock_file = tmp_dir.path().join("source.sock");
        let src_info_file = tmp_dir.path().join("source-server-info");

        let server_info = src_info_file.clone();
        let server_socket = src_sock_file.clone();
        let src_server_handle = tokio::spawn(async move {
            source::Server::new(SimpleSource)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(src_shutdown_rx)
                .await
                .unwrap();
        });
        let source_config = SourceConfig {
            socket_path: src_sock_file.to_str().unwrap().to_string(),
            server_info_file: src_info_file.to_str().unwrap().to_string(),
            max_message_size: 100,
        };

        let (sink_shutdown_tx, sink_shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sink_sock_file = tmp_dir.path().join("sink.sock");
        let sink_server_info = tmp_dir.path().join("sink-server-info");

        let server_socket = sink_sock_file.clone();
        let server_info = sink_server_info.clone();
        let sink_server_handle = tokio::spawn(async move {
            sink::Server::new(SimpleSink)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(sink_shutdown_rx)
                .await
                .unwrap();
        });
        let sink_config = SinkConfig {
            socket_path: sink_sock_file.to_str().unwrap().to_string(),
            server_info_file: sink_server_info.to_str().unwrap().to_string(),
            max_message_size: 100,
        };

        // wait for the servers to start
        // FIXME: we need to have a better way, this is flaky
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        env::set_var("SOURCE_SOCKET", src_sock_file.to_str().unwrap());
        env::set_var("SINK_SOCKET", sink_sock_file.to_str().unwrap());

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let forwarder_handle = tokio::spawn(async move {
            let result =
                super::run_forwarder(source_config, sink_config, None, Some(shutdown_rx)).await;
            //
        });

        // wait for the forwarder to start
        // FIXME: we need to have a better way, this is flaky
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // stop the forwarder
        shutdown_tx.send(()).unwrap();
        forwarder_handle.await.unwrap();

        // stop the source and sink servers
        src_shutdown_tx.send(()).unwrap();
        sink_shutdown_tx.send(()).unwrap();

        src_server_handle.await.unwrap();
        sink_server_handle.await.unwrap();
    }
}
