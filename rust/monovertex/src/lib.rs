use tokio::signal;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::config;
pub(crate) use crate::error::Error;
use crate::forwarder::ForwarderBuilder;
use crate::metrics::MetricsState;
use crate::shared::create_rpc_channel;
use crate::sink::{SinkWriter, FB_SINK_SOCKET, SINK_SOCKET};
use crate::sink_pb::sink_client::SinkClient;
use crate::source::{SourceReader, SOURCE_SOCKET};
use crate::source_pb::source_client::SourceClient;
use crate::sourcetransform_pb::source_transform_client::SourceTransformClient;
use crate::transformer::{SourceTransformer, TRANSFORMER_SOCKET};

pub(crate) use self::error::Result;

/// SourcerSinker orchestrates data movement from the Source to the Sink via the optional SourceTransformer.
/// The forward-a-chunk executes the following in an infinite loop till a shutdown signal is received:
/// - Read X messages from the source
/// - Invokes the SourceTransformer concurrently
/// - Calls the Sinker to write the batch to the Sink
/// - Send Acknowledgement back to the Source
pub mod error;

pub(crate) mod source;

pub(crate) mod sink;

pub(crate) mod transformer;

pub(crate) mod forwarder;

pub(crate) mod config;

pub(crate) mod message;

pub(crate) mod shared;

mod metrics;
mod server_info;
mod startup;

pub(crate) mod source_pb {
    tonic::include_proto!("source.v1");
}

pub(crate) mod sink_pb {
    tonic::include_proto!("sink.v1");
}

pub(crate) mod sourcetransform_pb {
    tonic::include_proto!("sourcetransformer.v1");
}

pub async fn mono_vertex() -> Result<()> {
    let cln_token = CancellationToken::new();
    let shutdown_cln_token = cln_token.clone();

    // wait for SIG{INT,TERM} and invoke cancellation token.
    let shutdown_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        shutdown_signal().await;
        shutdown_cln_token.cancel();
        Ok(())
    });

    // Run the forwarder with cancellation token.
    if let Err(e) = start_forwarder(cln_token).await {
        error!("Application error: {:?}", e);

        // abort the signal handler task since we have an error and we are shutting down
        if !shutdown_handle.is_finished() {
            shutdown_handle.abort();
        }
    }

    info!("Gracefully Exiting...");
    Ok(())
}

async fn shutdown_signal() {
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

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

async fn start_forwarder(cln_token: CancellationToken) -> Result<()> {
    // make sure that we have compatibility with the server
    startup::check_compatibility(&cln_token).await?;

    let mut source_grpc_client = SourceClient::new(create_rpc_channel(SOURCE_SOCKET.into()).await?)
        .max_encoding_message_size(config().grpc_max_message_size)
        .max_encoding_message_size(config().grpc_max_message_size);

    let mut sink_grpc_client = SinkClient::new(create_rpc_channel(SINK_SOCKET.into()).await?)
        .max_encoding_message_size(config().grpc_max_message_size)
        .max_encoding_message_size(config().grpc_max_message_size);

    let mut transformer_grpc_client = if config().is_transformer_enabled {
        let transformer_grpc_client =
            SourceTransformClient::new(create_rpc_channel(TRANSFORMER_SOCKET.into()).await?)
                .max_encoding_message_size(config().grpc_max_message_size)
                .max_encoding_message_size(config().grpc_max_message_size);

        Some(transformer_grpc_client.clone())
    } else {
        None
    };

    let mut fb_sink_grpc_client = if config().is_fallback_enabled {
        let fb_sink_grpc_client = SinkClient::new(create_rpc_channel(FB_SINK_SOCKET.into()).await?)
            .max_encoding_message_size(config().grpc_max_message_size)
            .max_encoding_message_size(config().grpc_max_message_size);

        Some(fb_sink_grpc_client.clone())
    } else {
        None
    };

    // readiness check for all the ud containers
    startup::wait_until_ready(
        &mut source_grpc_client,
        &mut sink_grpc_client,
        &mut transformer_grpc_client,
        &mut fb_sink_grpc_client,
    )
    .await?;

    // Start the metrics server in a separate background async spawn,
    // This should be running throughout the lifetime of the application, hence the handle is not
    // joined.
    let metrics_state = MetricsState {
        source_client: source_grpc_client.clone(),
        sink_client: sink_grpc_client.clone(),
        transformer_client: transformer_grpc_client.clone(),
        fb_sink_client: fb_sink_grpc_client.clone(),
    };

    // start the metrics server
    // FIXME: what to do with the handle
    let _ = startup::start_metrics_server(metrics_state);

    // start the lag reader to publish lag metrics
    startup::start_lag_reader(source_grpc_client.clone()).await;

    // build the forwarder
    let source_reader = SourceReader::new(source_grpc_client.clone()).await?;
    let sink_writer = SinkWriter::new(sink_grpc_client.clone()).await?;

    let mut forwarder_builder = ForwarderBuilder::new(source_reader, sink_writer, cln_token);

    // add transformer if exists
    if let Some(transformer_grpc_client) = transformer_grpc_client {
        let transformer = SourceTransformer::new(transformer_grpc_client).await?;
        forwarder_builder = forwarder_builder.source_transformer(transformer);
    }

    // add fallback sink if exists
    if let Some(fb_sink_grpc_client) = fb_sink_grpc_client {
        let fallback_writer = SinkWriter::new(fb_sink_grpc_client).await?;
        forwarder_builder = forwarder_builder.fallback_sink_writer(fallback_writer);
    }
    // build the final forwarder
    let mut forwarder = forwarder_builder.build();

    // start the forwarder, it will return only on Signal
    forwarder.start().await?;

    info!("Forwarder stopped gracefully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::env;

    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{sink, source};
    use tokio::sync::mpsc::Sender;
    use tokio_util::sync::CancellationToken;

    use crate::start_forwarder;

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

        // wait for the servers to start
        // FIXME: we need to have a better way, this is flaky
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        unsafe {
            env::set_var("SOURCE_SOCKET", src_sock_file.to_str().unwrap());
            env::set_var("SINK_SOCKET", sink_sock_file.to_str().unwrap());
        }

        let cln_token = CancellationToken::new();

        let token_clone = cln_token.clone();
        tokio::spawn(async move {
            // FIXME: we need to have a better way, this is flaky
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            token_clone.cancel();
        });

        let result = start_forwarder(cln_token.clone()).await;
        assert!(result.is_err());

        // stop the source and sink servers
        src_shutdown_tx.send(()).unwrap();
        sink_shutdown_tx.send(()).unwrap();

        src_server_handle.await.unwrap();
        sink_server_handle.await.unwrap();
    }
}
