pub(crate) use self::error::Result;
use crate::config::config;
pub(crate) use crate::error::Error;
use crate::forwarder::ForwarderBuilder;
use crate::metrics::{start_metrics_https_server, LagReaderBuilder, MetricsState};
use crate::proto::sink_client::SinkClient;
use crate::proto::source_client::SourceClient;
use crate::proto::source_transform_client::SourceTransformClient;
use crate::shared::create_rpc_channel;
use crate::sink::{
    SinkWriter, FB_SINK_SERVER_INFO_FILE, FB_SINK_SOCKET, SINK_SERVER_INFO_FILE, SINK_SOCKET,
};
use crate::source::{SourceReader, SOURCE_SERVER_INFO_FILE, SOURCE_SOCKET};
use crate::transformer::{SourceTransformer, TRANSFORMER_SERVER_INFO_FILE, TRANSFORMER_SOCKET};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::signal;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic::Request;
use tracing::{error, info, warn};

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

pub(crate) mod proto {
    tonic::include_proto!("source.v1");
    tonic::include_proto!("sink.v1");
    tonic::include_proto!("sourcetransformer.v1");
}

mod server_info;

mod metrics;

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
    if let Err(e) = init(cln_token).await {
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

pub async fn init(cln_token: CancellationToken) -> Result<()> {
    server_info::check_for_server_compatibility(SOURCE_SERVER_INFO_FILE, cln_token.clone())
        .await
        .map_err(|e| {
            warn!("Error waiting for source server info file: {:?}", e);
            Error::ForwarderError("Error waiting for server info file".to_string())
        })?;

    let mut source_grpc_client = SourceClient::new(create_rpc_channel(SOURCE_SOCKET.into()).await?)
        .max_encoding_message_size(config().grpc_max_message_size)
        .max_encoding_message_size(config().grpc_max_message_size);

    let source_reader = SourceReader::new(source_grpc_client.clone()).await?;

    server_info::check_for_server_compatibility(SINK_SERVER_INFO_FILE, cln_token.clone())
        .await
        .map_err(|e| {
            error!("Error waiting for sink server info file: {:?}", e);
            Error::ForwarderError("Error waiting for server info file".to_string())
        })?;

    let mut sink_grpc_client = SinkClient::new(create_rpc_channel(SINK_SOCKET.into()).await?)
        .max_encoding_message_size(config().grpc_max_message_size)
        .max_encoding_message_size(config().grpc_max_message_size);

    let mut sink_writer = SinkWriter::new(sink_grpc_client.clone()).await?;

    let (mut transformer_grpc_client, mut transformer) = if config().is_transformer_enabled {
        server_info::check_for_server_compatibility(
            TRANSFORMER_SERVER_INFO_FILE,
            cln_token.clone(),
        )
        .await
        .map_err(|e| {
            error!("Error waiting for transformer server info file: {:?}", e);
            Error::ForwarderError("Error waiting for server info file".to_string())
        })?;
        let transformer_grpc_client =
            SourceTransformClient::new(create_rpc_channel(TRANSFORMER_SOCKET.into()).await?)
                .max_encoding_message_size(config().grpc_max_message_size)
                .max_encoding_message_size(config().grpc_max_message_size);

        (
            Some(transformer_grpc_client),
            Some(SourceTransformer::new(transformer_grpc_client.clone()).await?),
        )
    } else {
        (None, None)
    };

    let (mut fb_sink_grpc_client, mut fallback_writer) = if config().is_fallback_enabled {
        server_info::check_for_server_compatibility(FB_SINK_SERVER_INFO_FILE, cln_token.clone())
            .await
            .map_err(|e| {
                warn!("Error waiting for fallback sink server info file: {:?}", e);
                Error::ForwarderError("Error waiting for server info file".to_string())
            })?;
        let fb_sink_grpc_client = SinkClient::new(create_rpc_channel(FB_SINK_SOCKET.into()).await?)
            .max_encoding_message_size(config().grpc_max_message_size)
            .max_encoding_message_size(config().grpc_max_message_size);

        (
            Some(fb_sink_grpc_client),
            Some(SinkWriter::new(fb_sink_grpc_client.clone()).await?),
        )
    } else {
        (None, None)
    };

    // readiness check for all the ud containers
    wait_until_ready(
        &mut source_grpc_client,
        &mut sink_grpc_client,
        &mut transformer_grpc_client,
        &mut fb_sink_grpc_client,
    )
    .await?;

    // Start the metrics server, which server the prometheus metrics.
    let metrics_addr: SocketAddr = format!("0.0.0.0:{}", &config().metrics_server_listen_port)
        .parse()
        .expect("Invalid address");

    // Start the metrics server in a separate background async spawn,
    // This should be running throughout the lifetime of the application, hence the handle is not
    // joined.
    let metrics_state = MetricsState {
        source_client: source_grpc_client.clone(),
        sink_client: sink_grpc_client.clone(),
        transformer_client: transformer_grpc_client.clone(),
        fb_sink_client: fb_sink_grpc_client.clone(),
    };

    tokio::spawn(async move {
        if let Err(e) = start_metrics_https_server(metrics_addr, metrics_state).await {
            error!("Metrics server error: {:?}", e);
        }
    });

    // start the lag reader to publish lag metrics
    let mut lag_reader = LagReaderBuilder::new(source_grpc_client.clone())
        .lag_checking_interval(Duration::from_secs(
            config().lag_check_interval_in_secs.into(),
        ))
        .refresh_interval(Duration::from_secs(
            config().lag_refresh_interval_in_secs.into(),
        ))
        .build();
    lag_reader.start().await;

    // build the forwarder
    let mut forwarder_builder = ForwarderBuilder::new(source_reader, sink_writer, cln_token);
    // add transformer if exists
    if let Some(transformer) = transformer {
        forwarder_builder = forwarder_builder.source_transformer(transformer);
    }
    // add fallback sink if exists
    if let Some(fallback_writer) = fallback_writer {
        forwarder_builder = forwarder_builder.fallback_sink_writer(fallback_writer);
    }
    // build the final forwarder
    let mut forwarder = forwarder_builder.build();

    // start the forwarder, it will return only on Signal
    forwarder.start().await?;

    info!("Forwarder stopped gracefully");
    Ok(())
}

async fn wait_until_ready(
    source_client: &mut SourceClient<Channel>,
    sink_client: &mut SinkClient<Channel>,
    transformer_client: &mut Option<SourceTransformClient<Channel>>,
    fb_sink_client: &mut Option<SinkClient<Channel>>,
) -> Result<()> {
    loop {
        let source_ready = source_client.is_ready(Request::new(())).await.is_ok();
        if !source_ready {
            info!("UDSource is not ready, waiting...");
        }

        let sink_ready = sink_client.is_ready(Request::new(())).await.is_ok();
        if !sink_ready {
            info!("UDSink is not ready, waiting...");
        }

        let transformer_ready = if let Some(client) = transformer_client {
            let ready = client.is_ready(Request::new(())).await.is_ok();
            if !ready {
                info!("UDTransformer is not ready, waiting...");
            }
            ready
        } else {
            true
        };

        let fb_sink_ready = if let Some(client) = fb_sink_client {
            let ready = client.is_ready(Request::new(())).await.is_ok();
            if !ready {
                info!("Fallback Sink is not ready, waiting...");
            }
            ready
        } else {
            true
        };

        if source_ready && sink_ready && transformer_ready && fb_sink_ready {
            break;
        }

        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::env;

    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{sink, source};
    use tokio::sync::mpsc::Sender;
    use tokio_util::sync::CancellationToken;

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

        unsafe {
            env::set_var("SOURCE_SOCKET", src_sock_file.to_str().unwrap());
            env::set_var("SINK_SOCKET", sink_sock_file.to_str().unwrap());
        }

        let cln_token = CancellationToken::new();

        let forwarder_cln_token = cln_token.clone();
        let forwarder_handle = tokio::spawn(async move {
            let result =
                super::init(source_config, sink_config, None, None, forwarder_cln_token).await;
            assert!(result.is_ok());
        });

        // wait for the forwarder to start
        // FIXME: we need to have a better way, this is flaky
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // stop the forwarder
        cln_token.cancel();
        forwarder_handle.await.unwrap();

        // stop the source and sink servers
        src_shutdown_tx.send(()).unwrap();
        sink_shutdown_tx.send(()).unwrap();

        src_server_handle.await.unwrap();
        sink_server_handle.await.unwrap();
    }
}
