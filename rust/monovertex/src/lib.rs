pub(crate) use self::error::Result;
use crate::config::config;
pub(crate) use crate::error::Error;
use crate::forwarder::ForwarderBuilder;
use crate::metrics::{start_metrics_https_server, LagReaderBuilder, MetricsState};
use crate::sink::{SinkClient, SinkConfig};
use crate::source::{SourceClient, SourceConfig};
use crate::transformer::{TransformerClient, TransformerConfig};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::signal;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
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

mod server_info;

mod metrics;

pub async fn mono_vertex() {
    // Initialize the source, sink and transformer configurations
    // We are using the default configurations for now.
    let source_config = SourceConfig {
        max_message_size: config().grpc_max_message_size,
        ..Default::default()
    };

    let sink_config = SinkConfig {
        max_message_size: config().grpc_max_message_size,
        ..Default::default()
    };

    let transformer_config = if config().is_transformer_enabled {
        Some(TransformerConfig {
            max_message_size: config().grpc_max_message_size,
            ..Default::default()
        })
    } else {
        None
    };

    let fb_sink_config = if config().is_fallback_enabled {
        Some(SinkConfig::fallback_default())
    } else {
        None
    };

    let cln_token = CancellationToken::new();
    let shutdown_cln_token = cln_token.clone();

    // wait for SIG{INT,TERM} and invoke cancellation token.
    let shutdown_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        shutdown_signal().await;
        shutdown_cln_token.cancel();
        Ok(())
    });

    // Run the forwarder with cancellation token.
    if let Err(e) = init(
        source_config,
        sink_config,
        transformer_config,
        fb_sink_config,
        cln_token,
    )
    .await
    {
        error!("Application error: {:?}", e);

        // abort the signal handler task since we have an error and we are shutting down
        if !shutdown_handle.is_finished() {
            shutdown_handle.abort();
        }
    }

    info!("Gracefully Exiting...");
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

/// forwards a chunk of data from the source to the sink via an optional transformer.
/// It takes an optional custom_shutdown_rx for shutting down the forwarder, useful for testing.
pub async fn init(
    source_config: SourceConfig,
    sink_config: SinkConfig,
    transformer_config: Option<TransformerConfig>,
    fb_sink_config: Option<SinkConfig>,
    cln_token: CancellationToken,
) -> Result<()> {
    server_info::check_for_server_compatibility(&source_config.server_info_file, cln_token.clone())
        .await
        .map_err(|e| {
            warn!("Error waiting for source server info file: {:?}", e);
            Error::ForwarderError(format!("Error waiting for source server info file: {}", e))
        })?;
    let mut source_client = SourceClient::connect(source_config).await?;

    server_info::check_for_server_compatibility(&sink_config.server_info_file, cln_token.clone())
        .await
        .map_err(|e| {
            warn!("Error waiting for sink server info file: {:?}", e);
            Error::ForwarderError(format!("Error waiting for sink server info file: {}", e))
        })?;

    let mut sink_client = SinkClient::connect(sink_config).await?;

    let mut transformer_client = if let Some(config) = transformer_config {
        server_info::check_for_server_compatibility(&config.server_info_file, cln_token.clone())
            .await
            .map_err(|e| {
                warn!("Error waiting for transformer server info file: {:?}", e);
                Error::ForwarderError(format!("Error waiting for transformer server info file: {}", e))
            })?;
        Some(TransformerClient::connect(config).await?)
    } else {
        None
    };

    let mut fb_sink_client = if let Some(config) = fb_sink_config {
        server_info::check_for_server_compatibility(&config.server_info_file, cln_token.clone())
            .await
            .map_err(|e| {
                warn!("Error waiting for fallback sink server info file: {:?}", e);
                Error::ForwarderError(format!("Error waiting for fallback sink server info file: {}", e))
            })?;
        Some(SinkClient::connect(config).await?)
    } else {
        None
    };

    // readiness check for all the ud containers
    wait_until_ready(
        &mut source_client,
        &mut sink_client,
        &mut transformer_client,
        &mut fb_sink_client,
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
        source_client: source_client.clone(),
        sink_client: sink_client.clone(),
        transformer_client: transformer_client.clone(),
        fb_sink_client: fb_sink_client.clone(),
    };
    tokio::spawn(async move {
        if let Err(e) = start_metrics_https_server(metrics_addr, metrics_state).await {
            error!("Metrics server error: {:?}", e);
        }
    });

    // start the lag reader to publish lag metrics
    let mut lag_reader = LagReaderBuilder::new(source_client.clone())
        .lag_checking_interval(Duration::from_secs(
            config().lag_check_interval_in_secs.into(),
        ))
        .refresh_interval(Duration::from_secs(
            config().lag_refresh_interval_in_secs.into(),
        ))
        .build();
    lag_reader.start().await;

    // build the forwarder
    let mut forwarder_builder = ForwarderBuilder::new(source_client, sink_client, cln_token);
    // add transformer if exists
    if let Some(transformer_client) = transformer_client {
        forwarder_builder = forwarder_builder.transformer_client(transformer_client);
    }
    // add fallback sink if exists
    if let Some(fb_sink_client) = fb_sink_client {
        forwarder_builder = forwarder_builder.fb_sink_client(fb_sink_client);
    }
    // build the final forwarder
    let mut forwarder = forwarder_builder.build();

    // start the forwarder, it will return only on Signal
    forwarder.start().await?;

    info!("Forwarder stopped gracefully");
    Ok(())
}

async fn wait_until_ready(
    source_client: &mut SourceClient,
    sink_client: &mut SinkClient,
    transformer_client: &mut Option<TransformerClient>,
    fb_sink_client: &mut Option<SinkClient>,
) -> Result<()> {
    loop {
        let source_ready = source_client.is_ready().await;
        if !source_ready {
            info!("UDSource is not ready, waiting...");
        }

        let sink_ready = sink_client.is_ready().await;
        if !sink_ready {
            info!("UDSink is not ready, waiting...");
        }

        let transformer_ready = if let Some(client) = transformer_client {
            let ready = client.is_ready().await;
            if !ready {
                info!("UDTransformer is not ready, waiting...");
            }
            ready
        } else {
            true
        };

        let fb_sink_ready = if let Some(client) = fb_sink_client {
            let ready = client.is_ready().await;
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
