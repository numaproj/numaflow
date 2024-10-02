use crate::config::{config, SDKConfig};
use crate::error;
use crate::shared::utils;
use crate::shared::utils::create_rpc_channel;
use crate::sink::user_defined::SinkWriter;
use crate::source::user_defined::Source;
use crate::transformer::user_defined::SourceTransformer;
use forwarder::ForwarderBuilder;
use metrics::MetricsState;
use sink_pb::sink_client::SinkClient;
use source_pb::source_client::SourceClient;
use sourcetransform_pb::source_transform_client::SourceTransformClient;
use tokio::signal;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// [forwarder] orchestrates data movement from the Source to the Sink via the optional SourceTransformer.
/// The forward-a-chunk executes the following in an infinite loop till a shutdown signal is received:
/// - Read X messages from the source
/// - Invokes the SourceTransformer concurrently
/// - Calls the Sinker to write the batch to the Sink
/// - Send Acknowledgement back to the Source
mod forwarder;
pub(crate) mod metrics;

pub(crate) mod source_pb {
    tonic::include_proto!("source.v1");
}

pub(crate) mod sink_pb {
    tonic::include_proto!("sink.v1");
}

pub(crate) mod sourcetransform_pb {
    tonic::include_proto!("sourcetransformer.v1");
}

pub async fn mono_vertex() -> error::Result<()> {
    let cln_token = CancellationToken::new();
    let shutdown_cln_token = cln_token.clone();

    // wait for SIG{INT,TERM} and invoke cancellation token.
    let shutdown_handle: JoinHandle<error::Result<()>> = tokio::spawn(async move {
        shutdown_signal().await;
        shutdown_cln_token.cancel();
        Ok(())
    });

    // Run the forwarder with cancellation token.
    if let Err(e) = start_forwarder(cln_token, config().sdk_config.clone()).await {
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

async fn start_forwarder(cln_token: CancellationToken, sdk_config: SDKConfig) -> error::Result<()> {
    // make sure that we have compatibility with the server
    utils::check_compatibility(
        &cln_token,
        sdk_config.source_server_info_path.into(),
        sdk_config.sink_server_info_path.into(),
        if sdk_config.is_transformer_enabled {
            Some(sdk_config.transformer_server_info_path.into())
        } else {
            None
        },
        if sdk_config.is_fallback_enabled {
            Some(sdk_config.fallback_server_info_path.into())
        } else {
            None
        },
    )
    .await?;

    let mut source_grpc_client =
        SourceClient::new(create_rpc_channel(sdk_config.source_socket_path.into()).await?)
            .max_encoding_message_size(sdk_config.grpc_max_message_size)
            .max_encoding_message_size(sdk_config.grpc_max_message_size);

    let mut sink_grpc_client =
        SinkClient::new(create_rpc_channel(sdk_config.sink_socket_path.into()).await?)
            .max_encoding_message_size(sdk_config.grpc_max_message_size)
            .max_encoding_message_size(sdk_config.grpc_max_message_size);

    let mut transformer_grpc_client = if sdk_config.is_transformer_enabled {
        let transformer_grpc_client = SourceTransformClient::new(
            create_rpc_channel(sdk_config.transformer_socket_path.into()).await?,
        )
        .max_encoding_message_size(sdk_config.grpc_max_message_size)
        .max_encoding_message_size(sdk_config.grpc_max_message_size);

        Some(transformer_grpc_client.clone())
    } else {
        None
    };

    let mut fb_sink_grpc_client = if sdk_config.is_fallback_enabled {
        let fb_sink_grpc_client =
            SinkClient::new(create_rpc_channel(sdk_config.fallback_socket_path.into()).await?)
                .max_encoding_message_size(sdk_config.grpc_max_message_size)
                .max_encoding_message_size(sdk_config.grpc_max_message_size);

        Some(fb_sink_grpc_client.clone())
    } else {
        None
    };

    // readiness check for all the ud containers
    utils::wait_until_ready(
        cln_token.clone(),
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
    utils::start_metrics_server(metrics_state).await;

    // start the lag reader to publish lag metrics
    let mut lag_reader = utils::create_lag_reader(source_grpc_client.clone()).await;
    lag_reader.start().await;

    // build the forwarder
    let source_reader = Source::new(source_grpc_client.clone()).await?;
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
    use crate::config::SDKConfig;
    use crate::error;
    use crate::monovertex::start_forwarder;
    use crate::shared::server_info::ServerInfo;
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{sink, source};
    use std::fs::File;
    use std::io::Write;
    use tokio::sync::mpsc::Sender;
    use tokio_util::sync::CancellationToken;

    struct SimpleSource;
    #[tonic::async_trait]
    impl source::Sourcer for SimpleSource {
        async fn read(&self, _: SourceReadRequest, _: Sender<Message>) {}

        async fn ack(&self, _: Offset) {}

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

    async fn write_server_info(file_path: &str, server_info: &ServerInfo) -> error::Result<()> {
        let serialized = serde_json::to_string(server_info).unwrap();
        let mut file = File::create(file_path).unwrap();
        file.write_all(serialized.as_bytes()).unwrap();
        file.write_all(b"U+005C__END__").unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn run_forwarder() {
        let (src_shutdown_tx, src_shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let src_sock_file = tmp_dir.path().join("source.sock");
        let src_info_file = tmp_dir.path().join("sourcer-server-info");
        let server_info_obj = ServerInfo {
            protocol: "uds".to_string(),
            language: "rust".to_string(),
            minimum_numaflow_version: "0.1.0".to_string(),
            version: "0.1.0".to_string(),
            metadata: None,
        };

        write_server_info(src_info_file.to_str().unwrap(), &server_info_obj)
            .await
            .unwrap();

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
        let sink_server_info = tmp_dir.path().join("sinker-server-info");

        write_server_info(sink_server_info.to_str().unwrap(), &server_info_obj)
            .await
            .unwrap();

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

        let cln_token = CancellationToken::new();

        let token_clone = cln_token.clone();
        tokio::spawn(async move {
            // FIXME: we need to have a better way, this is flaky
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            token_clone.cancel();
        });

        let sdk_config = SDKConfig {
            source_socket_path: src_sock_file.to_str().unwrap().to_string(),
            sink_socket_path: sink_sock_file.to_str().unwrap().to_string(),
            source_server_info_path: src_info_file.to_str().unwrap().to_string(),
            sink_server_info_path: sink_server_info.to_str().unwrap().to_string(),
            grpc_max_message_size: 1024,
            ..Default::default()
        };

        let result = start_forwarder(cln_token.clone(), sdk_config).await;
        assert!(result.is_ok());

        // stop the source and sink servers
        src_shutdown_tx.send(()).unwrap();
        sink_shutdown_tx.send(()).unwrap();

        src_server_handle.await.unwrap();
        sink_server_handle.await.unwrap();
    }
}
