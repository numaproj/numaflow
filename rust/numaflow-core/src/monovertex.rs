use tokio::signal;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{error, info};

use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;

use crate::config::{config, Settings};
use crate::error::{self, Error};
use crate::shared::utils;
use crate::shared::utils::create_rpc_channel;
use crate::sink::{SinkClientType, SinkHandle};
use crate::source::generator::{new_generator, GeneratorAck, GeneratorLagReader, GeneratorRead};
use crate::source::user_defined::{
    new_source, UserDefinedSourceAck, UserDefinedSourceLagReader, UserDefinedSourceRead,
};
use crate::source::SourceHandle;
use crate::transformer::user_defined::SourceTransformHandle;
use forwarder::ForwarderBuilder;
use metrics::UserDefinedContainerState;

/// [forwarder] orchestrates data movement from the Source to the Sink via the optional SourceTransformer.
/// The forward-a-chunk executes the following in an infinite loop till a shutdown signal is received:
/// - Read X messages from the source
/// - Invokes the SourceTransformer concurrently
/// - Calls the Sinker to write the batch to the Sink
/// - Send Acknowledgement back to the Source
mod forwarder;
pub(crate) mod metrics;

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
    if let Err(e) = start_forwarder(cln_token, config()).await {
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

pub(crate) enum SourceType {
    UserDefinedSource(
        UserDefinedSourceRead,
        UserDefinedSourceAck,
        UserDefinedSourceLagReader,
    ),
    Generator(GeneratorRead, GeneratorAck, GeneratorLagReader),
}

async fn start_forwarder(cln_token: CancellationToken, config: &Settings) -> error::Result<()> {
    // make sure that we have compatibility with the server
    utils::check_compatibility(
        &cln_token,
        config
            .udsource_config
            .as_ref()
            .map(|source_config| source_config.server_info_path.clone().into()),
        config
            .udsink_config
            .as_ref()
            .map(|sink_config| sink_config.server_info_path.clone().into()),
        config
            .transformer_config
            .as_ref()
            .map(|transformer_config| transformer_config.server_info_path.clone().into()),
        config
            .fallback_config
            .as_ref()
            .map(|fallback_config| fallback_config.server_info_path.clone().into()),
    )
    .await?;

    let mut source_grpc_client = if let Some(source_config) = &config.udsource_config {
        Some(
            SourceClient::new(create_rpc_channel(source_config.socket_path.clone().into()).await?)
                .max_encoding_message_size(source_config.grpc_max_message_size)
                .max_encoding_message_size(source_config.grpc_max_message_size),
        )
    } else {
        None
    };

    let mut sink_grpc_client = if let Some(udsink_config) = &config.udsink_config {
        Some(
            SinkClient::new(create_rpc_channel(udsink_config.socket_path.clone().into()).await?)
                .max_encoding_message_size(udsink_config.grpc_max_message_size)
                .max_encoding_message_size(udsink_config.grpc_max_message_size),
        )
    } else {
        None
    };

    let mut fb_sink_grpc_client = if let Some(fb_sink_config) = &config.fallback_config {
        let fb_sink_grpc_client =
            SinkClient::new(create_rpc_channel(fb_sink_config.socket_path.clone().into()).await?)
                .max_encoding_message_size(fb_sink_config.grpc_max_message_size)
                .max_encoding_message_size(fb_sink_config.grpc_max_message_size);

        Some(fb_sink_grpc_client.clone())
    } else {
        None
    };

    let mut transformer_grpc_client = if let Some(transformer_config) = &config.transformer_config {
        let transformer_grpc_client = SourceTransformClient::new(
            create_rpc_channel(transformer_config.socket_path.clone().into()).await?,
        )
        .max_encoding_message_size(transformer_config.grpc_max_message_size)
        .max_encoding_message_size(transformer_config.grpc_max_message_size);

        Some(transformer_grpc_client.clone())
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

    let source_type = fetch_source(config, &mut source_grpc_client).await?;
    let (sink, fb_sink) = fetch_sink(
        config,
        sink_grpc_client.clone(),
        fb_sink_grpc_client.clone(),
    )
    .await?;

    // Start the metrics server in a separate background async spawn,
    // This should be running throughout the lifetime of the application, hence the handle is not
    // joined.
    let metrics_state = UserDefinedContainerState {
        source_client: source_grpc_client.clone(),
        sink_client: sink_grpc_client.clone(),
        transformer_client: transformer_grpc_client.clone(),
        fb_sink_client: fb_sink_grpc_client.clone(),
    };

    // start the metrics server
    // FIXME: what to do with the handle
    utils::start_metrics_server(metrics_state).await;

    let source = SourceHandle::new(source_type);
    start_forwarder_with_source(source, sink, transformer_grpc_client, fb_sink, cln_token).await?;

    info!("Forwarder stopped gracefully");
    Ok(())
}

// fetch right the source.
// source_grpc_client can be optional because it is valid only for user-defined source.
async fn fetch_source(
    config: &Settings,
    source_grpc_client: &mut Option<SourceClient<Channel>>,
) -> crate::Result<SourceType> {
    // check whether the source grpc client is provided, this happens only of the source is a
    // user defined source
    if let Some(source_grpc_client) = source_grpc_client.clone() {
        let (source_read, source_ack, lag_reader) = new_source(
            source_grpc_client,
            config.batch_size as usize,
            config.timeout_in_ms as u16,
        )
        .await?;
        return Ok(SourceType::UserDefinedSource(
            source_read,
            source_ack,
            lag_reader,
        ));
    }

    // now that we know it is not a user-defined source, it has to be a built-in
    if let Some(generator_config) = &config.generator_config {
        let (source_read, source_ack, lag_reader) =
            new_generator(generator_config.clone(), config.batch_size as usize)?;
        Ok(SourceType::Generator(source_read, source_ack, lag_reader))
    } else {
        Err(Error::Config("No valid source configuration found".into()))
    }
}

// fetch the actor handle for the sink.
// sink_grpc_client can be optional because it is valid only for user-defined sink.
async fn fetch_sink(
    settings: &Settings,
    sink_grpc_client: Option<SinkClient<Channel>>,
    fallback_sink_grpc_client: Option<SinkClient<Channel>>,
) -> crate::Result<(SinkHandle, Option<SinkHandle>)> {
    let fb_sink = match fallback_sink_grpc_client {
        Some(fallback_sink) => {
            Some(SinkHandle::new(SinkClientType::UserDefined(fallback_sink)).await?)
        }
        None => None,
    };

    if let Some(sink_client) = sink_grpc_client {
        let sink = SinkHandle::new(SinkClientType::UserDefined(sink_client)).await?;
        return Ok((sink, fb_sink));
    }
    if settings.logsink_config.is_some() {
        let log = SinkHandle::new(SinkClientType::Log).await?;
        return Ok((log, fb_sink));
    }
    Err(Error::Config(
        "No valid Sink configuration found".to_string(),
    ))
}

async fn start_forwarder_with_source(
    source: SourceHandle,
    sink: SinkHandle,
    transformer_client: Option<SourceTransformClient<tonic::transport::Channel>>,
    fallback_sink: Option<SinkHandle>,
    cln_token: CancellationToken,
) -> error::Result<()> {
    // start the pending reader to publish pending metrics
    let pending_reader = utils::create_pending_reader(source.clone()).await;
    let _pending_reader_handle = pending_reader.start().await;

    let mut forwarder_builder = ForwarderBuilder::new(source, sink, cln_token);

    // add transformer if exists
    if let Some(transformer_client) = transformer_client {
        let transformer = SourceTransformHandle::new(transformer_client).await?;
        forwarder_builder = forwarder_builder.source_transformer(transformer);
    }

    // add fallback sink if exists
    if let Some(fallback_sink) = fallback_sink {
        forwarder_builder = forwarder_builder.fallback_sink_writer(fallback_sink);
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
    use crate::config::{Settings, UDSinkConfig, UDSourceConfig};
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

        let config = Settings {
            udsink_config: Some(UDSinkConfig {
                socket_path: sink_sock_file.to_str().unwrap().to_string(),
                server_info_path: sink_server_info.to_str().unwrap().to_string(),
                grpc_max_message_size: 1024,
            }),
            udsource_config: Some(UDSourceConfig {
                socket_path: src_sock_file.to_str().unwrap().to_string(),
                server_info_path: src_info_file.to_str().unwrap().to_string(),
                grpc_max_message_size: 1024,
            }),
            ..Default::default()
        };

        let result = start_forwarder(cln_token.clone(), &config).await;
        dbg!(&result);
        assert!(result.is_ok());

        // stop the source and sink servers
        src_shutdown_tx.send(()).unwrap();
        sink_shutdown_tx.send(()).unwrap();

        src_server_handle.await.unwrap();
        sink_server_handle.await.unwrap();
    }
}
