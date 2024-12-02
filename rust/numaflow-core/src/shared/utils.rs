use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::config::components::metrics::MetricsConfig;
use crate::config::components::sink::{SinkConfig, SinkType};
use crate::config::components::source::{SourceConfig, SourceType};
use crate::config::components::transformer::TransformerConfig;
use crate::metrics::{
    start_metrics_https_server, PendingReader, PendingReaderBuilder, UserDefinedContainerState,
};
use crate::shared::server_info::{sdk_server_info, ContainerType};
use crate::sink::{SinkClientType, SinkWriter, SinkWriterBuilder};
use crate::source::generator::new_generator;
use crate::source::user_defined::new_source;
use crate::source::Source;
use crate::transformer::Transformer;
use crate::{config, error};
use crate::{metrics, Result};
use crate::{source, Error};
use axum::http::Uri;
use backoff::retry::Retry;
use backoff::strategy::fixed;
use chrono::{DateTime, TimeZone, Timelike, Utc};
use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use prost_types::Timestamp;
use std::env;
use std::sync::OnceLock;
use tokio::net::UnixStream;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tower::service_fn;
use tracing::info;

const NUMAFLOW_MONO_VERTEX_NAME: &str = "NUMAFLOW_MONO_VERTEX_NAME";
const NUMAFLOW_VERTEX_NAME: &str = "NUMAFLOW_VERTEX_NAME";
const NUMAFLOW_REPLICA: &str = "NUMAFLOW_REPLICA";
static VERTEX_NAME: OnceLock<String> = OnceLock::new();

pub(crate) fn get_vertex_name() -> &'static str {
    VERTEX_NAME.get_or_init(|| {
        env::var(NUMAFLOW_MONO_VERTEX_NAME)
            .or_else(|_| env::var(NUMAFLOW_VERTEX_NAME))
            .unwrap_or_default()
    })
}

static IS_MONO_VERTEX: OnceLock<bool> = OnceLock::new();

pub(crate) fn is_mono_vertex() -> &'static bool {
    IS_MONO_VERTEX.get_or_init(|| env::var(NUMAFLOW_MONO_VERTEX_NAME).is_ok())
}

static COMPONENT_TYPE: OnceLock<String> = OnceLock::new();

pub(crate) fn get_component_type() -> &'static str {
    COMPONENT_TYPE.get_or_init(|| {
        if *is_mono_vertex() {
            "mono-vertex".to_string()
        } else {
            "pipeline".to_string()
        }
    })
}

static PIPELINE_NAME: OnceLock<String> = OnceLock::new();

pub(crate) fn get_pipeline_name() -> &'static str {
    PIPELINE_NAME.get_or_init(|| env::var("NUMAFLOW_PIPELINE_NAME").unwrap_or_default())
}

static VERTEX_REPLICA: OnceLock<u16> = OnceLock::new();

// fetch the vertex replica information from the environment variable
pub(crate) fn get_vertex_replica() -> &'static u16 {
    VERTEX_REPLICA.get_or_init(|| {
        env::var(NUMAFLOW_REPLICA)
            .unwrap_or_default()
            .parse()
            .unwrap_or_default()
    })
}

/// Starts the metrics server
pub(crate) async fn start_metrics_server(
    metrics_config: MetricsConfig,
    metrics_state: UserDefinedContainerState,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Start the metrics server, which server the prometheus metrics.
        let metrics_addr: SocketAddr =
            format!("0.0.0.0:{}", metrics_config.metrics_server_listen_port)
                .parse()
                .expect("Invalid address");

        if let Err(e) = start_metrics_https_server(metrics_addr, metrics_state).await {
            error!("metrics server error: {:?}", e);
        }
    })
}

/// Creates a pending reader
pub(crate) async fn create_pending_reader(
    metrics_config: &MetricsConfig,
    lag_reader_grpc_client: Source,
) -> PendingReader {
    PendingReaderBuilder::new(lag_reader_grpc_client)
        .lag_checking_interval(Duration::from_secs(
            metrics_config.lag_check_interval_in_secs.into(),
        ))
        .refresh_interval(Duration::from_secs(
            metrics_config.lag_refresh_interval_in_secs.into(),
        ))
        .build()
}

/// Waits until the source server is ready, by doing health checks
pub(crate) async fn wait_until_source_ready(
    cln_token: &CancellationToken,
    client: &mut SourceClient<Channel>,
) -> Result<()> {
    info!("Waiting for source client to be ready...");
    loop {
        if cln_token.is_cancelled() {
            return Err(Error::Forwarder(
                "Cancellation token is cancelled".to_string(),
            ));
        }
        match client.is_ready(Request::new(())).await {
            Ok(_) => break,
            Err(_) => sleep(Duration::from_secs(1)).await,
        }
        info!("Waiting for source client to be ready...");
    }
    Ok(())
}

/// Waits until the sink server is ready, by doing health checks
pub(crate) async fn wait_until_sink_ready(
    cln_token: &CancellationToken,
    client: &mut SinkClient<Channel>,
) -> Result<()> {
    loop {
        if cln_token.is_cancelled() {
            return Err(Error::Forwarder(
                "Cancellation token is cancelled".to_string(),
            ));
        }
        match client.is_ready(Request::new(())).await {
            Ok(_) => break,
            Err(_) => sleep(Duration::from_secs(1)).await,
        }
        info!("Waiting for sink client to be ready...");
    }
    Ok(())
}

/// Waits until the transformer server is ready, by doing health checks
pub(crate) async fn wait_until_transformer_ready(
    cln_token: &CancellationToken,
    client: &mut SourceTransformClient<Channel>,
) -> Result<()> {
    loop {
        if cln_token.is_cancelled() {
            return Err(Error::Forwarder(
                "Cancellation token is cancelled".to_string(),
            ));
        }
        match client.is_ready(Request::new(())).await {
            Ok(_) => break,
            Err(_) => sleep(Duration::from_secs(1)).await,
        }
        info!("Waiting for transformer client to be ready...");
    }
    Ok(())
}

pub(crate) fn utc_from_timestamp(t: Option<Timestamp>) -> DateTime<Utc> {
    t.map_or(Utc.timestamp_nanos(-1), |t| {
        DateTime::from_timestamp(t.seconds, t.nanos as u32).unwrap_or(Utc.timestamp_nanos(-1))
    })
}

pub(crate) fn prost_timestamp_from_utc(t: DateTime<Utc>) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: t.timestamp(),
        nanos: t.nanosecond() as i32,
    })
}

pub(crate) async fn create_rpc_channel(socket_path: PathBuf) -> Result<Channel> {
    const RECONNECT_INTERVAL: u64 = 1000;
    const MAX_RECONNECT_ATTEMPTS: usize = 5;

    let interval = fixed::Interval::from_millis(RECONNECT_INTERVAL).take(MAX_RECONNECT_ATTEMPTS);

    let channel = Retry::retry(
        interval,
        || async { connect_with_uds(socket_path.clone()).await },
        |_: &Error| true,
    )
    .await?;
    Ok(channel)
}

/// Connects to the UDS socket and returns a channel
pub(crate) async fn connect_with_uds(uds_path: PathBuf) -> Result<Channel> {
    let channel = Endpoint::try_from("http://[::]:50051")
        .map_err(|e| Error::Connection(format!("Failed to create endpoint: {:?}", e)))?
        .connect_with_connector(service_fn(move |_: Uri| {
            let uds_socket = uds_path.clone();
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    UnixStream::connect(uds_socket).await?,
                ))
            }
        }))
        .await
        .map_err(|e| Error::Connection(format!("Failed to connect: {:?}", e)))?;
    Ok(channel)
}

/// Creates a sink writer based on the configuration
pub(crate) async fn create_sink_writer(
    batch_size: usize,
    read_timeout: Duration,
    primary_sink: SinkConfig,
    fallback_sink: Option<SinkConfig>,
    cln_token: &CancellationToken,
) -> Result<(
    SinkWriter,
    Option<SinkClient<Channel>>,
    Option<SinkClient<Channel>>,
)> {
    let (sink_writer_builder, sink_rpc_client) = match primary_sink.sink_type.clone() {
        SinkType::Log(_) => (
            SinkWriterBuilder::new(batch_size, read_timeout, SinkClientType::Log),
            None,
        ),
        SinkType::Blackhole(_) => (
            SinkWriterBuilder::new(batch_size, read_timeout, SinkClientType::Blackhole),
            None,
        ),
        SinkType::UserDefined(ud_config) => {
            let sink_server_info =
                sdk_server_info(ud_config.server_info_path.clone().into(), cln_token.clone())
                    .await?;

            let metric_labels = metrics::sdk_info_labels(
                get_component_type().to_string(),
                get_vertex_name().to_string(),
                sink_server_info.language,
                sink_server_info.version,
                ContainerType::Sourcer.to_string(),
            );

            metrics::global_metrics()
                .sdk_info
                .get_or_create(&metric_labels)
                .set(1);

            let mut sink_grpc_client =
                SinkClient::new(create_rpc_channel(ud_config.socket_path.clone().into()).await?)
                    .max_encoding_message_size(ud_config.grpc_max_message_size)
                    .max_encoding_message_size(ud_config.grpc_max_message_size);
            wait_until_sink_ready(cln_token, &mut sink_grpc_client).await?;
            (
                SinkWriterBuilder::new(
                    batch_size,
                    read_timeout,
                    SinkClientType::UserDefined(sink_grpc_client.clone()),
                )
                .retry_config(primary_sink.retry_config.unwrap_or_default()),
                Some(sink_grpc_client),
            )
        }
    };

    if let Some(fb_sink) = fallback_sink {
        return match fb_sink.sink_type.clone() {
            SinkType::Log(_) => Ok((
                sink_writer_builder
                    .fb_sink_client(SinkClientType::Log)
                    .build()
                    .await?,
                sink_rpc_client.clone(),
                None,
            )),
            SinkType::Blackhole(_) => Ok((
                sink_writer_builder
                    .fb_sink_client(SinkClientType::Blackhole)
                    .build()
                    .await?,
                sink_rpc_client.clone(),
                None,
            )),
            SinkType::UserDefined(ud_config) => {
                let fb_server_info =
                    sdk_server_info(ud_config.server_info_path.clone().into(), cln_token.clone())
                        .await?;

                let metric_labels = metrics::sdk_info_labels(
                    get_component_type().to_string(),
                    get_vertex_name().to_string(),
                    fb_server_info.language,
                    fb_server_info.version,
                    ContainerType::Sourcer.to_string(),
                );

                metrics::global_metrics()
                    .sdk_info
                    .get_or_create(&metric_labels)
                    .set(1);

                let mut sink_grpc_client = SinkClient::new(
                    create_rpc_channel(ud_config.socket_path.clone().into()).await?,
                )
                .max_encoding_message_size(ud_config.grpc_max_message_size)
                .max_encoding_message_size(ud_config.grpc_max_message_size);
                wait_until_sink_ready(cln_token, &mut sink_grpc_client).await?;

                Ok((
                    sink_writer_builder
                        .fb_sink_client(SinkClientType::UserDefined(sink_grpc_client.clone()))
                        .build()
                        .await?,
                    sink_rpc_client.clone(),
                    Some(sink_grpc_client),
                ))
            }
        };
    }
    Ok((sink_writer_builder.build().await?, sink_rpc_client, None))
}

/// Creates a transformer if it is configured
pub async fn create_transformer(
    batch_size: usize,
    transformer_config: Option<TransformerConfig>,
    cln_token: CancellationToken,
) -> Result<(Option<Transformer>, Option<SourceTransformClient<Channel>>)> {
    if let Some(transformer_config) = transformer_config {
        if let config::components::transformer::TransformerType::UserDefined(ud_transformer) =
            &transformer_config.transformer_type
        {
            let server_info = sdk_server_info(
                ud_transformer.server_info_path.clone().into(),
                cln_token.clone(),
            )
            .await?;
            let metric_labels = metrics::sdk_info_labels(
                get_component_type().to_string(),
                get_vertex_name().to_string(),
                server_info.language,
                server_info.version,
                ContainerType::Sourcer.to_string(),
            );
            metrics::global_metrics()
                .sdk_info
                .get_or_create(&metric_labels)
                .set(1);

            let mut transformer_grpc_client = SourceTransformClient::new(
                create_rpc_channel(ud_transformer.socket_path.clone().into()).await?,
            )
            .max_encoding_message_size(ud_transformer.grpc_max_message_size)
            .max_encoding_message_size(ud_transformer.grpc_max_message_size);
            wait_until_transformer_ready(&cln_token, &mut transformer_grpc_client).await?;
            return Ok((
                Some(
                    Transformer::new(
                        batch_size,
                        transformer_config.concurrency,
                        transformer_grpc_client.clone(),
                    )
                    .await?,
                ),
                Some(transformer_grpc_client),
            ));
        }
    }
    Ok((None, None))
}

/// Creates a source type based on the configuration
pub async fn create_source(
    batch_size: usize,
    read_timeout: Duration,
    source_config: &SourceConfig,
    cln_token: CancellationToken,
) -> Result<(Source, Option<SourceClient<Channel>>)> {
    match &source_config.source_type {
        SourceType::Generator(generator_config) => {
            let (generator_read, generator_ack, generator_lag) =
                new_generator(generator_config.clone(), batch_size)?;
            Ok((
                Source::new(
                    batch_size,
                    source::SourceType::Generator(generator_read, generator_ack, generator_lag),
                ),
                None,
            ))
        }
        SourceType::UserDefined(udsource_config) => {
            let server_info = sdk_server_info(
                udsource_config.server_info_path.clone().into(),
                cln_token.clone(),
            )
            .await?;

            let metric_labels = metrics::sdk_info_labels(
                get_component_type().to_string(),
                get_vertex_name().to_string(),
                server_info.language,
                server_info.version,
                ContainerType::Sourcer.to_string(),
            );
            metrics::global_metrics()
                .sdk_info
                .get_or_create(&metric_labels)
                .set(1);

            // TODO: Add sdk info metric
            let mut source_grpc_client = SourceClient::new(
                create_rpc_channel(udsource_config.socket_path.clone().into()).await?,
            )
            .max_encoding_message_size(udsource_config.grpc_max_message_size)
            .max_encoding_message_size(udsource_config.grpc_max_message_size);
            wait_until_source_ready(&cln_token, &mut source_grpc_client).await?;
            let (ud_read, ud_ack, ud_lag) =
                new_source(source_grpc_client.clone(), batch_size, read_timeout).await?;
            Ok((
                Source::new(
                    batch_size,
                    source::SourceType::UserDefinedSource(ud_read, ud_ack, ud_lag),
                ),
                Some(source_grpc_client),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{sink, source, sourcetransform};
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Sender;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::shared::utils::create_rpc_channel;

    struct SimpleSource {}

    #[tonic::async_trait]
    impl source::Sourcer for SimpleSource {
        async fn read(&self, _request: SourceReadRequest, _transmitter: Sender<Message>) {}

        async fn ack(&self, _offset: Vec<Offset>) {}

        async fn pending(&self) -> usize {
            0
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![0])
        }
    }

    struct SimpleTransformer;
    #[tonic::async_trait]
    impl sourcetransform::SourceTransformer for SimpleTransformer {
        async fn transform(
            &self,
            _input: sourcetransform::SourceTransformRequest,
        ) -> Vec<sourcetransform::Message> {
            vec![]
        }
    }

    struct InMemorySink {}

    #[tonic::async_trait]
    impl sink::Sinker for InMemorySink {
        async fn sink(&self, mut _input: mpsc::Receiver<sink::SinkRequest>) -> Vec<sink::Response> {
            vec![]
        }
    }

    #[tokio::test]
    async fn test_wait_until_ready() {
        // Start the source server
        let (source_shutdown_tx, source_shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let source_sock_file = tmp_dir.path().join("source.sock");
        let server_info_file = tmp_dir.path().join("source-server-info");

        let server_info = server_info_file.clone();
        let source_socket = source_sock_file.clone();
        let source_server_handle = tokio::spawn(async move {
            source::Server::new(SimpleSource {})
                .with_socket_file(source_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(source_shutdown_rx)
                .await
                .unwrap();
        });

        // Start the sink server
        let (sink_shutdown_tx, sink_shutdown_rx) = tokio::sync::oneshot::channel();
        let sink_tmp_dir = tempfile::TempDir::new().unwrap();
        let sink_sock_file = sink_tmp_dir.path().join("sink.sock");
        let server_info_file = sink_tmp_dir.path().join("sink-server-info");

        let server_info = server_info_file.clone();
        let sink_socket = sink_sock_file.clone();
        let sink_server_handle = tokio::spawn(async move {
            sink::Server::new(InMemorySink {})
                .with_socket_file(sink_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(sink_shutdown_rx)
                .await
                .unwrap();
        });

        // Start the transformer server
        let (transformer_shutdown_tx, transformer_shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let transformer_sock_file = tmp_dir.path().join("transformer.sock");
        let server_info_file = tmp_dir.path().join("transformer-server-info");

        let server_info = server_info_file.clone();
        let transformer_socket = transformer_sock_file.clone();
        let transformer_server_handle = tokio::spawn(async move {
            sourcetransform::Server::new(SimpleTransformer {})
                .with_socket_file(transformer_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(transformer_shutdown_rx)
                .await
                .unwrap();
        });

        // Wait for the servers to start
        sleep(Duration::from_millis(100)).await;

        let cln_token = CancellationToken::new();

        let mut source_grpc_client =
            SourceClient::new(create_rpc_channel(source_sock_file.clone()).await.unwrap());
        wait_until_source_ready(&cln_token, &mut source_grpc_client)
            .await
            .unwrap();

        let mut sink_grpc_client =
            SinkClient::new(create_rpc_channel(sink_sock_file.clone()).await.unwrap());
        wait_until_sink_ready(&cln_token, &mut sink_grpc_client)
            .await
            .unwrap();

        let mut transformer_grpc_client = Some(SourceTransformClient::new(
            create_rpc_channel(transformer_sock_file.clone())
                .await
                .unwrap(),
        ));
        wait_until_transformer_ready(&cln_token, transformer_grpc_client.as_mut().unwrap())
            .await
            .unwrap();

        source_shutdown_tx.send(()).unwrap();
        sink_shutdown_tx.send(()).unwrap();
        transformer_shutdown_tx.send(()).unwrap();

        source_server_handle.await.unwrap();
        sink_server_handle.await.unwrap();
        transformer_server_handle.await.unwrap();
    }
}
