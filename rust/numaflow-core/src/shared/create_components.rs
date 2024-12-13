use std::time::Duration;

use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use crate::config::components::sink::{SinkConfig, SinkType};
use crate::config::components::source::{SourceConfig, SourceType};
use crate::config::components::transformer::TransformerConfig;
use crate::shared::grpc;
use crate::shared::server_info::{sdk_server_info, ContainerType};
use crate::sink::{SinkClientType, SinkWriter, SinkWriterBuilder};
use crate::source::generator::new_generator;
use crate::source::pulsar::new_pulsar_source;
use crate::source::user_defined::new_source;
use crate::source::Source;
use crate::tracker::TrackerHandle;
use crate::transformer::Transformer;
use crate::{config, error, metrics, source};

/// Creates a sink writer based on the configuration
pub(crate) async fn create_sink_writer(
    batch_size: usize,
    read_timeout: Duration,
    primary_sink: SinkConfig,
    fallback_sink: Option<SinkConfig>,
    tracker_handle: TrackerHandle,
    cln_token: &CancellationToken,
) -> error::Result<(
    SinkWriter,
    Option<SinkClient<Channel>>,
    Option<SinkClient<Channel>>,
)> {
    let (sink_writer_builder, sink_rpc_client) = match primary_sink.sink_type.clone() {
        SinkType::Log(_) => (
            SinkWriterBuilder::new(
                batch_size,
                read_timeout,
                SinkClientType::Log,
                tracker_handle,
            ),
            None,
        ),
        SinkType::Blackhole(_) => (
            SinkWriterBuilder::new(
                batch_size,
                read_timeout,
                SinkClientType::Blackhole,
                tracker_handle,
            ),
            None,
        ),
        SinkType::UserDefined(ud_config) => {
            let sink_server_info =
                sdk_server_info(ud_config.server_info_path.clone().into(), cln_token.clone())
                    .await?;

            let metric_labels = metrics::sdk_info_labels(
                config::get_component_type().to_string(),
                config::get_vertex_name().to_string(),
                sink_server_info.language,
                sink_server_info.version,
                ContainerType::Sourcer.to_string(),
            );

            metrics::global_metrics()
                .sdk_info
                .get_or_create(&metric_labels)
                .set(1);

            let mut sink_grpc_client = SinkClient::new(
                grpc::create_rpc_channel(ud_config.socket_path.clone().into()).await?,
            )
            .max_encoding_message_size(ud_config.grpc_max_message_size)
            .max_decoding_message_size(ud_config.grpc_max_message_size);
            grpc::wait_until_sink_ready(cln_token, &mut sink_grpc_client).await?;
            (
                SinkWriterBuilder::new(
                    batch_size,
                    read_timeout,
                    SinkClientType::UserDefined(sink_grpc_client.clone()),
                    tracker_handle,
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
                    config::get_component_type().to_string(),
                    config::get_vertex_name().to_string(),
                    fb_server_info.language,
                    fb_server_info.version,
                    ContainerType::Sourcer.to_string(),
                );

                metrics::global_metrics()
                    .sdk_info
                    .get_or_create(&metric_labels)
                    .set(1);

                let mut sink_grpc_client = SinkClient::new(
                    grpc::create_rpc_channel(ud_config.socket_path.clone().into()).await?,
                )
                .max_encoding_message_size(ud_config.grpc_max_message_size)
                .max_decoding_message_size(ud_config.grpc_max_message_size);
                grpc::wait_until_sink_ready(cln_token, &mut sink_grpc_client).await?;

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
    tracker_handle: TrackerHandle,
    cln_token: CancellationToken,
) -> error::Result<(Option<Transformer>, Option<SourceTransformClient<Channel>>)> {
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
                config::get_component_type().to_string(),
                config::get_vertex_name().to_string(),
                server_info.language,
                server_info.version,
                ContainerType::Sourcer.to_string(),
            );
            metrics::global_metrics()
                .sdk_info
                .get_or_create(&metric_labels)
                .set(1);

            let mut transformer_grpc_client = SourceTransformClient::new(
                grpc::create_rpc_channel(ud_transformer.socket_path.clone().into()).await?,
            )
            .max_encoding_message_size(ud_transformer.grpc_max_message_size)
            .max_decoding_message_size(ud_transformer.grpc_max_message_size);
            grpc::wait_until_transformer_ready(&cln_token, &mut transformer_grpc_client).await?;
            return Ok((
                Some(
                    Transformer::new(
                        batch_size,
                        transformer_config.concurrency,
                        transformer_grpc_client.clone(),
                        tracker_handle,
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
    tracker_handle: TrackerHandle,
    cln_token: CancellationToken,
) -> error::Result<(Source, Option<SourceClient<Channel>>)> {
    match &source_config.source_type {
        SourceType::Generator(generator_config) => {
            let (generator_read, generator_ack, generator_lag) =
                new_generator(generator_config.clone(), batch_size)?;
            Ok((
                Source::new(
                    batch_size,
                    source::SourceType::Generator(generator_read, generator_ack, generator_lag),
                    tracker_handle,
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
                config::get_component_type().to_string(),
                config::get_vertex_name().to_string(),
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
                grpc::create_rpc_channel(udsource_config.socket_path.clone().into()).await?,
            )
            .max_encoding_message_size(udsource_config.grpc_max_message_size)
            .max_decoding_message_size(udsource_config.grpc_max_message_size);
            grpc::wait_until_source_ready(&cln_token, &mut source_grpc_client).await?;
            let (ud_read, ud_ack, ud_lag) =
                new_source(source_grpc_client.clone(), batch_size, read_timeout).await?;
            Ok((
                Source::new(
                    batch_size,
                    source::SourceType::UserDefinedSource(ud_read, ud_ack, ud_lag),
                    tracker_handle,
                ),
                Some(source_grpc_client),
            ))
        }
        SourceType::Pulsar(pulsar_config) => {
            let pulsar = new_pulsar_source(pulsar_config.clone(), batch_size, read_timeout).await?;
            Ok((
                Source::new(
                    batch_size,
                    source::SourceType::Pulsar(pulsar),
                    tracker_handle,
                ),
                None,
            ))
        }
    }
}

// Retrieve value from mounted secret volume
// "/var/numaflow/secrets/${secretRef.name}/${secretRef.key}" is expected to be the file path
pub(crate) fn get_secret_from_volume(name: &str, key: &str) -> Result<String, String> {
    let path = format!("/var/numaflow/secrets/{name}/{key}");
    let val = std::fs::read_to_string(path.clone())
        .map_err(|e| format!("Reading secret from file {path}: {e:?}"))?;
    Ok(val.trim().into())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{sink, source, sourcetransform};
    use numaflow_pb::clients::sink::sink_client::SinkClient;
    use numaflow_pb::clients::source::source_client::SourceClient;
    use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Sender;
    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;

    use crate::shared::grpc::{
        create_rpc_channel, wait_until_sink_ready, wait_until_source_ready,
        wait_until_transformer_ready,
    };

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
