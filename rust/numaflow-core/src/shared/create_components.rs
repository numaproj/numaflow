use crate::config::components::sink::{SinkConfig, SinkType};
use crate::config::components::source::{SourceConfig, SourceType};
use crate::config::components::transformer::TransformerConfig;
use crate::shared::server_info::{sdk_server_info, ContainerType};
use crate::shared::{grpc, utils};
use crate::sink::{SinkClientType, SinkWriter, SinkWriterBuilder};
use crate::source::generator::new_generator;
use crate::source::user_defined::new_source;
use crate::source::Source;
use crate::transformer::Transformer;
use crate::{config, error, metrics, source};
use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

/// Creates a sink writer based on the configuration
pub(crate) async fn create_sink_writer(
    batch_size: usize,
    read_timeout: Duration,
    primary_sink: SinkConfig,
    fallback_sink: Option<SinkConfig>,
    cln_token: &CancellationToken,
) -> error::Result<(
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
                utils::get_component_type().to_string(),
                utils::get_vertex_name().to_string(),
                sink_server_info.language,
                sink_server_info.version,
                ContainerType::Sourcer.to_string(),
            );

            metrics::global_metrics()
                .sdk_info
                .get_or_create(&metric_labels)
                .set(1);

            let mut sink_grpc_client =
                SinkClient::new(grpc::create_rpc_channel(ud_config.socket_path.clone().into()).await?)
                    .max_encoding_message_size(ud_config.grpc_max_message_size)
                    .max_encoding_message_size(ud_config.grpc_max_message_size);
            grpc::wait_until_sink_ready(cln_token, &mut sink_grpc_client).await?;
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
                    utils::get_component_type().to_string(),
                    utils::get_vertex_name().to_string(),
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
                .max_encoding_message_size(ud_config.grpc_max_message_size);
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
                utils::get_component_type().to_string(),
                utils::get_vertex_name().to_string(),
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
            .max_encoding_message_size(ud_transformer.grpc_max_message_size);
            grpc::wait_until_transformer_ready(&cln_token, &mut transformer_grpc_client).await?;
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
) -> error::Result<(Source, Option<SourceClient<Channel>>)> {
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
                utils::get_component_type().to_string(),
                utils::get_vertex_name().to_string(),
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
            .max_encoding_message_size(udsource_config.grpc_max_message_size);
            grpc::wait_until_source_ready(&cln_token, &mut source_grpc_client).await?;
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