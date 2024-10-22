use crate::config::components::source::SourceType;
use crate::config::pipeline;
use crate::config::pipeline::PipelineConfig;
use crate::monovertex::metrics::UserDefinedContainerState;
use crate::pipeline::isb::jetstream::WriterHandle;
use crate::shared::server_info::check_for_server_compatibility;
use crate::shared::utils::{
    create_rpc_channel, start_metrics_server, wait_until_source_ready, wait_until_transformer_ready,
};
use crate::source::generator::new_generator;
use crate::source::user_defined::new_source;
use crate::transformer::user_defined::SourceTransformHandle;
use crate::{config, error, source, Result};
use async_nats::jetstream;
use async_nats::jetstream::Context;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

mod forwarder;
mod isb;

pub(crate) async fn start_forwarder(
    cln_token: CancellationToken,
    config: &PipelineConfig,
) -> Result<()> {
    let js_context = create_js_context(config.js_client_config.clone()).await?;
    // FIXME: use appropriate clients and the metrics server
    start_metrics_server(
        config.metrics_config.clone(),
        UserDefinedContainerState {
            source_client: None,
            sink_client: None,
            transformer_client: None,
            fb_sink_client: None,
        },
    )
    .await;

    match &config.vertex_config {
        pipeline::VertexType::Source(source) => {
            let buffer_writers =
                create_buffer_writers(config, js_context.clone(), cln_token.clone()).await?;
            let source_type = create_source_type(source, config, cln_token.clone()).await?;
            let source_handle = source::SourceHandle::new(source_type, config.batch_size);
            let transformer = create_transformer(source, cln_token.clone()).await?;
            let mut forwarder = forwarder::source_forwarder::ForwarderBuilder::new(
                source_handle,
                transformer,
                buffer_writers,
                cln_token,
            )
            .build();
            forwarder.start().await?;
        }
        pipeline::VertexType::Sink(_) => {
            return Err(error::Error::Config(
                "Sink vertex is not supported yet".to_string(),
            ))
        }
    }
    Ok(())
}

async fn create_buffer_writers(
    config: &PipelineConfig,
    js_context: Context,
    cln_token: CancellationToken,
) -> Result<HashMap<String, Vec<WriterHandle>>> {
    let mut buffer_writers = HashMap::new();
    for to_vertex in &config.to_vertex_config {
        let writers = to_vertex
            .writer_config
            .streams
            .iter()
            .map(|stream| {
                WriterHandle::new(
                    stream.0.clone(),
                    stream.1,
                    to_vertex.writer_config.clone(),
                    js_context.clone(),
                    config.batch_size,
                    cln_token.clone(),
                )
            })
            .collect();
        buffer_writers.insert(to_vertex.name.clone(), writers);
    }
    Ok(buffer_writers)
}

async fn create_source_type(
    source: &pipeline::SourceVtxConfig,
    config: &PipelineConfig,
    cln_token: CancellationToken,
) -> Result<source::SourceType> {
    match &source.source_config.source_type {
        SourceType::Generator(generator_config) => {
            let (generator_read, generator_ack, generator_lag) =
                new_generator(generator_config.clone(), config.batch_size)?;
            Ok(source::SourceType::Generator(
                generator_read,
                generator_ack,
                generator_lag,
            ))
        }
        SourceType::UserDefined(udsource_config) => {
            check_for_server_compatibility(
                udsource_config.server_info_path.clone().into(),
                cln_token.clone(),
            )
            .await?;
            let mut source_grpc_client = SourceClient::new(
                create_rpc_channel(udsource_config.socket_path.clone().into()).await?,
            )
            .max_encoding_message_size(udsource_config.grpc_max_message_size)
            .max_encoding_message_size(udsource_config.grpc_max_message_size);
            wait_until_source_ready(&cln_token, &mut source_grpc_client).await?;
            let (ud_read, ud_ack, ud_lag) = new_source(
                source_grpc_client,
                config.batch_size,
                config.timeout_in_ms as u16,
            )
            .await?;
            Ok(source::SourceType::UserDefinedSource(
                ud_read, ud_ack, ud_lag,
            ))
        }
    }
}

async fn create_transformer(
    source: &pipeline::SourceVtxConfig,
    cln_token: CancellationToken,
) -> Result<Option<SourceTransformHandle>> {
    if let Some(transformer_config) = &source.transformer_config {
        if let config::components::transformer::TransformerType::UserDefined(ud_transformer) =
            &transformer_config.transformer_type
        {
            check_for_server_compatibility(
                ud_transformer.socket_path.clone().into(),
                cln_token.clone(),
            )
            .await?;
            let mut transformer_grpc_client = SourceTransformClient::new(
                create_rpc_channel(ud_transformer.socket_path.clone().into()).await?,
            )
            .max_encoding_message_size(ud_transformer.grpc_max_message_size)
            .max_encoding_message_size(ud_transformer.grpc_max_message_size);
            wait_until_transformer_ready(&cln_token, &mut transformer_grpc_client).await?;
            return Ok(Some(
                SourceTransformHandle::new(transformer_grpc_client).await?,
            ));
        }
    }
    Ok(None)
}

async fn create_js_context(config: pipeline::isb::jetstream::ClientConfig) -> Result<Context> {
    let js_client = match (config.user, config.password) {
        (Some(user), Some(password)) => {
            async_nats::connect_with_options(
                config.url,
                async_nats::ConnectOptions::with_user_and_password(user, password),
            )
            .await
        }
        _ => async_nats::connect(config.url).await,
    }
    .map_err(|e| error::Error::Connection(e.to_string()))?;
    Ok(jetstream::new(js_client))
}
