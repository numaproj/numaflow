use std::collections::HashMap;

use async_nats::jetstream;
use async_nats::jetstream::Context;
use futures::future::try_join_all;
use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use crate::config::components::source::SourceType;
use crate::config::pipeline;
use crate::config::pipeline::PipelineConfig;
use crate::metrics::{PipelineContainerState, UserDefinedContainerState};
use crate::pipeline::isb::jetstream::reader::JetstreamReader;
use crate::pipeline::isb::jetstream::WriterHandle;
use crate::shared::server_info::check_for_server_compatibility;
use crate::shared::utils;
use crate::shared::utils::{
    create_rpc_channel, start_metrics_server, wait_until_source_ready, wait_until_transformer_ready,
};
use crate::sink::SinkWriter;
use crate::source::generator::new_generator;
use crate::source::user_defined::new_source;
use crate::transformer::user_defined::SourceTransformHandle;
use crate::{config, error, source, Result};

mod forwarder;
mod isb;

/// Starts the appropriate forwarder based on the pipeline configuration.
pub(crate) async fn start_forwarder(
    cln_token: CancellationToken,
    config: PipelineConfig,
) -> Result<()> {
    let js_context = create_js_context(config.js_client_config.clone()).await?;

    match &config.vertex_config {
        pipeline::VertexType::Source(source) => {
            let buffer_writers =
                create_buffer_writers(&config, js_context.clone(), cln_token.clone()).await?;

            let (source_type, source_grpc_client) =
                create_source_type(source, &config, cln_token.clone()).await?;
            let (transformer, transformer_grpc_client) =
                create_transformer(source, cln_token.clone()).await?;

            start_metrics_server(
                config.metrics_config.clone(),
                UserDefinedContainerState::Pipeline(PipelineContainerState::Source((
                    source_grpc_client.clone(),
                    transformer_grpc_client.clone(),
                ))),
            )
            .await;

            let source_handle = source::SourceHandle::new(source_type, config.batch_size);
            let mut forwarder = forwarder::source_forwarder::ForwarderBuilder::new(
                source_handle,
                transformer,
                buffer_writers,
                cln_token.clone(),
                config.clone(),
            )
            .build();
            forwarder.start().await?;
        }
        pipeline::VertexType::Sink(sink) => {
            // Create buffer readers for each partition
            let buffer_readers = create_buffer_readers(&config, js_context.clone()).await?;

            // Create sink writers and clients
            let mut sink_writers = Vec::new();
            for _ in &buffer_readers {
                let (sink_writer, sink_grpc_client, fb_sink_grpc_client) =
                    create_sink_writer(&config, sink, cln_token.clone()).await?;
                sink_writers.push((sink_writer, sink_grpc_client, fb_sink_grpc_client));
            }

            // Start the metrics server with one of the clients
            if let Some((_, sink, fb_sink)) = sink_writers.first() {
                start_metrics_server(
                    config.metrics_config.clone(),
                    UserDefinedContainerState::Pipeline(PipelineContainerState::Sink((
                        sink.clone(),
                        fb_sink.clone(),
                    ))),
                )
                .await;
            }

            // Start a new forwarder for each buffer reader
            let mut forwarder_tasks = Vec::new();
            for (buffer_reader, (sink_writer, _, _)) in buffer_readers.into_iter().zip(sink_writers)
            {
                let forwarder = forwarder::sink_forwarder::SinkForwarder::new(
                    buffer_reader,
                    sink_writer,
                    cln_token.clone(),
                )
                .await;

                let task = tokio::spawn({
                    let config = config.clone();
                    async move { forwarder.start(config.clone()).await }
                });

                forwarder_tasks.push(task);
            }

            try_join_all(forwarder_tasks)
                .await
                .map_err(|e| error::Error::Forwarder(e.to_string()))?;
        }
    }
    Ok(())
}

/// Creates the required buffer writers based on the pipeline configuration, it creates a map
/// of vertex name to a list of writer handles.
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
                    config.paf_batch_size,
                    cln_token.clone(),
                )
            })
            .collect();
        buffer_writers.insert(to_vertex.name.clone(), writers);
    }
    Ok(buffer_writers)
}

async fn create_buffer_readers(
    config: &PipelineConfig,
    js_context: Context,
) -> Result<Vec<JetstreamReader>> {
    // Only the reader config of the first "from" vertex is needed, as all "from" vertices currently write
    // to a common buffer, in the case of a join.
    let reader_config = config
        .from_vertex_config
        .first()
        .ok_or_else(|| error::Error::Config("No from vertex config found".to_string()))?
        .reader_config
        .clone();

    let mut readers = Vec::new();
    for stream in &reader_config.streams {
        let reader = JetstreamReader::new(
            stream.0.clone(),
            stream.1,
            js_context.clone(),
            reader_config.clone(),
        )
        .await?;
        readers.push(reader);
    }

    Ok(readers)
}

// Creates a sink writer based on the pipeline configuration
async fn create_sink_writer(
    config: &PipelineConfig,
    sink_vtx_config: &pipeline::SinkVtxConfig,
    cln_token: CancellationToken,
) -> Result<(
    SinkWriter,
    Option<SinkClient<Channel>>,
    Option<SinkClient<Channel>>,
)> {
    let (sink_handle, sink_grpc_client) = utils::create_sink_handle(
        config.batch_size,
        &sink_vtx_config.sink_config.sink_type,
        &cln_token,
    )
    .await?;
    let (fb_sink_handle, fb_sink_grpc_client) = match &sink_vtx_config.fb_sink_config {
        None => (None, None),
        Some(fb_sink_config) => {
            let (handle, client) =
                utils::create_sink_handle(config.batch_size, &fb_sink_config.sink_type, &cln_token)
                    .await?;
            (Some(handle), client)
        }
    };

    Ok((
        SinkWriter::new(
            config.batch_size,
            config.read_timeout,
            sink_vtx_config.clone(),
            sink_handle,
            fb_sink_handle,
        )
        .await?,
        sink_grpc_client,
        fb_sink_grpc_client,
    ))
}

/// Creates a source type based on the pipeline configuration
async fn create_source_type(
    source: &pipeline::SourceVtxConfig,
    config: &PipelineConfig,
    cln_token: CancellationToken,
) -> Result<(source::SourceType, Option<SourceClient<Channel>>)> {
    match &source.source_config.source_type {
        SourceType::Generator(generator_config) => {
            let (generator_read, generator_ack, generator_lag) =
                new_generator(generator_config.clone(), config.batch_size)?;
            Ok((
                source::SourceType::Generator(generator_read, generator_ack, generator_lag),
                None,
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
                source_grpc_client.clone(),
                config.batch_size,
                config.read_timeout,
            )
            .await?;
            Ok((
                source::SourceType::UserDefinedSource(ud_read, ud_ack, ud_lag),
                Some(source_grpc_client),
            ))
        }
    }
}
/// Creates a transformer if it is configured in the pipeline
async fn create_transformer(
    source: &pipeline::SourceVtxConfig,
    cln_token: CancellationToken,
) -> Result<(
    Option<SourceTransformHandle>,
    Option<SourceTransformClient<Channel>>,
)> {
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
            return Ok((
                Some(SourceTransformHandle::new(transformer_grpc_client.clone()).await?),
                Some(transformer_grpc_client),
            ));
        }
    }
    Ok((None, None))
}

/// Creates a jetstream context based on the provided configuration
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};
    use futures::StreamExt;

    use super::*;

    use crate::config::components::metrics::MetricsConfig;
    use crate::config::components::source::GeneratorConfig;
    use crate::config::components::source::SourceConfig;
    use crate::config::components::source::SourceType;
    use crate::config::pipeline::PipelineConfig;
    use crate::pipeline::pipeline::isb;
    use crate::pipeline::pipeline::isb::BufferWriterConfig;
    use crate::pipeline::pipeline::SourceVtxConfig;
    use crate::pipeline::pipeline::ToVertexConfig;
    use crate::pipeline::pipeline::VertexType;
    use crate::pipeline::tests::isb::BufferFullStrategy::RetryUntilSuccess;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_forwarder_for_source_vetex() {
        // Unique names for the streams we use in this test
        let streams = vec![
            "default-test-forwarder-for-source-vertex-out-0",
            "default-test-forwarder-for-source-vertex-out-1",
            "default-test-forwarder-for-source-vertex-out-2",
            "default-test-forwarder-for-source-vertex-out-3",
            "default-test-forwarder-for-source-vertex-out-4",
        ];

        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        let mut consumers = vec![];
        // Create streams to which the generator source vertex we create later will forward
        // messages to. The consumers created for the corresponding streams will be used to ensure
        // that messages were actually written to the streams.
        for stream_name in &streams {
            let stream_name = *stream_name;
            let _stream = context
                .get_or_create_stream(stream::Config {
                    name: stream_name.into(),
                    subjects: vec![stream_name.into()],
                    max_message_size: 64 * 1024,
                    max_messages: 10000,
                    ..Default::default()
                })
                .await
                .unwrap();

            let c: consumer::PullConsumer = context
                .create_consumer_on_stream(
                    consumer::pull::Config {
                        name: Some(stream_name.to_string()),
                        ack_policy: consumer::AckPolicy::Explicit,
                        ..Default::default()
                    },
                    stream_name,
                )
                .await
                .unwrap();
            consumers.push((stream_name.to_string(),c));
        }

        let pipeline_config = PipelineConfig {
            pipeline_name: "simple-pipeline".to_string(),
            vertex_name: "in".to_string(),
            replica: 0,
            batch_size: 1000,
            paf_batch_size: 30000,
            read_timeout: Duration::from_secs(1),
            js_client_config: isb::jetstream::ClientConfig {
                url: "localhost:4222".to_string(),
                user: None,
                password: None,
            },
            from_vertex_config: vec![],
            to_vertex_config: vec![ToVertexConfig {
                name: "out".to_string(),
                writer_config: BufferWriterConfig {
                    streams: streams
                        .iter()
                        .enumerate()
                        .map(|(i, stream_name)| (stream_name.to_string(), i as u16))
                        .collect(),
                    partitions: 5,
                    max_length: 30000,
                    refresh_interval: Duration::from_secs(1),
                    usage_limit: 0.8,
                    buffer_full_strategy: RetryUntilSuccess,
                    retry_interval: Duration::from_millis(10),
                },
                partitions: 5,
                conditions: None,
            }],
            vertex_config: VertexType::Source(SourceVtxConfig {
                source_config: SourceConfig {
                    source_type: SourceType::Generator(GeneratorConfig {
                        rpu: 10,
                        content: bytes::Bytes::new(),
                        duration: Duration::from_secs(1),
                        value: None,
                        key_count: 0,
                        msg_size_bytes: 300,
                        jitter: Duration::from_millis(0),
                    }),
                },
                transformer_config: None,
            }),
            metrics_config: MetricsConfig {
                metrics_server_listen_port: 2469,
                lag_check_interval_in_secs: 5,
                lag_refresh_interval_in_secs: 3,
            },
        };

        let cancellation_token = tokio_util::sync::CancellationToken::new();
        let forwarder_task = tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            async move {
                start_forwarder(cancellation_token, pipeline_config)
                    .await
                    .unwrap();
            }
        });

        // Wait for a few messages to be forwarded
        tokio::time::sleep(Duration::from_secs(2)).await;
        cancellation_token.cancel();
        forwarder_task.abort();

        for (stream_name, stream_consumer) in consumers {
            let messages: Vec<async_nats::jetstream::Message> = stream_consumer
                .batch()
                .max_messages(10)
                .expires(Duration::from_millis(50))
                .messages()
                .await
                .unwrap()
                .map(|msg| msg.unwrap())
                .collect()
                .await;
            assert!(!messages.is_empty(), "Stream {} is expected to have messages", stream_name);
        }
        // Delete all streams created in this test
        for stream_name in streams {
            context.delete_stream(stream_name).await.unwrap();
        }
    }
}
