use std::collections::HashMap;
use std::time::Duration;

use crate::config::components::reduce::UnalignedWindowType;
use crate::config::components::sink::{SinkConfig, SinkType};
use crate::config::components::source::{SourceConfig, SourceType};
use crate::config::components::transformer::TransformerConfig;
use crate::config::get_vertex_replica;
use crate::config::pipeline::map::{MapMode, MapType, MapVtxConfig};
use crate::config::pipeline::watermark::WatermarkConfig;
use crate::config::pipeline::{
    DEFAULT_BATCH_MAP_SOCKET, DEFAULT_STREAM_MAP_SOCKET, PipelineConfig, ToVertexConfig,
};
use crate::error::Error;
use crate::mapper::map::MapHandle;
use crate::pipeline::isb::jetstream::js_writer::JetStreamWriter;
use crate::reduce::reducer::WindowManager;
use crate::reduce::reducer::aligned::user_defined::UserDefinedAlignedReduce;
use crate::reduce::reducer::unaligned::user_defined::UserDefinedUnalignedReduce;
use crate::reduce::reducer::unaligned::user_defined::accumulator::UserDefinedAccumulator;
use crate::reduce::reducer::unaligned::user_defined::session::UserDefinedSessionReduce;
use crate::shared::grpc;
use crate::shared::grpc::{create_rpc_channel, wait_until_source_ready};
use crate::sinker::sink::serve::ServingStore;
use crate::sinker::sink::{SinkClientType, SinkWriter, SinkWriterBuilder};
use crate::source::Source;
use crate::source::generator::new_generator;
use crate::source::http::CoreHttpSource;
use crate::source::jetstream::new_jetstream_source;
use crate::source::kafka::new_kafka_source;
use crate::source::nats::new_nats_source;
use crate::source::pulsar::new_pulsar_source;
use crate::source::sqs::new_sqs_source;
use crate::source::user_defined::new_source;
use crate::tracker::Tracker;
use crate::transformer::Transformer;
use crate::typ::NumaflowTypeConfig;
use crate::watermark::isb::ISBWatermarkHandle;
use crate::watermark::source::SourceWatermarkHandle;
use crate::{config, error, metrics, source};
use async_nats::jetstream::Context;
use numaflow_models::models::{NatsAuth, Tls};
use numaflow_nats::{TlsClientAuthCerts, TlsConfig};
use numaflow_pb::clients::accumulator::accumulator_client::AccumulatorClient;
use numaflow_pb::clients::map::map_client::MapClient;
use numaflow_pb::clients::reduce::reduce_client::ReduceClient;
use numaflow_pb::clients::sessionreduce::session_reduce_client::SessionReduceClient;
use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use numaflow_shared::server_info::{
    ContainerType, Protocol, ServerInfo, sdk_server_info, supports_nack,
};
use numaflow_sqs::sink::SqsSinkBuilder;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

/// Creates a sink writer based on the configuration
pub(crate) async fn create_sink_writer(
    batch_size: usize,
    read_timeout: Duration,
    primary_sink: SinkConfig,
    fallback_sink: Option<SinkConfig>,
    on_success_sink: Option<SinkConfig>,
    serving_store: Option<ServingStore>,
    cln_token: &CancellationToken,
) -> error::Result<SinkWriter> {
    let mut sink_writer_builder =
        append_primary_sink_client(batch_size, read_timeout, primary_sink, cln_token).await?;

    sink_writer_builder = if let Some(fb_sink) = fallback_sink {
        append_fallback_sink_client(cln_token, sink_writer_builder, fb_sink).await?
    } else {
        sink_writer_builder
    };

    sink_writer_builder = if let Some(os_sink) = on_success_sink {
        append_ons_sink_client(cln_token, sink_writer_builder, os_sink).await?
    } else {
        sink_writer_builder
    };

    if let Some(serving_store) = serving_store {
        sink_writer_builder = sink_writer_builder.serving_store(serving_store);
    }

    sink_writer_builder.build().await
}

async fn append_primary_sink_client(
    batch_size: usize,
    read_timeout: Duration,
    primary_sink: SinkConfig,
    cln_token: &CancellationToken,
) -> Result<SinkWriterBuilder, Error> {
    Ok(match primary_sink.sink_type.clone() {
        SinkType::Log(_) => SinkWriterBuilder::new(batch_size, read_timeout, SinkClientType::Log),
        SinkType::Blackhole(_) => {
            SinkWriterBuilder::new(batch_size, read_timeout, SinkClientType::Blackhole)
        }
        SinkType::Serve => SinkWriterBuilder::new(batch_size, read_timeout, SinkClientType::Serve),
        SinkType::UserDefined(ud_config) => {
            let sink_server_info =
                sdk_server_info(ud_config.server_info_path.clone().into(), cln_token.clone())
                    .await?;

            let metric_labels = metrics::sdk_info_labels(
                config::get_component_type().to_string(),
                config::get_vertex_name().to_string(),
                sink_server_info.language,
                sink_server_info.version,
                ContainerType::Sinker.to_string(),
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
            SinkWriterBuilder::new(
                batch_size,
                read_timeout,
                SinkClientType::UserDefined(sink_grpc_client.clone()),
            )
            .retry_config(primary_sink.retry_config.unwrap_or_default())
        }
        SinkType::Sqs(sqs_sink_config) => {
            let sqs_sink = SqsSinkBuilder::new(sqs_sink_config).build().await?;
            SinkWriterBuilder::new(
                batch_size,
                read_timeout,
                SinkClientType::Sqs(sqs_sink.clone()),
            )
        }
        SinkType::Kafka(sink_config) => {
            let sink_config = *sink_config;
            let kafka_sink = numaflow_kafka::sink::new_sink(sink_config)?;
            SinkWriterBuilder::new(batch_size, read_timeout, SinkClientType::Kafka(kafka_sink))
        }
        SinkType::Pulsar(pulsar_sink_config) => {
            let pulsar_sink = numaflow_pulsar::sink::new_sink(*pulsar_sink_config).await?;
            SinkWriterBuilder::new(
                batch_size,
                read_timeout,
                SinkClientType::Pulsar(Box::new(pulsar_sink)),
            )
        }
    })
}

async fn append_fallback_sink_client(
    cln_token: &CancellationToken,
    sink_writer_builder: SinkWriterBuilder,
    fb_sink: SinkConfig,
) -> Result<SinkWriterBuilder, Error> {
    Ok(match fb_sink.sink_type.clone() {
        SinkType::Log(_) => sink_writer_builder.fb_sink_client(SinkClientType::Log),
        SinkType::Serve => sink_writer_builder.fb_sink_client(SinkClientType::Serve),
        SinkType::Blackhole(_) => sink_writer_builder.fb_sink_client(SinkClientType::Blackhole),
        SinkType::UserDefined(ud_config) => {
            let fb_server_info =
                sdk_server_info(ud_config.server_info_path.clone().into(), cln_token.clone())
                    .await?;

            let metric_labels = metrics::sdk_info_labels(
                config::get_component_type().to_string(),
                config::get_vertex_name().to_string(),
                fb_server_info.language,
                fb_server_info.version,
                ContainerType::FbSinker.to_string(),
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

            sink_writer_builder
                .fb_sink_client(SinkClientType::UserDefined(sink_grpc_client.clone()))
        }
        SinkType::Sqs(sqs_sink_config) => {
            let sqs_sink = SqsSinkBuilder::new(sqs_sink_config).build().await?;
            sink_writer_builder.fb_sink_client(SinkClientType::Sqs(sqs_sink.clone()))
        }
        SinkType::Kafka(sink_config) => {
            let sink_config = *sink_config.clone();
            let kafka_sink = numaflow_kafka::sink::new_sink(sink_config)?;
            sink_writer_builder.fb_sink_client(SinkClientType::Kafka(kafka_sink))
        }
        SinkType::Pulsar(pulsar_sink_config) => {
            let pulsar_sink = numaflow_pulsar::sink::new_sink(*pulsar_sink_config).await?;
            sink_writer_builder.fb_sink_client(SinkClientType::Pulsar(Box::new(pulsar_sink)))
        }
    })
}

async fn append_ons_sink_client(
    cln_token: &CancellationToken,
    sink_writer_builder: SinkWriterBuilder,
    os_sink: SinkConfig,
) -> Result<SinkWriterBuilder, Error> {
    Ok(match os_sink.sink_type.clone() {
        SinkType::Log(_) => sink_writer_builder.on_success_sink_client(SinkClientType::Log),
        SinkType::Serve => sink_writer_builder.on_success_sink_client(SinkClientType::Serve),
        SinkType::Blackhole(_) => {
            sink_writer_builder.on_success_sink_client(SinkClientType::Blackhole)
        }
        SinkType::UserDefined(ud_config) => {
            let os_server_info =
                sdk_server_info(ud_config.server_info_path.clone().into(), cln_token.clone())
                    .await?;

            let metric_labels = metrics::sdk_info_labels(
                config::get_component_type().to_string(),
                config::get_vertex_name().to_string(),
                os_server_info.language,
                os_server_info.version,
                ContainerType::OnsSinker.to_string(),
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

            sink_writer_builder
                .on_success_sink_client(SinkClientType::UserDefined(sink_grpc_client.clone()))
        }
        SinkType::Sqs(sqs_sink_config) => {
            let sqs_sink = SqsSinkBuilder::new(sqs_sink_config).build().await?;
            sink_writer_builder.on_success_sink_client(SinkClientType::Sqs(sqs_sink.clone()))
        }
        SinkType::Kafka(sink_config) => {
            let sink_config = *sink_config.clone();
            let kafka_sink = numaflow_kafka::sink::new_sink(sink_config)?;
            sink_writer_builder.on_success_sink_client(SinkClientType::Kafka(kafka_sink))
        }
        SinkType::Pulsar(pulsar_sink_config) => {
            let pulsar_sink = numaflow_pulsar::sink::new_sink(*pulsar_sink_config).await?;
            sink_writer_builder
                .on_success_sink_client(SinkClientType::Pulsar(Box::new(pulsar_sink)))
        }
    })
}

/// Creates a transformer if it is configured
pub(crate) async fn create_transformer(
    batch_size: usize,
    graceful_timeout: Duration,
    transformer_config: Option<TransformerConfig>,
    tracker: Tracker,
    cln_token: CancellationToken,
) -> error::Result<Option<Transformer>> {
    if let Some(transformer_config) = transformer_config
        && let config::components::transformer::TransformerType::UserDefined(ud_transformer) =
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
            ContainerType::SourceTransformer.to_string(),
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
        return Ok(Some(
            Transformer::new(
                batch_size,
                transformer_config.concurrency,
                graceful_timeout,
                transformer_grpc_client.clone(),
                tracker,
            )
            .await?,
        ));
    }
    Ok(None)
}

pub(crate) async fn create_mapper(
    batch_size: usize,
    read_timeout: Duration,
    graceful_timeout: Duration,
    map_config: MapVtxConfig,
    tracker: Tracker,
    cln_token: CancellationToken,
) -> error::Result<MapHandle> {
    match map_config.map_type {
        MapType::UserDefined(mut config) => {
            let server_info =
                sdk_server_info(config.server_info_path.clone().into(), cln_token.clone()).await?;

            // add sdk info metric
            let metric_labels = metrics::sdk_info_labels(
                config::get_component_type().to_string(),
                config::get_vertex_name().to_string(),
                server_info.language.clone(),
                server_info.version.clone(),
                ContainerType::Mapper.to_string(),
            );
            metrics::global_metrics()
                .sdk_info
                .get_or_create(&metric_labels)
                .set(1);

            match server_info.get_protocol() {
                Protocol::TCP => {
                    // tcp is only used for multi proc mode in python
                    let endpoints = server_info.get_http_endpoints();

                    // Bug in tonic, https://github.com/hyperium/tonic/issues/2257 we will enable it
                    // once it's fixed.
                    if endpoints.len() > 1 {
                        return Err(Error::Mapper(
                            "Multi proc mode is not supported".to_string(),
                        ));
                    }

                    let channel = grpc::create_multi_rpc_channel(endpoints).await?;

                    let map_grpc_client = MapClient::new(channel)
                        .max_encoding_message_size(config.grpc_max_message_size)
                        .max_decoding_message_size(config.grpc_max_message_size);

                    Ok(MapHandle::new(
                        server_info.get_map_mode().unwrap_or(MapMode::Unary),
                        batch_size,
                        read_timeout,
                        graceful_timeout,
                        map_config.concurrency,
                        map_grpc_client.clone(),
                        tracker,
                    )
                    .await?)
                }
                Protocol::UDS => {
                    // based on the map mode that is set in the server info, we will override the socket path
                    // so that the clients can connect to the appropriate socket.
                    let config = match server_info.get_map_mode().unwrap_or(MapMode::Unary) {
                        MapMode::Unary => config,
                        MapMode::Batch => {
                            config.socket_path = DEFAULT_BATCH_MAP_SOCKET.into();
                            config
                        }
                        MapMode::Stream => {
                            config.socket_path = DEFAULT_STREAM_MAP_SOCKET.into();
                            config
                        }
                    };

                    let mut map_grpc_client = MapClient::new(
                        grpc::create_rpc_channel(config.socket_path.clone().into()).await?,
                    )
                    .max_encoding_message_size(config.grpc_max_message_size)
                    .max_decoding_message_size(config.grpc_max_message_size);

                    grpc::wait_until_mapper_ready(&cln_token, &mut map_grpc_client).await?;
                    Ok(MapHandle::new(
                        server_info.get_map_mode().unwrap_or(MapMode::Unary),
                        batch_size,
                        read_timeout,
                        graceful_timeout,
                        map_config.concurrency,
                        map_grpc_client.clone(),
                        tracker,
                    )
                    .await?)
                }
            }
        }
    }
}

/// Creates a source type with rate limiter based on the configuration
#[allow(clippy::too_many_arguments)]
pub async fn create_source<C: NumaflowTypeConfig>(
    batch_size: usize,
    read_timeout: Duration,
    source_config: &SourceConfig,
    tracker: Tracker,
    transformer: Option<Transformer>,
    watermark_handle: Option<SourceWatermarkHandle>,
    cln_token: CancellationToken,
    rate_limiter: Option<C::RateLimiter>,
) -> error::Result<Source<C>> {
    match &source_config.source_type {
        SourceType::Generator(generator_config) => {
            let (generator, generator_ack, generator_lag) =
                new_generator(generator_config.clone(), batch_size, cln_token.clone())?;
            Ok(Source::new(
                batch_size,
                source::SourceType::Generator(generator, generator_ack, generator_lag),
                tracker,
                source_config.read_ahead,
                transformer,
                watermark_handle,
                rate_limiter,
            )
            .await)
        }
        SourceType::Pulsar(pulsar_config) => {
            let pulsar = new_pulsar_source(
                pulsar_config.clone(),
                batch_size,
                read_timeout,
                *get_vertex_replica(),
                cln_token.clone(),
            )
            .await?;
            Ok(crate::source::Source::new(
                batch_size,
                source::SourceType::Pulsar(pulsar),
                tracker,
                source_config.read_ahead,
                transformer,
                watermark_handle,
                rate_limiter,
            )
            .await)
        }
        SourceType::Sqs(sqs_source_config) => {
            let sqs = new_sqs_source(
                sqs_source_config.clone(),
                batch_size,
                read_timeout,
                *get_vertex_replica(),
                cln_token.clone(),
            )
            .await?;
            Ok(Source::new(
                batch_size,
                source::SourceType::Sqs(sqs),
                tracker,
                source_config.read_ahead,
                transformer,
                watermark_handle,
                rate_limiter,
            )
            .await)
        }
        SourceType::Jetstream(jetstream_config) => {
            let jetstream = new_jetstream_source(
                jetstream_config.clone(),
                batch_size,
                read_timeout,
                cln_token.clone(),
            )
            .await?;
            Ok(Source::new(
                batch_size,
                source::SourceType::Jetstream(jetstream),
                tracker,
                source_config.read_ahead,
                transformer,
                watermark_handle,
                rate_limiter,
            )
            .await)
        }
        SourceType::Nats(nats_config) => {
            let nats = new_nats_source(
                nats_config.clone(),
                batch_size,
                read_timeout,
                cln_token.clone(),
            )
            .await?;
            Ok(Source::new(
                batch_size,
                source::SourceType::Nats(nats),
                tracker,
                source_config.read_ahead,
                transformer,
                watermark_handle,
                rate_limiter,
            )
            .await)
        }
        SourceType::Kafka(kafka_config) => {
            let config = *kafka_config.clone();
            let kafka =
                new_kafka_source(config, batch_size, read_timeout, cln_token.clone()).await?;
            Ok(Source::new(
                batch_size,
                source::SourceType::Kafka(kafka),
                tracker,
                source_config.read_ahead,
                transformer,
                watermark_handle,
                rate_limiter,
            )
            .await)
        }
        SourceType::Http(http_source_config) => {
            let http_source =
                numaflow_http::HttpSourceHandle::new(http_source_config.clone(), cln_token.clone())
                    .await;
            Ok(Source::new(
                batch_size,
                source::SourceType::Http(CoreHttpSource::new(batch_size, http_source)),
                tracker,
                source_config.read_ahead,
                transformer,
                watermark_handle,
                rate_limiter,
            )
            .await)
        }

        SourceType::UserDefined(user_defined_config) => {
            let (source_client, server_info) =
                create_source_client(user_defined_config, cln_token.clone()).await?;
            // Check if the SDK version supports nack functionality
            let supports_nack = supports_nack(&server_info.version, &server_info.language);
            let (ud_read, ud_ack, ud_lag) = new_source(
                source_client,
                batch_size,
                read_timeout,
                cln_token.clone(),
                supports_nack,
            )
            .await?;
            Ok(Source::new(
                batch_size,
                source::SourceType::UserDefinedSource(Box::new(ud_read), Box::new(ud_ack), ud_lag),
                tracker,
                source_config.read_ahead,
                transformer,
                watermark_handle,
                rate_limiter,
            )
            .await)
        }
    }
}

/// Creates a source client from user-defined config
/// Returns both the client and server info for version-aware functionality
async fn create_source_client(
    user_defined_config: &config::components::source::UserDefinedConfig,
    cln_token: CancellationToken,
) -> error::Result<(SourceClient<Channel>, ServerInfo)> {
    let server_info = sdk_server_info(
        user_defined_config.server_info_path.clone().into(),
        cln_token.clone(),
    )
    .await?;

    let metric_labels = metrics::sdk_info_labels(
        config::get_component_type().to_string(),
        config::get_vertex_name().to_string(),
        server_info.language.clone(),
        server_info.version.clone(),
        ContainerType::Sourcer.to_string(),
    );
    metrics::global_metrics()
        .sdk_info
        .get_or_create(&metric_labels)
        .set(1);

    let channel =
        create_rpc_channel(PathBuf::from(user_defined_config.socket_path.clone())).await?;
    let mut client = SourceClient::new(channel);
    wait_until_source_ready(&cln_token, &mut client).await?;
    Ok((client, server_info))
}

/// Creates a user-defined aligned reducer client
pub(crate) async fn create_aligned_reducer(
    reducer_config: config::components::reduce::AlignedReducerConfig,
    cln_token: CancellationToken,
) -> crate::Result<UserDefinedAlignedReduce> {
    let server_info = sdk_server_info(
        reducer_config.user_defined_config.server_info_path.into(),
        cln_token.clone(),
    )
    .await?;

    let metric_labels = metrics::sdk_info_labels(
        config::get_component_type().to_string(),
        config::get_vertex_name().to_string(),
        server_info.language,
        server_info.version,
        ContainerType::Reducer.to_string(),
    );
    metrics::global_metrics()
        .sdk_info
        .get_or_create(&metric_labels)
        .set(1);

    // Create gRPC channel
    let channel = create_rpc_channel(reducer_config.user_defined_config.socket_path.into()).await?;

    // Create client
    let client = UserDefinedAlignedReduce::new(
        ReduceClient::new(channel)
            .max_encoding_message_size(reducer_config.user_defined_config.grpc_max_message_size)
            .max_decoding_message_size(reducer_config.user_defined_config.grpc_max_message_size),
    )
    .await;

    Ok(client)
}

pub(crate) async fn create_unaligned_reducer(
    reducer_config: config::components::reduce::UnalignedReducerConfig,
    cln_token: CancellationToken,
) -> crate::Result<UserDefinedUnalignedReduce> {
    let server_info = sdk_server_info(
        reducer_config.user_defined_config.server_info_path.into(),
        cln_token.clone(),
    )
    .await?;

    let metric_labels = metrics::sdk_info_labels(
        config::get_component_type().to_string(),
        config::get_vertex_name().to_string(),
        server_info.language,
        server_info.version,
        ContainerType::Reducer.to_string(),
    );
    metrics::global_metrics()
        .sdk_info
        .get_or_create(&metric_labels)
        .set(1);

    // Create gRPC channel
    let channel =
        grpc::create_rpc_channel(reducer_config.user_defined_config.socket_path.into()).await?;

    match reducer_config.window_config.window_type {
        UnalignedWindowType::Accumulator(_) => Ok(UserDefinedUnalignedReduce::Accumulator(
            UserDefinedAccumulator::new(
                AccumulatorClient::new(channel)
                    .max_encoding_message_size(
                        reducer_config.user_defined_config.grpc_max_message_size,
                    )
                    .max_decoding_message_size(
                        reducer_config.user_defined_config.grpc_max_message_size,
                    ),
            )
            .await,
        )),
        UnalignedWindowType::Session(_) => Ok(UserDefinedUnalignedReduce::Session(
            UserDefinedSessionReduce::new(
                SessionReduceClient::new(channel)
                    .max_encoding_message_size(
                        reducer_config.user_defined_config.grpc_max_message_size,
                    )
                    .max_decoding_message_size(
                        reducer_config.user_defined_config.grpc_max_message_size,
                    ),
            )
            .await,
        )),
    }
}

pub(crate) fn parse_nats_auth(
    auth: Option<Box<NatsAuth>>,
) -> Result<Option<numaflow_nats::NatsAuth>, Error> {
    match auth {
        Some(auth) => {
            if let Some(basic_auth) = auth.basic {
                let user_secret_selector = &basic_auth.user.ok_or_else(|| {
                    Error::Config("Username can not be empty for basic auth".into())
                })?;
                let username = get_secret_from_volume(
                    &user_secret_selector.name,
                    &user_secret_selector.key,
                )
                .map_err(|e| Error::Config(format!("Failed to get username secret: {e:?}")))?;

                let password_secret_selector = &basic_auth.password.ok_or_else(|| {
                    Error::Config("Password can not be empty for basic auth".into())
                })?;
                let password = get_secret_from_volume(
                    &password_secret_selector.name,
                    &password_secret_selector.key,
                )
                .map_err(|e| Error::Config(format!("Failed to get password secret: {e:?}")))?;

                Ok(Some(numaflow_nats::NatsAuth::Basic { username, password }))
            } else if let Some(nkey_auth) = auth.nkey {
                let nkey = get_secret_from_volume(&nkey_auth.name, &nkey_auth.key)
                    .map_err(|e| Error::Config(format!("Failed to get nkey secret: {e:?}")))?;
                Ok(Some(numaflow_nats::NatsAuth::NKey(nkey)))
            } else if let Some(token_auth) = auth.token {
                let token = get_secret_from_volume(&token_auth.name, &token_auth.key)
                    .map_err(|e| Error::Config(format!("Failed to get token secret: {e:?}")))?;
                Ok(Some(numaflow_nats::NatsAuth::Token(token)))
            } else {
                Err(Error::Config(
                    "Authentication is specified, but auth setting is empty".into(),
                ))
            }
        }
        None => Ok(None),
    }
}

pub(crate) fn parse_tls_config(tls_config: Option<Box<Tls>>) -> Result<Option<TlsConfig>, Error> {
    let Some(tls_config) = tls_config else {
        return Ok(None);
    };

    let tls_skip_verify = tls_config.insecure_skip_verify.unwrap_or(false);
    if tls_skip_verify {
        return Ok(Some(TlsConfig {
            insecure_skip_verify: true,
            ca_cert: None,
            client_auth: None,
        }));
    }

    let ca_cert = tls_config
        .ca_cert_secret
        .map(|ca_cert_secret| {
            get_secret_from_volume(&ca_cert_secret.name, &ca_cert_secret.key)
                .map_err(|e| Error::Config(format!("Failed to get CA cert secret: {e:?}")))
        })
        .transpose()?;

    let client_auth = match tls_config.cert_secret {
        Some(client_cert_secret) => {
            let client_cert =
                get_secret_from_volume(&client_cert_secret.name, &client_cert_secret.key).map_err(
                    |e| Error::Config(format!("Failed to get client cert secret: {e:?}")),
                )?;

            let Some(private_key_secret) = tls_config.key_secret else {
                return Err(Error::Config(
                    "Client cert is specified for TLS authentication, but private key is not specified".into(),
                ));
            };

            let client_cert_private_key =
                get_secret_from_volume(&private_key_secret.name, &private_key_secret.key).map_err(
                    |e| {
                        Error::Config(format!(
                            "Failed to get client cert private key secret: {e:?}"
                        ))
                    },
                )?;

            Some(TlsClientAuthCerts {
                client_cert,
                client_cert_private_key,
            })
        }
        None => None,
    };

    Ok(Some(TlsConfig {
        insecure_skip_verify: false,
        ca_cert,
        client_auth,
    }))
}

#[cfg(test)]
const SECRET_BASE_PATH: &str = "/tmp/numaflow";

#[cfg(not(test))]
const SECRET_BASE_PATH: &str = "/var/numaflow/secrets";

// Retrieve value from mounted secret volume
// "/var/numaflow/secrets/${secretRef.name}/${secretRef.key}" is expected to be the file path
pub(crate) fn get_secret_from_volume(name: &str, key: &str) -> Result<String, String> {
    let path = format!("{SECRET_BASE_PATH}/{name}/{key}");
    let val = std::fs::read_to_string(path.clone())
        .map_err(|e| format!("Reading secret from file {path}: {e:?}"))?;
    Ok(val.trim().into())
}

/// Creates an ISBWatermarkHandle if watermark is enabled in the configuration
pub async fn create_edge_watermark_handle(
    config: &PipelineConfig,
    js_context: &Context,
    cln_token: &CancellationToken,
    window_manager: Option<WindowManager>,
    tracker: Tracker,
    from_partitions: Vec<u16>,
) -> error::Result<Option<ISBWatermarkHandle>> {
    match &config.watermark_config {
        Some(WatermarkConfig::Edge(edge_config)) => {
            let handle = ISBWatermarkHandle::new(
                config.vertex_name,
                config.replica,
                config.vertex_type,
                2 * config.read_timeout,
                js_context.clone(),
                edge_config,
                &config.to_vertex_config,
                cln_token.clone(),
                window_manager,
                tracker,
                from_partitions,
            )
            .await?;
            Ok(Some(handle))
        }
        _ => Ok(None),
    }
}

/// Creates JetStreamWriters for all streams in the to_vertex_config
pub(crate) async fn create_js_writers(
    to_vertex_config: &[ToVertexConfig],
    js_context: Context,
    isb_config: Option<&crate::config::pipeline::isb::ISBConfig>,
    cln_token: CancellationToken,
) -> crate::Result<HashMap<&'static str, JetStreamWriter>> {
    let mut writers = HashMap::new();
    for vertex_config in to_vertex_config {
        for stream in &vertex_config.writer_config.streams {
            let writer = JetStreamWriter::new(
                stream.clone(),
                js_context.clone(),
                vertex_config.writer_config.clone(),
                isb_config.map(|c| c.compression.compress_type),
                cln_token.clone(),
            )
            .await?;
            writers.insert(stream.name, writer);
        }
    }
    Ok(writers)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use numaflow::shared::ServerExtras;
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

        async fn nack(&self, _offsets: Vec<Offset>) {}

        async fn pending(&self) -> Option<usize> {
            Some(0)
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
        async fn sink(&self, _input: mpsc::Receiver<sink::SinkRequest>) -> Vec<sink::Response> {
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
