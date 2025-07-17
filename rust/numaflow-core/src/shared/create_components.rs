use std::time::Duration;

use crate::config::components::reduce::UnalignedWindowType;
use crate::config::components::sink::{SinkConfig, SinkType};
use crate::config::components::source::{SourceConfig, SourceType};
use crate::config::components::transformer::TransformerConfig;
use crate::config::get_vertex_replica;
use crate::config::pipeline::map::{MapMode, MapType, MapVtxConfig};
use crate::config::pipeline::watermark::WatermarkConfig;
use crate::config::pipeline::{
    DEFAULT_BATCH_MAP_SOCKET, DEFAULT_STREAM_MAP_SOCKET, PipelineConfig,
};
use crate::error::Error;
use crate::mapper::map::MapHandle;
use crate::reduce::reducer::WindowManager;
use crate::reduce::reducer::aligned::user_defined::UserDefinedAlignedReduce;
use crate::reduce::reducer::unaligned::user_defined::UserDefinedUnalignedReduce;
use crate::reduce::reducer::unaligned::user_defined::accumulator::UserDefinedAccumulator;
use crate::reduce::reducer::unaligned::user_defined::session::UserDefinedSessionReduce;
use crate::shared::grpc;
use crate::sink::serve::ServingStore;
use crate::sink::{SinkClientType, SinkWriter, SinkWriterBuilder};
use crate::source::Source;
use crate::source::generator::new_generator;
use crate::source::http::CoreHttpSource;
use crate::source::jetstream::new_jetstream_source;
use crate::source::kafka::new_kafka_source;
use crate::source::nats::new_nats_source;
use crate::source::pulsar::new_pulsar_source;
use crate::source::sqs::new_sqs_source;
use crate::source::user_defined::new_source;
use crate::tracker::TrackerHandle;
use crate::transformer::Transformer;
use crate::watermark::isb::ISBWatermarkHandle;
use crate::watermark::source::SourceWatermarkHandle;
use crate::{config, error, metrics, source};
use async_nats::jetstream::Context;
use numaflow_pb::clients::accumulator::accumulator_client::AccumulatorClient;
use numaflow_pb::clients::map::map_client::MapClient;
use numaflow_pb::clients::reduce::reduce_client::ReduceClient;
use numaflow_pb::clients::sessionreduce::session_reduce_client::SessionReduceClient;
use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use numaflow_shared::server_info::{ContainerType, Protocol, sdk_server_info};
use numaflow_sqs::sink::SqsSinkBuilder;
use tokio_util::sync::CancellationToken;

/// Creates a sink writer based on the configuration
pub(crate) async fn create_sink_writer(
    batch_size: usize,
    read_timeout: Duration,
    primary_sink: SinkConfig,
    fallback_sink: Option<SinkConfig>,
    tracker_handle: TrackerHandle,
    serving_store: Option<ServingStore>,
    cln_token: &CancellationToken,
) -> error::Result<SinkWriter> {
    let mut sink_writer_builder = match primary_sink.sink_type.clone() {
        SinkType::Log(_) => SinkWriterBuilder::new(
            batch_size,
            read_timeout,
            SinkClientType::Log,
            tracker_handle,
        ),
        SinkType::Blackhole(_) => SinkWriterBuilder::new(
            batch_size,
            read_timeout,
            SinkClientType::Blackhole,
            tracker_handle,
        ),
        SinkType::Serve => SinkWriterBuilder::new(
            batch_size,
            read_timeout,
            SinkClientType::Serve,
            tracker_handle,
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
                tracker_handle,
            )
            .retry_config(primary_sink.retry_config.unwrap_or_default())
        }
        SinkType::Sqs(sqs_sink_config) => {
            let sqs_sink = SqsSinkBuilder::new(sqs_sink_config).build().await?;
            SinkWriterBuilder::new(
                batch_size,
                read_timeout,
                SinkClientType::Sqs(sqs_sink.clone()),
                tracker_handle,
            )
        }
        SinkType::Kafka(sink_config) => {
            let sink_config = *sink_config;
            let kafka_sink = numaflow_kafka::sink::new_sink(sink_config)?;
            SinkWriterBuilder::new(
                batch_size,
                read_timeout,
                SinkClientType::Kafka(kafka_sink),
                tracker_handle,
            )
        }
        SinkType::Pulsar(pulsar_sink_config) => {
            let pulsar_sink = numaflow_pulsar::sink::new_sink(*pulsar_sink_config).await?;
            SinkWriterBuilder::new(
                batch_size,
                read_timeout,
                SinkClientType::Pulsar(Box::new(pulsar_sink)),
                tracker_handle,
            )
        }
    };

    if let Some(fb_sink) = fallback_sink {
        return match fb_sink.sink_type.clone() {
            SinkType::Log(_) => Ok(sink_writer_builder
                .fb_sink_client(SinkClientType::Log)
                .build()
                .await?),
            SinkType::Serve => Ok(sink_writer_builder
                .fb_sink_client(SinkClientType::Serve)
                .build()
                .await?),
            SinkType::Blackhole(_) => Ok(sink_writer_builder
                .fb_sink_client(SinkClientType::Blackhole)
                .build()
                .await?),
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

                Ok(sink_writer_builder
                    .fb_sink_client(SinkClientType::UserDefined(sink_grpc_client.clone()))
                    .build()
                    .await?)
            }
            SinkType::Sqs(sqs_sink_config) => {
                let sqs_sink = SqsSinkBuilder::new(sqs_sink_config).build().await?;
                Ok(sink_writer_builder
                    .fb_sink_client(SinkClientType::Sqs(sqs_sink.clone()))
                    .build()
                    .await?)
            }
            SinkType::Kafka(sink_config) => {
                let sink_config = *sink_config.clone();
                let kafka_sink = numaflow_kafka::sink::new_sink(sink_config)?;
                Ok(sink_writer_builder
                    .fb_sink_client(SinkClientType::Kafka(kafka_sink))
                    .build()
                    .await?)
            }
            SinkType::Pulsar(pulsar_sink_config) => {
                let pulsar_sink = numaflow_pulsar::sink::new_sink(*pulsar_sink_config).await?;
                Ok(sink_writer_builder
                    .fb_sink_client(SinkClientType::Pulsar(Box::new(pulsar_sink)))
                    .build()
                    .await?)
            }
        };
    }

    if let Some(serving_store) = serving_store {
        sink_writer_builder = sink_writer_builder.serving_store(serving_store);
    }

    sink_writer_builder.build().await
}

/// Creates a transformer if it is configured
pub(crate) async fn create_transformer(
    batch_size: usize,
    graceful_timeout: Duration,
    transformer_config: Option<TransformerConfig>,
    tracker_handle: TrackerHandle,
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
                tracker_handle,
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
    tracker_handle: TrackerHandle,
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
                        tracker_handle,
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
                        tracker_handle,
                    )
                    .await?)
                }
            }
        }
    }
}

/// Creates a source type based on the configuration
#[allow(clippy::too_many_arguments)]
pub async fn create_source(
    batch_size: usize,
    read_timeout: Duration,
    source_config: &SourceConfig,
    tracker_handle: TrackerHandle,
    transformer: Option<Transformer>,
    watermark_handle: Option<SourceWatermarkHandle>,
    cln_token: CancellationToken,
) -> error::Result<Source> {
    match &source_config.source_type {
        SourceType::Generator(generator_config) => {
            let (generator_read, generator_ack, generator_lag) =
                new_generator(generator_config.clone(), batch_size)?;
            Ok(Source::new(
                batch_size,
                source::SourceType::Generator(generator_read, generator_ack, generator_lag),
                tracker_handle,
                source_config.read_ahead,
                transformer,
                watermark_handle,
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
            Ok(Source::new(
                batch_size,
                source::SourceType::UserDefinedSource(Box::new(ud_read), Box::new(ud_ack), ud_lag),
                tracker_handle,
                source_config.read_ahead,
                transformer,
                watermark_handle,
            ))
        }
        SourceType::Pulsar(pulsar_config) => {
            let pulsar = new_pulsar_source(
                pulsar_config.clone(),
                batch_size,
                read_timeout,
                *get_vertex_replica(),
            )
            .await?;
            Ok(Source::new(
                batch_size,
                source::SourceType::Pulsar(pulsar),
                tracker_handle,
                source_config.read_ahead,
                transformer,
                watermark_handle,
            ))
        }
        SourceType::Sqs(sqs_source_config) => {
            let sqs = new_sqs_source(
                sqs_source_config.clone(),
                batch_size,
                read_timeout,
                *get_vertex_replica(),
            )
            .await?;
            Ok(Source::new(
                batch_size,
                source::SourceType::Sqs(sqs),
                tracker_handle,
                source_config.read_ahead,
                transformer,
                watermark_handle,
            ))
        }
        SourceType::Jetstream(jetstream_config) => {
            let jetstream =
                new_jetstream_source(jetstream_config.clone(), batch_size, read_timeout).await?;
            Ok(Source::new(
                batch_size,
                source::SourceType::Jetstream(jetstream),
                tracker_handle,
                source_config.read_ahead,
                transformer,
                watermark_handle,
            ))
        }
        SourceType::Nats(nats_config) => {
            let nats = new_nats_source(nats_config.clone(), batch_size, read_timeout).await?;
            Ok(Source::new(
                batch_size,
                source::SourceType::Nats(nats),
                tracker_handle,
                source_config.read_ahead,
                transformer,
                watermark_handle,
            ))
        }
        SourceType::Kafka(kafka_config) => {
            let config = *kafka_config.clone();
            let kafka = new_kafka_source(config, batch_size, read_timeout).await?;
            Ok(Source::new(
                batch_size,
                source::SourceType::Kafka(kafka),
                tracker_handle,
                source_config.read_ahead,
                transformer,
                watermark_handle,
            ))
        }
        SourceType::Http(http_source_config) => {
            let http_source =
                numaflow_http::HttpSourceHandle::new(http_source_config.clone()).await;
            Ok(Source::new(
                batch_size,
                source::SourceType::Http(CoreHttpSource::new(batch_size, http_source)),
                tracker_handle,
                source_config.read_ahead,
                transformer,
                watermark_handle,
            ))
        }
    }
}

/// Creates a user-defined aligned reducer client
pub(crate) async fn create_aligned_reducer(
    reducer_config: config::components::reduce::AlignedReducerConfig,
) -> crate::Result<UserDefinedAlignedReduce> {
    // Create gRPC channel
    let channel =
        grpc::create_rpc_channel(reducer_config.user_defined_config.socket_path.into()).await?;

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
) -> crate::Result<UserDefinedUnalignedReduce> {
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
) -> error::Result<Option<ISBWatermarkHandle>> {
    match &config.watermark_config {
        Some(WatermarkConfig::Edge(edge_config)) => {
            let handle = ISBWatermarkHandle::new(
                config.vertex_name,
                config.replica,
                config.vertex_type,
                config.read_timeout,
                js_context.clone(),
                edge_config,
                &config.to_vertex_config,
                cln_token.clone(),
                window_manager,
            )
            .await?;
            Ok(Some(handle))
        }
        _ => Ok(None),
    }
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
