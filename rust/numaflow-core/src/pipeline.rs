use std::time::Duration;

use async_nats::jetstream::Context;
use async_nats::{jetstream, ConnectOptions};
use futures::future::try_join_all;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::pipeline;
use crate::config::pipeline::{PipelineConfig, SinkVtxConfig, SourceVtxConfig};
use crate::metrics::{PipelineContainerState, UserDefinedContainerState};
use crate::pipeline::forwarder::source_forwarder;
use crate::pipeline::isb::jetstream::reader::JetstreamReader;
use crate::pipeline::isb::jetstream::writer::JetstreamWriter;
use crate::pipeline::pipeline::isb::BufferReaderConfig;
use crate::shared::create_components;
use crate::shared::create_components::create_sink_writer;
use crate::shared::metrics::start_metrics_server;
use crate::tracker::TrackerHandle;
use crate::{error, Result};

mod forwarder;
mod isb;

/// Starts the appropriate forwarder based on the pipeline configuration.
pub(crate) async fn start_forwarder(
    cln_token: CancellationToken,
    config: PipelineConfig,
) -> Result<()> {
    match &config.vertex_config {
        pipeline::VertexType::Source(source) => {
            info!("Starting source forwarder");
            start_source_forwarder(cln_token, config.clone(), source.clone()).await?;
        }
        pipeline::VertexType::Sink(sink) => {
            info!("Starting sink forwarder");
            start_sink_forwarder(cln_token, config.clone(), sink.clone()).await?;
        }
    }
    Ok(())
}

async fn start_source_forwarder(
    cln_token: CancellationToken,
    config: PipelineConfig,
    source_config: SourceVtxConfig,
) -> Result<()> {
    let tracker_handle = TrackerHandle::new();
    let js_context = create_js_context(config.js_client_config.clone()).await?;

    let buffer_writer = create_buffer_writer(
        &config,
        js_context.clone(),
        tracker_handle.clone(),
        cln_token.clone(),
    )
    .await;

    let (source, source_grpc_client) = create_components::create_source(
        config.batch_size,
        config.read_timeout,
        &source_config.source_config,
        tracker_handle.clone(),
        cln_token.clone(),
    )
    .await?;
    let (transformer, transformer_grpc_client) = create_components::create_transformer(
        config.batch_size,
        source_config.transformer_config.clone(),
        tracker_handle,
        cln_token.clone(),
    )
    .await?;

    start_metrics_server(
        config.metrics_config.clone(),
        UserDefinedContainerState::Pipeline(PipelineContainerState::Source((
            source_grpc_client.clone(),
            transformer_grpc_client.clone(),
        ))),
    )
    .await;

    let forwarder =
        source_forwarder::SourceForwarderBuilder::new(source, buffer_writer, cln_token.clone());

    let forwarder = if let Some(transformer) = transformer {
        forwarder.with_transformer(transformer).build()
    } else {
        forwarder.build()
    };

    forwarder.start().await?;
    Ok(())
}

async fn start_sink_forwarder(
    cln_token: CancellationToken,
    config: PipelineConfig,
    sink: SinkVtxConfig,
) -> Result<()> {
    let js_context = create_js_context(config.js_client_config.clone()).await?;

    // Only the reader config of the first "from" vertex is needed, as all "from" vertices currently write
    // to a common buffer, in the case of a join.
    let reader_config = &config
        .from_vertex_config
        .first()
        .ok_or_else(|| error::Error::Config("No from vertex config found".to_string()))?
        .reader_config;

    // Create sink writers and buffer readers for each stream
    let mut sink_writers = vec![];
    let mut buffer_readers = vec![];
    for stream in reader_config.streams.clone() {
        let tracker_handle = TrackerHandle::new();

        let buffer_reader = create_buffer_reader(
            stream,
            reader_config.clone(),
            js_context.clone(),
            tracker_handle.clone(),
        )
        .await?;
        buffer_readers.push(buffer_reader);

        let (sink_writer, sink_grpc_client, fb_sink_grpc_client) = create_sink_writer(
            config.batch_size,
            config.read_timeout,
            sink.sink_config.clone(),
            sink.fb_sink_config.clone(),
            tracker_handle,
            &cln_token,
        )
        .await?;
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
    for (buffer_reader, (sink_writer, _, _)) in buffer_readers.into_iter().zip(sink_writers) {
        info!(%buffer_reader, "Starting forwarder for buffer reader");
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
    info!("All forwarders have stopped successfully");
    Ok(())
}

async fn create_buffer_writer(
    config: &PipelineConfig,
    js_context: Context,
    tracker_handle: TrackerHandle,
    cln_token: CancellationToken,
) -> JetstreamWriter {
    JetstreamWriter::new(
        config.to_vertex_config.clone(),
        js_context,
        config.paf_concurrency,
        tracker_handle,
        cln_token,
    )
}

async fn create_buffer_reader(
    stream: (&'static str, u16),
    reader_config: BufferReaderConfig,
    js_context: Context,
    tracker_handle: TrackerHandle,
) -> Result<JetstreamReader> {
    JetstreamReader::new(
        stream.0,
        stream.1,
        js_context,
        reader_config,
        tracker_handle,
    )
    .await
}

/// Creates a jetstream context based on the provided configuration
async fn create_js_context(config: pipeline::isb::jetstream::ClientConfig) -> Result<Context> {
    // TODO: make these configurable. today this is hardcoded on Golang code too.
    let mut opts = ConnectOptions::new()
        .max_reconnects(None) // -1 for unlimited reconnects
        .ping_interval(Duration::from_secs(3))
        .max_reconnects(None)
        .ping_interval(Duration::from_secs(3))
        .retry_on_initial_connect();

    if let (Some(user), Some(password)) = (config.user, config.password) {
        opts = opts.user_and_password(user, password);
    }

    let js_client = async_nats::connect_with_options(&config.url, opts)
        .await
        .map_err(|e| error::Error::Connection(e.to_string()))?;

    Ok(jetstream::new(js_client))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use async_nats::jetstream;
    use async_nats::jetstream::{consumer, stream};
    use tokio_stream::StreamExt;

    use super::*;
    use crate::config::components::metrics::MetricsConfig;
    use crate::config::components::sink::{BlackholeConfig, SinkConfig, SinkType};
    use crate::config::components::source::GeneratorConfig;
    use crate::config::components::source::SourceConfig;
    use crate::config::components::source::SourceType;
    use crate::config::pipeline::PipelineConfig;
    use crate::pipeline::pipeline::isb;
    use crate::pipeline::pipeline::isb::{BufferReaderConfig, BufferWriterConfig};
    use crate::pipeline::pipeline::VertexType;
    use crate::pipeline::pipeline::{FromVertexConfig, ToVertexConfig};
    use crate::pipeline::pipeline::{SinkVtxConfig, SourceVtxConfig};
    use crate::pipeline::tests::isb::BufferFullStrategy::RetryUntilSuccess;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_forwarder_for_source_vertex() {
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
            // Delete stream if it exists
            let _ = context.delete_stream(stream_name).await;
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
            consumers.push((stream_name.to_string(), c));
        }

        let pipeline_config = PipelineConfig {
            pipeline_name: "simple-pipeline".to_string(),
            vertex_name: "in".to_string(),
            replica: 0,
            batch_size: 1000,
            paf_concurrency: 30000,
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
                    usage_limit: 0.8,
                    buffer_full_strategy: RetryUntilSuccess,
                },
                conditions: None,
            }],
            vertex_config: VertexType::Source(SourceVtxConfig {
                source_config: SourceConfig {
                    read_ahead: false,
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
                lookback_window_in_secs: 120,
            },
        };

        let cancellation_token = CancellationToken::new();
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
        forwarder_task.await.unwrap();

        for (stream_name, stream_consumer) in consumers {
            let messages: Vec<jetstream::Message> = stream_consumer
                .batch()
                .max_messages(10)
                .expires(Duration::from_millis(50))
                .messages()
                .await
                .unwrap()
                .map(|msg| msg.unwrap())
                .collect()
                .await;
            assert!(
                !messages.is_empty(),
                "Stream {} is expected to have messages",
                stream_name
            );
        }

        // Delete all streams created in this test
        for stream_name in streams {
            context.delete_stream(stream_name).await.unwrap();
        }
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_forwarder_for_sink_vertex() {
        // Unique names for the streams we use in this test
        let streams = vec![
            "default-test-forwarder-for-sink-vertex-out-0",
            "default-test-forwarder-for-sink-vertex-out-1",
            "default-test-forwarder-for-sink-vertex-out-2",
            "default-test-forwarder-for-sink-vertex-out-3",
            "default-test-forwarder-for-sink-vertex-out-4",
        ];

        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);

        const MESSAGE_COUNT: usize = 10;
        let mut consumers = vec![];
        // Create streams to which the generator source vertex we create later will forward
        // messages to. The consumers created for the corresponding streams will be used to ensure
        // that messages were actually written to the streams.
        for stream_name in &streams {
            let stream_name = *stream_name;
            // Delete stream if it exists
            let _ = context.delete_stream(stream_name).await;
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

            // Publish some messages into the stream
            use chrono::{TimeZone, Utc};

            use crate::message::{Message, MessageID, Offset, StringOffset};
            let message = Message {
                keys: Arc::from(vec!["key1".to_string()]),
                tags: None,
                value: vec![1, 2, 3].into(),
                offset: Some(Offset::String(StringOffset::new("123".to_string(), 0))),
                event_time: Utc.timestamp_opt(1627846261, 0).unwrap(),
                id: MessageID {
                    vertex_name: "vertex".to_string().into(),
                    offset: "123".to_string().into(),
                    index: 0,
                },
                headers: HashMap::new(),
            };
            let message: bytes::BytesMut = message.try_into().unwrap();

            for _ in 0..MESSAGE_COUNT {
                context
                    .publish(stream_name.to_string(), message.clone().into())
                    .await
                    .unwrap()
                    .await
                    .unwrap();
            }

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
            consumers.push((stream_name.to_string(), c));
        }

        let pipeline_config = PipelineConfig {
            pipeline_name: "simple-pipeline".to_string(),
            vertex_name: "in".to_string(),
            replica: 0,
            batch_size: 1000,
            paf_concurrency: 1000,
            read_timeout: Duration::from_secs(1),
            js_client_config: isb::jetstream::ClientConfig {
                url: "localhost:4222".to_string(),
                user: None,
                password: None,
            },
            to_vertex_config: vec![],
            from_vertex_config: vec![FromVertexConfig {
                name: "in".to_string(),
                reader_config: BufferReaderConfig {
                    partitions: 5,
                    streams: streams
                        .iter()
                        .enumerate()
                        .map(|(i, key)| (*key, i as u16))
                        .collect(),
                    wip_ack_interval: Duration::from_secs(1),
                },
                partitions: 0,
            }],
            vertex_config: VertexType::Sink(SinkVtxConfig {
                sink_config: SinkConfig {
                    sink_type: SinkType::Blackhole(BlackholeConfig::default()),
                    retry_config: None,
                },
                fb_sink_config: None,
            }),
            metrics_config: MetricsConfig {
                metrics_server_listen_port: 2469,
                lag_check_interval_in_secs: 5,
                lag_refresh_interval_in_secs: 3,
                lookback_window_in_secs: 120,
            },
        };

        let cancellation_token = CancellationToken::new();
        let forwarder_task = tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            async move {
                start_forwarder(cancellation_token, pipeline_config)
                    .await
                    .unwrap();
            }
        });

        // Wait for a few messages to be forwarded
        tokio::time::sleep(Duration::from_secs(3)).await;
        cancellation_token.cancel();
        // token cancellation is not aborting the forwarder since we fetch messages from jetstream
        // as a stream of messages (not using `consumer.batch()`).
        // See `JetstreamReader::start` method in src/pipeline/isb/jetstream/reader.rs
        //forwarder_task.await.unwrap();
        forwarder_task.abort();

        for (stream_name, mut stream_consumer) in consumers {
            let stream_info = stream_consumer.info().await.unwrap();
            assert_eq!(
                stream_info.delivered.stream_sequence, MESSAGE_COUNT as u64,
                "Stream={}, expected delivered stream sequence to be {}, current value is {}",
                stream_name, MESSAGE_COUNT, stream_info.delivered.stream_sequence
            );
            assert_eq!(
                stream_info.ack_floor.stream_sequence, MESSAGE_COUNT as u64,
                "Stream={}, expected ack'ed stream sequence to be {}, current value is {}",
                stream_name, MESSAGE_COUNT, stream_info.ack_floor.stream_sequence
            );
        }

        // Delete all streams created in this test
        for stream_name in streams {
            context.delete_stream(stream_name).await.unwrap();
        }
    }
}
