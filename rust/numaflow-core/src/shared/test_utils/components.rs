//! Test component creation utility for integration tests.
//!
//! Provides utilities to create components (sources, sinks, transformers, mappers) to eliminate
//! boilerplate code when adding tests.

use crate::mapper::map::MapHandle;
use crate::shared::grpc;
use crate::shared::test_utils::server;
use crate::shared::test_utils::server::TestServerHandle;
use crate::sinker::sink::{SinkClientType, SinkWriter, SinkWriterBuilder};
use crate::source::user_defined::new_source;
use crate::source::{Source, SourceType};
use crate::tracker::Tracker;
use crate::transformer::Transformer;
use chrono::Utc;
use numaflow::sink::{Response, SinkRequest};
use numaflow::sourcetransform::{SourceTransformRequest, SourceTransformer};
use numaflow::{batchmap, map, mapstream, sink, source, sourcetransform};
use numaflow_pb::clients::map::map_client::MapClient;
use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use numaflow_shared::server_info::MapMode;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// A handle to a source transformer component.
///
/// Contains the transformer handle and server handle (for the transformer server).
pub(crate) struct SourceTransformerTestHandle {
    pub transformer: Option<Transformer>,
    pub server_handle: TestServerHandle,
}

/// A handle to a source component.
///
/// Contains the source handle, server handle (for the source server),
/// and an optional source transformer handle.
pub(crate) struct SourceTestHandle {
    pub source_transformer_test_handle: Option<SourceTransformerTestHandle>,
    pub source: Source<crate::typ::WithoutRateLimiter>,
    pub server_handle: TestServerHandle,
}

/// A handle to a mapper component.
///
/// Contains the mapper handle and server handle (for the mapper server).
pub(crate) struct MapperTestHandle {
    pub mapper: MapHandle,
    pub server_handle: TestServerHandle,
}

/// Enum to represent the type of sink to be used.
pub(crate) enum SinkType<T = NoOpSink> {
    /// Pass all sinkClientTypes except UserDefined
    BuiltIn(SinkClientType),
    /// Use for SinkClientType::UserDefined
    UserDefined(T),
}

/// A handle to a sink component.
///
/// Contains the sink writer and server handles (for each of the user defined sink servers).
pub(crate) struct SinkTestHandle {
    pub sink_writer: SinkWriter,
    pub ud_sink_server_handle: Option<TestServerHandle>,
    pub fb_ud_sink_server_handle: Option<TestServerHandle>,
    pub ons_ud_sink_server_handle: Option<TestServerHandle>,
}

/// A no-op transformer for testing.
///
/// This is used for the turbofish operator to pass a `None` transformer
/// when a transformer svc is not provided.
pub(crate) struct NoOpTransformer;

#[tonic::async_trait]
impl SourceTransformer for NoOpTransformer {
    async fn transform(&self, input: SourceTransformRequest) -> Vec<sourcetransform::Message> {
        vec![
            sourcetransform::Message::new(input.value, Utc::now())
                .with_keys(input.keys)
                .with_user_metadata(input.user_metadata),
        ]
    }
}

/// A no-op sink for testing.
///
/// This is used for the turbofish operator to pass a `None` sink
/// when a sink svc is not provided.
pub(crate) struct NoOpSink;

#[tonic::async_trait]
impl sink::Sinker for NoOpSink {
    async fn sink(&self, _input: tokio::sync::mpsc::Receiver<SinkRequest>) -> Vec<Response> {
        vec![]
    }
}

/// Create a user defined source component with the given source and transformer services.
///
/// Initializes the sourcer and transformer handles along with their servers.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_ud_source<S, T>(
    source_svc: S,
    source_transformer_svc: Option<T>,
    batch_size: usize,
    cln_token: CancellationToken,
    tracker: Tracker,
) -> SourceTestHandle
where
    T: SourceTransformer + Send + Sync + 'static,
    S: source::Sourcer + Send + Sync + 'static,
{
    // create a transformer for this source if it is provided
    let mut transformer_test_handle = match source_transformer_svc {
        Some(transformer_svc) => {
            let server_handle = server::start_source_transform_server(transformer_svc);

            let mut client = SourceTransformClient::new(
                server_handle
                    .create_rpc_channel()
                    .await
                    .expect("failed to create source transformer rpc channel"),
            );

            grpc::wait_until_transformer_ready(&cln_token, &mut client)
                .await
                .expect("failed to wait for source transformer server to be ready");

            let transformer = Transformer::new(
                batch_size,
                10,
                Duration::from_secs(10),
                client,
                tracker.clone(),
            )
            .await
            .expect("failed to create source transformer");

            Some(SourceTransformerTestHandle {
                server_handle,
                transformer: Some(transformer),
            })
        }
        None => None,
    };

    // create the source
    let server_handle = server::start_source_server(source_svc);
    let mut client = SourceClient::new(
        server_handle
            .create_rpc_channel()
            .await
            .expect("failed to create source rpc channel"),
    );

    grpc::wait_until_source_ready(&cln_token, &mut client)
        .await
        .expect("failed to wait for source server to be ready");

    let (src_read, src_ack, lag_reader) = new_source(
        client,
        batch_size,
        Duration::from_millis(1000),
        cln_token.clone(),
        true,
    )
    .await
    .map_err(|e| panic!("failed to create source reader: {:?}", e))
    .expect("failed to create source");

    let source: Source<crate::typ::WithoutRateLimiter> = match transformer_test_handle {
        Some(ref mut source_transform) => {
            Source::new(
                batch_size,
                SourceType::UserDefinedSource(Box::new(src_read), Box::new(src_ack), lag_reader),
                tracker.clone(),
                true,
                source_transform.transformer.take(),
                None,
                None,
            )
            .await
        }
        None => {
            Source::new(
                batch_size,
                SourceType::UserDefinedSource(Box::new(src_read), Box::new(src_ack), lag_reader),
                tracker.clone(),
                true,
                None,
                None,
                None,
            )
            .await
        }
    };

    SourceTestHandle {
        server_handle,
        source,
        source_transformer_test_handle: transformer_test_handle,
    }
}

/// Create a mapper component with the given mapper service.
///
/// Initializes the mapper handle and server handle (for the mapper server).
pub(crate) async fn create_mapper<M>(
    map_svc: M,
    tracker: Tracker,
    map_mode: MapMode,
    batch_size: usize,
    read_timeout: Duration,
    graceful_timeout: Duration,
    concurrency: usize,
) -> MapperTestHandle
where
    M: map::Mapper + Send + Sync + 'static,
{
    // create a mapper
    let server_handle = server::start_map_server(map_svc);
    let mut client = MapClient::new(
        server_handle
            .create_rpc_channel()
            .await
            .expect("failed to create mapper rpc channel"),
    );

    grpc::wait_until_mapper_ready(&CancellationToken::new(), &mut client)
        .await
        .expect("failed to wait for mapper server to be ready");

    let mapper = MapHandle::new(
        map_mode,
        batch_size,
        read_timeout,
        graceful_timeout,
        concurrency,
        client,
        tracker.clone(),
    )
    .await
    .expect("failed to create mapper");

    MapperTestHandle {
        mapper,
        server_handle,
    }
}

/// Create a batch mapper component with the given batch mapper service.
///
/// Initializes the mapper handle and server handle (for the batch mapper server).
pub(crate) async fn create_batch_mapper<M>(
    map_svc: M,
    tracker: Tracker,
    map_mode: MapMode,
    batch_size: usize,
    read_timeout: Duration,
    graceful_timeout: Duration,
    concurrency: usize,
) -> MapperTestHandle
where
    M: batchmap::BatchMapper + Send + Sync + 'static,
{
    // create a mapper
    let server_handle = server::start_batch_map_server(map_svc);
    let mut client = MapClient::new(
        server_handle
            .create_rpc_channel()
            .await
            .expect("failed to create batch map rpc channel"),
    );

    grpc::wait_until_mapper_ready(&CancellationToken::new(), &mut client)
        .await
        .expect("failed to wait for batch map server to be ready");

    let mapper = MapHandle::new(
        map_mode,
        batch_size,
        read_timeout,
        graceful_timeout,
        concurrency,
        client,
        tracker.clone(),
    )
    .await
    .expect("failed to create batch mapper");

    MapperTestHandle {
        mapper,
        server_handle,
    }
}

/// Create a map streamer component with the given map streamer service.
///
/// Initializes the mapper handle and server handle (for the map streamer server).
pub(crate) async fn create_map_streamer<M>(
    map_svc: M,
    tracker: Tracker,
    map_mode: MapMode,
    batch_size: usize,
    read_timeout: Duration,
    graceful_timeout: Duration,
    concurrency: usize,
) -> MapperTestHandle
where
    M: mapstream::MapStreamer + Send + Sync + 'static,
{
    // create a mapper
    let server_handle = server::start_map_stream_server(map_svc);
    let mut client = MapClient::new(
        server_handle
            .create_rpc_channel()
            .await
            .expect("failed to create map streamer rpc channel"),
    );

    grpc::wait_until_mapper_ready(&CancellationToken::new(), &mut client)
        .await
        .expect("failed to wait for map streamer server to be ready");

    let mapper = MapHandle::new(
        map_mode,
        batch_size,
        read_timeout,
        graceful_timeout,
        concurrency,
        client,
        tracker.clone(),
    )
    .await
    .expect("failed to create map streamer");

    MapperTestHandle {
        mapper,
        server_handle,
    }
}

/// Create a sink component with the given sink service.
///
/// Initializes the sink handle and server handle (for the user defined sink servers).
pub(crate) async fn create_sink<T>(
    sink: SinkType<T>,
    fallback: Option<SinkType<T>>,
    on_success: Option<SinkType<T>>,
    batch_size: usize,
) -> SinkTestHandle
where
    T: sink::Sinker + Send + Sync + 'static,
{
    let (sink_client_type, ud_sink_server_handle) = match sink {
        SinkType::UserDefined(sink) => {
            let (sink_client_type, sink_server_handle) = create_ud_sink(sink).await;
            (sink_client_type, Some(sink_server_handle))
        }
        SinkType::BuiltIn(sink) => (sink, None),
    };

    let (fb_client_type, fb_ud_sink_server_handle) = match fallback {
        Some(SinkType::UserDefined(fb_sink)) => {
            let (sink_client_type, sink_server_handle) = create_ud_sink(fb_sink).await;
            (Some(sink_client_type), Some(sink_server_handle))
        }
        Some(SinkType::BuiltIn(fb_sink)) => (Some(fb_sink), None),
        None => (None, None),
    };

    let (ons_client_type, ons_ud_sink_server_handle) = match on_success {
        Some(SinkType::UserDefined(ons_sink)) => {
            let (sink_client_type, sink_server_handle) = create_ud_sink(ons_sink).await;
            (Some(sink_client_type), Some(sink_server_handle))
        }
        Some(SinkType::BuiltIn(ons_sink)) => (Some(ons_sink), None),
        None => (None, None),
    };

    let sink_writer = create_sink_writer(
        sink_client_type,
        fb_client_type,
        ons_client_type,
        batch_size,
    )
    .await;

    SinkTestHandle {
        sink_writer,
        ud_sink_server_handle,
        fb_ud_sink_server_handle,
        ons_ud_sink_server_handle,
    }
}

async fn create_ud_sink<T>(sink_svc: T) -> (SinkClientType, TestServerHandle)
where
    T: sink::Sinker + Send + Sync + 'static,
{
    // Create the sink
    let server_handle = server::start_sink_server(sink_svc);
    let mut sink_client = SinkClient::new(
        server_handle
            .create_rpc_channel()
            .await
            .expect("failed to create sink client"),
    );

    grpc::wait_until_sink_ready(&CancellationToken::new(), &mut sink_client)
        .await
        .expect("failed to wait for sink server to be ready");

    (SinkClientType::UserDefined(sink_client), server_handle)
}

async fn create_sink_writer(
    sink: SinkClientType,
    fallback: Option<SinkClientType>,
    on_success: Option<SinkClientType>,
    batch_size: usize,
) -> SinkWriter {
    let mut sink_writer = SinkWriterBuilder::new(batch_size, Duration::from_millis(100), sink);

    sink_writer = match fallback {
        Some(fallback) => sink_writer.fb_sink_client(fallback),
        None => sink_writer,
    };

    sink_writer = match on_success {
        Some(on_success) => sink_writer.on_success_sink_client(on_success),
        None => sink_writer,
    };

    sink_writer.build().await.unwrap()
}
