use crate::mapper::map::MapHandle;
use crate::shared::grpc;
use crate::shared::test_utils::server::{TestServerHandle, get_rand_str, start_server};
use crate::tracker::Tracker;
use numaflow::shared::ServerExtras;
use numaflow::{batchmap, map, mapstream};
use numaflow_pb::clients::map::map_client::MapClient;
use numaflow_shared::server_info::MapMode;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// A handle to a mapper component.
///
/// Contains the mapper handle and server handle (for the mapper server).
pub(crate) struct MapperTestHandle {
    pub mapper: MapHandle,
    pub server_handle: TestServerHandle,
}

impl MapperTestHandle {
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
        let server_handle = MapperTestHandle::start_map_server(map_svc);
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
        let server_handle = MapperTestHandle::start_batch_map_server(map_svc);
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
        let server_handle = MapperTestHandle::start_map_stream_server(map_svc);
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

    /// Start a map server with the given handler.
    pub(crate) fn start_map_server<M>(handler: M) -> TestServerHandle
    where
        M: map::Mapper + Send + Sync + 'static,
    {
        start_server(
            &format!("map-{}", get_rand_str()),
            |sock, info, shutdown_rx| async move {
                map::Server::new(handler)
                    .with_socket_file(sock)
                    .with_server_info_file(info)
                    .start_with_shutdown(shutdown_rx)
                    .await
                    .expect("map server failed");
            },
        )
    }

    /// Start a batch map server with the given handler.
    pub(crate) fn start_batch_map_server<M>(handler: M) -> TestServerHandle
    where
        M: batchmap::BatchMapper + Send + Sync + 'static,
    {
        start_server(
            &format!("batch-map-{}", get_rand_str()),
            |sock, info, shutdown_rx| async move {
                batchmap::Server::new(handler)
                    .with_socket_file(sock)
                    .with_server_info_file(info)
                    .start_with_shutdown(shutdown_rx)
                    .await
                    .expect("batch map server failed");
            },
        )
    }

    /// Start a map stream server with the given handler.
    pub(crate) fn start_map_stream_server<M>(handler: M) -> TestServerHandle
    where
        M: mapstream::MapStreamer + Send + Sync + 'static,
    {
        start_server(
            &format!("map-stream-{}", get_rand_str()),
            |sock, info, shutdown_rx| async move {
                mapstream::Server::new(handler)
                    .with_socket_file(sock)
                    .with_server_info_file(info)
                    .start_with_shutdown(shutdown_rx)
                    .await
                    .expect("map stream server failed");
            },
        )
    }
}
