//! Test server creation utility for integration tests.
//!
//! Provides [`TestServerHandle`] and [`start_server`] to eliminate boilerplate when
//! spinning up numaflow SDK gRPC servers in tests. Each server runs on a dedicated
//! thread with its own tokio runtime, trying to match the production container
//! isolation behavior.
//!
//! # Example
//!
//! ```ignore
//! let server = start_map_server(MyCatMapper);
//! let channel = server.create_rpc_channel().await.unwrap();
//! let client = MapClient::new(channel);
//! // ... run assertions ...
//! // server is automatically shut down when `server` is dropped
//! ```

use std::future::Future;
use std::path::PathBuf;

use numaflow::shared::ServerExtras;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tonic::transport::Channel;

use crate::shared::grpc::create_rpc_channel;

/// Handle returned by [`start_server`] (and the convenience methods).
///
/// Dropping the handle sends a shutdown signal and joins the thread.
pub(crate) struct TestServerHandle {
    /// The UDS socket path where the server is listening.
    sock_file: PathBuf,
    /// Channel to trigger server shutdown.
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// thread handle for the server runtime.
    thread_handle: Option<std::thread::JoinHandle<()>>,
    /// Temp directory kept alive so the socket file persists.
    _tmp_dir: TempDir,
}

impl TestServerHandle {
    /// Create a gRPC [`Channel`] connected to this server's UDS socket.
    ///
    /// Uses the production [`create_rpc_channel`] helper which has built-in
    /// retry logic, replacing the flaky `sleep(100ms)` pattern.
    pub async fn create_rpc_channel(&self) -> crate::error::Result<Channel> {
        create_rpc_channel(self.sock_file.clone()).await
    }

    /// Explicitly shut down the server and wait for the thread to finish.
    pub fn shutdown(mut self) {
        self.do_shutdown();
    }

    fn do_shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            // It's okay if the receiver is already gone (server exited early).
            let _ = tx.send(());
        }
        if let Some(handle) = self.thread_handle.take() {
            let thread_name = handle.thread().name().unwrap();
            // FIXME: waiting to join the server thread can block the test thread
            // TODO: find a way to avoid this
            // * `handle.join()` blocks the tokio runtime thread, waiting for the source server thread to exit
            // * The source server thread waits for the gRPC connection to close
            // * The gRPC connection closes when `read_tx` is dropped
            // * `read_tx` is dropped when SourceActor exits
            // * `SourceActor` exits when all `Source.sender` clones are dropped
            // * Some `sender` clones are held by ack tasks running on the test's tokio runtime
            // * Those ack tasks can't run because the runtime thread is blocked by handle.join()
            let result = handle.join();
            if let Err(e) = result {
                println!("Thread join failed: {:?}", e);
            }
        }
    }
}

impl Drop for TestServerHandle {
    fn drop(&mut self) {
        self.do_shutdown();
    }
}

/// Start a gRPC server on a dedicated thread with its own tokio runtime.
///
/// `server_fn` is a closure that receives the socket path, server-info path,
/// and a shutdown receiver.
/// It must run the server to completion (i.e., until shutdown is signaled).
///
/// Returns a [`TestServerHandle`] that can create client channels and will
/// automatically shut down the server on drop.
pub(crate) fn start_server<F, Fut>(name: &str, server_fn: F) -> TestServerHandle
where
    F: FnOnce(PathBuf, PathBuf, oneshot::Receiver<()>) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let tmp_dir = TempDir::new().expect("failed to create temp dir");
    let sock_file = tmp_dir.path().join(format!("{name}.sock"));
    let server_info_file = tmp_dir.path().join(format!("{name}-server-info"));

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let sock = sock_file.clone();

    let thread_handle = std::thread::Builder::new()
        .name(format!("{name}-server"))
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime for test server")
                .block_on(server_fn(sock, server_info_file, shutdown_rx));
        })
        .expect("failed to spawn test server thread");

    // TODO: add checks to ensure the server is running

    TestServerHandle {
        sock_file,
        shutdown_tx: Some(shutdown_tx),
        thread_handle: Some(thread_handle),
        _tmp_dir: tmp_dir,
    }
}

/// Start a map server with the given handler.
#[allow(dead_code)]
pub(crate) fn start_map_server<M>(handler: M) -> TestServerHandle
where
    M: numaflow::map::Mapper + Send + Sync + 'static,
{
    start_server(
        &format!("map-{}", get_rand_str()),
        |sock, info, shutdown_rx| async move {
            numaflow::map::Server::new(handler)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("map server failed");
        },
    )
}

/// Start a batch map server with the given handler.

#[allow(dead_code)]
pub(crate) fn start_batch_map_server<M>(handler: M) -> TestServerHandle
where
    M: numaflow::batchmap::BatchMapper + Send + Sync + 'static,
{
    start_server(
        &format!("batch-map-{}", get_rand_str()),
        |sock, info, shutdown_rx| async move {
            numaflow::batchmap::Server::new(handler)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("batch map server failed");
        },
    )
}

/// Start a map stream server with the given handler.
#[allow(dead_code)]
pub(crate) fn start_map_stream_server<M>(handler: M) -> TestServerHandle
where
    M: numaflow::mapstream::MapStreamer + Send + Sync + 'static,
{
    start_server(
        &format!("map-stream-{}", get_rand_str()),
        |sock, info, shutdown_rx| async move {
            numaflow::mapstream::Server::new(handler)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("map stream server failed");
        },
    )
}

/// Start a sink server with the given handler.
pub(crate) fn start_sink_server<S>(handler: S) -> TestServerHandle
where
    S: numaflow::sink::Sinker + Send + Sync + 'static,
{
    start_server(
        &format!("sink-{}", get_rand_str()),
        |sock, info, shutdown_rx| async move {
            numaflow::sink::Server::new(handler)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("sink server failed");
        },
    )
}

/// Start a source server with the given handler.
pub(crate) fn start_source_server<S>(handler: S) -> TestServerHandle
where
    S: numaflow::source::Sourcer + Send + Sync + 'static,
{
    start_server(
        &format!("source-{}", get_rand_str()),
        |sock, info, shutdown_rx| async move {
            numaflow::source::Server::new(handler)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("source server failed");
        },
    )
}

/// Start a source transform server with the given handler.
pub(crate) fn start_source_transform_server<T>(handler: T) -> TestServerHandle
where
    T: numaflow::sourcetransform::SourceTransformer + Send + Sync + 'static,
{
    start_server(
        &format!("src-transform-{}", get_rand_str()),
        |sock, info, shutdown_rx| async move {
            numaflow::sourcetransform::Server::new(handler)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("source transform server failed");
        },
    )
}

/// Start a reduce (aligned) server with the given creator.
#[allow(dead_code)]
pub(crate) fn start_reduce_server<C>(creator: C) -> TestServerHandle
where
    C: numaflow::reduce::ReducerCreator + Send + Sync + 'static,
{
    start_server(
        &format!("reduce-{}", get_rand_str()),
        |sock, info, shutdown_rx| async move {
            numaflow::reduce::Server::new(creator)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("reduce server failed");
        },
    )
}

/// Start a session reduce server with the given creator.
#[allow(dead_code)]
pub(crate) fn start_session_reduce_server<C>(creator: C) -> TestServerHandle
where
    C: numaflow::session_reduce::SessionReducerCreator + Send + Sync + 'static,
{
    start_server(
        &format!("session-reduce-{}", get_rand_str()),
        |sock, info, shutdown_rx| async move {
            numaflow::session_reduce::Server::new(creator)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("session reduce server failed");
        },
    )
}

/// Start an accumulator server with the given creator.///
#[allow(dead_code)]
pub(crate) fn start_accumulator_server<C>(creator: C) -> TestServerHandle
where
    C: numaflow::accumulator::AccumulatorCreator + Send + Sync + 'static,
{
    start_server(
        &format!("accumulator-{}", get_rand_str()),
        |sock, info, shutdown_rx| async move {
            numaflow::accumulator::Server::new(creator)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("accumulator server failed");
        },
    )
}

fn get_rand_str() -> String {
    use rand::distr::{Alphanumeric, SampleString};

    Alphanumeric.sample_string(&mut rand::rng(), 10)
}
