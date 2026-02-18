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
            // Note: waiting to join the server thread can block in case of single threaded tokio
            // test runtime, since `join()` parks the calling tokio worker thread until the server
            // thread exits. This can create deadlock issues.
            // Run [tokio::test] with "multi_thread" runtime flavor to avoid this issue.
            //
            // Example deadlock scenario observed:
            // * `handle.join()` blocks the tokio runtime thread, waiting for the source server thread to exit
            // * The source server thread waits for the gRPC connection to close
            // * The gRPC connection closes when `read_tx` is dropped
            // * `read_tx` is dropped when SourceActor exits
            // * `SourceActor` exits when all `Source.sender` clones are dropped
            // * Some `sender` clones are held by ack tasks running on the test's tokio runtime
            // * Those ack tasks can't run because the runtime thread is blocked by `handle.join()`
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

    // Why a separate thread and runtime?
    //
    // Tests annotated with `#[tokio::test]` run inside a tokio runtime. If we started the
    // gRPC server as a task, the server future and the test future would share the same tokio runtime.
    //
    // By spawning a new `std::thread` we get an entirely independent tokio runtime that isn't
    // coupled to the test runtime. The server's `block_on` call blocks only this new OS thread,
    // leaving the test runtime's threads free to drive client requests and assertions.
    //
    // This was done to try to mimic the production container isolation behavior for testing.
    let thread_handle = std::thread::Builder::new()
        .name(format!("{name}-server"))
        .spawn(move || {
            // Temp directory kept alive so the socket file persists
            // when client makes a connection.
            let _tmp_dir = tmp_dir;
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime for test server")
                .block_on(server_fn(sock, server_info_file, shutdown_rx));
        })
        .expect("failed to spawn test server thread");

    TestServerHandle {
        sock_file,
        shutdown_tx: Some(shutdown_tx),
        thread_handle: Some(thread_handle),
    }
}

pub(crate) fn get_rand_str() -> String {
    use rand::distr::{Alphanumeric, SampleString};

    Alphanumeric.sample_string(&mut rand::rng(), 10)
}
