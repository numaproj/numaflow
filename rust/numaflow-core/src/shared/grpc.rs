use std::path::PathBuf;
use std::time::Duration;

use axum::http::Uri;
use chrono::{DateTime, TimeZone, Timelike, Utc};
use numaflow_pb::clients::map::map_client::MapClient;
use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use prost_types::Timestamp;
use tokio::net::UnixStream;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;
use tracing::{info, warn};

use numaflow_shared::server_info::{ServerInfo, sdk_server_info};

use crate::error;
use crate::error::Error;

async fn sleep_or_cancel(duration: Duration, cln_token: &CancellationToken) -> error::Result<()> {
    tokio::select! {
        _ = cln_token.cancelled() => Err(Error::Cancelled()),
        _ = sleep(duration) => Ok(()),
    }
}

/// Waits until the source server is ready, by doing health checks
pub(crate) async fn wait_until_source_ready(
    cln_token: &CancellationToken,
    client: &mut SourceClient<Channel>,
) -> error::Result<()> {
    loop {
        info!("Waiting for source client to be ready...");
        if cln_token.is_cancelled() {
            return Err(Error::Cancelled());
        }
        match client.is_ready(Request::new(())).await {
            Ok(_) => break,
            Err(e) => {
                warn!(error = ?e, "Failed to check source client readiness");
                sleep_or_cancel(Duration::from_secs(1), cln_token).await?;
            }
        }
    }
    Ok(())
}

/// Waits until the sink server is ready, by doing health checks
pub(crate) async fn wait_until_sink_ready(
    cln_token: &CancellationToken,
    client: &mut SinkClient<Channel>,
) -> error::Result<()> {
    loop {
        info!("Waiting for sink client to be ready...");
        if cln_token.is_cancelled() {
            return Err(Error::Cancelled());
        }
        match client.is_ready(Request::new(())).await {
            Ok(_) => break,
            Err(e) => {
                warn!(error = ?e, "Failed to check sink client readiness");
                sleep_or_cancel(Duration::from_secs(1), cln_token).await?;
            }
        }
    }
    Ok(())
}

/// Waits until the transformer server is ready, by doing health checks
pub(crate) async fn wait_until_transformer_ready(
    cln_token: &CancellationToken,
    client: &mut SourceTransformClient<Channel>,
) -> error::Result<()> {
    loop {
        info!("Waiting for transformer client to be ready...");
        if cln_token.is_cancelled() {
            return Err(Error::Cancelled());
        }
        match client.is_ready(Request::new(())).await {
            Ok(_) => break,
            Err(e) => {
                warn!(error = ?e, "Failed to check transformer client readiness");
                sleep_or_cancel(Duration::from_secs(1), cln_token).await?;
            }
        }
    }
    Ok(())
}

/// Waits until the mapper server is ready, by doing health checks
pub(crate) async fn wait_until_mapper_ready(
    cln_token: &CancellationToken,
    client: &mut MapClient<Channel>,
) -> error::Result<()> {
    loop {
        info!("Waiting for mapper client to be ready...");
        if cln_token.is_cancelled() {
            return Err(Error::Cancelled());
        }
        match client.is_ready(Request::new(())).await {
            Ok(_) => break,
            Err(e) => {
                warn!(error = ?e, "Failed to check mapper client readiness");
                sleep_or_cancel(Duration::from_secs(1), cln_token).await?;
            }
        }
    }
    Ok(())
}

pub(crate) fn prost_timestamp_from_utc(t: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: t.timestamp(),
        nanos: t.nanosecond() as i32,
    }
}

/// Default reconnect / startup-retry interval for UDF UDS connections. Tests inject a shorter
/// `Duration` via [`create_rpc_channel_with_interval`] to keep reconnect-scenario tests sub-100ms.
pub(crate) const DEFAULT_RECONNECT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
pub(crate) struct UdfReconnectConfig {
    socket_path: PathBuf,
    server_info_path: PathBuf,
    cln_token: CancellationToken,
    grpc_max_message_size: usize,
    retry_interval: Duration,
}

impl UdfReconnectConfig {
    pub(crate) fn new(
        socket_path: impl Into<PathBuf>,
        server_info_path: impl Into<PathBuf>,
        cln_token: CancellationToken,
        grpc_max_message_size: usize,
    ) -> Self {
        Self {
            socket_path: socket_path.into(),
            server_info_path: server_info_path.into(),
            cln_token,
            grpc_max_message_size,
            retry_interval: DEFAULT_RECONNECT_INTERVAL,
        }
    }

    pub(crate) fn socket_path(&self) -> PathBuf {
        self.socket_path.clone()
    }

    pub(crate) fn server_info_path(&self) -> PathBuf {
        self.server_info_path.clone()
    }

    pub(crate) fn cln_token(&self) -> CancellationToken {
        self.cln_token.clone()
    }

    pub(crate) fn grpc_max_message_size(&self) -> usize {
        self.grpc_max_message_size
    }

    pub(crate) fn retry_interval(&self) -> Duration {
        self.retry_interval
    }
}

pub(crate) async fn create_rpc_channel(socket_path: PathBuf) -> error::Result<Channel> {
    let cln_token = CancellationToken::new();
    create_rpc_channel_with_interval(socket_path, DEFAULT_RECONNECT_INTERVAL, &cln_token).await
}

/// Same as [`create_rpc_channel`] but takes an explicit retry interval. Use this in tests to keep
/// per-attempt backoff short.
pub(crate) async fn create_rpc_channel_with_interval(
    socket_path: PathBuf,
    retry_interval: Duration,
    cln_token: &CancellationToken,
) -> error::Result<Channel> {
    loop {
        if cln_token.is_cancelled() {
            return Err(Error::Cancelled());
        }

        let connect = connect_with_uds(socket_path.clone());
        match tokio::select! {
            _ = cln_token.cancelled() => return Err(Error::Cancelled()),
            result = connect => result,
        } {
            Ok(channel) => return Ok(channel),
            Err(e) => {
                warn!(error = ?e, ?socket_path, "Failed to connect to UDS socket");
                sleep_or_cancel(retry_interval, cln_token).await?;
            }
        }
    }
}

/// Connects to the UDS socket and returns a channel
pub(crate) async fn connect_with_uds(uds_path: PathBuf) -> error::Result<Channel> {
    let channel = Endpoint::try_from("http://[::1]:50051")
        .map_err(|e| Error::Connection(format!("Failed to create endpoint: {e:?}")))?
        .connect_with_connector(service_fn(move |_: Uri| {
            let uds_socket = uds_path.clone();
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    UnixStream::connect(uds_socket).await?,
                ))
            }
        }))
        .await
        .map_err(|e| Error::Connection(format!("Failed to connect: {e}")))?;
    Ok(channel)
}

pub(crate) fn utc_from_timestamp(t: Timestamp) -> DateTime<Utc> {
    DateTime::from_timestamp(t.seconds, t.nanos as u32).unwrap_or(Utc.timestamp_nanos(-1))
}

/// Creates a load-balanced channel across multiple HTTP endpoints.
pub(crate) async fn create_multi_rpc_channel(endpoints: Vec<String>) -> error::Result<Channel> {
    if endpoints.is_empty() {
        return Err(Error::Connection("No endpoints provided".to_string()));
    }

    let endpoints: Vec<Endpoint> = endpoints
        .into_iter()
        .map(|a| Channel::from_shared(a).expect("valid address"))
        .collect();

    let channel = Channel::balance_list(endpoints.into_iter());

    Ok(channel)
}

/// Creates a user-defined source client over UDS and returns the SDK server info.
///
/// The helper creates a new UDS-backed channel, applies the configured gRPC message-size limits,
/// waits until the typed client reports ready, then re-reads SDK server-info from the running
/// sidecar.
pub(crate) async fn create_source_client(
    socket_path: PathBuf,
    server_info_path: PathBuf,
    cln_token: CancellationToken,
    grpc_max_message_size: usize,
    retry_interval: Duration,
) -> error::Result<(SourceClient<Channel>, ServerInfo)> {
    let channel = create_rpc_channel_with_interval(socket_path, retry_interval, &cln_token).await?;
    let mut client = SourceClient::new(channel)
        .max_encoding_message_size(grpc_max_message_size)
        .max_decoding_message_size(grpc_max_message_size);
    wait_until_source_ready(&cln_token, &mut client).await?;
    let server_info = sdk_server_info(server_info_path, cln_token.clone()).await?;
    Ok((client, server_info))
}

/// Creates a user-defined sink client over UDS. See [`create_source_client`] for the setup
/// sequence.
pub(crate) async fn create_sink_client(
    socket_path: PathBuf,
    server_info_path: PathBuf,
    cln_token: CancellationToken,
    grpc_max_message_size: usize,
    retry_interval: Duration,
) -> error::Result<(SinkClient<Channel>, ServerInfo)> {
    let channel = create_rpc_channel_with_interval(socket_path, retry_interval, &cln_token).await?;
    let mut client = SinkClient::new(channel)
        .max_encoding_message_size(grpc_max_message_size)
        .max_decoding_message_size(grpc_max_message_size);
    wait_until_sink_ready(&cln_token, &mut client).await?;
    let server_info = sdk_server_info(server_info_path, cln_token.clone()).await?;
    Ok((client, server_info))
}

/// Creates a user-defined source transformer client over UDS. See [`create_source_client`] for the
/// setup sequence.
pub(crate) async fn create_transformer_client(
    socket_path: PathBuf,
    server_info_path: PathBuf,
    cln_token: CancellationToken,
    grpc_max_message_size: usize,
    retry_interval: Duration,
) -> error::Result<(SourceTransformClient<Channel>, ServerInfo)> {
    let channel = create_rpc_channel_with_interval(socket_path, retry_interval, &cln_token).await?;
    let mut client = SourceTransformClient::new(channel)
        .max_encoding_message_size(grpc_max_message_size)
        .max_decoding_message_size(grpc_max_message_size);
    wait_until_transformer_ready(&cln_token, &mut client).await?;
    let server_info = sdk_server_info(server_info_path, cln_token.clone()).await?;
    Ok((client, server_info))
}

/// Creates a user-defined mapper client over UDS. See [`create_source_client`] for the setup
/// sequence.
pub(crate) async fn create_mapper_client(
    socket_path: PathBuf,
    server_info_path: PathBuf,
    cln_token: CancellationToken,
    grpc_max_message_size: usize,
    retry_interval: Duration,
) -> error::Result<(MapClient<Channel>, ServerInfo)> {
    let channel = create_rpc_channel_with_interval(socket_path, retry_interval, &cln_token).await?;
    let mut client = MapClient::new(channel)
        .max_encoding_message_size(grpc_max_message_size)
        .max_decoding_message_size(grpc_max_message_size);
    wait_until_mapper_ready(&cln_token, &mut client).await?;
    let server_info = sdk_server_info(server_info_path, cln_token.clone()).await?;
    Ok((client, server_info))
}

/// Macro to create a guard that automatically aborts a task handle when dropped.
#[macro_export]
macro_rules! jh_abort_guard {
    ($handle:expr) => {{
        struct AbortGuard<T> {
            handle: Option<JoinHandle<T>>,
        }

        impl<T> AbortGuard<T> {
            fn new(handle: JoinHandle<T>) -> Self {
                Self {
                    handle: Some(handle),
                }
            }
        }

        impl<T> Drop for AbortGuard<T> {
            fn drop(&mut self) {
                if let Some(handle) = self.handle.take() {
                    handle.abort();
                }
            }
        }

        AbortGuard::new($handle)
    }};
}

#[cfg(test)]
mod tests {
    //! UDF client creation tests. Each test stands up a real `numaflow` SDK server on a UDS
    //! socket, then calls the production helper so channel creation, client
    //! configuration, readiness, and server-info validation are exercised end-to-end.
    //!
    //! Retries use a 10ms cadence so the "wait for server to come up" branch stays sub-100ms.
    use super::*;
    use crate::shared::test_utils::server::start_server;
    use numaflow::shared::ServerExtras;
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;
    use tonic::Request;

    const FAST_RETRY: Duration = Duration::from_millis(10);
    const CUSTOM_GRPC_MAX_MESSAGE_SIZE: usize = 1024 * 1024;

    struct PassThroughSource;
    #[tonic::async_trait]
    impl numaflow::source::Sourcer for PassThroughSource {
        async fn read(
            &self,
            _request: numaflow::source::SourceReadRequest,
            _tx: tokio::sync::mpsc::Sender<numaflow::source::Message>,
        ) {
        }
        async fn ack(&self, _offsets: Vec<numaflow::source::Offset>) {}
        async fn nack(&self, _offsets: Vec<numaflow::source::Offset>) {}
        async fn pending(&self) -> Option<usize> {
            Some(0)
        }
        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![0])
        }
    }

    struct PassThroughMap;
    #[tonic::async_trait]
    impl numaflow::map::Mapper for PassThroughMap {
        async fn map(&self, input: numaflow::map::MapRequest) -> Vec<numaflow::map::Message> {
            vec![numaflow::map::Message::new(input.value).with_keys(input.keys)]
        }
    }

    struct PassThroughSink;
    #[tonic::async_trait]
    impl numaflow::sink::Sinker for PassThroughSink {
        async fn sink(
            &self,
            mut input: tokio::sync::mpsc::Receiver<numaflow::sink::SinkRequest>,
        ) -> Vec<numaflow::sink::Response> {
            let mut responses = Vec::new();
            while let Some(req) = input.recv().await {
                responses.push(numaflow::sink::Response::ok(req.id));
            }
            responses
        }
    }

    struct PassThroughTransformer;
    #[tonic::async_trait]
    impl numaflow::sourcetransform::SourceTransformer for PassThroughTransformer {
        async fn transform(
            &self,
            input: numaflow::sourcetransform::SourceTransformRequest,
        ) -> Vec<numaflow::sourcetransform::Message> {
            vec![
                numaflow::sourcetransform::Message::new(input.value, chrono::Utc::now())
                    .with_keys(input.keys),
            ]
        }
    }

    /// Confirms the source helper returns a healthy client when the SDK server is up.
    /// The test uses the shared server primitive so it can pass both the socket and server-info
    /// paths into the helper.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_source_client_returns_healthy_client() {
        // The server-info filename must end with `<container-type>-server-info` so
        // `ContainerType::from(&Path)` (numaflow-shared/src/server_info.rs:129) resolves the SDK
        // version constraint correctly. `start_server` builds the filename from this `name` arg
        // as `{name}-server-info`, so passing the literal container-type string is what we need.
        let handle = start_server("sourcer", |sock, info, shutdown_rx| async move {
            numaflow::source::Server::new(PassThroughSource)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("source server failed");
        });

        let cln_token = CancellationToken::new();
        let (mut client, _server_info) = create_source_client(
            handle.socket_path(),
            handle.server_info_path(),
            cln_token.clone(),
            CUSTOM_GRPC_MAX_MESSAGE_SIZE,
            FAST_RETRY,
        )
        .await
        .expect("create_source_client should succeed when SDK server is up");

        // Sanity-check the returned client is functional.
        assert!(client.is_ready(Request::new(())).await.is_ok());
    }

    /// Same shape for the mapper helper.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_mapper_client_returns_healthy_client() {
        let handle = start_server("mapper", |sock, info, shutdown_rx| async move {
            numaflow::map::Server::new(PassThroughMap)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("map server failed");
        });

        let cln_token = CancellationToken::new();
        let (mut client, _server_info) = create_mapper_client(
            handle.socket_path(),
            handle.server_info_path(),
            cln_token,
            CUSTOM_GRPC_MAX_MESSAGE_SIZE,
            FAST_RETRY,
        )
        .await
        .expect("create_mapper_client should succeed when SDK server is up");
        assert!(client.is_ready(Request::new(())).await.is_ok());
    }

    /// Same shape for the sink helper.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_sink_client_returns_healthy_client() {
        let handle = start_server("sinker", |sock, info, shutdown_rx| async move {
            numaflow::sink::Server::new(PassThroughSink)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("sink server failed");
        });

        let cln_token = CancellationToken::new();
        let (mut client, _server_info) = create_sink_client(
            handle.socket_path(),
            handle.server_info_path(),
            cln_token,
            CUSTOM_GRPC_MAX_MESSAGE_SIZE,
            FAST_RETRY,
        )
        .await
        .expect("create_sink_client should succeed when SDK server is up");
        assert!(client.is_ready(Request::new(())).await.is_ok());
    }

    /// Same shape for the transformer helper.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_transformer_client_returns_healthy_client() {
        let handle = start_server("sourcetransformer", |sock, info, shutdown_rx| async move {
            numaflow::sourcetransform::Server::new(PassThroughTransformer)
                .with_socket_file(sock)
                .with_server_info_file(info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("transformer server failed");
        });

        let cln_token = CancellationToken::new();
        let (mut client, _server_info) = create_transformer_client(
            handle.socket_path(),
            handle.server_info_path(),
            cln_token,
            CUSTOM_GRPC_MAX_MESSAGE_SIZE,
            FAST_RETRY,
        )
        .await
        .expect("create_transformer_client should succeed when SDK server is up");
        assert!(client.is_ready(Request::new(())).await.is_ok());
    }

    /// Confirms the helper retries when the socket is missing at first and the SDK server only
    /// comes up later. This is the production "UDF crashed, restarted, and is now alive again"
    /// path. The FAST_RETRY cadence keeps the test sub-100ms even with several retry attempts.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_source_client_waits_for_socket_to_appear() {
        // Pre-allocate a TempDir and paths *outside* `start_server` so we can hand them to the
        // helper before the server comes up. The server is started on a delayed tokio task.
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("sourcer.sock");
        // Filename must end with `<container-type>-server-info` so `ContainerType::from(&Path)`
        // resolves the SDK version constraint correctly. See the comment in the success-path
        // test above.
        let server_info_file = tmp_dir.path().join("sourcer-server-info");

        // The SDK server runs on its own thread / runtime so it doesn't compete with the test's
        // reconnect retry loop.
        let sock = sock_file.clone();
        let info = server_info_file.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                // Delay the server start so the helper observes a missing socket and exercises
                // the retry loop.
                tokio::time::sleep(Duration::from_millis(50)).await;
                numaflow::source::Server::new(PassThroughSource)
                    .with_socket_file(sock)
                    .with_server_info_file(info)
                    .start_with_shutdown(shutdown_rx)
                    .await
                    .expect("delayed source server failed");
            });
        });

        let cln_token = CancellationToken::new();
        let (client, _server_info) = create_source_client(
            sock_file,
            server_info_file,
            cln_token,
            CUSTOM_GRPC_MAX_MESSAGE_SIZE,
            FAST_RETRY,
        )
        .await
        .expect("create_source_client should wait through retries and return Ok");
        drop(client);

        let _ = shutdown_tx.send(());
        server_thread.join().expect("server thread panicked");
    }
}
