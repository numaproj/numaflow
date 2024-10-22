use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::config::components::metrics::MetricsConfig;
use crate::config::monovertex::MonovertexConfig;
use crate::error;
use crate::monovertex::metrics::{
    start_metrics_https_server, PendingReader, PendingReaderBuilder, UserDefinedContainerState,
};
use crate::shared::server_info;
use crate::source::SourceHandle;
use crate::Error;
use crate::Result;
use axum::http::Uri;
use backoff::retry::Retry;
use backoff::strategy::fixed;
use chrono::{DateTime, TimeZone, Timelike, Utc};
use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use prost_types::Timestamp;
use tokio::net::UnixStream;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tower::service_fn;
use tracing::{info, warn};

pub(crate) async fn check_compatibility(
    cln_token: &CancellationToken,
    source_file_path: Option<PathBuf>,
    sink_file_path: Option<PathBuf>,
    transformer_file_path: Option<PathBuf>,
    fb_sink_file_path: Option<PathBuf>,
) -> error::Result<()> {
    if let Some(source_file_path) = source_file_path {
        server_info::check_for_server_compatibility(source_file_path, cln_token.clone())
            .await
            .map_err(|e| {
                warn!("Error waiting for source server info file: {:?}", e);
                Error::Forwarder("Error waiting for server info file".to_string())
            })?;
    }

    if let Some(sink_file_path) = sink_file_path {
        server_info::check_for_server_compatibility(sink_file_path, cln_token.clone())
            .await
            .map_err(|e| {
                error!("Error waiting for sink server info file: {:?}", e);
                Error::Forwarder("Error waiting for server info file".to_string())
            })?;
    }

    if let Some(transformer_path) = transformer_file_path {
        server_info::check_for_server_compatibility(transformer_path, cln_token.clone())
            .await
            .map_err(|e| {
                error!("Error waiting for transformer server info file: {:?}", e);
                Error::Forwarder("Error waiting for server info file".to_string())
            })?;
    }

    if let Some(fb_sink_path) = fb_sink_file_path {
        server_info::check_for_server_compatibility(fb_sink_path, cln_token.clone())
            .await
            .map_err(|e| {
                warn!("Error waiting for fallback sink server info file: {:?}", e);
                Error::Forwarder("Error waiting for server info file".to_string())
            })?;
    }
    Ok(())
}

pub(crate) async fn start_metrics_server(
    metrics_config: MetricsConfig,
    metrics_state: UserDefinedContainerState,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Start the metrics server, which server the prometheus metrics.
        let metrics_addr: SocketAddr =
            format!("0.0.0.0:{}", metrics_config.metrics_server_listen_port)
                .parse()
                .expect("Invalid address");

        if let Err(e) = start_metrics_https_server(metrics_addr, metrics_state).await {
            error!("metrics server error: {:?}", e);
        }
    })
}

pub(crate) async fn create_pending_reader(
    mvtx_config: &MonovertexConfig,
    lag_reader_grpc_client: SourceHandle,
) -> PendingReader {
    PendingReaderBuilder::new(
        mvtx_config.name.clone(),
        mvtx_config.replica,
        lag_reader_grpc_client,
    )
    .lag_checking_interval(Duration::from_secs(
        mvtx_config.metrics_config.lag_check_interval_in_secs.into(),
    ))
    .refresh_interval(Duration::from_secs(
        mvtx_config
            .metrics_config
            .lag_refresh_interval_in_secs
            .into(),
    ))
    .build()
}
pub(crate) async fn wait_until_source_ready(
    cln_token: &CancellationToken,
    client: &mut SourceClient<Channel>,
) -> Result<()> {
    info!("Waiting for source client to be ready...");
    loop {
        if cln_token.is_cancelled() {
            return Err(Error::Forwarder(
                "Cancellation token is cancelled".to_string(),
            ));
        }
        match client.is_ready(Request::new(())).await {
            Ok(_) => break,
            Err(_) => sleep(Duration::from_secs(1)).await,
        }
        info!("Waiting for source client to be ready...");
    }
    Ok(())
}

pub(crate) async fn wait_until_sink_ready(
    cln_token: &CancellationToken,
    client: &mut SinkClient<Channel>,
) -> Result<()> {
    loop {
        if cln_token.is_cancelled() {
            return Err(Error::Forwarder(
                "Cancellation token is cancelled".to_string(),
            ));
        }
        match client.is_ready(Request::new(())).await {
            Ok(_) => break,
            Err(_) => sleep(Duration::from_secs(1)).await,
        }
        info!("Waiting for sink client to be ready...");
    }
    Ok(())
}

pub(crate) async fn wait_until_transformer_ready(
    cln_token: &CancellationToken,
    client: &mut SourceTransformClient<Channel>,
) -> Result<()> {
    loop {
        if cln_token.is_cancelled() {
            return Err(Error::Forwarder(
                "Cancellation token is cancelled".to_string(),
            ));
        }
        match client.is_ready(Request::new(())).await {
            Ok(_) => break,
            Err(_) => sleep(Duration::from_secs(1)).await,
        }
        info!("Waiting for transformer client to be ready...");
    }
    Ok(())
}

pub(crate) fn utc_from_timestamp(t: Option<Timestamp>) -> DateTime<Utc> {
    t.map_or(Utc.timestamp_nanos(-1), |t| {
        DateTime::from_timestamp(t.seconds, t.nanos as u32).unwrap_or(Utc.timestamp_nanos(-1))
    })
}

pub(crate) fn prost_timestamp_from_utc(t: DateTime<Utc>) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: t.timestamp(),
        nanos: t.nanosecond() as i32,
    })
}

pub(crate) async fn create_rpc_channel(socket_path: PathBuf) -> crate::error::Result<Channel> {
    const RECONNECT_INTERVAL: u64 = 1000;
    const MAX_RECONNECT_ATTEMPTS: usize = 5;

    let interval = fixed::Interval::from_millis(RECONNECT_INTERVAL).take(MAX_RECONNECT_ATTEMPTS);

    let channel = Retry::retry(
        interval,
        || async { connect_with_uds(socket_path.clone()).await },
        |_: &Error| true,
    )
    .await?;
    Ok(channel)
}

pub(crate) async fn connect_with_uds(uds_path: PathBuf) -> Result<Channel> {
    let channel = Endpoint::try_from("http://[::]:50051")
        .map_err(|e| Error::Connection(format!("Failed to create endpoint: {:?}", e)))?
        .connect_with_connector(service_fn(move |_: Uri| {
            let uds_socket = uds_path.clone();
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    UnixStream::connect(uds_socket).await?,
                ))
            }
        }))
        .await
        .map_err(|e| Error::Connection(format!("Failed to connect: {:?}", e)))?;
    Ok(channel)
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use numaflow::source::{Message, Offset, SourceReadRequest};
    use numaflow::{sink, source, sourcetransform};
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Sender;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::shared::server_info::ServerInfo;
    use crate::shared::utils::create_rpc_channel;

    async fn write_server_info(file_path: &str, server_info: &ServerInfo) -> error::Result<()> {
        let serialized = serde_json::to_string(server_info).unwrap();
        let mut file = File::create(file_path).unwrap();
        file.write_all(serialized.as_bytes()).unwrap();
        file.write_all(b"U+005C__END__").unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn test_check_compatibility_success() {
        let dir = tempdir().unwrap();
        let source_file_path = dir.path().join("sourcer-server-info");
        let sink_file_path = dir.path().join("sinker-server-info");
        let transformer_file_path = dir.path().join("sourcetransformer-server-info");
        let fb_sink_file_path = dir.path().join("fb-sink-server-info");

        let server_info = ServerInfo {
            protocol: "uds".to_string(),
            language: "rust".to_string(),
            minimum_numaflow_version: "0.1.0".to_string(),
            version: "0.1.0".to_string(),
            metadata: None,
        };

        write_server_info(source_file_path.to_str().unwrap(), &server_info)
            .await
            .unwrap();
        write_server_info(sink_file_path.to_str().unwrap(), &server_info)
            .await
            .unwrap();
        write_server_info(transformer_file_path.to_str().unwrap(), &server_info)
            .await
            .unwrap();
        write_server_info(fb_sink_file_path.to_str().unwrap(), &server_info)
            .await
            .unwrap();

        let cln_token = CancellationToken::new();
        let result = check_compatibility(
            &cln_token,
            Some(source_file_path),
            Some(sink_file_path),
            None,
            None,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_check_compatibility_failure() {
        let cln_token = CancellationToken::new();
        let dir = tempdir().unwrap();
        let source_file_path = dir.path().join("source_server_info.json");
        let sink_file_path = dir.path().join("sink_server_info.json");
        let transformer_file_path = dir.path().join("transformer_server_info.json");
        let fb_sink_file_path = dir.path().join("fb_sink_server_info.json");

        // do not write server info files to simulate failure
        // cancel the token after 100ms to simulate cancellation
        let token = cln_token.clone();
        let handle = tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            token.cancel();
        });
        let result = check_compatibility(
            &cln_token,
            Some(source_file_path),
            Some(sink_file_path),
            Some(transformer_file_path),
            Some(fb_sink_file_path),
        )
        .await;

        assert!(result.is_err());
        handle.await.unwrap();
    }

    struct SimpleSource {}

    #[tonic::async_trait]
    impl source::Sourcer for SimpleSource {
        async fn read(&self, _request: SourceReadRequest, _transmitter: Sender<Message>) {}

        async fn ack(&self, _offset: Offset) {}

        async fn pending(&self) -> usize {
            0
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
        async fn sink(&self, mut _input: mpsc::Receiver<sink::SinkRequest>) -> Vec<sink::Response> {
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
