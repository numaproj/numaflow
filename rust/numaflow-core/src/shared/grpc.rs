use std::path::PathBuf;
use std::time::Duration;

use axum::http::Uri;
use backoff::retry::Retry;
use backoff::strategy::fixed;
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

use crate::error;
use crate::error::Error;

/// Waits until the source server is ready, by doing health checks
pub(crate) async fn wait_until_source_ready(
    cln_token: &CancellationToken,
    client: &mut SourceClient<Channel>,
) -> error::Result<()> {
    loop {
        info!("Waiting for source client to be ready...");
        if cln_token.is_cancelled() {
            return Err(Error::Forwarder(
                "Cancellation token is cancelled".to_string(),
            ));
        }
        match client.is_ready(Request::new(())).await {
            Ok(_) => break,
            Err(e) => {
                warn!(error = ?e, "Failed to check source client readiness");
                sleep(Duration::from_secs(1)).await
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
            return Err(Error::Forwarder(
                "Cancellation token is cancelled".to_string(),
            ));
        }
        match client.is_ready(Request::new(())).await {
            Ok(_) => break,
            Err(e) => {
                warn!(error = ?e, "Failed to check sink client readiness");
                sleep(Duration::from_secs(1)).await
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
            return Err(Error::Forwarder(
                "Cancellation token is cancelled".to_string(),
            ));
        }
        match client.is_ready(Request::new(())).await {
            Ok(_) => break,
            Err(e) => {
                warn!(error = ?e, "Failed to check transformer client readiness");
                sleep(Duration::from_secs(1)).await
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
            return Err(Error::Forwarder(
                "Cancellation token is cancelled".to_string(),
            ));
        }
        match client.is_ready(Request::new(())).await {
            Ok(_) => break,
            Err(e) => {
                warn!(error = ?e, "Failed to check mapper client readiness");
                sleep(Duration::from_secs(1)).await
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

pub(crate) async fn create_rpc_channel(socket_path: PathBuf) -> error::Result<Channel> {
    const RECONNECT_INTERVAL: u64 = 1000;
    const MAX_RECONNECT_ATTEMPTS: usize = usize::MAX;

    let interval = fixed::Interval::from_millis(RECONNECT_INTERVAL).take(MAX_RECONNECT_ATTEMPTS);
    let channel = Retry::new(
        interval,
        async || match connect_with_uds(socket_path.clone()).await {
            Ok(channel) => Ok(channel),
            Err(e) => {
                warn!(error = ?e, ?socket_path, "Failed to connect to UDS socket");
                Err(Error::Connection(format!(
                    "Failed to connect {socket_path:?}: {e:?}"
                )))
            }
        },
        |_: &Error| true,
    )
    .await?;
    Ok(channel)
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
