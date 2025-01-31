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
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tower::service_fn;
use tracing::{info, warn};

use crate::error;
use crate::error::Error;

/// Waits until the source server is ready, by doing health checks
pub(crate) async fn wait_until_source_ready(
    cln_token: &CancellationToken,
    client: &mut SourceClient<Channel>,
) -> error::Result<()> {
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

/// Waits until the sink server is ready, by doing health checks
pub(crate) async fn wait_until_sink_ready(
    cln_token: &CancellationToken,
    client: &mut SinkClient<Channel>,
) -> error::Result<()> {
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

/// Waits until the transformer server is ready, by doing health checks
pub(crate) async fn wait_until_transformer_ready(
    cln_token: &CancellationToken,
    client: &mut SourceTransformClient<Channel>,
) -> error::Result<()> {
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

/// Waits until the mapper server is ready, by doing health checks
pub(crate) async fn wait_until_mapper_ready(
    cln_token: &CancellationToken,
    client: &mut MapClient<Channel>,
) -> error::Result<()> {
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
        info!("Waiting for mapper client to be ready...");
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
    const MAX_RECONNECT_ATTEMPTS: usize = 60;

    let interval = fixed::Interval::from_millis(RECONNECT_INTERVAL).take(MAX_RECONNECT_ATTEMPTS);

    let channel = Retry::retry(
        interval,
        || async {
            match connect_with_uds(socket_path.clone()).await {
                Ok(channel) => Ok(channel),
                Err(e) => {
                    warn!(?e, "Failed to connect to UDS socket");
                    Err(Error::Connection(format!("Failed to connect: {:?}", e)))
                }
            }
        },
        |_: &Error| true,
    )
    .await?;
    Ok(channel)
}

/// Connects to the UDS socket and returns a channel
pub(crate) async fn connect_with_uds(uds_path: PathBuf) -> error::Result<Channel> {
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

pub(crate) fn utc_from_timestamp(t: Option<Timestamp>) -> DateTime<Utc> {
    t.map_or(Utc.timestamp_nanos(-1), |t| {
        DateTime::from_timestamp(t.seconds, t.nanos as u32).unwrap_or(Utc.timestamp_nanos(-1))
    })
}
