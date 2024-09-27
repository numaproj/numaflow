use std::path::PathBuf;

use crate::error::Error;
use backoff::retry::Retry;
use backoff::strategy::fixed;
use chrono::{DateTime, TimeZone, Timelike, Utc};
use prost_types::Timestamp;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

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

pub(crate) async fn connect_with_uds(uds_path: PathBuf) -> Result<Channel, Error> {
    let channel = Endpoint::try_from("http://[::]:50051")
        .map_err(|e| Error::ConnectionError(format!("Failed to create endpoint: {:?}", e)))?
        .connect_with_connector(service_fn(move |_: Uri| {
            let uds_socket = uds_path.clone();
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    UnixStream::connect(uds_socket).await?,
                ))
            }
        }))
        .await
        .map_err(|e| Error::ConnectionError(format!("Failed to connect: {:?}", e)))?;
    Ok(channel)
}
