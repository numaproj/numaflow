use std::path::PathBuf;

use chrono::{DateTime, TimeZone, Timelike, Utc};
use prost_types::Timestamp;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

use crate::error::Error;

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
