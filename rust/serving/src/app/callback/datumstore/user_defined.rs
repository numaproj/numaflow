use backoff::retry::Retry;
use backoff::strategy::fixed;
use bytes::Bytes;
use http::Uri;
use numaflow_pb::clients::serving::serving_store_client::ServingStoreClient;
use numaflow_pb::clients::serving::GetRequest;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::UnixStream;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;
use tracing::warn;

use crate::app::callback::datumstore::{DatumStore, Error as StoreError, Result as StoreResult};
use crate::config::UserDefinedStoreConfig;

#[derive(Clone)]
pub(crate) struct UserDefinedStore {
    client: ServingStoreClient<Channel>,
}

impl UserDefinedStore {
    pub(crate) async fn new(config: UserDefinedStoreConfig) -> crate::Result<Self> {
        let channel = create_rpc_channel(config.socket_path.into()).await?;
        let client = ServingStoreClient::new(channel)
            .max_encoding_message_size(config.grpc_max_message_size)
            .max_decoding_message_size(config.grpc_max_message_size);
        Ok(Self { client })
    }
}

impl DatumStore for UserDefinedStore {
    // FIXME(serving): we need to return the origin details along with the payload
    async fn retrieve_datum(&mut self, id: &str) -> StoreResult<Option<Vec<Vec<u8>>>> {
        let request = GetRequest { id: id.to_string() };
        let response = self
            .client
            .get(request)
            .await
            .map_err(|e| StoreError::StoreRead(format!("gRPC Get request failed: {e:?}")))?;
        let payloads = response.into_inner().payloads;
        if payloads.is_empty() {
            Ok(None)
        } else {
            Ok(Some(payloads.iter().map(|p| p.value.clone()).collect()))
        }
    }

    async fn stream_response(
        &mut self,
        _id: &str,
    ) -> StoreResult<(ReceiverStream<Arc<Bytes>>, JoinHandle<()>)> {
        unimplemented!("stream_response is not supported for UserDefinedStore")
    }

    async fn ready(&mut self) -> bool {
        match self.client.is_ready(tonic::Request::new(())).await {
            Ok(response) => response.into_inner().ready,
            Err(_) => false,
        }
    }
}

pub(crate) async fn create_rpc_channel(socket_path: PathBuf) -> StoreResult<Channel> {
    const RECONNECT_INTERVAL: u64 = 1000;
    const MAX_RECONNECT_ATTEMPTS: usize = usize::MAX;

    let interval = fixed::Interval::from_millis(RECONNECT_INTERVAL).take(MAX_RECONNECT_ATTEMPTS);
    let channel = Retry::retry(
        interval,
        || async {
            match connect_with_uds(socket_path.clone()).await {
                Ok(channel) => Ok(channel),
                Err(e) => {
                    warn!(?e, "Failed to connect to UDS socket");
                    Err(StoreError::Connection(format!(
                        "Failed to connect: {:?}",
                        e
                    )))
                }
            }
        },
        |_: &StoreError| true,
    )
    .await
    .map_err(|e| StoreError::Connection(format!("Failed to connect: {:?}", e)))?;
    Ok(channel)
}

/// Connects to the UDS socket and returns a channel
pub(crate) async fn connect_with_uds(uds_path: PathBuf) -> StoreResult<Channel> {
    let channel = Endpoint::try_from("http://[::]:50051")
        .map_err(|e| StoreError::Connection(format!("Failed to create endpoint: {:?}", e)))?
        .connect_with_connector(service_fn(move |_: Uri| {
            let uds_socket = uds_path.clone();
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    UnixStream::connect(uds_socket).await?,
                ))
            }
        }))
        .await
        .map_err(|e| StoreError::Connection(format!("Failed to connect: {:?}", e)))?;
    Ok(channel)
}
