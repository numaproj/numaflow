use std::path::PathBuf;
use std::sync::Arc;

use backoff::retry::Retry;
use backoff::strategy::fixed;
use bytes::Bytes;
use http::Uri;
use numaflow_pb::clients::serving::GetRequest;
use numaflow_pb::clients::serving::serving_store_client::ServingStoreClient;
use tokio::net::UnixStream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;
use tracing::warn;

use crate::app::store::datastore::{DataStore, Error as StoreError, Result as StoreResult};
use crate::config::UserDefinedStoreConfig;

/// A user-defined implementation of the datum store.
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

impl DataStore for UserDefinedStore {
    // FIXME(serving): we need to return the origin details along with the payload
    async fn retrieve_data(
        &mut self,
        id: &str,
        _pod_hash: Option<String>,
    ) -> StoreResult<Vec<Vec<u8>>> {
        let request = GetRequest { id: id.to_string() };
        let response = self
            .client
            .get(request)
            .await
            .map_err(|e| StoreError::StoreRead(format!("gRPC Get request failed: {e:?}")))?;
        let payloads = response.into_inner().payloads;
        Ok(payloads.iter().map(|p| p.value.clone()).collect())
    }

    async fn stream_data(
        &mut self,
        _id: &str,
        _pod_hash: Option<String>,
    ) -> StoreResult<ReceiverStream<Arc<Bytes>>> {
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
    let channel = Retry::new(
        interval,
        async || match connect_with_uds(socket_path.clone()).await {
            Ok(channel) => Ok(channel),
            Err(e) => {
                warn!(error = ?e, ?socket_path, "Failed to connect to UDS socket");
                Err(StoreError::Connection(format!(
                    "Failed to connect to uds socket {socket_path:?}: {e:?}"
                )))
            }
        },
        |_: &StoreError| true,
    )
    .await
    .map_err(|e| StoreError::Connection(format!("Failed to connect: {e:?}")))?;
    Ok(channel)
}

/// Connects to the UDS socket and returns a channel
pub(crate) async fn connect_with_uds(uds_path: PathBuf) -> StoreResult<Channel> {
    let channel = Endpoint::try_from("http://[::1]:50051")
        .map_err(|e| StoreError::Connection(format!("Failed to create endpoint: {e:?}")))?
        .connect_with_connector(service_fn(move |_: Uri| {
            let uds_socket = uds_path.clone();
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    UnixStream::connect(uds_socket).await?,
                ))
            }
        }))
        .await
        .map_err(|e| StoreError::Connection(format!("Failed to connect: {e:?}")))?;
    Ok(channel)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::UserDefinedStoreConfig;
    use numaflow::serving_store;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    struct TestStore {
        store: Arc<Mutex<HashMap<String, Vec<serving_store::Payload>>>>,
    }

    #[tonic::async_trait]
    impl serving_store::ServingStore for TestStore {
        async fn put(&self, data: serving_store::Data) {
            let mut data_map = self.store.lock().unwrap();
            data_map.insert(data.id, data.payloads);
        }

        async fn get(&self, id: String) -> serving_store::Data {
            let data_map = self.store.lock().unwrap();
            let payloads = data_map.get(&id).cloned().unwrap_or_default();
            serving_store::Data { id, payloads }
        }
    }

    #[tokio::test]
    async fn test_user_defined_store() {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let sock_file = tmp_dir.path().join("serving.sock");
        let server_info_file = tmp_dir.path().join("serving-server-info");

        let server_info = server_info_file.clone();
        let server_socket = sock_file.clone();

        // Prepopulate the data
        let test_store = TestStore {
            store: Arc::new(Mutex::new(HashMap::new())),
        };
        let id = "test_id".to_string();
        let payload = vec![serving_store::Payload {
            origin: "test_origin".to_string(),
            value: vec![1, 2, 3],
        }];
        test_store
            .store
            .lock()
            .unwrap()
            .insert(id.clone(), payload.clone());

        let server_handle = tokio::spawn(async move {
            serving_store::Server::new(test_store)
                .with_socket_file(server_socket)
                .with_server_info_file(server_info)
                .start_with_shutdown(shutdown_rx)
                .await
                .expect("failed to start serving store server");
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let config = UserDefinedStoreConfig {
            grpc_max_message_size: 4 * 1024 * 1024,
            socket_path: sock_file.to_str().unwrap().to_string(),
            server_info_path: server_info_file.to_str().unwrap().to_string(),
        };
        let mut store = UserDefinedStore::new(config).await.unwrap();

        // Test is_ready
        assert!(store.ready().await);

        // Test retrieve_data
        let retrieved_data = store.retrieve_data(&id, None).await.unwrap();
        assert_eq!(retrieved_data, vec![payload[0].value.clone()]);

        drop(store);
        shutdown_tx.send(()).unwrap();
        server_handle.await.unwrap();
    }
}
