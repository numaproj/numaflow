use numaflow_pb::clients::serving::serving_store_client::ServingStoreClient;
use numaflow_pb::clients::serving::{Payload, PutRequest};
use tokio::task::JoinSet;
use tonic::transport::Channel;

use crate::config::pipeline::UserDefinedStoreConfig;
use crate::shared;
use crate::sinker::sink::serve::StoreEntry;

/// User defined serving store to store the serving responses.
#[derive(Clone)]
pub(crate) struct UserDefinedStore {
    client: ServingStoreClient<Channel>,
}

impl UserDefinedStore {
    /// Create a new user defined serving store.
    pub(crate) async fn new(config: UserDefinedStoreConfig) -> crate::Result<Self> {
        let channel = shared::grpc::create_rpc_channel(config.socket_path.into()).await?;
        let client = ServingStoreClient::new(channel);
        Ok(Self { client })
    }

    /// Puts a datum into the serving store.
    pub(crate) async fn put_datum(
        &mut self,
        origin: &str,
        payloads: Vec<StoreEntry>,
    ) -> crate::Result<()> {
        let mut jhset = JoinSet::new();

        for payload in payloads {
            let origin = origin.to_string();
            let mut client = self.client.clone();

            jhset.spawn(async move {
                let request = PutRequest {
                    id: payload.id,
                    payloads: vec![Payload {
                        origin: origin.clone(),
                        value: payload.value.to_vec(),
                    }],
                };
                client.put(request).await.map_err(|e| {
                    crate::Error::Sink(format!("gRPC Put request failed on serving store: {e:?}"))
                })
            });
        }

        while let Some(task) = jhset.join_next().await {
            let result = task.map_err(|e| crate::Error::Sink(format!("Task failed: {e:?}")))?;
            result?; // Propagate the first error, if any
        }

        Ok(())
    }

    pub(crate) fn get_store_client(&self) -> ServingStoreClient<Channel> {
        self.client.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::pipeline::UserDefinedStoreConfig;
    use bytes::Bytes;
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
            // Implement the put logic for testing
            data_map.insert(data.id, data.payloads);
        }

        async fn get(&self, id: String) -> serving_store::Data {
            let data_map = self.store.lock().unwrap();
            // Implement the get logic for testing
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

        let server_handle = tokio::spawn(async move {
            serving_store::Server::new(TestStore {
                store: Arc::new(Mutex::new(HashMap::new())),
            })
            .with_socket_file(server_socket)
            .with_server_info_file(server_info)
            .start_with_shutdown(shutdown_rx)
            .await
            .expect("failed to start sink server");
        });

        // wait for the server to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let config = UserDefinedStoreConfig {
            grpc_max_message_size: 4 * 1024 * 1024,
            socket_path: sock_file.to_str().unwrap().to_string(),
            server_info_path: server_info_file.to_str().unwrap().to_string(),
        };
        let mut store = UserDefinedStore::new(config).await.unwrap();
        let id = "test_id";
        let origin = "test_origin";
        let payloads = vec![StoreEntry {
            pod_hash: "test_pod_hash".to_string(),
            id: id.to_string(),
            value: Bytes::from("test_value"),
        }];
        let result = store.put_datum(origin, payloads).await;
        assert!(result.is_ok());

        drop(store);
        shutdown_tx.send(()).unwrap();
        server_handle.await.unwrap();
    }
}
