use numaflow_pb::clients::serving::serving_store_client::ServingStoreClient;
use numaflow_pb::clients::serving::{Payload, PutRequest};
use tonic::transport::Channel;

use crate::config::pipeline::UserDefinedStoreConfig;
use crate::shared;

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
        id: &str,
        origin: &str,
        payload: Vec<u8>,
    ) -> crate::Result<()> {
        let request = PutRequest {
            id: id.to_string(),
            payloads: vec![Payload {
                origin: origin.to_string(),
                value: payload.to_vec(),
            }],
        };
        self.client.put(request).await.map_err(|e| {
            crate::Error::Sink(format!("gRPC Put request failed on serving store: {e:?}"))
        })?;
        Ok(())
    }
}
