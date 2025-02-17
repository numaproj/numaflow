use std::sync::Arc;

use crate::app::callback::Callback;

// in-memory store
pub(crate) mod memstore;
// redis as the store
pub(crate) mod redisstore;

pub(crate) enum PayloadToSave {
    /// Callback as sent by Numaflow to track the progression
    Callback { key: String, value: Arc<Callback> },
    /// Data sent by the Numaflow pipeline which is to be delivered as the response
    DatumFromPipeline {
        key: String,
        value: axum::body::Bytes,
    },
}

#[derive(Debug, PartialEq)]
pub(crate) enum PipelineResult {
    Processing,
    Completed(Vec<Vec<u8>>),
}

/// Store trait to store the callback information.
#[trait_variant::make(Store: Send)]
#[allow(dead_code)]
pub(crate) trait LocalStore {
    /// Register a request id in the store. If user provides a request id, the same will be returned
    /// if the same doesn't already exist in the store. An error is returned if the user-specified request id
    /// already exists in the store. If the `id` is `None`, the store will generate a new unique request id.
    async fn register(&mut self, id: Option<String>) -> crate::Result<String>;
    async fn deregister(&mut self, id: String) -> crate::Result<()>;
    async fn save(&mut self, messages: Vec<PayloadToSave>) -> crate::Result<()>;
    /// retrieve the callback payloads
    async fn retrieve_callbacks(&mut self, id: &str) -> Result<Vec<Arc<Callback>>, crate::Error>;
    async fn retrieve_datum(&mut self, id: &str) -> Result<PipelineResult, crate::Error>;
    async fn ready(&mut self) -> bool;
}
