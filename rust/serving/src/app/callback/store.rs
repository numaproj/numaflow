use std::sync::Arc;

use thiserror::Error;

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
/// Represents the current processing status of a request id in the `Store`.
pub(crate) enum ProcessingStatus {
    InProgress,
    Completed(Vec<Vec<u8>>),
}

#[derive(Error, Debug, Clone)]
pub(crate) enum Error {
    #[error("Connecting to the store: {0}")]
    Connection(String),

    #[error("Request id {0} doesn't exist in store")]
    InvalidRequestId(String),

    #[error("Request id {0} already exists in the store")]
    DuplicateRequest(String),

    #[error("Reading from the store: {0}")]
    StoreRead(String),

    #[error("Writing payload to the store: {0}")]
    StoreWrite(String),
}

impl From<Error> for crate::Error {
    fn from(value: Error) -> Self {
        crate::Error::Store(value.to_string())
    }
}

pub(crate) type Result<T> = std::result::Result<T, Error>;

/// Store trait to store the callback information.
#[trait_variant::make(Store: Send)]
#[allow(dead_code)]
pub(crate) trait LocalStore {
    /// Register a request id in the store. If user provides a request id, the same should be returned
    /// if it doesn't already exist in the store. An error should be returned if the user-specified request id
    /// already exists in the store. If the `id` is `None`, the store should generate a new unique request id.
    async fn register(&mut self, id: Option<String>) -> Result<String>;
    /// This method will be called when processing is completed for a request id.
    async fn done(&mut self, id: String) -> Result<()>;
    async fn save(&mut self, messages: Vec<PayloadToSave>) -> Result<()>;
    /// retrieve the callback payloads
    async fn retrieve_callbacks(&mut self, id: &str) -> Result<Vec<Arc<Callback>>>;
    async fn retrieve_datum(&mut self, id: &str) -> Result<ProcessingStatus>;
    async fn ready(&mut self) -> bool;
}
