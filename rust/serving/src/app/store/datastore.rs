//! The results of the processing from the Sinks are stored in the Data Store.
use std::sync::Arc;

use bytes::Bytes;
use thiserror::Error;
use tokio_stream::wrappers::ReceiverStream;

/// JetStream based datum store
pub(crate) mod jetstream;

/// in-memory based datum store
pub(crate) mod inmemory;

/// user defined datum store
pub(crate) mod user_defined;

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

/// Store trait to get the result from the store.
#[trait_variant::make(DataStore: Send)]
#[allow(dead_code)]
pub(crate) trait LocalDataStore {
    /// retrieve the data from the store
    async fn retrieve_data(&mut self, id: &str) -> Result<Vec<Vec<u8>>>;
    /// streams the data from the store
    async fn stream_data(&mut self, id: &str) -> Result<ReceiverStream<Arc<Bytes>>>;
    /// check if the store is ready
    async fn ready(&mut self) -> bool;
}
