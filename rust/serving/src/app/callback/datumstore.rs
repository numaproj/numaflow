use std::sync::Arc;

use bytes::Bytes;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

// in-memory store
pub(crate) mod jetstreamstore;
pub(crate) mod memstore;
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

/// Store trait to datastore the callback information.
#[trait_variant::make(DatumStore: Send)]
#[allow(dead_code)]
pub(crate) trait LocalDatumStore {
    /// retrieve the data from the store
    async fn retrieve_datum(&mut self, id: &str) -> Result<Option<Vec<Vec<u8>>>>;
    /// streams the responses
    async fn stream_response(
        &mut self,
        id: &str,
    ) -> Result<(ReceiverStream<Arc<Bytes>>, JoinHandle<()>)>;
    /// check if the store is ready
    async fn ready(&mut self) -> bool;
}
