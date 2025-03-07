use std::sync::Arc;

use bytes::Bytes;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::app::callback::datastore::Result as StoreResult;
use crate::callback::Callback;

/// jetstream based callback store
pub(crate) mod jetstreamstore;

/// In-memory based callback store
pub(crate) mod memstore;

/// Represents the current processing status of a request id in the `Store`.
#[derive(Debug, PartialEq)]
pub(crate) enum ProcessingStatus {
    InProgress,
    Completed(String), // subgraph of the completed request
    Failed(String),    // error message of the failed request
}

impl From<Bytes> for ProcessingStatus {
    fn from(value: Bytes) -> Self {
        let in_progress = Bytes::from_static(b"inprogress");
        let completed_prefix = b"completed:";
        let failed_prefix = b"failed:";

        if value == in_progress {
            ProcessingStatus::InProgress
        } else if value.starts_with(completed_prefix) {
            let subgraph = String::from_utf8(value[completed_prefix.len()..].to_vec())
                .expect("Invalid UTF-8 sequence");
            ProcessingStatus::Completed(subgraph)
        } else if value.starts_with(failed_prefix) {
            let error = String::from_utf8(value[failed_prefix.len()..].to_vec())
                .expect("Invalid UTF-8 sequence");
            ProcessingStatus::Failed(error)
        } else {
            panic!("Invalid processing status")
        }
    }
}

impl From<ProcessingStatus> for Bytes {
    fn from(value: ProcessingStatus) -> Self {
        match value {
            ProcessingStatus::InProgress => Bytes::from_static(b"inprogress"),
            ProcessingStatus::Completed(subgraph) => Bytes::from(format!("completed:{}", subgraph)),
            ProcessingStatus::Failed(error) => Bytes::from(format!("failed:{}", error)),
        }
    }
}

/// Store trait to store the callback information.
#[trait_variant::make(CallbackStore: Send)]
#[allow(dead_code)]
pub(crate) trait LocalCallbackStore {
    /// Register a request id in the store. If the `id` already exists in the store,
    /// `StoreError::DuplicateRequest` error is returned.
    async fn register(&mut self, id: &str) -> StoreResult<()>;
    /// This method will be called when processing is completed for a request id.
    async fn deregister(&mut self, id: &str, sub_graph: &str) -> StoreResult<()>;
    /// This method will be called when processing is failed for a request id.
    async fn mark_as_failed(&mut self, id: &str, error: &str) -> StoreResult<()>;
    /// retrieve the callback payloads for a given request id.
    async fn watch_callbacks(
        &mut self,
        id: &str,
    ) -> StoreResult<(ReceiverStream<Arc<Callback>>, JoinHandle<()>)>;
    /// retrieve the processing status of a request id.
    async fn status(&mut self, id: &str) -> StoreResult<ProcessingStatus>;
    /// check if the store is ready
    async fn ready(&mut self) -> bool;
}
