//! The callback store stores the progress on the processing (i.e., each state at the vertex) to
//! build the processing graph which is used to check whether the processing is complete or not.
//! The progress as the message moves from one vertex to another is **streamed** and the serving
//! host that accepted that message from the client will be **watching** that stream of callbacks.
use std::sync::Arc;

use bytes::Bytes;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::app::store::datastore::Result as StoreResult;
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

const IN_PROGRESS: &str = "in-progress";
const COMPLETED_PREFIX: &str = "completed:"; // prefix because we encode extra data after `:`
const FAILED_PREFIX: &str = "failed:";

impl From<Bytes> for ProcessingStatus {
    fn from(value: Bytes) -> Self {
        if value == IN_PROGRESS {
            ProcessingStatus::InProgress
        } else if value.starts_with(COMPLETED_PREFIX.as_ref()) {
            let subgraph = String::from_utf8(value[COMPLETED_PREFIX.len()..].to_vec())
                .expect("Invalid UTF-8 sequence");
            ProcessingStatus::Completed(subgraph)
        } else if value.starts_with(FAILED_PREFIX.as_ref()) {
            let error = String::from_utf8(value[FAILED_PREFIX.len()..].to_vec())
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
            ProcessingStatus::InProgress => Bytes::from_static(IN_PROGRESS.as_ref()),
            ProcessingStatus::Completed(subgraph) => {
                Bytes::from(format!("{}{}", COMPLETED_PREFIX, subgraph))
            }
            ProcessingStatus::Failed(error) => Bytes::from(format!("{}{}", FAILED_PREFIX, error)),
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
    /// watch the callback payloads for a given request id to determine whether the
    /// request is complete.
    async fn watch_callbacks(
        &mut self,
        id: &str,
    ) -> StoreResult<(ReceiverStream<Arc<Callback>>, JoinHandle<()>)>;
    /// retrieve the processing status of a request id.
    async fn status(&mut self, id: &str) -> StoreResult<ProcessingStatus>;
    /// check if the store is ready
    async fn ready(&mut self) -> bool;
}
