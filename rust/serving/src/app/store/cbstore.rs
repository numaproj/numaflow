//! The callback store stores the progress on the processing (i.e., each state at the vertex) to
//! build the processing graph which is used to check whether the processing is complete or not.
//! The progress as the message moves from one vertex to another is **streamed** and the serving
//! host that accepted that message from the client will be **watching** that stream of callbacks.
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;

use crate::app::store::datastore::Result as StoreResult;
use crate::callback::Callback;
use crate::config::RequestType;

/// JetStream based callback store
pub(crate) mod jetstreamstore;

/// In-memory based callback store
pub(crate) mod memstore;

/// Represents the current processing status of a request id in the `Store`.
// Redefined ProcessingStatus - Merged Status and Replica Info
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) enum ProcessingStatus {
    InProgress {
        replica_id: String,
    },
    Completed {
        subgraph: String, // subgraph of the completed request
        replica_id: String,
    },
    Failed {
        error: String, // error message of the failed request
        replica_id: String,
    },
}

impl TryFrom<Bytes> for ProcessingStatus {
    type Error = serde_json::Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice(&value)
    }
}

impl TryFrom<ProcessingStatus> for Bytes {
    type Error = serde_json::Error;
    fn try_from(status: ProcessingStatus) -> Result<Bytes, serde_json::Error> {
        Ok(Bytes::from(serde_json::to_vec(&status)?))
    }
}

/// Store trait to store the callback information.
#[trait_variant::make(CallbackStore: Send)]
#[allow(dead_code)]
pub(crate) trait LocalCallbackStore {
    /// This method will be called when processing is completed for a request id.
    async fn deregister(&mut self, id: &str, sub_graph: &str) -> StoreResult<()>;
    /// This method will be called when processing is failed for a request id.
    async fn mark_as_failed(&mut self, id: &str, error: &str) -> StoreResult<()>;
    /// watch the callback payloads for a given request id to determine whether the
    /// request is complete.
    async fn register_and_watch(
        &mut self,
        id: &str,
        request_type: RequestType,
    ) -> StoreResult<ReceiverStream<Arc<Callback>>>;
    /// retrieve the processing status of a request id.
    async fn status(&mut self, id: &str) -> StoreResult<ProcessingStatus>;
    /// check if the store is ready
    async fn ready(&mut self) -> bool;
}
