//! The callback store stores the progress on the processing (i.e., each state at the vertex) to
//! build the processing graph which is used to check whether the processing is complete or not.
//! The progress as the message moves from one vertex to another is **streamed** and the serving
//! host that accepted that message from the client will be **watching** that stream of callbacks.
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;

use crate::app::store::datastore::Result as StoreResult;
use crate::callback::Callback;
use crate::config::RequestType;

/// JetStream based callback store
pub(crate) mod jetstreamstore;

/// In-memory based callback store
pub(crate) mod memstore;

/// Store trait to store the callback information.
#[trait_variant::make(CallbackStore: Send)]
#[allow(dead_code)]
pub(crate) trait LocalCallbackStore {
    /// This method will be called when processing is completed for a request id.
    async fn deregister(&mut self, id: &str) -> StoreResult<()>;
    /// watch the callback payloads for a given request id to determine whether the
    /// request is complete.
    async fn register_and_watch(
        &mut self,
        id: &str,
        request_type: RequestType,
        pod_hash: &str,
    ) -> StoreResult<ReceiverStream<Arc<Callback>>>;
    /// check if the store is ready
    async fn ready(&mut self) -> bool;
}
