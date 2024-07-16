use std::sync::Arc;

use crate::app::callback::CallbackRequest;

// in-memory store
pub(crate) mod memstore;
// redis as the store
pub(crate) mod redisstore;

pub(crate) enum PayloadToSave {
    /// Callback as sent by Numaflow to track the progression
    Callback {
        key: String,
        value: Arc<CallbackRequest>,
    },
    /// Data sent by the Numaflow pipeline which is to be delivered as the response
    DatumFromPipeline {
        key: String,
        value: axum::body::Bytes,
    },
}

/// Store trait to store the callback information.
#[trait_variant::make(Store: Send)]
#[allow(dead_code)]
pub(crate) trait LocalStore {
    async fn save(&mut self, messages: Vec<PayloadToSave>) -> crate::Result<()>;
    /// retrieve the callback payloads
    async fn retrieve_callbacks(
        &mut self,
        id: &str,
    ) -> Result<Vec<Arc<CallbackRequest>>, crate::Error>;
    async fn retrieve_datum(&mut self, id: &str) -> Result<Vec<Vec<u8>>, crate::Error>;
    async fn ready(&mut self) -> bool;
}
