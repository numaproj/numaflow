use bytes::Bytes;

/// User defined serving store to store the serving responses.
pub(crate) mod user_defined;

/// Nats serving store to store the serving responses.
pub(crate) mod nats;

/// Enum to represent different types of serving stores.
#[derive(Clone)]
pub(crate) enum ServingStore {
    UserDefined(user_defined::UserDefinedStore),
    Nats(nats::NatsServingStore),
}

/// Entry in the serving store.
#[derive(Clone, Debug)]
pub(crate) struct StoreEntry {
    /// Pod Hash is for filtering the stream by each request originating pod while listening for
    /// "put" onto the KV store. This is used only on ISB KV store and this enables SSE (converse is
    /// that SSE is not supported on user defined store).
    pub(crate) pod_hash: String,
    /// Unique ID Of the request to which the result belongs. There could be multiple results for
    /// the same request.
    pub(crate) id: String,
    /// The result of the computation.
    pub(crate) value: Bytes,
}
