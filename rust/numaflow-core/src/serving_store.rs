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
    pub(crate) pod_hash: String,
    pub(crate) id: String,
    pub(crate) value: Bytes,
}
