/// User defined serving store to store the serving responses.
pub(crate) mod user_defined;

/// Nats serving store to store the serving responses.
pub(crate) mod nats;

/// Enum to represent the serving store.
#[derive(Clone)]
pub(crate) enum ServingStore {
    UserDefined(user_defined::UserDefinedStore),
    Nats(nats::NatsObjectStore),
}
