use numaflow_throttling::state::Store;
use numaflow_throttling::state::store::in_memory_store::InMemoryStore;
use numaflow_throttling::state::store::redis_store::RedisStore;
use numaflow_throttling::{RateLimit, RateLimiter};
use std::time::Duration;

// pub(crate) trait NumaflowTypeConfig: Send + Sync + Clone + 'static {
//     type Throttling: NumaflowThrottling;
// }
//
// pub(crate) trait NumaflowThrottling: Send + Sync + Clone + 'static {
//     type RateLimit: RateLimiter;
//     type ThrottlingStore: Store;
// }
//
// #[derive(Clone)]
// pub(crate) struct InmemoryThrottlingConfig {}
//
// impl NumaflowThrottling for InmemoryThrottlingConfig {
//     type RateLimit = numaflow_throttling::RateLimit<
//         numaflow_throttling::WithDistributedState<Self::ThrottlingStore>,
//     >;
//     type ThrottlingStore = InMemoryStore;
// }
//
// #[derive(Clone)]
// pub(crate) struct RedisThrottlingConfig {}
//
// impl NumaflowThrottling for RedisThrottlingConfig {
//     type RateLimit = numaflow_throttling::RateLimit<
//         numaflow_throttling::WithDistributedState<Self::ThrottlingStore>,
//     >;
//     type ThrottlingStore = RedisStore;
// }
//
// #[derive(Clone)]
// pub(crate) struct NumaflowConfigWithInmemoryThrottling {}
//
// impl NumaflowTypeConfig for NumaflowConfigWithInmemoryThrottling {
//     type Throttling = InmemoryThrottlingConfig;
// }
//
// #[derive(Clone)]
// pub(crate) struct NumaflowConfigRedisThrottling {}
//
// impl NumaflowTypeConfig for NumaflowConfigRedisThrottling {
//     type Throttling = RedisThrottlingConfig;
// }
//
// #[derive(Clone)]
// pub(crate) struct NumaflowConfig<T: NumaflowThrottling> {
//     pub(crate) throttling_config: Option<T>,
// }
//
// impl<T: NumaflowThrottling> NumaflowTypeConfig for NumaflowConfig<T> {
//     type Throttling = T;
// }
//
// pub(crate) fn default_numaflow_config() -> NumaflowConfig<NumaflowConfigWithInmemoryThrottling> {
//     NumaflowConfig {
//         throttling_config: Some(NumaflowConfigWithInmemoryThrottling {}),
//     }
// }

pub(crate) trait NumaflowTypeConfig: Send + Sync + Clone + 'static {
    type RateLimit: RateLimiter + Send + Sync + Clone + 'static;
    type ThrottlingStore: Store;
}

#[derive(Clone)]
pub(crate) struct NumaflowConfigWithInmemoryThrottling {}

impl NumaflowTypeConfig for NumaflowConfigWithInmemoryThrottling {
    type RateLimit = RateLimit<numaflow_throttling::WithDistributedState<Self::ThrottlingStore>>;
    type ThrottlingStore = InMemoryStore;
}

#[derive(Clone)]
pub(crate) struct NumaflowConfig<T: NumaflowTypeConfig + Send + Sync + Clone + 'static> {
    pub(crate) throttling_config: Option<T::RateLimit>,
}

pub(crate) async fn default_numaflow_config() -> NumaflowConfig<NumaflowConfigWithInmemoryThrottling>
{
    let store = InMemoryStore::new();
    let rate_limit = RateLimit::<numaflow_throttling::WithDistributedState<InMemoryStore>>::new(
        numaflow_throttling::TokenCalcBounds::default(),
        store,
        "processor_1",
        tokio_util::sync::CancellationToken::new(),
        Duration::from_millis(100),
        numaflow_throttling::state::OptimisticValidityUpdateSecs::default(),
    )
    .await
    .unwrap();

    NumaflowConfig {
        throttling_config: Some(rate_limit),
    }
}
