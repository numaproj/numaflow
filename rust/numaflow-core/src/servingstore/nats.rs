use crate::config::pipeline::NatsStoreConfig;
use async_nats::jetstream::Context;

#[derive(Clone)]
pub(crate) struct NatsObjectStore {}

impl NatsObjectStore {
    pub(crate) async fn new(
        js_context: Context,
        nats_store_config: NatsStoreConfig,
    ) -> crate::Result<Self> {
        Ok(Self {})
    }

    pub(crate) async fn put_datum(
        &mut self,
        id: &str,
        origin: &str,
        payload: Vec<u8>,
    ) -> crate::Result<()> {
        todo!()
    }
}
