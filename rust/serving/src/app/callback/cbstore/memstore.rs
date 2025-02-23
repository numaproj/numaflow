use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::app::callback::cbstore::LocalCallbackStore;
use crate::app::callback::datumstore::{Error as StoreError, Result as StoreResult};
use crate::app::callback::Callback;

#[derive(Clone)]
pub(crate) struct InMemoryCallbackStore {
    data: Arc<Mutex<HashMap<String, Vec<Arc<Callback>>>>>,
}

impl InMemoryCallbackStore {
    pub(crate) fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl LocalCallbackStore for InMemoryCallbackStore {
    async fn register(&mut self, id: &str) -> StoreResult<()> {
        let mut data = self.data.lock().await;
        if data.contains_key(id) {
            return Err(StoreError::DuplicateRequest(id.to_string()));
        }
        data.insert(id.to_string(), Vec::new());
        Ok(())
    }

    async fn deregister(&mut self, id: &str, sub_graph: &str) -> StoreResult<()> {
        let mut data = self.data.lock().await;
        if data.remove(id).is_none() {
            return Err(StoreError::InvalidRequestId(id.to_string()));
        }
        Ok(())
    }

    async fn mark_as_failed(&mut self, _id: &str, _error: &str) -> StoreResult<()> {
        Ok(())
    }

    async fn watch_callbacks(
        &mut self,
        id: &str,
    ) -> StoreResult<(ReceiverStream<Arc<Callback>>, JoinHandle<()>)> {
        let (tx, rx) = mpsc::channel(10);
        let data = self.data.lock().await;
        if let Some(callbacks) = data.get(id) {
            for callback in callbacks {
                tx.send(Arc::clone(callback))
                    .await
                    .map_err(|_| StoreError::StoreRead("Failed to send callback".to_string()))?;
            }
        } else {
            return Err(StoreError::InvalidRequestId(id.to_string()));
        }
        Ok((ReceiverStream::new(rx), tokio::spawn(async {})))
    }

    async fn ready(&mut self) -> bool {
        true
    }

    async fn status(&mut self, id: &str) -> StoreResult<super::ProcessingStatus> {
        unimplemented!();
    }
}
