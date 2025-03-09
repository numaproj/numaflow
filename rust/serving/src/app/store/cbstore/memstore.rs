use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::app::store::datastore::{Error as StoreError, Result as StoreResult};
use crate::callback::Callback;

/// An in-memory implementation of the callback store. Only used for testing.
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct InMemoryCallbackStore {
    data: Arc<Mutex<HashMap<String, Vec<Arc<Callback>>>>>,
}

impl InMemoryCallbackStore {
    #[allow(dead_code)]
    pub(crate) fn new(callback_map: Option<HashMap<String, Vec<Arc<Callback>>>>) -> Self {
        Self {
            data: Arc::new(Mutex::new(callback_map.unwrap_or_default())),
        }
    }
}

impl super::CallbackStore for InMemoryCallbackStore {
    /// Register a request id in the store. If the `id` already exists in the store,
    /// `StoreError::DuplicateRequest` error is returned.
    async fn register(&mut self, id: &str) -> StoreResult<()> {
        let mut data = self.data.lock().await;
        if data.contains_key(id) {
            return Err(StoreError::DuplicateRequest(id.to_string()));
        }
        data.insert(id.to_string(), Vec::new());
        Ok(())
    }

    /// De-register a request id from the store. If the `id` does not exist in the store,
    /// `StoreError::InvalidRequestId` error is returned.
    async fn deregister(&mut self, id: &str, _sub_graph: &str) -> StoreResult<()> {
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

    async fn status(&mut self, _id: &str) -> StoreResult<super::ProcessingStatus> {
        let data = self.data.lock().await;
        if data.get(_id).is_none() {
            return Err(StoreError::InvalidRequestId(_id.to_string()));
        }
        Ok(super::ProcessingStatus::InProgress)
    }

    async fn ready(&mut self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use tokio_stream::StreamExt;

    use super::*;
    use crate::app::store::cbstore::{CallbackStore, ProcessingStatus};
    use crate::callback::Callback;

    fn create_test_store() -> InMemoryCallbackStore {
        let mut callback_map: HashMap<String, Vec<Arc<Callback>>> = HashMap::new();
        let callback = Arc::new(Callback {
            id: "test_id".to_string(),
            vertex: "vertex".to_string(),
            cb_time: 12345,
            from_vertex: "from_vertex".to_string(),
            responses: vec![],
        });
        callback_map.insert("test_id".to_string(), vec![callback]);
        InMemoryCallbackStore::new(Some(callback_map))
    }

    #[tokio::test]
    async fn test_register() {
        let mut store = create_test_store();
        let id = "new_test_id";
        let result = store.register(id).await;
        assert!(result.is_ok());

        // Try to register the same id again, should return an error
        let result = store.register(id).await;
        assert!(matches!(result, Err(StoreError::DuplicateRequest(_))));
    }

    #[tokio::test]
    async fn test_deregister() {
        let mut store = create_test_store();
        let id = "test_id";

        let result = store.deregister(id, "sub_graph").await;
        assert!(result.is_ok());

        // Try to deregister the same id again, should return an error
        let result = store.deregister(id, "sub_graph").await;
        assert!(matches!(result, Err(StoreError::InvalidRequestId(_))));
    }

    #[tokio::test]
    async fn test_watch_callbacks() {
        let mut store = create_test_store();
        let id = "test_id";

        let (mut rx, _handle) = store.watch_callbacks(id).await.unwrap();
        let received_callback = rx.next().await.unwrap();
        assert_eq!(received_callback.id, "test_id");
        assert_eq!(received_callback.vertex, "vertex");
        assert_eq!(received_callback.cb_time, 12345);
        assert_eq!(received_callback.from_vertex, "from_vertex");
    }

    #[tokio::test]
    async fn test_status() {
        let mut store = create_test_store();
        let id = "test_id";

        let status = store.status(id).await.unwrap();
        assert!(matches!(status, ProcessingStatus::InProgress));
    }
}
