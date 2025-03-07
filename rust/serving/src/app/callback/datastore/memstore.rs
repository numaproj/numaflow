use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::app::callback::datastore::{Error as StoreError, Result as StoreResult};
const STORE_KEY_SUFFIX: &str = "saved";

/// In-memory implementation of data store.
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct InMemoryDataStore {
    /// The data field is a `HashMap` where the key is a `String` and the value is a `Vec<Vec<u8>>`.
    /// It is wrapped in an `Arc<Mutex<_>>` to allow shared ownership and thread safety.
    pub(crate) data: Arc<tokio::sync::Mutex<HashMap<String, Vec<Vec<u8>>>>>,
}

impl InMemoryDataStore {
    /// Creates a new `InMemoryStore` with an empty `HashMap`.
    #[allow(dead_code)]
    pub(crate) fn new(datum_map: Option<HashMap<String, Vec<Vec<u8>>>>) -> Self {
        Self {
            data: Arc::new(datum_map.unwrap_or_default().into()),
        }
    }
}

impl super::DataStore for InMemoryDataStore {
    /// Retrieves data for a given id from the `HashMap`.
    /// Each piece of data is deserialized from bytes into a `String`.
    async fn retrieve_data(&mut self, id: &str) -> StoreResult<Option<Vec<Vec<u8>>>> {
        let id = format!("{id}_{STORE_KEY_SUFFIX}");
        let data = self.data.lock().await;
        match data.get(&id) {
            Some(result) => Ok(Some(result.to_vec())),
            None => Err(StoreError::InvalidRequestId(format!(
                "No entry found for id: {}",
                id
            ))),
        }
    }

    /// Streams the responses for a given id from the `HashMap`.
    async fn stream_data(
        &mut self,
        id: &str,
    ) -> StoreResult<(ReceiverStream<Arc<Bytes>>, JoinHandle<()>)> {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let data = self.data.lock().await;
        if let Some(response) = data.get(id) {
            for datum in response {
                tx.send(Arc::new(Bytes::from(datum.clone())))
                    .await
                    .map_err(|_| StoreError::StoreRead("Failed to send datum".to_string()))?;
            }
        }
        Ok((ReceiverStream::new(rx), tokio::task::spawn(async {})))
    }

    async fn ready(&mut self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use tokio_stream::StreamExt;

    use super::*;
    use crate::app::callback::datastore::memstore::InMemoryDataStore;
    use crate::app::callback::datastore::DataStore;

    fn create_test_store() -> InMemoryDataStore {
        let mut datum_map: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        datum_map.insert("test_id_saved".to_string(), vec![b"test_payload".to_vec()]);
        InMemoryDataStore::new(Some(datum_map))
    }

    #[tokio::test]
    async fn test_retrieve_datum() {
        let mut store = create_test_store();
        let id = "test_id";
        let result = store.retrieve_data(id).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap()[0], b"test_payload");
    }

    #[tokio::test]
    async fn test_retrieve_datum_not_found() {
        let mut store = create_test_store();
        let id = "non_existent_id";
        let result = store.retrieve_data(id).await;
        assert!(matches!(result, Err(StoreError::InvalidRequestId(_))));
    }

    #[tokio::test]
    async fn test_stream_response() {
        let mut store = create_test_store();
        let id = "test_id_saved";
        let (mut rx, _handle) = store.stream_data(id).await.unwrap();
        let received_response = rx.next().await.unwrap();
        assert_eq!(
            received_response,
            Arc::new(Bytes::from_static(b"test_payload"))
        );
    }
}
