use crate::app::callback::datumstore::{Error as StoreError, Result as StoreResult};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
const STORE_KEY_SUFFIX: &str = "saved";

/// `InMemoryStore` is an in-memory implementation of the `Store` trait.
/// It uses a `HashMap` to store data in memory.
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct InMemoryDatumStore {
    /// The data field is a `HashMap` where the key is a `String` and the value is a `Vec<Vec<u8>>`.
    /// It is wrapped in an `Arc<Mutex<_>>` to allow shared ownership and thread safety.
    pub(crate) data: Arc<tokio::sync::Mutex<HashMap<String, Vec<Vec<u8>>>>>,
}

impl InMemoryDatumStore {
    /// Creates a new `InMemoryStore` with an empty `HashMap`.
    #[allow(dead_code)]
    pub(crate) fn new(datum_map: Option<HashMap<String, Vec<Vec<u8>>>>) -> Self {
        Self {
            data: Arc::new(datum_map.unwrap_or_default().into()),
        }
    }
}

impl super::DatumStore for InMemoryDatumStore {
    /// Retrieves data for a given id from the `HashMap`.
    /// Each piece of data is deserialized from bytes into a `String`.
    async fn retrieve_datum(&mut self, id: &str) -> StoreResult<Option<Vec<Vec<u8>>>> {
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
    async fn stream_response(
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
    use super::*;
    use crate::app::callback::datumstore::memstore::InMemoryDatumStore;
    use crate::app::callback::datumstore::DatumStore;
    use bytes::Bytes;
    use std::collections::HashMap;
    use tokio_stream::StreamExt;

    fn create_test_store() -> InMemoryDatumStore {
        let mut datum_map: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
        datum_map.insert("test_id_saved".to_string(), vec![b"test_payload".to_vec()]);
        InMemoryDatumStore::new(Some(datum_map))
    }

    #[tokio::test]
    async fn test_retrieve_datum() {
        let mut store = create_test_store();
        let id = "test_id";
        let result = store.retrieve_datum(id).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap()[0], b"test_payload");
    }

    #[tokio::test]
    async fn test_retrieve_datum_not_found() {
        let mut store = create_test_store();
        let id = "non_existent_id";
        let result = store.retrieve_datum(id).await;
        assert!(matches!(result, Err(StoreError::InvalidRequestId(_))));
    }

    #[tokio::test]
    async fn test_stream_response() {
        let mut store = create_test_store();
        let id = "test_id_saved";
        let (mut rx, _handle) = store.stream_response(id).await.unwrap();
        let received_response = rx.next().await.unwrap();
        assert_eq!(
            received_response,
            Arc::new(Bytes::from_static(b"test_payload"))
        );
    }
}
