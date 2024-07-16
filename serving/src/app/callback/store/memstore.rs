use std::collections::HashMap;
use std::sync::Arc;

use crate::app::callback::CallbackRequest;
use crate::consts::SAVED;
use crate::Error;

use super::PayloadToSave;

/// `InMemoryStore` is an in-memory implementation of the `Store` trait.
/// It uses a `HashMap` to store data in memory.
#[derive(Clone)]
pub(crate) struct InMemoryStore {
    /// The data field is a `HashMap` where the key is a `String` and the value is a `Vec<Vec<u8>>`.
    /// It is wrapped in an `Arc<Mutex<_>>` to allow shared ownership and thread safety.
    data: Arc<std::sync::Mutex<HashMap<String, Vec<Vec<u8>>>>>,
}

impl InMemoryStore {
    /// Creates a new `InMemoryStore` with an empty `HashMap`.
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self {
            data: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }
}

impl super::Store for InMemoryStore {
    /// Saves a vector of `PayloadToSave` into the `HashMap`.
    /// Each `PayloadToSave` is serialized into bytes and stored in the `HashMap` under its key.
    async fn save(&mut self, messages: Vec<PayloadToSave>) -> crate::Result<()> {
        let mut data = self.data.lock().unwrap();
        for msg in messages {
            match msg {
                PayloadToSave::Callback { key, value } => {
                    if key.is_empty() {
                        return Err(Error::StoreWrite("Key cannot be empty".to_string()));
                    }
                    let bytes = serde_json::to_vec(&*value)
                        .map_err(|e| Error::StoreWrite(format!("Serializing to bytes - {}", e)))?;
                    data.entry(key).or_default().push(bytes);
                }
                PayloadToSave::DatumFromPipeline { key, value } => {
                    if key.is_empty() {
                        return Err(Error::StoreWrite("Key cannot be empty".to_string()));
                    }
                    data.entry(format!("{}_{}", key, SAVED))
                        .or_default()
                        .push(value.into());
                }
            }
        }
        Ok(())
    }

    /// Retrieves callbacks for a given id from the `HashMap`.
    /// Each callback is deserialized from bytes into a `CallbackRequest`.
    async fn retrieve_callbacks(&mut self, id: &str) -> Result<Vec<Arc<CallbackRequest>>, Error> {
        let data = self.data.lock().unwrap();
        match data.get(id) {
            Some(result) => {
                let messages: Result<Vec<_>, _> = result
                    .iter()
                    .map(|msg| {
                        let cbr: CallbackRequest = serde_json::from_slice(msg).map_err(|_| {
                            Error::StoreRead(
                                "Failed to parse CallbackRequest from bytes".to_string(),
                            )
                        })?;
                        Ok(Arc::new(cbr))
                    })
                    .collect();
                messages
            }
            None => Err(Error::StoreRead(format!("No entry found for id: {}", id))),
        }
    }

    /// Retrieves data for a given id from the `HashMap`.
    /// Each piece of data is deserialized from bytes into a `String`.
    async fn retrieve_datum(&mut self, id: &str) -> Result<Vec<Vec<u8>>, Error> {
        let id = format!("{}_{}", id, SAVED);
        let data = self.data.lock().unwrap();
        match data.get(&id) {
            Some(result) => Ok(result.to_vec()),
            None => Err(Error::StoreRead(format!("No entry found for id: {}", id))),
        }
    }

    async fn ready(&mut self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::app::callback::store::{PayloadToSave, Store};
    use crate::app::callback::CallbackRequest;

    use super::*;

    #[tokio::test]
    async fn test_save_and_retrieve_callbacks() {
        let mut store = InMemoryStore::new();
        let key = "test_key".to_string();
        let value = Arc::new(CallbackRequest {
            id: "test_id".to_string(),
            vertex: "in".to_string(),
            cb_time: 12345,
            from_vertex: "in".to_string(),
            tags: None,
        });

        // Save a callback
        store
            .save(vec![PayloadToSave::Callback {
                key: key.clone(),
                value: value.clone(),
            }])
            .await
            .unwrap();

        // Retrieve the callback
        let retrieved = store.retrieve_callbacks(&key).await.unwrap();

        // Check that the retrieved callback is the same as the one we saved
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0].id, "test_id".to_string())
    }

    #[tokio::test]
    async fn test_save_and_retrieve_datum() {
        let mut store = InMemoryStore::new();
        let key = "test_key".to_string();
        let value = "test_value".to_string();

        // Save a datum
        store
            .save(vec![PayloadToSave::DatumFromPipeline {
                key: key.clone(),
                value: value.clone().into(),
            }])
            .await
            .unwrap();

        // Retrieve the datum
        let retrieved = store.retrieve_datum(&key).await.unwrap();

        // Check that the retrieved datum is the same as the one we saved
        assert_eq!(retrieved.len(), 1);
        assert_eq!(retrieved[0], value.as_bytes());
    }

    #[tokio::test]
    async fn test_retrieve_callbacks_no_entry() {
        let mut store = InMemoryStore::new();
        let key = "nonexistent_key".to_string();

        // Try to retrieve a callback for a key that doesn't exist
        let result = store.retrieve_callbacks(&key).await;

        // Check that an error is returned
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_retrieve_datum_no_entry() {
        let mut store = InMemoryStore::new();
        let key = "nonexistent_key".to_string();

        // Try to retrieve a datum for a key that doesn't exist
        let result = store.retrieve_datum(&key).await;

        // Check that an error is returned
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_save_invalid_callback() {
        let mut store = InMemoryStore::new();
        let value = Arc::new(CallbackRequest {
            id: "test_id".to_string(),
            vertex: "in".to_string(),
            cb_time: 12345,
            from_vertex: "in".to_string(),
            tags: None,
        });

        // Try to save a callback with an invalid key
        let result = store
            .save(vec![PayloadToSave::Callback {
                key: "".to_string(),
                value: value.clone(),
            }])
            .await;

        // Check that an error is returned
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_save_invalid_datum() {
        let mut store = InMemoryStore::new();

        // Try to save a datum with an invalid key
        let result = store
            .save(vec![PayloadToSave::DatumFromPipeline {
                key: "".to_string(),
                value: "test_value".into(),
            }])
            .await;

        // Check that an error is returned
        assert!(result.is_err());
    }
}
