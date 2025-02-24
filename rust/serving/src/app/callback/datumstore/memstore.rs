use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;

use crate::app::callback::datumstore::{Error as StoreError, Result as StoreResult};
const STORE_KEY_SUFFIX: &str = "saved";

/// `InMemoryStore` is an in-memory implementation of the `Store` trait.
/// It uses a `HashMap` to store data in memory.
#[derive(Clone)]
pub(crate) struct InMemoryStore {
    /// The data field is a `HashMap` where the key is a `String` and the value is a `Vec<Vec<u8>>`.
    /// It is wrapped in an `Arc<Mutex<_>>` to allow shared ownership and thread safety.
    pub(crate) data: Arc<std::sync::Mutex<HashMap<String, Vec<Vec<u8>>>>>,
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

impl super::DatumStore for InMemoryStore {
    /// Retrieves data for a given id from the `HashMap`.
    /// Each piece of data is deserialized from bytes into a `String`.
    async fn retrieve_datum(&mut self, id: &str) -> StoreResult<Option<Vec<Vec<u8>>>> {
        let id = format!("{id}_{STORE_KEY_SUFFIX}");
        let data = self.data.lock().unwrap();
        match data.get(&id) {
            Some(result) => Ok(Some(result.to_vec())),
            None => Err(StoreError::InvalidRequestId(format!(
                "No entry found for id: {}",
                id
            ))),
        }
    }

    async fn ready(&mut self) -> bool {
        true
    }
}

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;
//
//     use super::*;
//     use crate::app::callback::datumstore::DatumStore;
//     use crate::callback::{Callback, Response};
//
//     #[tokio::test]
//     async fn test_save_and_retrieve_callbacks() {
//         let mut store = InMemoryStore::new();
//         let key = "test_key".to_string();
//         let value = Arc::new(Callback {
//             id: "test_id".to_string(),
//             vertex: "in".to_string(),
//             cb_time: 12345,
//             from_vertex: "in".to_string(),
//             responses: vec![Response { tags: None }],
//         });
//
//         // Save a callback
//         store
//             .save(vec![PayloadToSave::Callback {
//                 key: key.clone(),
//                 value: Arc::clone(&value),
//             }])
//             .await
//             .unwrap();
//
//         // Retrieve the callback
//         let retrieved = store.retrieve_callbacks(&key).await.unwrap();
//
//         // Check that the retrieved callback is the same as the one we saved
//         assert_eq!(retrieved.len(), 1);
//         assert_eq!(retrieved[0].id, "test_id".to_string())
//     }
//
//     #[tokio::test]
//     async fn test_save_and_retrieve_datum() {
//         let mut store = InMemoryStore::new();
//         let key = "test_key".to_string();
//         let value = "test_value".to_string();
//
//         // Save a datum
//         store
//             .save(vec![PayloadToSave::DatumFromPipeline {
//                 key: key.clone(),
//                 value: value.clone().into(),
//             }])
//             .await
//             .unwrap();
//
//         // Retrieve the datum
//         let retrieved = store.retrieve_datum(&key).await.unwrap();
//         let ProcessingStatus::Completed(retrieved) = retrieved else {
//             panic!("Expected pipeline processing to be completed");
//         };
//
//         // Check that the retrieved datum is the same as the one we saved
//         assert_eq!(retrieved.len(), 1);
//         assert_eq!(retrieved[0], value.as_bytes());
//     }
//
//     #[tokio::test]
//     async fn test_retrieve_callbacks_no_entry() {
//         let mut store = InMemoryStore::new();
//         let key = "nonexistent_key".to_string();
//
//         // Try to retrieve a callback for a key that doesn't exist
//         let result = store.retrieve_callbacks(&key).await;
//
//         // Check that an error is returned
//         assert!(result.is_err());
//     }
//
//     #[tokio::test]
//     async fn test_retrieve_datum_no_entry() {
//         let mut store = InMemoryStore::new();
//         let key = "nonexistent_key".to_string();
//
//         // Try to retrieve a datum for a key that doesn't exist
//         let result = store.retrieve_datum(&key).await;
//
//         // Check that an error is returned
//         assert!(result.is_err());
//     }
//
//     #[tokio::test]
//     async fn test_save_invalid_callback() {
//         let mut store = InMemoryStore::new();
//         let value = Arc::new(Callback {
//             id: "test_id".to_string(),
//             vertex: "in".to_string(),
//             cb_time: 12345,
//             from_vertex: "in".to_string(),
//             responses: vec![Response { tags: None }],
//         });
//
//         // Try to save a callback with an invalid key
//         let result = store
//             .save(vec![PayloadToSave::Callback {
//                 key: "".to_string(),
//                 value: Arc::clone(&value),
//             }])
//             .await;
//
//         // Check that an error is returned
//         assert!(result.is_err());
//     }
//
//     #[tokio::test]
//     async fn test_save_invalid_datum() {
//         let mut store = InMemoryStore::new();
//
//         // Try to save a datum with an invalid key
//         let result = store
//             .save(vec![PayloadToSave::DatumFromPipeline {
//                 key: "".to_string(),
//                 value: "test_value".into(),
//             }])
//             .await;
//
//         // Check that an error is returned
//         assert!(result.is_err());
//     }
// }
