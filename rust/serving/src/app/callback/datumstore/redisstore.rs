use redis::aio::ConnectionManager;
use redis::RedisError;
use tracing::info;

use crate::app::callback::datumstore::{DatumStore, Error as StoreError, Result as StoreResult};
use crate::config::RedisConfig;

const LPUSH: &str = "LPUSH";
const LRANGE: &str = "LRANGE";
const EXPIRE: &str = "EXPIRE";

// Handle to the Redis actor.
#[derive(Clone)]
pub(crate) struct RedisConnection {
    conn_manager: ConnectionManager,
    config: RedisConfig,
}

impl RedisConnection {
    /// Creates a new RedisConnection with concurrent operations on Redis set by max_tasks.
    pub(crate) async fn new(config: RedisConfig) -> crate::Result<Self> {
        let client = redis::Client::open(config.addr.as_str())
            .map_err(|e| StoreError::Connection(format!("Creating Redis client: {e:?}")))?;
        let conn = client
            .get_connection_manager()
            .await
            .map_err(|e| StoreError::Connection(format!("Connecting to Redis server: {e:?}")))?;
        Ok(Self {
            conn_manager: conn,
            config,
        })
    }

    async fn execute_redis_cmd(
        conn_manager: &mut ConnectionManager,
        ttl_secs: Option<u32>,
        key: &str,
        val: &Vec<u8>,
    ) -> Result<(), RedisError> {
        let mut pipe = redis::pipe();
        pipe.cmd(LPUSH).arg(key).arg(val);

        // if the ttl is configured, add the EXPIRE command to the pipeline
        if let Some(ttl) = ttl_secs {
            pipe.cmd(EXPIRE).arg(key).arg(ttl);
        }

        // Execute the pipeline
        pipe.exec_async(conn_manager).await
    }
}

// It is possible to move the methods defined here to be methods on the Redis actor and communicate through channels.
// With that, all public APIs defined on RedisConnection can be on &self (immutable).
impl DatumStore for RedisConnection {
    async fn retrieve_datum(&mut self, id: &str) -> StoreResult<Option<Vec<Vec<u8>>>> {
        let key = format!("request:{id}:results");
        info!(?key, "Reading from Redis");
        let result: Result<Vec<Vec<u8>>, RedisError> = redis::cmd(LRANGE)
            .arg(key)
            .arg(0)
            .arg(-1)
            .query_async(&mut self.conn_manager)
            .await;

        match result {
            Ok(result) => {
                if result.is_empty() {
                    info!("No results found in Redis");
                    Ok(None)
                } else {
                    info!("Results found in Redis");
                    Ok(Some(result))
                }
            }
            Err(e) => Err(StoreError::StoreRead(format!(
                "Failed to read from redis: {:?}",
                e
            ))),
        }
    }

    // Check if the Redis connection is healthy
    async fn ready(&mut self) -> bool {
        let mut conn = self.conn_manager.clone();
        match redis::cmd("PING").query_async::<String>(&mut conn).await {
            Ok(response) => response == "PONG",
            Err(_) => false,
        }
    }
}

// #[cfg(feature = "redis-tests")]
// #[cfg(test)]
// mod tests {
//     use axum::body::Bytes;
//     use redis::AsyncCommands;
//
//     use super::*;
//     use crate::app::callback::datumstore::LocalDatumStore;
//     use crate::callback::Response;
//
//     #[tokio::test]
//     async fn test_redis_store() {
//         let redis_config = RedisConfig {
//             addr: "no_such_redis://127.0.0.1:6379".to_owned(),
//             max_tasks: 10,
//             ..Default::default()
//         };
//         let redis_connection = RedisConnection::new(redis_config).await;
//         assert!(redis_connection.is_err());
//
//         // Test Redis connection
//         let redis_connection = RedisConnection::new(RedisConfig::default()).await;
//         assert!(redis_connection.is_ok());
//
//         let key = uuid::Uuid::new_v4().to_string();
//
//         let ps_cb = PayloadToSave::Callback {
//             key: key.clone(),
//             value: Arc::new(Callback {
//                 id: String::from("1234"),
//                 vertex: String::from("prev_vertex"),
//                 cb_time: 1234,
//                 from_vertex: String::from("next_vertex"),
//                 responses: vec![Response { tags: None }],
//             }),
//         };
//
//         let mut redis_conn = redis_connection.unwrap();
//         redis_conn.register(key.as_str()).await.unwrap();
//
//         // Test Redis save
//         assert!(redis_conn.save(vec![ps_cb]).await.is_ok());
//
//         let ps_datum = PayloadToSave::DatumFromPipeline {
//             key: key.clone(),
//             value: Bytes::from("hello world"),
//         };
//
//         assert!(redis_conn.save(vec![ps_datum]).await.is_ok());
//
//         // Test Redis retrieve callbacks
//         let callbacks = redis_conn.retrieve_callbacks(&key).await;
//         assert!(callbacks.is_ok());
//
//         let callbacks = callbacks.unwrap();
//         assert_eq!(callbacks.len(), 1);
//
//         // Additional validations
//         let callback = callbacks.first().unwrap();
//         assert_eq!(callback.id, "1234");
//         assert_eq!(callback.vertex, "prev_vertex");
//         assert_eq!(callback.cb_time, 1234);
//         assert_eq!(callback.from_vertex, "next_vertex");
//
//         // Test Redis retrieve datum
//         let datums = redis_conn.retrieve_datum(&key).await;
//         assert!(datums.is_ok());
//
//         assert_eq!(datums.unwrap(), ProcessingStatus::InProgress);
//
//         redis_conn.deregister(key.as_str()).await.unwrap();
//         let datums = redis_conn.retrieve_datum(&key).await.unwrap();
//         let ProcessingStatus::Completed(datums) = datums else {
//             panic!("Expected completed results");
//         };
//         assert_eq!(datums.len(), 1);
//
//         let datum = datums.first().unwrap();
//         assert_eq!(datum, "hello world".as_bytes());
//
//         // Test Redis retrieve callbacks error
//         let result = redis_conn.retrieve_callbacks("non_existent_key").await;
//         assert!(matches!(result, Err(StoreError::StoreRead(_))));
//
//         // Test Redis retrieve datum error
//         let result = redis_conn.retrieve_datum("non_existent_key").await;
//         assert!(matches!(result, Err(StoreError::InvalidRequestId(_))));
//     }
//
//     #[tokio::test]
//     async fn test_redis_ttl() {
//         let redis_config = RedisConfig {
//             max_tasks: 10,
//             ..Default::default()
//         };
//         let redis_connection = RedisConnection::new(redis_config)
//             .await
//             .expect("Failed to connect to Redis");
//
//         let key = uuid::Uuid::new_v4().to_string();
//         let value = Arc::new(Callback {
//             id: String::from("test-redis-ttl"),
//             vertex: String::from("vertex"),
//             cb_time: 1234,
//             from_vertex: String::from("next_vertex"),
//             responses: vec![Response { tags: None }],
//         });
//
//         // Save with TTL of 1 second
//         redis_connection
//             .write_to_redis(&key, &serde_json::to_vec(&*value).unwrap())
//             .await
//             .expect("Failed to write to Redis");
//
//         let mut conn_manager = redis_connection.conn_manager.clone();
//
//         let exists: bool = conn_manager
//             .exists(&key)
//             .await
//             .expect("Failed to check existence immediately");
//
//         // if the key exists, the TTL should be set to 1 second
//         if exists {
//             let ttl: isize = conn_manager.ttl(&key).await.expect("Failed to check TTL");
//             assert_eq!(ttl, 86400, "TTL should be set to 1 second");
//         }
//     }
// }
