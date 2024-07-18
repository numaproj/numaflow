use std::sync::Arc;

use redis::aio::ConnectionManager;
use redis::RedisError;
use tokio::sync::Semaphore;

use backoff::retry::Retry;
use backoff::strategy::fixed;

use crate::app::callback::CallbackRequest;
use crate::consts::SAVED;
use crate::{config, Error};

use super::PayloadToSave;

const LPUSH: &str = "LPUSH";
const LRANGE: &str = "LRANGE";
const EXPIRE: &str = "EXPIRE";

// Handle to the Redis actor.
#[derive(Clone)]
pub(crate) struct RedisConnection {
    max_tasks: usize,
    conn_manager: ConnectionManager,
}

impl RedisConnection {
    /// Creates a new RedisConnection with concurrent operations on Redis set by max_tasks.
    pub(crate) async fn new(addr: &str, max_tasks: usize) -> crate::Result<Self> {
        let client =
            redis::Client::open(addr).map_err(|e| format!("Creating Redis client: {e:?}"))?;
        let conn = client
            .get_connection_manager()
            .await
            .map_err(|e| format!("Connecting to Redis server: {e:?}"))?;
        Ok(Self {
            conn_manager: conn,
            max_tasks,
        })
    }

    async fn handle_write_requests(
        conn_manager: &mut ConnectionManager,
        msg: PayloadToSave,
    ) -> crate::Result<()> {
        match msg {
            PayloadToSave::Callback { key, value } => {
                // Convert the CallbackRequest to a byte array
                let value = serde_json::to_vec(&*value)
                    .map_err(|e| Error::StoreWrite(format!("Serializing payload - {}", e)))?;

                Self::write_to_redis(conn_manager, &key, &value).await
            }

            // Write the byte array to Redis
            PayloadToSave::DatumFromPipeline { key, value } => {
                // we have to differentiate between the saved responses and the callback requests
                // saved responses are stored in "id_SAVED", callback requests are stored in "id"
                let key = format!("{}_{}", key, SAVED);
                let value: Vec<u8> = value.into();

                Self::write_to_redis(conn_manager, &key, &value).await
            }
        }
    }

    async fn execute_redis_cmd(
        conn_manager: &mut ConnectionManager,
        key: &str,
        val: &Vec<u8>,
    ) -> Result<(), RedisError> {
        let mut pipe = redis::pipe();
        pipe.cmd(LPUSH).arg(key).arg(val);

        // if the ttl is configured, add the EXPIRE command to the pipeline
        if let Some(ttl) = config().redis.ttl_secs {
            pipe.cmd(EXPIRE).arg(key).arg(ttl);
        }

        // Execute the pipeline
        pipe.query_async(conn_manager).await.map(|_: ()| ())
    }

    // write to Redis with retries
    async fn write_to_redis(
        conn_manager: &mut ConnectionManager,
        key: &str,
        value: &Vec<u8>,
    ) -> crate::Result<()> {
        let interval = fixed::Interval::from_millis(config().redis.retries_duration_millis.into())
            .take(config().redis.retries);

        Retry::retry(
            interval,
            || async {
                // https://hackmd.io/@compiler-errors/async-closures
                Self::execute_redis_cmd(&mut conn_manager.clone(), key, value).await
            },
            |e: &RedisError| !e.is_unrecoverable_error(),
        )
        .await
        .map_err(|err| Error::StoreWrite(format!("Saving to redis: {}", err).to_string()))
    }
}

// It is possible to move the methods defined here to be methods on the Redis actor and communicate through channels.
// With that, all public APIs defined on RedisConnection can be on &self (immutable).
impl super::Store for RedisConnection {
    // Attempt to save all payloads. Returns error if we fail to save at least one message.
    async fn save(&mut self, messages: Vec<PayloadToSave>) -> crate::Result<()> {
        let mut tasks = vec![];
        // This is put in place not to overload Redis and also way some kind of
        // flow control.
        let sem = Arc::new(Semaphore::new(self.max_tasks));
        for msg in messages {
            let permit = Arc::clone(&sem).acquire_owned().await;
            let mut _conn_mgr = self.conn_manager.clone();
            let task = tokio::spawn(async move {
                let _permit = permit;
                Self::handle_write_requests(&mut _conn_mgr, msg).await
            });
            tasks.push(task);
        }
        for task in tasks {
            task.await.unwrap()?;
        }
        Ok(())
    }

    async fn retrieve_callbacks(&mut self, id: &str) -> Result<Vec<Arc<CallbackRequest>>, Error> {
        let result: Result<Vec<Vec<u8>>, RedisError> = redis::cmd(LRANGE)
            .arg(id)
            .arg(0)
            .arg(-1)
            .query_async(&mut self.conn_manager)
            .await;

        match result {
            Ok(result) => {
                if result.is_empty() {
                    return Err(Error::StoreRead(format!("No entry found for id: {}", id)));
                }

                let messages: Result<Vec<_>, _> = result
                    .into_iter()
                    .map(|msg| {
                        let cbr: CallbackRequest = serde_json::from_slice(&msg).map_err(|e| {
                            Error::StoreRead(format!("Parsing payload from bytes - {}", e))
                        })?;
                        Ok(Arc::new(cbr))
                    })
                    .collect();

                messages
            }
            Err(e) => Err(Error::StoreRead(format!(
                "Failed to read from redis: {:?}",
                e
            ))),
        }
    }

    async fn retrieve_datum(&mut self, id: &str) -> Result<Vec<Vec<u8>>, Error> {
        // saved responses are stored in "id_SAVED"
        let key = format!("{}_{}", id, SAVED);
        let result: Result<Vec<Vec<u8>>, RedisError> = redis::cmd(LRANGE)
            .arg(key)
            .arg(0)
            .arg(-1)
            .query_async(&mut self.conn_manager)
            .await;

        match result {
            Ok(result) => {
                if result.is_empty() {
                    return Err(Error::StoreRead(format!("No entry found for id: {}", id)));
                }

                Ok(result)
            }
            Err(e) => Err(Error::StoreRead(format!(
                "Failed to read from redis: {:?}",
                e
            ))),
        }
    }

    // Check if the Redis connection is healthy
    async fn ready(&mut self) -> bool {
        let mut conn = self.conn_manager.clone();
        match redis::cmd("PING").query_async::<_, String>(&mut conn).await {
            Ok(response) => response == "PONG",
            Err(_) => false,
        }
    }
}

#[cfg(feature = "redis-tests")]
#[cfg(test)]
mod tests {
    use crate::app::callback::store::LocalStore;
    use axum::body::Bytes;
    use redis::AsyncCommands;

    use super::*;

    #[tokio::test]
    async fn test_redis_store() {
        let redis_connection = RedisConnection::new("no_such_redis://127.0.0.1:6379", 10).await;
        assert!(redis_connection.is_err());

        // Test Redis connection
        let redis_connection =
            RedisConnection::new(format!("redis://127.0.0.1:{}", "6379").as_str(), 10).await;
        assert!(redis_connection.is_ok());

        let key = uuid::Uuid::new_v4().to_string();

        let ps_cb = PayloadToSave::Callback {
            key: key.clone(),
            value: Arc::new(CallbackRequest {
                id: String::from("1234"),
                vertex: String::from("prev_vertex"),
                cb_time: 1234,
                from_vertex: String::from("next_vertex"),
                tags: None,
            }),
        };

        let mut redis_conn = redis_connection.unwrap();

        // Test Redis save
        assert!(redis_conn.save(vec![ps_cb]).await.is_ok());

        let ps_datum = PayloadToSave::DatumFromPipeline {
            key: key.clone(),
            value: Bytes::from("hello world"),
        };

        assert!(redis_conn.save(vec![ps_datum]).await.is_ok());

        // Test Redis retrieve callbacks
        let callbacks = redis_conn.retrieve_callbacks(&key).await;
        assert!(callbacks.is_ok());

        let callbacks = callbacks.unwrap();
        assert_eq!(callbacks.len(), 1);

        // Additional validations
        let callback = callbacks.first().unwrap();
        assert_eq!(callback.id, "1234");
        assert_eq!(callback.vertex, "prev_vertex");
        assert_eq!(callback.cb_time, 1234);
        assert_eq!(callback.from_vertex, "next_vertex");

        // Test Redis retrieve datum
        let datums = redis_conn.retrieve_datum(&key).await;
        assert!(datums.is_ok());

        let datums = datums.unwrap();
        assert_eq!(datums.len(), 1);

        let datum = datums.first().unwrap();
        assert_eq!(datum, "hello world".as_bytes());

        // Test Redis retrieve callbacks error
        let result = redis_conn.retrieve_callbacks("non_existent_key").await;
        assert!(matches!(result, Err(Error::StoreRead(_))));

        // Test Redis retrieve datum error
        let result = redis_conn.retrieve_datum("non_existent_key").await;
        assert!(matches!(result, Err(Error::StoreRead(_))));
    }

    #[tokio::test]
    async fn test_redis_ttl() {
        let redis_connection = RedisConnection::new("redis://127.0.0.1:6379", 10)
            .await
            .expect("Failed to connect to Redis");

        let key = uuid::Uuid::new_v4().to_string();
        let value = Arc::new(CallbackRequest {
            id: String::from("test-redis-ttl"),
            vertex: String::from("vertex"),
            cb_time: 1234,
            from_vertex: String::from("next_vertex"),
            tags: None,
        });

        // Save with TTL of 1 second
        let mut conn_manager = redis_connection.conn_manager.clone();
        RedisConnection::write_to_redis(
            &mut conn_manager,
            &key,
            &serde_json::to_vec(&*value).unwrap(),
        )
        .await
        .expect("Failed to write to Redis");

        let exists: bool = conn_manager
            .exists(&key)
            .await
            .expect("Failed to check existence immediately");

        // if the key exists, the TTL should be set to 1 second
        if exists {
            let ttl: isize = conn_manager.ttl(&key).await.expect("Failed to check TTL");
            assert_eq!(ttl, 1, "TTL should be set to 1 second");
        }
    }
}
