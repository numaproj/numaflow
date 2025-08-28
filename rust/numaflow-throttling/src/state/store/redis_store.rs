use crate::state::Consensus;
use crate::state::store::Store;
use redis::TlsMode;
use redis::sentinel::{SentinelClientBuilder, SentinelServerType};
use redis::{Client, ConnectionAddr, IntoConnectionInfo, RedisError, Script};
use tokio_util::sync::CancellationToken;

// Embed Lua scripts at compile time
const REGISTER_SCRIPT: &str = include_str!("lua/register.lua");
const DEREGISTER_SCRIPT: &str = include_str!("lua/deregister.lua");
const SYNC_POOL_SIZE_SCRIPT: &str = include_str!("lua/sync_pool_size.lua");

#[derive(Clone)]
pub struct RedisStore {
    /// Prefix for all keys used by this store
    key_prefix: &'static str,
    client: redis::aio::ConnectionManager,
    /// Precompiled Lua scripts
    register_script: Script,
    deregister_script: Script,
    sync_pool_size_script: Script,
}

pub enum RedisMode {
    SingleUrl {
        url: String,
        db: Option<i32>,
    },
    Sentinel {
        master_name: String,
        endpoints: Vec<String>,
        role: Option<String>,
        sentinel_auth: Option<RedisAuth>,
        redis_auth: Option<RedisAuth>,
        sentinel_tls: Option<TlsMode>,
        redis_tls: Option<TlsMode>,
        db: Option<i32>,
    },
}

#[derive(Clone, Debug)]
pub struct RedisAuth {
    pub username: Option<String>,
    pub password: Option<String>,
}

impl RedisMode {
    /// Create a builder for Single URL mode with mandatory URL
    pub fn single_url(url: String) -> SingleUrlBuilder {
        SingleUrlBuilder::new(url)
    }

    /// Create a builder for Sentinel mode with mandatory master name and endpoints
    pub fn sentinel(master_name: String, endpoints: Vec<String>) -> SentinelBuilder {
        SentinelBuilder::new(master_name, endpoints)
    }
}

/// Builder for Single URL Redis mode
pub struct SingleUrlBuilder {
    url: String,
    db: Option<i32>,
}

impl SingleUrlBuilder {
    fn new(url: String) -> Self {
        Self { url, db: None }
    }

    pub fn db(mut self, db: i32) -> Self {
        self.db = Some(db);
        self
    }

    pub fn build(self) -> Result<RedisMode, String> {
        Ok(RedisMode::SingleUrl {
            url: self.url,
            db: self.db,
        })
    }
}

/// Builder for Sentinel Redis mode
pub struct SentinelBuilder {
    master_name: String,
    endpoints: Vec<String>,
    role: Option<String>,
    sentinel_auth: Option<RedisAuth>,
    redis_auth: Option<RedisAuth>,
    sentinel_tls: Option<TlsMode>,
    redis_tls: Option<TlsMode>,
    db: Option<i32>,
}

impl SentinelBuilder {
    fn new(master_name: String, endpoints: Vec<String>) -> Self {
        Self {
            master_name,
            endpoints,
            role: None,
            sentinel_auth: None,
            redis_auth: None,
            sentinel_tls: None,
            redis_tls: None,
            db: None,
        }
    }

    pub fn role(mut self, role: String) -> Self {
        self.role = Some(role);
        self
    }

    pub fn sentinel_auth(mut self, auth: RedisAuth) -> Self {
        self.sentinel_auth = Some(auth);
        self
    }

    pub fn redis_auth(mut self, auth: RedisAuth) -> Self {
        self.redis_auth = Some(auth);
        self
    }

    pub fn sentinel_tls(mut self, tls_mode: TlsMode) -> Self {
        self.sentinel_tls = Some(tls_mode);
        self
    }

    pub fn redis_tls(mut self, tls_mode: TlsMode) -> Self {
        self.redis_tls = Some(tls_mode);
        self
    }

    pub fn db(mut self, db: i32) -> Self {
        self.db = Some(db);
        self
    }

    pub fn build(self) -> Result<RedisMode, String> {
        if self.endpoints.is_empty() {
            return Err("At least one endpoint is required for Sentinel mode".to_string());
        }

        Ok(RedisMode::Sentinel {
            master_name: self.master_name,
            endpoints: self.endpoints,
            role: self.role,
            sentinel_auth: self.sentinel_auth,
            redis_auth: self.redis_auth,
            sentinel_tls: self.sentinel_tls,
            redis_tls: self.redis_tls,
            db: self.db,
        })
    }
}

impl RedisStore {
    pub async fn new(key_prefix: &'static str, mode: RedisMode) -> Result<Self, RedisError> {
        let client = Self::make_client(mode).await?;

        // Create script objects
        let register_script = Script::new(REGISTER_SCRIPT);
        let deregister_script = Script::new(DEREGISTER_SCRIPT);
        let sync_pool_size_script = Script::new(SYNC_POOL_SIZE_SCRIPT);

        let store = Self {
            key_prefix,
            client,
            register_script,
            deregister_script,
            sync_pool_size_script,
        };

        // Load scripts into Redis
        store.load_lua_scripts().await.map_err(|e| {
            RedisError::from((
                redis::ErrorKind::IoError,
                "Failed to load Lua scripts",
                format!("{:?}", e),
            ))
        })?;

        Ok(store)
    }

    /// Connect via redis-rs using async and ConnectionManager.
    async fn make_client(mode: RedisMode) -> Result<redis::aio::ConnectionManager, RedisError> {
        match mode {
            RedisMode::SingleUrl { url, db } => {
                // Parse the URL and modify the DB if specified
                let mut connection_info = url.into_connection_info()?;
                if let Some(db_index) = db {
                    connection_info.redis.db = db_index as i64;
                }

                let client = Client::open(connection_info)?;
                let mgr = client.get_connection_manager().await?;
                Ok(mgr)
            }
            RedisMode::Sentinel {
                master_name,
                endpoints,
                role,
                sentinel_auth,
                redis_auth,
                sentinel_tls,
                redis_tls,
                db,
            } => {
                // Determine server type
                let server_type = match role.as_deref() {
                    Some("Replica") => SentinelServerType::Replica,
                    _ => SentinelServerType::Master, // Default to Master
                };

                // Convert string endpoints to ConnectionAddr
                let sentinel_addrs: Result<Vec<ConnectionAddr>, _> = endpoints
                    .iter()
                    .map(|endpoint| {
                        endpoint
                            .as_str()
                            .into_connection_info()
                            .map(|info| info.addr)
                    })
                    .collect();
                let sentinel_addrs = sentinel_addrs?;

                // Build SentinelClient using builder pattern
                let mut builder =
                    SentinelClientBuilder::new(sentinel_addrs, master_name, server_type)?;

                // Apply sentinel authentication if provided
                if let Some(auth) = sentinel_auth {
                    if let Some(username) = auth.username {
                        builder = builder.set_client_to_sentinel_username(username);
                    }
                    if let Some(password) = auth.password {
                        builder = builder.set_client_to_sentinel_password(password);
                    }
                }

                // Apply sentinel TLS if provided
                if let Some(tls_mode) = sentinel_tls {
                    builder = builder.set_client_to_sentinel_tls_mode(tls_mode);
                }

                // Apply Redis data node authentication if provided
                if let Some(auth) = redis_auth {
                    if let Some(username) = auth.username {
                        builder = builder.set_client_to_redis_username(username);
                    }
                    if let Some(password) = auth.password {
                        builder = builder.set_client_to_redis_password(password);
                    }
                }

                // Apply Redis data node TLS if provided
                if let Some(tls_mode) = redis_tls {
                    builder = builder.set_client_to_redis_tls_mode(tls_mode);
                }

                // Apply database selection if provided
                if let Some(db_index) = db {
                    builder = builder.set_client_to_redis_db(db_index as i64);
                }

                // Build the sentinel client
                let mut sentinel_client = builder.build()?;

                // Get a Client to the target server
                let target_client = sentinel_client.async_get_client().await?;

                let mgr = target_client.get_connection_manager().await?;
                Ok(mgr)
            }
        }
    }

    /// Load lua scripts into Redis. The scripts are in lua dir.
    async fn load_lua_scripts(&self) -> crate::Result<()> {
        let mut conn = self.client.clone();

        self.register_script
            .load_async(&mut conn)
            .await
            .map_err(|e| crate::Error::Redis(e.to_string()))?;
        self.deregister_script
            .load_async(&mut conn)
            .await
            .map_err(|e| crate::Error::Redis(e.to_string()))?;
        self.sync_pool_size_script
            .load_async(&mut conn)
            .await
            .map_err(|e| crate::Error::Redis(e.to_string()))?;

        Ok(())
    }

    async fn exec_lua_script<T: redis::FromRedisValue>(
        &self,
        script: &Script,
        keys: &[&str],
        args: &[&str],
    ) -> redis::RedisResult<T> {
        let mut conn = self.client.clone();

        // Use the Script object which handles EVALSHA/EVAL fallback automatically
        let mut invocation = script.prepare_invoke();

        // Add keys
        for key in keys {
            invocation.key(*key);
        }

        // Add arguments
        for arg in args {
            invocation.arg(*arg);
        }

        // invoke async internally handles loading the script if the SHA is not found
        invocation.invoke_async(&mut conn).await
    }
}

impl Store for RedisStore {
    async fn register(
        &self,
        processor_id: &str,
        cancel: CancellationToken,
    ) -> crate::Result<usize> {
        if cancel.is_cancelled() {
            return Err(crate::Error::Cancellation);
        }
        let pool_size: usize = self
            .exec_lua_script(&self.register_script, &[self.key_prefix], &[processor_id])
            .await
            .map_err(|e| crate::Error::Redis(e.to_string()))?;

        Ok(pool_size)
    }

    async fn deregister(&self, processor_id: &str, cancel: CancellationToken) -> crate::Result<()> {
        if cancel.is_cancelled() {
            return Err(crate::Error::Cancellation);
        }

        let _: String = self
            .exec_lua_script(&self.deregister_script, &[self.key_prefix], &[processor_id])
            .await
            .map_err(|e| crate::Error::Redis(e.to_string()))?;

        Ok(())
    }

    async fn sync_pool_size(
        &self,
        processor_id: &str,
        pool_size: usize,
        cancel: CancellationToken,
    ) -> crate::Result<Consensus> {
        if cancel.is_cancelled() {
            return Err(crate::Error::Cancellation);
        }

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs()
            .to_string();

        let pool_size_str = pool_size.to_string();

        let result: Vec<String> = self
            .exec_lua_script(
                &self.sync_pool_size_script,
                &[self.key_prefix],
                &[processor_id, &timestamp, &pool_size_str],
            )
            .await
            .map_err(|e| crate::Error::Redis(e.to_string()))?;

        if result.len() != 3 {
            return Err(crate::Error::Redis(
                "Invalid response from sync script".to_string(),
            ));
        }

        let consensus_type = &result.first().expect("should have consensus type");
        let size: usize = result
            .get(1)
            .expect("should have pool size")
            .parse()
            .map_err(|_| crate::Error::Redis("Invalid pool size in response".to_string()))?;

        match consensus_type.as_str() {
            "AGREE" => Ok(Consensus::Agree(size)),
            "DISAGREE" => Ok(Consensus::Disagree(size)),
            _ => Err(crate::Error::Redis("Unknown consensus type".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_mode_single_url_builder() {
        let redis_mode = RedisMode::single_url("redis://localhost:6379".to_string())
            .db(1)
            .build()
            .unwrap();

        match redis_mode {
            RedisMode::SingleUrl { url, db } => {
                assert_eq!(url, "redis://localhost:6379");
                assert_eq!(db, Some(1));
            }
            _ => panic!("Expected SingleUrl mode"),
        }
    }

    #[test]
    fn test_redis_mode_sentinel_builder() {
        let redis_mode = RedisMode::sentinel(
            "mymaster".to_string(),
            vec!["sentinel1:26379".to_string(), "sentinel2:26379".to_string()],
        )
        .role("Master".to_string())
        .db(2)
        .build()
        .unwrap();

        match redis_mode {
            RedisMode::Sentinel {
                master_name,
                endpoints,
                role,
                db,
                ..
            } => {
                assert_eq!(master_name, "mymaster");
                assert_eq!(endpoints, vec!["sentinel1:26379", "sentinel2:26379"]);
                assert_eq!(role, Some("Master".to_string()));
                assert_eq!(db, Some(2));
            }
            _ => panic!("Expected Sentinel mode"),
        }
    }

    #[test]
    fn test_redis_mode_sentinel_builder_with_auth() {
        let sentinel_auth = RedisAuth {
            username: Some("sentinel_user".to_string()),
            password: Some("sentinel_pass".to_string()),
        };

        let redis_auth = RedisAuth {
            username: Some("redis_user".to_string()),
            password: Some("redis_pass".to_string()),
        };

        let redis_mode =
            RedisMode::sentinel("mymaster".to_string(), vec!["sentinel1:26379".to_string()])
                .sentinel_auth(sentinel_auth.clone())
                .redis_auth(redis_auth.clone())
                .sentinel_tls(TlsMode::Secure)
                .build()
                .unwrap();

        match redis_mode {
            RedisMode::Sentinel {
                master_name,
                sentinel_auth: s_auth,
                redis_auth: r_auth,
                sentinel_tls,
                ..
            } => {
                assert_eq!(master_name, "mymaster");
                assert_eq!(s_auth.unwrap().username, Some("sentinel_user".to_string()));
                assert_eq!(r_auth.unwrap().username, Some("redis_user".to_string()));
                assert!(sentinel_tls.is_some()); // Just check that TLS is configured
            }
            _ => panic!("Expected Sentinel mode"),
        }
    }

    #[test]
    fn test_script_constants_are_valid() {
        // Test that the embedded scripts are not empty and contain expected content
        assert!(!REGISTER_SCRIPT.is_empty());
        assert!(!DEREGISTER_SCRIPT.is_empty());
        assert!(!SYNC_POOL_SIZE_SCRIPT.is_empty());

        // Test that scripts contain expected Redis commands
        assert!(REGISTER_SCRIPT.contains("ZADD"));
        assert!(DEREGISTER_SCRIPT.contains("ZREM"));
        assert!(SYNC_POOL_SIZE_SCRIPT.contains("ZADD"));

        // Test that scripts contain expected key patterns
        assert!(REGISTER_SCRIPT.contains(":heartbeats"));
        assert!(REGISTER_SCRIPT.contains(":poolsize"));
        assert!(DEREGISTER_SCRIPT.contains(":heartbeats"));
        assert!(DEREGISTER_SCRIPT.contains(":poolsize"));
        assert!(SYNC_POOL_SIZE_SCRIPT.contains(":heartbeats"));
        assert!(SYNC_POOL_SIZE_SCRIPT.contains(":poolsize"));
    }

    #[test]
    fn test_script_objects_creation() {
        // Test that Script objects can be created from the embedded strings
        let register_script = Script::new(REGISTER_SCRIPT);
        let deregister_script = Script::new(DEREGISTER_SCRIPT);
        let sync_pool_size_script = Script::new(SYNC_POOL_SIZE_SCRIPT);

        // Test that scripts have valid SHA1 hashes
        assert!(!register_script.get_hash().is_empty());
        assert!(!deregister_script.get_hash().is_empty());
        assert!(!sync_pool_size_script.get_hash().is_empty());

        // Test that each script has a unique hash
        assert_ne!(register_script.get_hash(), deregister_script.get_hash());
        assert_ne!(register_script.get_hash(), sync_pool_size_script.get_hash());
        assert_ne!(
            deregister_script.get_hash(),
            sync_pool_size_script.get_hash()
        );
    }
}
