use crate::error::Error;
use crate::state::Consensus;
use crate::state::store::Store;
use numaflow_models::models::RateLimiterRedisStore;
use redis::sentinel::{SentinelClientBuilder, SentinelServerType};
use redis::{
    Client, ClientTlsConfig, ConnectionAddr, IntoConnectionInfo, RedisError, Script,
    TlsCertificates, TlsMode,
};
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
    stale_age: usize,
}

/// Different ways to connect to Redis based on different Redis deployments.
// We will not be supporting Redis Cluster mode because our key space is too small, and we cannot
// do key ops across multiple nodes. This means mostly all keys are in the same node.
pub enum RedisMode {
    SingleUrl {
        url: String,
        db: Option<i32>,
    },
    Sentinel {
        master_name: String,
        endpoints: Vec<String>,
        sentinel_auth: Option<RedisAuth>,
        redis_auth: Option<RedisAuth>,
        sentinel_tls: Box<Option<RedisTlsInfo>>,
        redis_tls: Box<Option<RedisTlsInfo>>,
        db: Option<i32>,
    },
}

impl std::fmt::Debug for RedisMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisMode::SingleUrl { url, db } => f
                .debug_struct("SingleUrl")
                .field("url", url)
                .field("db", db)
                .finish(),
            RedisMode::Sentinel {
                master_name,
                endpoints,
                sentinel_auth,
                redis_auth,
                db,
                ..
            } => f
                .debug_struct("Sentinel")
                .field("master_name", master_name)
                .field("endpoints", endpoints)
                .field("sentinel_auth", sentinel_auth)
                .field("redis_auth", redis_auth)
                .field("db", db)
                .finish(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RedisAuth {
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Clone)]
pub struct RedisTlsInfo {
    pub is_secure: bool,
    pub certificates: Option<TlsCertificates>,
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

    /// Create RedisMode from the rate limiter Redis store configuration using builder pattern
    pub fn new(redis_store_config: &RateLimiterRedisStore) -> crate::Result<Self> {
        match redis_store_config.mode.as_str() {
            "single" => {
                let url = redis_store_config
                    .url
                    .as_ref()
                    .ok_or_else(|| {
                        Error::RedisStore("URL is required for Single mode".to_string())
                    })?
                    .clone();

                let mut builder = RedisMode::single_url(url);

                if let Some(db) = redis_store_config.db {
                    builder = builder.db(db);
                }

                builder.build().map_err(|e| {
                    Error::RedisStore(format!("Failed to create Single Redis mode: {}", e))
                })
            }
            "sentinel" => {
                let sentinel_config = redis_store_config.sentinel.as_ref().ok_or_else(|| {
                    Error::RedisStore("Sentinel config is required for Sentinel mode".to_string())
                })?;

                let mut builder = RedisMode::sentinel(
                    sentinel_config.master_name.clone(),
                    sentinel_config.endpoints.clone(),
                );

                if let Some(sentinel_auth) = &sentinel_config.sentinel_auth {
                    let auth = parse_redis_auth(sentinel_auth.as_ref())?;
                    builder = builder.sentinel_auth(auth);
                }

                if let Some(redis_auth) = &sentinel_config.redis_auth {
                    let auth = parse_redis_auth(redis_auth.as_ref())?;
                    builder = builder.redis_auth(auth);
                }

                if let Some(sentinel_tls) = &sentinel_config.sentinel_tls {
                    let tls_info = create_redis_tls_info(sentinel_tls.as_ref())?;
                    builder = builder.sentinel_tls(tls_info);
                }

                if let Some(redis_tls) = &sentinel_config.redis_tls {
                    let tls_info = create_redis_tls_info(redis_tls.as_ref())?;
                    builder = builder.redis_tls(tls_info);
                }

                if let Some(db) = redis_store_config.db {
                    builder = builder.db(db);
                }

                builder.build().map_err(|e| {
                    Error::RedisStore(format!("Failed to create Sentinel Redis mode: {}", e))
                })
            }
            _ => Err(Error::RedisStore(format!(
                "Unsupported Redis mode: {}",
                redis_store_config.mode
            ))),
        }
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
    sentinel_auth: Option<RedisAuth>,
    redis_auth: Option<RedisAuth>,
    sentinel_tls: Option<RedisTlsInfo>,
    redis_tls: Option<RedisTlsInfo>,
    db: Option<i32>,
}

impl SentinelBuilder {
    fn new(master_name: String, endpoints: Vec<String>) -> Self {
        Self {
            master_name,
            endpoints,
            sentinel_auth: None,
            redis_auth: None,
            sentinel_tls: None,
            redis_tls: None,
            db: None,
        }
    }

    pub fn sentinel_auth(mut self, auth: RedisAuth) -> Self {
        self.sentinel_auth = Some(auth);
        self
    }

    pub fn redis_auth(mut self, auth: RedisAuth) -> Self {
        self.redis_auth = Some(auth);
        self
    }

    pub fn sentinel_tls(mut self, tls_info: RedisTlsInfo) -> Self {
        self.sentinel_tls = Some(tls_info);
        self
    }

    pub fn redis_tls(mut self, tls_info: RedisTlsInfo) -> Self {
        self.redis_tls = Some(tls_info);
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
            sentinel_auth: self.sentinel_auth,
            redis_auth: self.redis_auth,
            sentinel_tls: Box::new(self.sentinel_tls),
            redis_tls: Box::new(self.redis_tls),
            db: self.db,
        })
    }
}

impl RedisStore {
    pub async fn new(
        key_prefix: &'static str,
        stale_age: usize,
        mode: RedisMode,
    ) -> Result<Self, RedisError> {
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
            stale_age,
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
                sentinel_auth,
                redis_auth,
                sentinel_tls,
                redis_tls,
                db,
            } => {
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
                let mut builder = SentinelClientBuilder::new(
                    sentinel_addrs,
                    master_name,
                    SentinelServerType::Master,
                )?;

                // Apply sentinel authentication if provided
                if let Some(auth) = sentinel_auth {
                    if let Some(username) = auth.username {
                        builder = builder.set_client_to_sentinel_username(username);
                    }
                    if let Some(password) = auth.password {
                        builder = builder.set_client_to_sentinel_password(password);
                    }
                }

                // Apply sentinel TLS mode and certificates if provided
                if let Some(tls_info) = sentinel_tls.as_ref() {
                    let tls_mode = if tls_info.is_secure {
                        TlsMode::Secure
                    } else {
                        TlsMode::Insecure
                    };
                    builder = builder.set_client_to_sentinel_tls_mode(tls_mode);

                    if let Some(tls_certificates) = &tls_info.certificates {
                        builder =
                            builder.set_client_to_sentinel_certificates(tls_certificates.clone());
                    }
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

                // Apply Redis data node TLS mode and certificates if provided
                if let Some(tls_info) = redis_tls.as_ref() {
                    let tls_mode = if tls_info.is_secure {
                        TlsMode::Secure
                    } else {
                        TlsMode::Insecure
                    };
                    builder = builder.set_client_to_redis_tls_mode(tls_mode);

                    if let Some(tls_certificates) = &tls_info.certificates {
                        builder =
                            builder.set_client_to_redis_certificates(tls_certificates.clone());
                    }
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

/// Parse Redis authentication from the models.
fn parse_redis_auth(auth: &numaflow_models::models::RedisAuth) -> crate::Result<RedisAuth> {
    let username = auth
        .username
        .as_ref()
        .map(|secret_ref| get_secret_from_volume(&secret_ref.name, &secret_ref.key))
        .transpose()
        .map_err(|e| Error::RedisStore(format!("Failed to get username secret: {}", e)))?;

    let password = auth
        .password
        .as_ref()
        .map(|secret_ref| get_secret_from_volume(&secret_ref.name, &secret_ref.key))
        .transpose()
        .map_err(|e| Error::RedisStore(format!("Failed to get password secret: {}", e)))?;

    Ok(RedisAuth { username, password })
}

/// Create RedisTlsInfo from TLS configuration
fn create_redis_tls_info(tls: &numaflow_models::models::Tls) -> crate::Result<RedisTlsInfo> {
    let is_secure = !tls.insecure_skip_verify.unwrap_or(false);
    let certificates = if is_secure {
        Some(parse_redis_tls_config(tls)?)
    } else {
        None
    };

    Ok(RedisTlsInfo {
        is_secure,
        certificates,
    })
}

/// Parse Redis TLS configuration from the models.
fn parse_redis_tls_config(tls: &numaflow_models::models::Tls) -> crate::Result<TlsCertificates> {
    // Only extract certificates when secure mode is enabled
    let ca_cert = tls
        .ca_cert_secret
        .as_ref()
        .map(|secret_ref| get_secret_from_volume(&secret_ref.name, &secret_ref.key))
        .transpose()
        .map_err(|e| Error::RedisStore(format!("Failed to get CA cert secret: {}", e)))?
        .map(|cert| cert.into_bytes());

    let client_tls = match (&tls.cert_secret, &tls.key_secret) {
        (Some(cert_secret), Some(key_secret)) => {
            let client_cert =
                get_secret_from_volume(&cert_secret.name, &cert_secret.key).map_err(|e| {
                    Error::RedisStore(format!("Failed to get client cert secret: {}", e))
                })?;

            let client_key =
                get_secret_from_volume(&key_secret.name, &key_secret.key).map_err(|e| {
                    Error::RedisStore(format!("Failed to get client key secret: {}", e))
                })?;

            Some(ClientTlsConfig {
                client_cert: client_cert.into_bytes(),
                client_key: client_key.into_bytes(),
            })
        }
        (None, None) => None,
        (_, _) => {
            return Err(Error::RedisStore(
                "Both cert and key secrets must be provided for client TLS authentication"
                    .to_string(),
            ));
        }
    };

    Ok(TlsCertificates {
        client_tls,
        root_cert: ca_cert,
    })
}

#[cfg(test)]
const SECRET_BASE_PATH: &str = "/tmp/numaflow";

#[cfg(not(test))]
const SECRET_BASE_PATH: &str = "/var/numaflow/secrets";

/// Retrieve value from mounted secret volume
/// "/var/numaflow/secrets/${secretRef.name}/${secretRef.key}" is expected to be the file path
pub(crate) fn get_secret_from_volume(name: &str, key: &str) -> Result<String, String> {
    let path = format!("{SECRET_BASE_PATH}/{name}/{key}");
    let val = std::fs::read_to_string(path.clone())
        .map_err(|e| format!("Reading secret from file {path}: {e:?}"))?;
    Ok(val.trim().into())
}

impl Store for RedisStore {
    async fn register(
        &self,
        processor_id: &str,
        _cancel: CancellationToken,
    ) -> crate::Result<(usize, f32)> {
        // Currently registration script returns a tuple (pool_size, prev_max_filled)
        let result: Vec<String> = self
            .exec_lua_script(
                &self.register_script,
                &[self.key_prefix],
                &[processor_id, &self.stale_age.to_string()],
            )
            .await
            .map_err(|e| Error::Redis(e.to_string()))?;

        if result.len() != 2 {
            return Err(Error::Redis(
                "Invalid response from register script".to_string(),
            ));
        }

        let pool_size: usize = result
            .first()
            .expect("should have pool size")
            .parse()
            .map_err(|_| Error::Redis("Invalid pool size in response".to_string()))?;

        let prev_max_filled: f32 = result
            .get(1)
            .expect("should have prev_max_filled")
            .parse()
            .map_err(|_| Error::Redis("Invalid prev_max_filled in response".to_string()))?;

        Ok((pool_size, prev_max_filled))
    }

    async fn deregister(
        &self,
        processor_id: &str,
        prev_max_filled: f32,
        _cancel: CancellationToken,
    ) -> crate::Result<()> {
        let _: String = self
            .exec_lua_script(
                &self.deregister_script,
                &[self.key_prefix],
                &[processor_id, &prev_max_filled.to_string()],
            )
            .await
            .map_err(|e| Error::Redis(e.to_string()))?;

        Ok(())
    }

    async fn sync_pool_size(
        &self,
        processor_id: &str,
        pool_size: usize,
        _cancel: CancellationToken,
    ) -> crate::Result<Consensus> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs()
            .to_string();

        let pool_size_str = pool_size.to_string();
        let stale_age_str = self.stale_age.to_string();

        let result: Vec<String> = self
            .exec_lua_script(
                &self.sync_pool_size_script,
                &[self.key_prefix],
                &[processor_id, &timestamp, &pool_size_str, &stale_age_str],
            )
            .await
            .map_err(|e| Error::Redis(e.to_string()))?;

        if result.len() != 3 {
            return Err(Error::Redis(
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
            _ => Err(Error::Redis("Unknown consensus type".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use numaflow_models::models::{RedisSentinelConfig, Tls};
    use std::time::Duration;

    /// Test utilities for integration tests
    mod test_utils {
        use crate::state::store::redis_store::{RedisMode, RedisStore};

        /// Creates a Redis store for testing with a unique key prefix
        /// Returns None if Redis is not available
        pub async fn create_test_redis_store(
            test_name: &str,
            stale_age: usize,
        ) -> Option<RedisStore> {
            let redis_url = "redis://127.0.0.1:6379";

            // Check if Redis is available
            if let Err(_) = redis::Client::open(redis_url) {
                println!(
                    "Skipping Redis test - Redis server not available at {}",
                    redis_url
                );
                return None;
            }

            let test_key_prefix =
                Box::leak(format!("test_{}_{}", test_name, std::process::id()).into_boxed_str());
            let redis_mode = RedisMode::single_url(redis_url.to_string())
                .build()
                .unwrap();

            match RedisStore::new(test_key_prefix, stale_age, redis_mode).await {
                Ok(store) => Some(store),
                Err(e) => {
                    println!("Skipping Redis test - Failed to connect to Redis: {}", e);
                    None
                }
            }
        }

        /// Cleans up Redis keys after a test
        pub fn cleanup_redis_keys(test_name: &str) {
            let redis_url = "redis://127.0.0.1:6379";
            if let Ok(client) = redis::Client::open(redis_url)
                && let Ok(mut conn) = client.get_connection()
            {
                let test_key_prefix = format!("test_{}_{}", test_name, std::process::id());
                let _: Result<(), _> = redis::cmd("DEL")
                    .arg(format!("{}:heartbeats", test_key_prefix))
                    .arg(format!("{}:poolsize", test_key_prefix))
                    .arg(format!("{}:prev_max_filled", test_key_prefix))
                    .arg(format!("{}:prev_max_filled_ttl", test_key_prefix))
                    .query(&mut conn);
            }
        }
    }

    /// Comprehensive test for Redis mode creation from RateLimiterRedisStore configurations
    #[test]
    fn test_redis_mode_creation_comprehensive() {
        // Test 1: Single mode with minimal configuration
        let single_config = RateLimiterRedisStore {
            mode: "single".to_string(),
            url: Some("redis://localhost:6379".to_string()),
            db: None,
            sentinel: None,
        };

        let result = RedisMode::new(&single_config).unwrap();
        match result {
            RedisMode::SingleUrl { url, db } => {
                assert_eq!(url, "redis://localhost:6379");
                assert_eq!(db, None);
            }
            _ => panic!("Expected SingleUrl mode"),
        }

        // Test 2: Single mode with database specified
        let single_config_with_db = RateLimiterRedisStore {
            mode: "single".to_string(),
            url: Some("redis://localhost:6379".to_string()),
            db: Some(5),
            sentinel: None,
        };

        let result = RedisMode::new(&single_config_with_db).unwrap();
        match result {
            RedisMode::SingleUrl { url, db } => {
                assert_eq!(url, "redis://localhost:6379");
                assert_eq!(db, Some(5));
            }
            _ => panic!("Expected SingleUrl mode"),
        }

        // Test 3: Single mode missing URL (should fail)
        let single_config_no_url = RateLimiterRedisStore {
            mode: "single".to_string(),
            url: None,
            db: None,
            sentinel: None,
        };

        let result = RedisMode::new(&single_config_no_url);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("URL is required for Single mode")
        );

        // Test 4: Sentinel mode with minimal configuration
        let sentinel_config = RateLimiterRedisStore {
            mode: "sentinel".to_string(),
            url: None,
            db: None,
            sentinel: Some(Box::new(RedisSentinelConfig {
                master_name: "mymaster".to_string(),
                endpoints: vec!["sentinel1:26379".to_string(), "sentinel2:26379".to_string()],
                sentinel_auth: None,
                redis_auth: None,
                sentinel_tls: None,
                redis_tls: None,
            })),
        };

        let result = RedisMode::new(&sentinel_config).unwrap();
        match result {
            RedisMode::Sentinel {
                master_name,
                endpoints,
                sentinel_auth,
                redis_auth,
                sentinel_tls,
                redis_tls,
                db,
                ..
            } => {
                assert_eq!(master_name, "mymaster");
                assert_eq!(endpoints, vec!["sentinel1:26379", "sentinel2:26379"]);
                assert!(sentinel_auth.is_none());
                assert!(redis_auth.is_none());
                assert!(sentinel_tls.is_none());
                assert!(redis_tls.is_none());
                assert_eq!(db, None);
            }
            _ => panic!("Expected Sentinel mode"),
        }

        // Test 5: Sentinel mode with database specified
        let sentinel_config_with_db = RateLimiterRedisStore {
            mode: "sentinel".to_string(),
            url: None,
            db: Some(3),
            sentinel: Some(Box::new(RedisSentinelConfig {
                master_name: "mymaster".to_string(),
                endpoints: vec!["sentinel1:26379".to_string()],
                sentinel_auth: None,
                redis_auth: None,
                sentinel_tls: None,
                redis_tls: None,
            })),
        };

        let result = RedisMode::new(&sentinel_config_with_db).unwrap();
        match result {
            RedisMode::Sentinel { db, .. } => {
                assert_eq!(db, Some(3));
            }
            _ => panic!("Expected Sentinel mode"),
        }

        // Test 6: Sentinel mode with TLS configurations
        let sentinel_config_with_tls = RateLimiterRedisStore {
            mode: "sentinel".to_string(),
            url: None,
            db: None,
            sentinel: Some(Box::new(RedisSentinelConfig {
                master_name: "mymaster".to_string(),
                endpoints: vec!["sentinel1:26379".to_string()],
                sentinel_auth: None,
                redis_auth: None,
                sentinel_tls: Some(Box::new(Tls {
                    insecure_skip_verify: Some(true),
                    ca_cert_secret: None,
                    cert_secret: None,
                    key_secret: None,
                })),
                redis_tls: Some(Box::new(Tls {
                    insecure_skip_verify: Some(false),
                    ca_cert_secret: None,
                    cert_secret: None,
                    key_secret: None,
                })),
            })),
        };

        let result = RedisMode::new(&sentinel_config_with_tls).unwrap();
        match result {
            RedisMode::Sentinel {
                sentinel_tls,
                redis_tls,
                ..
            } => {
                assert!(sentinel_tls.is_some());
                assert!(redis_tls.is_some());
            }
            _ => panic!("Expected Sentinel mode"),
        }

        // Test 7: Sentinel mode missing sentinel config (should fail)
        let sentinel_config_no_sentinel = RateLimiterRedisStore {
            mode: "sentinel".to_string(),
            url: None,
            db: None,
            sentinel: None,
        };

        let result = RedisMode::new(&sentinel_config_no_sentinel);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Sentinel config is required for Sentinel mode")
        );

        // Test 8: Unsupported mode (should fail)
        let unsupported_config = RateLimiterRedisStore {
            mode: "cluster".to_string(),
            url: None,
            db: None,
            sentinel: None,
        };

        let result = RedisMode::new(&unsupported_config);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported Redis mode: cluster")
        );
    }

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
        .db(2)
        .build()
        .unwrap();

        match redis_mode {
            RedisMode::Sentinel {
                master_name,
                endpoints,
                db,
                ..
            } => {
                assert_eq!(master_name, "mymaster");
                assert_eq!(endpoints, vec!["sentinel1:26379", "sentinel2:26379"]);
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

        let tls_info = RedisTlsInfo {
            is_secure: true,
            certificates: Some(TlsCertificates {
                client_tls: None,
                root_cert: None,
            }),
        };

        let redis_mode =
            RedisMode::sentinel("mymaster".to_string(), vec!["sentinel1:26379".to_string()])
                .sentinel_auth(sentinel_auth.clone())
                .redis_auth(redis_auth.clone())
                .sentinel_tls(tls_info)
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
    fn test_parse_redis_tls_config_insecure_skip_verify() {
        use numaflow_models::models::Tls;

        // Test with insecure_skip_verify = true
        let tls_config_insecure = Tls {
            insecure_skip_verify: Some(true),
            ca_cert_secret: None,
            cert_secret: None,
            key_secret: None,
        };

        let result = parse_redis_tls_config(&tls_config_insecure).unwrap();
        assert!(result.client_tls.is_none());
        assert!(result.root_cert.is_none());

        // Test with insecure_skip_verify = false (should require certificates for full functionality)
        let tls_config_secure = Tls {
            insecure_skip_verify: Some(false),
            ca_cert_secret: None,
            cert_secret: None,
            key_secret: None,
        };

        let result = parse_redis_tls_config(&tls_config_secure).unwrap();
        assert!(result.client_tls.is_none()); // No client certs provided
        assert!(result.root_cert.is_none()); // No CA cert provided

        // Test with insecure_skip_verify = None (defaults to false)
        let tls_config_default = Tls {
            insecure_skip_verify: None,
            ca_cert_secret: None,
            cert_secret: None,
            key_secret: None,
        };

        let result = parse_redis_tls_config(&tls_config_default).unwrap();
        assert!(result.client_tls.is_none());
        assert!(result.root_cert.is_none());
    }

    #[test]
    fn test_create_redis_tls_info() {
        use numaflow_models::models::Tls;

        // Test with insecure_skip_verify = true
        let tls_config_insecure = Tls {
            insecure_skip_verify: Some(true),
            ca_cert_secret: None,
            cert_secret: None,
            key_secret: None,
        };

        let result = create_redis_tls_info(&tls_config_insecure).unwrap();
        assert!(!result.is_secure); // Should be insecure
        assert!(result.certificates.is_none()); // No certificates should be extracted

        // Test with insecure_skip_verify = false
        let tls_config_secure = Tls {
            insecure_skip_verify: Some(false),
            ca_cert_secret: None,
            cert_secret: None,
            key_secret: None,
        };

        let result = create_redis_tls_info(&tls_config_secure).unwrap();
        assert!(result.is_secure); // Should be secure
        assert!(result.certificates.is_some()); // Certificates should be extracted (even if empty)

        // Test with insecure_skip_verify = None (defaults to false/secure)
        let tls_config_default = Tls {
            insecure_skip_verify: None,
            ca_cert_secret: None,
            cert_secret: None,
            key_secret: None,
        };

        let result = create_redis_tls_info(&tls_config_default).unwrap();
        assert!(result.is_secure); // Should default to secure
        assert!(result.certificates.is_some()); // Certificates should be extracted
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

    #[tokio::test]
    async fn test_previously_stored_max_filled_at_deregister_redis() {
        let test_name = "test_previously_stored_max_filled_at_deregister_redis";
        let store = match test_utils::create_test_redis_store(test_name, 180).await {
            Some(store) => store,
            None => return, // Skip test if Redis is not available
        };

        let cancel = CancellationToken::new();

        store.register("processor_a", cancel.clone()).await.unwrap();
        let _ = store
            .sync_pool_size("processor_a", 1, cancel.clone())
            .await
            .unwrap();

        // Deregister processor_a and store max_filled as 10
        store
            .deregister("processor_a", 10.0, cancel.clone())
            .await
            .unwrap();

        // Register processor_a again and check if max_filled is 10
        let (_, prev_max_filled) = store.register("processor_a", cancel.clone()).await.unwrap();

        assert_eq!(prev_max_filled, 10.0);

        test_utils::cleanup_redis_keys(test_name);
    }

    #[tokio::test]
    async fn test_expired_stored_max_filled_at_deregister_redis() {
        let test_name = "test_expired_stored_max_filled_at_deregister_redis";
        let store = match test_utils::create_test_redis_store(test_name, 1).await {
            Some(store) => store,
            None => return, // Skip test if Redis is not available
        };
        let cancel = CancellationToken::new();

        store.register("processor_a", cancel.clone()).await.unwrap();
        let _ = store
            .sync_pool_size("processor_a", 1, cancel.clone())
            .await
            .unwrap();

        // Deregister processor_a and store max_filled as 10
        store
            .deregister("processor_a", 10.0, cancel.clone())
            .await
            .unwrap();

        // Wait for the stored max_filled to expire
        tokio::time::sleep(Duration::from_millis(2000)).await;

        // Register processor_a again and check if max_filled is 0
        let (_, prev_max_filled) = store.register("processor_a", cancel.clone()).await.unwrap();

        assert_eq!(prev_max_filled, 0.0);
        test_utils::cleanup_redis_keys(test_name);
    }
}
