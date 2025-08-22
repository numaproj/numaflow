use crate::state::Consensus;
use crate::state::store::Store;
use redis::sentinel::{SentinelClient, SentinelNodeConnectionInfo, SentinelServerType};
use redis::{Client, RedisError, Script};
use tokio_util::sync::CancellationToken;
use tracing::info;

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
    },
    Sentinel {
        sentinel_urls: Vec<String>,
        master_name: String,
        sentinel_conn_info: Option<SentinelNodeConnectionInfo>,
    },
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
            RedisMode::SingleUrl { url } => {
                // Plain single-instance
                let client = Client::open(url.as_str())?;
                let mgr = client.get_connection_manager().await?;
                Ok(mgr)
            }
            RedisMode::Sentinel {
                sentinel_urls,
                master_name,
                sentinel_conn_info: sentinel_auth,
            } => {
                // Build SentinelClient: wraps sentinel discovery and connection
                let mut sentinel_client = SentinelClient::build(
                    sentinel_urls.clone(),
                    master_name.clone(),
                    sentinel_auth,
                    SentinelServerType::Master,
                )?;

                // Get a Client to the master
                let master_client = sentinel_client.async_get_client().await?;

                let mgr = master_client.get_connection_manager().await?;
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

        let consensus_type = &result[0];
        let size: usize = result[1]
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
