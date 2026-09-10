/// Jetstream ISB related utilities.
pub mod jetstream;

use crate::error::{Error, Result};
use crate::kv::KVStoreFactory;
use crate::kv::inmemory::InMemoryKVStoreFactory;
use crate::kv::jetstream::JetstreamKVStoreFactory;
use std::collections::HashMap;
use std::sync::Arc;

/// Env var used to select the ISB backend at startup.
pub const ENV_NUMAFLOW_ISBSVC_TYPE: &str = "NUMAFLOW_ISBSVC_TYPE";

/// ISB backend selected at startup. Single source of truth for core and sideinput.
#[derive(Debug, Clone, PartialEq)]
pub enum ISBClientConfig {
    Jetstream(jetstream::config::ClientConfig),
    /// Local testing only. Deliberately NOT reachable from `from_env`.
    InMemory,
}

impl ISBClientConfig {
    /// Reads `NUMAFLOW_ISBSVC_TYPE` (default `"jetstream"`); errors on anything else.
    pub fn from_env(env_vars: HashMap<String, String>) -> Result<Self> {
        let isb_type = env_vars
            .get(ENV_NUMAFLOW_ISBSVC_TYPE)
            .map(|s| s.as_str())
            .unwrap_or("jetstream");
        match isb_type {
            "jetstream" => {
                let js_cfg = jetstream::config::ClientConfig::load(env_vars.clone())
                    .map_err(|e| Error::Config(e.to_string()))?;
                Ok(ISBClientConfig::Jetstream(js_cfg))
            }
            other => Err(Error::Config(format!(
                "Unsupported ISB service type '{other}'. Supported: jetstream"
            ))),
        }
    }
}

/// Builds the KV-store factory for the configured backend.
/// One NATS connection per call — construct once per process.
pub async fn create_kv_store_factory(config: &ISBClientConfig) -> Result<Arc<dyn KVStoreFactory>> {
    match config {
        ISBClientConfig::Jetstream(cfg) => {
            let ctx = jetstream::create_js_context(cfg.clone()).await?;
            Ok(Arc::new(JetstreamKVStoreFactory::new(ctx)))
        }
        ISBClientConfig::InMemory => {
            tracing::warn!("in-memory ISB backend selected — for local testing only");
            Ok(Arc::new(InMemoryKVStoreFactory::new()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_env_unset_defaults_to_jetstream() {
        let mut env_vars = HashMap::new();
        env_vars.insert(
            "NUMAFLOW_ISBSVC_JETSTREAM_URL".to_string(),
            "nats://localhost:4222".to_string(),
        );

        let config = ISBClientConfig::from_env(env_vars).unwrap();
        match config {
            ISBClientConfig::Jetstream(cfg) => {
                assert_eq!(cfg.url, "nats://localhost:4222");
                assert_eq!(cfg.user, None);
                assert_eq!(cfg.password, None);
                assert!(!cfg.tls_enabled);
            }
            ISBClientConfig::InMemory => panic!("expected Jetstream variant"),
        }
    }

    #[test]
    fn test_from_env_explicit_jetstream() {
        let mut env_vars = HashMap::new();
        env_vars.insert(
            ENV_NUMAFLOW_ISBSVC_TYPE.to_string(),
            "jetstream".to_string(),
        );
        env_vars.insert(
            "NUMAFLOW_ISBSVC_JETSTREAM_URL".to_string(),
            "nats://localhost:4222".to_string(),
        );

        let config = ISBClientConfig::from_env(env_vars).unwrap();
        assert!(matches!(config, ISBClientConfig::Jetstream(_)));
    }

    #[test]
    fn test_from_env_unsupported_type_errors() {
        let mut env_vars = HashMap::new();
        env_vars.insert(ENV_NUMAFLOW_ISBSVC_TYPE.to_string(), "redis".to_string());

        let err = ISBClientConfig::from_env(env_vars).unwrap_err();
        assert!(
            err.to_string()
                .contains("Unsupported ISB service type 'redis'"),
            "unexpected error message: {err}"
        );
    }

    /// `InMemory` is only constructible programmatically (e.g. tests), never via
    /// `NUMAFLOW_ISBSVC_TYPE`. This test pins that: if a future reader "fixes" this by
    /// recognising "inmemory" as a valid env value, this test will fail.
    #[test]
    fn test_from_env_inmemory_is_not_env_reachable() {
        let mut env_vars = HashMap::new();
        env_vars.insert(ENV_NUMAFLOW_ISBSVC_TYPE.to_string(), "inmemory".to_string());

        let err = ISBClientConfig::from_env(env_vars).unwrap_err();
        assert!(
            err.to_string()
                .contains("Unsupported ISB service type 'inmemory'"),
            "unexpected error message: {err}"
        );
    }

    #[tokio::test]
    async fn test_create_kv_store_factory_inmemory_succeeds_without_nats() {
        let factory = create_kv_store_factory(&ISBClientConfig::InMemory)
            .await
            .expect("in-memory factory should build without a NATS server");

        let store = factory
            .create_kv_store("test-bucket".to_string())
            .await
            .expect("should create a kv store");
        assert_eq!(store.name(), "test-bucket");
    }
}
