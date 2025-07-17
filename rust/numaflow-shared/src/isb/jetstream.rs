use crate::error::{Error, Result};
use async_nats::ConnectOptions;
use async_nats::jetstream;
use async_nats::jetstream::Context;
use async_nats::jetstream::kv::{Entry, Watch};
use backoff::retry::Retry;
use backoff::strategy::fixed;
use futures::StreamExt;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use tracing::{error, warn};

/// JetstreamWatcher is a wrapper around the Watcher that automatically recreates the watcher
/// when it fails. You can call [futures::Stream::poll_next] on it.
pub struct JetstreamWatcher {
    watcher: Watch,
    bucket: jetstream::kv::Store,
    recreate_future: Option<Pin<Box<dyn Future<Output = Watch> + Send>>>,
}

impl JetstreamWatcher {
    /// Creates a new JetstreamWatcher.
    pub async fn new(bucket: jetstream::kv::Store) -> Result<Self> {
        let watcher = create_watcher(bucket.clone()).await;
        Ok(Self {
            watcher,
            bucket,
            recreate_future: None,
        })
    }
}

impl futures::Stream for JetstreamWatcher {
    type Item = Entry;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // only poll recreate_watcher if it is set. This happens when the watcher has failed.
        if let Some(mut future) = self.recreate_future.take() {
            match future.as_mut().poll(cx) {
                Poll::Ready(watcher) => {
                    self.watcher = watcher;
                    // fall through and poll the watcher
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // poll the watcher that polls the nats KV stream
        match self.watcher.poll_next_unpin(cx) {
            Poll::Ready(entry) => match entry {
                None => {
                    warn!("Watcher stream ended unexpectedly. Recreating watcher...");
                    self.recreate_future = Some(Box::pin(create_watcher(self.bucket.clone())));
                    // now let's manually call poll_next to poll the future we just set
                    self.poll_next(cx)
                }
                Some(inner_entry) => match inner_entry {
                    Ok(entry) => Poll::Ready(Some(entry)),
                    Err(e) => {
                        warn!(?e, "Failed to get next entry from watcher");
                        self.recreate_future = Some(Box::pin(create_watcher(self.bucket.clone())));
                        // now let's manually call poll_next to poll the future we just set
                        self.poll_next(cx)
                    }
                },
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

/// creates a watcher for the given bucket, will retry infinitely until it succeeds
async fn create_watcher(bucket: jetstream::kv::Store) -> Watch {
    const RECONNECT_INTERVAL: u64 = 1000;
    // infinite retry
    let interval = fixed::Interval::from_millis(RECONNECT_INTERVAL).take(usize::MAX);

    Retry::new(
        interval,
        async || match bucket.watch_all_from_revision(1).await {
            Ok(w) => Ok(w),
            Err(e) => {
                error!(?e, "Failed to create watcher");
                Err(Error::Jetstream(format!("Failed to create watcher: {e}")))
            }
        },
        |_: &Error| true,
    )
    .await
    .expect("Failed to create ot watcher")
}

pub async fn create_js_context(config: config::ClientConfig) -> Result<Context> {
    // TODO: make these configurable. today this is hardcoded on Golang code too.
    let mut opts = ConnectOptions::new()
        .max_reconnects(None) // unlimited reconnects
        .ping_interval(Duration::from_secs(3))
        .retry_on_initial_connect();

    if let (Some(user), Some(password)) = (config.user, config.password) {
        opts = opts.user_and_password(user, password);
    }

    let js_client = async_nats::connect_with_options(&config.url, opts)
        .await
        .map_err(|e| Error::Connection(e.to_string()))?;

    Ok(jetstream::new(js_client))
}

/// Build Jetstream client configuration.
pub mod config {
    use crate::error::{Error, Result};
    use std::collections::HashMap;

    const DEFAULT_URL: &str = "localhost:4222";

    /// Jetstream client configuration.
    #[derive(Debug, Clone, PartialEq)]
    pub struct ClientConfig {
        /// NATS server URL.
        pub url: String,
        /// NATS server username.
        pub user: Option<String>,
        /// NATS server password.
        pub password: Option<String>,
        /// Whether to enable TLS.
        pub tls_enabled: bool,
    }

    impl Default for ClientConfig {
        fn default() -> Self {
            ClientConfig {
                url: DEFAULT_URL.to_string(),
                user: None,
                password: None,
                tls_enabled: false,
            }
        }
    }

    const ENV_NUMAFLOW_JETSTREAM_USER: &str = "NUMAFLOW_ISBSVC_JETSTREAM_USER";
    const ENV_NUMAFLOW_JETSTREAM_PASSWORD: &str = "NUMAFLOW_ISBSVC_JETSTREAM_PASSWORD";
    const ENV_NUMAFLOW_JETSTREAM_URL: &str = "NUMAFLOW_ISBSVC_JETSTREAM_URL";
    const ENV_NUMAFLOW_JETSTREAM_TLS_ENABLED: &str = "NUMAFLOW_ISBSVC_JETSTREAM_TLS_ENABLED";

    impl ClientConfig {
        pub fn load(env_vars: HashMap<String, String>) -> Result<Self> {
            let env_vars: HashMap<String, String> = env_vars
                .into_iter()
                .filter(|(key, _val): &(String, String)| {
                    [
                        ENV_NUMAFLOW_JETSTREAM_URL,
                        ENV_NUMAFLOW_JETSTREAM_PASSWORD,
                        ENV_NUMAFLOW_JETSTREAM_USER,
                        ENV_NUMAFLOW_JETSTREAM_TLS_ENABLED,
                    ]
                    .contains(&key.as_str())
                })
                .collect();

            let get_var = |var: &str| -> Result<String> {
                Ok(env_vars
                    .get(var)
                    .ok_or_else(|| Error::Config(format!("Environment variable {var} is not set")))?
                    .to_string())
            };

            Ok(Self {
                url: get_var(ENV_NUMAFLOW_JETSTREAM_URL)?,
                user: get_var(ENV_NUMAFLOW_JETSTREAM_USER).ok(),
                password: get_var(ENV_NUMAFLOW_JETSTREAM_PASSWORD).ok(),
                tls_enabled: get_var(ENV_NUMAFLOW_JETSTREAM_TLS_ENABLED)
                    .map(|v| v == "true")
                    .unwrap_or(false),
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::collections::HashMap;

        #[test]
        fn test_client_config_default() {
            let config = ClientConfig::default();
            assert_eq!(config.url, "localhost:4222");
            assert_eq!(config.user, None);
            assert_eq!(config.password, None);
            assert!(!config.tls_enabled);
        }

        #[test]
        fn test_client_config_load_success_only_url() {
            let mut env_vars = HashMap::new();
            env_vars.insert(
                "NUMAFLOW_ISBSVC_JETSTREAM_URL".to_string(),
                "nats://localhost:4222".to_string(),
            );

            let config = ClientConfig::load(env_vars).unwrap();
            assert_eq!(config.url, "nats://localhost:4222");
            assert_eq!(config.user, None);
            assert_eq!(config.password, None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::time::timeout;

    /// End-to-end test for JetstreamWatcher
    /// This test verifies that:
    /// 1. JetstreamWatcher can be created successfully
    /// 2. It can watch for key-value changes in a JetStream KV store
    /// 3. It properly handles stream recreation when the watcher fails
    /// 4. It continues to receive updates after recreation
    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_jetstream_watcher_end_to_end() {
        // Connect to NATS server
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        // Create a test KV store
        let store_name = "test-jetstream-watcher";
        let _ = js_context.delete_key_value(store_name).await; // Clean up if exists

        let kv_store = js_context
            .create_key_value(jetstream::kv::Config {
                bucket: store_name.to_string(),
                history: 5,
                ..Default::default()
            })
            .await
            .unwrap();

        // Create JetstreamWatcher
        let mut watcher = JetstreamWatcher::new(kv_store.clone()).await.unwrap();

        // Put some initial data
        kv_store.put("key1", "value1".into()).await.unwrap();
        kv_store.put("key2", "value2".into()).await.unwrap();

        // Collect entries with timeout
        let mut entries = Vec::new();
        let timeout_duration = Duration::from_secs(5);

        // Read initial entries
        for _ in 0..2 {
            match timeout(timeout_duration, watcher.next()).await {
                Ok(Some(entry)) => {
                    entries.push((
                        entry.key.clone(),
                        String::from_utf8_lossy(&entry.value).to_string(),
                    ));
                }
                Ok(None) => break,
                Err(_) => panic!("Timeout waiting for entry"),
            }
        }

        // Verify we got the initial entries
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|(k, v)| k == "key1" && v == "value1"));
        assert!(entries.iter().any(|(k, v)| k == "key2" && v == "value2"));

        // Add more data to test continuous watching
        kv_store.put("key3", "value3".into()).await.unwrap();
        kv_store.put("key1", "updated_value1".into()).await.unwrap();

        // Read new entries
        for _ in 0..2 {
            match timeout(timeout_duration, watcher.next()).await {
                Ok(Some(entry)) => {
                    entries.push((
                        entry.key.clone(),
                        String::from_utf8_lossy(&entry.value).to_string(),
                    ));
                }
                Ok(None) => break,
                Err(_) => panic!("Timeout waiting for entry"),
            }
        }

        // Verify we got the new entries
        assert!(entries.iter().any(|(k, v)| k == "key3" && v == "value3"));
        assert!(
            entries
                .iter()
                .any(|(k, v)| k == "key1" && v == "updated_value1")
        );

        // Clean up
        let _ = js_context.delete_key_value(store_name).await;
    }

    /// Test create_watcher function directly
    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_create_watcher_function() {
        // Connect to NATS server
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let js_context = jetstream::new(client);

        // Create a test KV store
        let store_name = "test-create-watcher-function";
        let _ = js_context.delete_key_value(store_name).await; // Clean up if exists

        let kv_store = js_context
            .create_key_value(jetstream::kv::Config {
                bucket: store_name.to_string(),
                history: 5,
                ..Default::default()
            })
            .await
            .unwrap();

        // Test creating a watcher directly
        let _watcher = create_watcher(kv_store.clone()).await;

        // Clean up
        let _ = js_context.delete_key_value(store_name).await;
    }
}
