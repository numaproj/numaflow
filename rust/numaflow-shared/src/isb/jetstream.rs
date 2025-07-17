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

pub struct JetstreamWatcher {
    watcher: Watch,
    bucket: jetstream::kv::Store,
    recreate_future: Option<Pin<Box<dyn Future<Output = Watch> + Send>>>,
}

impl JetstreamWatcher {
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
    type Item = Result<Entry>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // only poll the future when the watcher has failed (that is when we set this future)
        if let Some(mut future) = self.recreate_future.take() {
            match future.as_mut().poll(cx) {
                Poll::Ready(watcher) => {
                    self.watcher = watcher;
                    // fall through and poll the watcher
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        match self.watcher.poll_next_unpin(cx) {
            Poll::Ready(entry) => match entry {
                None => {
                    warn!("Watcher stream ended unexpectedly. Recreating watcher...");
                    self.recreate_future = Some(Box::pin(create_watcher(self.bucket.clone())));
                    // now let's manually call poll_next to poll the future we just set
                    self.poll_next(cx)
                }
                Some(inner_entry) => match inner_entry {
                    Ok(entry) => Poll::Ready(Some(Ok(entry))),
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
}
