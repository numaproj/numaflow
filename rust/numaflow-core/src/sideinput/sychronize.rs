use crate::config::pipeline::isb;
use crate::error::{Error, Result};
use crate::pipeline::create_js_context;
use async_nats::jetstream::kv::Watch;
use backoff::retry::Retry;
use backoff::strategy::fixed;
use tracing::error;

pub(crate) struct SideInputSynchronizer {
    side_inputs: Vec<&'static str>,
    js_client_config: isb::jetstream::ClientConfig,
}

impl SideInputSynchronizer {
    pub(crate) fn new(
        side_inputs: Vec<&'static str>,
        js_client_config: isb::jetstream::ClientConfig,
    ) -> Self {
        SideInputSynchronizer {
            side_inputs,
            js_client_config,
        }
    }

    pub(crate) async fn synchronize(self) -> Result<()> {
        let js_context = create_js_context(self.js_client_config.clone()).await?;

        unimplemented!()
    }

    async fn run(self) {}
}

/// creates a watcher for the given bucket, will retry infinitely until it succeeds
async fn create_watcher(bucket: async_nats::jetstream::kv::Store, key: &str) -> Watch {
    const RECONNECT_INTERVAL: u64 = 1000;
    // infinite retry
    let interval = fixed::Interval::from_millis(RECONNECT_INTERVAL).take(usize::MAX);

    Retry::retry(
        interval,
        async || match bucket.watch_all().await {
            Ok(w) => Ok(w),
            Err(e) => {
                error!(?e, "Failed to create watcher");
                Err(Error::Watermark(format!("Failed to create watcher: {}", e)))
            }
        },
        |_: &Error| true,
    )
    .await
    .expect("Failed to create ot watcher")
}
