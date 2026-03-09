//! Runtime error cache: periodically scrapes /runtime/errors from each MonoVertex pod
//! and caches the results for serving via GetMonoVertexErrors.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// How often to re-fetch errors from all active pods.
const REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Port on each MonoVertex pod that serves /runtime/errors. Matches
/// v1alpha1.MonoVertexMonitorPort in Go.
const MONITOR_PORT: u16 = 2470;

/// HTTP path for the runtime errors endpoint.
const RUNTIME_ERRORS_PATH: &str = "runtime/errors";

#[cfg(test)]
const DEFAULT_MAX_REPLICAS: i32 = 50;

// ── wire types from the pod HTTP API ─────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ErrorApiResponse {
    // The Go monitor server serializes this as `null` when there is no error,
    // so we must use Option<String> rather than String.
    #[serde(rename = "errorMessage")]
    err_message: Option<String>,
    #[serde(default)]
    data: Vec<ErrorDetails>,
}

#[derive(Debug, Deserialize)]
struct ErrorDetails {
    container: String,
    /// Unix milliseconds, matching the Go ErrorDetails.Timestamp field.
    timestamp: i64,
    code: String,
    message: String,
    details: String,
}

// ── internal types returned to callers ───────────────────────────────────────

/// Container-level error for a single replica.
#[derive(Debug, Clone)]
pub(crate) struct ContainerError {
    pub container: String,
    /// Unix milliseconds.
    pub timestamp: i64,
    pub code: String,
    pub message: String,
    pub details: String,
}

/// All container errors observed for one replica pod.
#[derive(Debug, Clone)]
pub(crate) struct ReplicaErrors {
    pub replica: String,
    pub container_errors: Vec<ContainerError>,
}

// ── RuntimeCache ──────────────────────────────────────────────────────────────

/// Caches runtime errors scraped from MonoVertex pods.
///
/// Background task fetches from all pods every [`REFRESH_INTERVAL`] and
/// overwrites individual replica entries in the cache.
pub(crate) struct RuntimeCache {
    /// MonoVertex name, e.g. "simple-mono-vertex".
    name: String,
    /// Kubernetes namespace.
    namespace: String,
    /// Upper bound for pod index probing (i.e. spec.scale.max).
    max_replicas: i32,
    /// Cache keyed by replica name, e.g. "simple-mono-vertex-mv-0".
    cache: Arc<RwLock<HashMap<String, ReplicaErrors>>>,
    client: Client,
}

impl RuntimeCache {
    pub(crate) fn new(name: String, namespace: String, max_replicas: i32) -> Self {
        let client = reqwest::ClientBuilder::new()
            // Pods use a self-signed TLS cert — skip verification, same as Go.
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(1))
            .build()
            .expect("Failed to build reqwest client for RuntimeCache");

        Self {
            name,
            namespace,
            max_replicas,
            cache: Arc::new(RwLock::new(HashMap::new())),
            client,
        }
    }

    /// Runs the background refresh loop until the cancellation token is fired.
    pub(crate) async fn run(&self, cln_token: CancellationToken) {
        // Fetch immediately on startup so the cache is warm before the first API call.
        self.fetch_all().await;

        let mut ticker = tokio::time::interval(REFRESH_INTERVAL);
        ticker.tick().await; // consume the instant first tick

        loop {
            tokio::select! {
                _ = ticker.tick() => self.fetch_all().await,
                _ = cln_token.cancelled() => {
                    debug!("RuntimeCache: cancellation received, stopping");
                    break;
                }
            }
        }
    }

    /// Fetches errors from all pods in parallel.
    async fn fetch_all(&self) {
        let mut handles = Vec::with_capacity(self.max_replicas as usize);
        for index in 0..self.max_replicas {
            let url = self.pod_url(index);
            let replica = format!("{}-mv-{}", self.name, index);
            let client = self.client.clone();
            let cache = Arc::clone(&self.cache);
            handles.push(tokio::spawn(async move {
                fetch_pod_errors(client, cache, replica, url).await;
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    }

    fn pod_url(&self, index: i32) -> String {
        // e.g. https://simple-mono-vertex-mv-0.simple-mono-vertex-mv-headless.default.svc:2470/runtime/errors
        format!(
            "https://{name}-mv-{index}.{name}-mv-headless.{ns}.svc:{port}/{path}",
            name = self.name,
            index = index,
            ns = self.namespace,
            port = MONITOR_PORT,
            path = RUNTIME_ERRORS_PATH,
        )
    }

    /// Returns a snapshot of all cached replica errors.
    pub(crate) async fn get_errors(&self) -> Vec<ReplicaErrors> {
        self.cache.read().await.values().cloned().collect()
    }
}

async fn fetch_pod_errors(
    client: Client,
    cache: Arc<RwLock<HashMap<String, ReplicaErrors>>>,
    replica: String,
    url: String,
) {
    let resp = match client.get(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            // Pod might be scaled down — warn rather than error.
            warn!("RuntimeCache: failed to reach {url}: {e}");
            return;
        }
    };

    let api_resp: ErrorApiResponse = match resp.json().await {
        Ok(r) => r,
        Err(e) => {
            error!("RuntimeCache: failed to parse response from {url}: {e}");
            return;
        }
    };

    // Skip if the API returned an error or no data (same logic as Go).
    if api_resp.err_message.is_some() || api_resp.data.is_empty() {
        return;
    }

    let container_errors: Vec<ContainerError> = api_resp
        .data
        .into_iter()
        .map(|e| ContainerError {
            container: e.container,
            timestamp: e.timestamp,
            code: e.code,
            message: e.message,
            details: e.details,
        })
        .collect();

    let entry = ReplicaErrors {
        replica: replica.clone(),
        container_errors,
    };

    debug!("RuntimeCache: persisting errors for {replica}");
    cache.write().await.insert(replica, entry);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pod_url_format() {
        let cache = RuntimeCache::new(
            "simple-mono-vertex".to_string(),
            "default".to_string(),
            DEFAULT_MAX_REPLICAS,
        );
        assert_eq!(
            cache.pod_url(0),
            "https://simple-mono-vertex-mv-0.simple-mono-vertex-mv-headless.default.svc:2470/runtime/errors"
        );
        assert_eq!(
            cache.pod_url(3),
            "https://simple-mono-vertex-mv-3.simple-mono-vertex-mv-headless.default.svc:2470/runtime/errors"
        );
    }
}
