use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::config::DEFAULT_ID_HEADER;

/// As message passes through each component (map, transformer, sink, etc.). it emits a beacon via callback
/// to inform that message has been processed by this component.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Callback {
    pub(crate) id: String,
    pub(crate) vertex: String,
    pub(crate) cb_time: u64,
    pub(crate) from_vertex: String,
    /// Due to flat-map operation, we can have 0 or more responses.
    pub(crate) responses: Vec<Response>,
}

/// It contains details about the `To` vertex via tags (conditional forwarding).
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Response {
    /// If tags is None, the message is forwarded to all vertices, if len(Vec) == 0, it means that
    /// the message has been dropped.
    pub(crate) tags: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct CallbackHandler {
    client: Client,
    vertex_name: String,
    semaphore: Arc<Semaphore>,
}

impl CallbackHandler {
    pub fn new(vertex_name: String, concurrency_limit: usize) -> Self {
        let client = Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(1))
            .build()
            .expect("Creating callback client for Serving source");

        let semaphore = Arc::new(Semaphore::new(concurrency_limit));

        Self {
            client,
            vertex_name,
            semaphore,
        }
    }

    pub async fn callback(
        &self,
        id: String,
        callback_url: String,
        previous_vertex: String,
        responses: Vec<Option<Vec<String>>>,
    ) -> crate::Result<()> {
        let cb_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time is older than Unix epoch time")
            .as_millis() as u64;

        let responses = responses
            .into_iter()
            .map(|tags| Response { tags })
            .collect();

        let callback_payload = Callback {
            vertex: self.vertex_name.clone(),
            id: id.clone(),
            cb_time,
            responses,
            from_vertex: previous_vertex,
        };

        let permit = Arc::clone(&self.semaphore).acquire_owned().await.unwrap();
        let client = self.client.clone();
        tokio::spawn(async move {
            let _permit = permit;
            // Retry in case of failure in making request.
            // When there is a failure, we retry after wait_secs. This value is doubled after each retry attempt.
            // Then longest wait time will be 64 seconds.
            let mut wait_secs = 1;
            const TOTAL_ATTEMPTS: usize = 7;
            for i in 1..=TOTAL_ATTEMPTS {
                let resp = client
                    .post(&callback_url)
                    .header(DEFAULT_ID_HEADER, id.clone())
                    .json(&[&callback_payload])
                    .send()
                    .await;
                let resp = match resp {
                    Ok(resp) => resp,
                    Err(e) => {
                        if i < TOTAL_ATTEMPTS {
                            tracing::warn!(
                                ?e,
                                "Sending callback request failed. Will retry after a delay"
                            );
                            tokio::time::sleep(Duration::from_secs(wait_secs)).await;
                            wait_secs *= 2;
                        } else {
                            tracing::error!(?e, "Sending callback request failed");
                        }
                        continue;
                    }
                };

                if resp.status().is_success() {
                    break;
                }

                if resp.status().is_client_error() {
                    // TODO: When the source serving pod restarts, the callbacks will fail with 4xx status
                    // since the request ID won't be available in it's in-memory tracker.
                    // No point in retrying such cases
                    // 4xx can also happen if payload is wrong (due to bugs in the code). We should differentiate
                    // between what can be retried and not.
                    let status_code = resp.status();
                    let response_body = resp.text().await;
                    tracing::error!(
                        ?status_code,
                        ?response_body,
                        "Received client error while making callback. Callback will not be retried"
                    );
                    break;
                }

                let status_code = resp.status();
                let response_body = resp.text().await;
                if i < TOTAL_ATTEMPTS {
                    tracing::warn!(
                        ?status_code,
                        ?response_body,
                        "Received non-OK status for callback request. Will retry after a delay"
                    );
                    tokio::time::sleep(Duration::from_secs(wait_secs)).await;
                    wait_secs *= 2;
                } else {
                    tracing::error!(
                        ?status_code,
                        ?response_body,
                        "Received non-OK status for callback request"
                    );
                }
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::app::callback::state::State as CallbackState;
    use crate::app::callback::store::memstore::InMemoryStore;
    use crate::app::start_main_server;
    use crate::app::tracker::MessageGraph;
    use crate::callback::CallbackHandler;
    use crate::config::generate_certs;
    use crate::pipeline::PipelineDCG;
    use crate::{AppState, Settings};
    use axum_server::tls_rustls::RustlsConfig;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn test_callback() -> Result<()> {
        // Set up the CryptoProvider (controls core cryptography used by rustls) for the process
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let (cert, key) = generate_certs()?;

        let tls_config = RustlsConfig::from_pem(cert.pem().into(), key.serialize_pem().into())
            .await
            .map_err(|e| format!("Failed to create tls config {:?}", e))?;

        let settings = Settings {
            app_listen_port: 3003,
            ..Default::default()
        };
        // We start the 'Serving' https server with an in-memory store
        // When the server receives callback request, the in-memory store will be populated.
        // This is verified at the end of the test.
        let store = InMemoryStore::new();
        let message_graph = MessageGraph::from_pipeline(&PipelineDCG::default())?;
        let (tx, _) = mpsc::channel(10);

        let mut app_state = AppState {
            message: tx,
            settings: Arc::new(settings),
            callback_state: CallbackState::new(message_graph, store.clone()).await?,
        };

        // We use this value as the request id of the callback request
        const ID_VALUE: &str = "1234";

        // Register the request id in the store. This normally happens when the Serving source
        // receives a request from the client. The callbacks for this request must only happen after this.
        let _callback_notify_rx = app_state.callback_state.register(ID_VALUE.into());

        let server_handle = tokio::spawn(start_main_server(app_state, tls_config));

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .danger_accept_invalid_certs(true)
            .build()?;

        // Wait for the server to be ready
        let mut server_ready = false;
        for _ in 0..10 {
            let resp = client.get("https://localhost:3003/livez").send().await?;
            if resp.status().is_success() {
                server_ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(server_ready, "Server is not ready");

        let callback_handler = CallbackHandler::new("test".into(), 10);

        // On the server, this fails with SubGraphInvalidInput("Invalid callback: 1234, vertex: in")
        // We get 200 OK response from the server, since we already registered this request ID in the store.
        callback_handler
            .callback(
                ID_VALUE.into(),
                "https://localhost:3003/v1/process/callback".into(),
                "in".into(),
                vec![],
            )
            .await?;
        let mut data = None;
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(2)).await;
            data = {
                let guard = store.data.lock().unwrap();
                guard.get(ID_VALUE).cloned()
            };
            if data.is_some() {
                break;
            }
        }
        assert!(data.is_some(), "Callback data not found in store");
        server_handle.abort();
        Ok(())
    }
}