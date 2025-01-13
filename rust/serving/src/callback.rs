use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use reqwest::Client;
use tokio::sync::Semaphore;

use crate::Error;

const ID_HEADER: &str = "X-Numaflow-Id";
const CALLBACK_URL_HEADER_KEY: &str = "X-Numaflow-Callback-Url";

/// The data to be sent in the POST request
#[derive(serde::Serialize)]
struct CallbackPayload {
    /// Unique identifier of the message
    id: String,
    /// Name of the vertex
    vertex: String,
    /// Time when the callback was made
    cb_time: u64,
    /// Name of the vertex from which the message was sent
    from_vertex: String,
    /// List of tags associated with the message
    tags: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct CallbackHandler {
    client: Client,
    vertex_name: String,
    semaphore: Arc<Semaphore>,
}

impl CallbackHandler {
    pub fn new(vertex_name: String, concurrency_limit: usize) -> Self {
        // if env::var(ENV_CALLBACK_ENABLED).is_err() {
        //     return Ok(None);
        // };

        // let Ok(callback_url) = env::var(ENV_CALLBACK_URL) else {
        //     return Err(Error::Source(format!("Environment variable {ENV_CALLBACK_ENABLED} is set, but {ENV_CALLBACK_URL} is not set")));
        // };

        let client = Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(1))
            .build()
            .expect("Creating callback client for Serving source");

        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency_limit));

        Self {
            client,
            vertex_name,
            semaphore,
        }
    }

    pub async fn callback(
        &self,
        message_headers: &HashMap<String, String>,
        message_tags: &Option<Arc<[String]>>,
        previous_vertex: String,
    ) -> crate::Result<()> {
        let callback_url = message_headers
            .get(CALLBACK_URL_HEADER_KEY)
            .ok_or_else(|| {
                Error::Source(format!(
                    "{CALLBACK_URL_HEADER_KEY} header is not present in the message headers",
                ))
            })?
            .to_owned();
        let uuid = message_headers
            .get(ID_HEADER)
            .ok_or_else(|| Error::Source(format!("{ID_HEADER} is not found in message headers",)))?
            .to_owned();
        let cb_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time is older than Unix epoch time")
            .as_millis() as u64;

        let mut msg_tags = None;
        if let Some(tags) = message_tags {
            if !tags.is_empty() {
                msg_tags = Some(tags.iter().cloned().collect());
            }
        };

        let callback_payload = CallbackPayload {
            vertex: self.vertex_name.clone(),
            id: uuid.clone(),
            cb_time,
            tags: msg_tags,
            from_vertex: previous_vertex,
        };

        let permit = Arc::clone(&self.semaphore).acquire_owned().await.unwrap();
        let client = self.client.clone();
        tokio::spawn(async move {
            let _permit = permit;
            // Retry incase of failure in making request.
            // When there is a failure, we retry after wait_secs. This value is doubled after each retry attempt.
            // Then longest wait time will be 64 seconds.
            let mut wait_secs = 1;
            const TOTAL_ATTEMPTS: usize = 7;
            for i in 1..=TOTAL_ATTEMPTS {
                let resp = client
                    .post(&callback_url)
                    .header(ID_HEADER, uuid.clone())
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
