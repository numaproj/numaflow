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
    /// Name of the vertex
    vertex: String,
    /// Name of the pipeline
    pipeline: String,
    /// Unique identifier of the message
    id: String,
    /// Time when the callback was made
    cb_time: u64,
    /// List of tags associated with the message
    tags: Vec<String>,
    /// Name of the vertex from which the message was sent
    from_vertex: String,
}

#[derive(Clone)]
pub struct CallbackHandler {
    client: Client,
    pipeline_name: String,
    vertex_name: String,
    semaphore: Arc<Semaphore>,
}

impl CallbackHandler {
    pub fn new(pipeline_name: String, vertex_name: String, concurrency_limit: usize) -> Self {
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
            pipeline_name,
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
        let uuid = message_headers.get(ID_HEADER).ok_or_else(|| {
            Error::Source(format!("{ID_HEADER} is not found in message headers",))
        })?;
        let cb_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time is older than Unix epoch time")
            .as_millis() as u64;

        let mut msg_tags = vec![];
        if let Some(tags) = message_tags {
            msg_tags = tags.iter().cloned().collect();
        };

        let callback_payload = CallbackPayload {
            vertex: self.vertex_name.clone(),
            pipeline: self.pipeline_name.clone(),
            id: uuid.to_owned(),
            cb_time,
            tags: msg_tags,
            from_vertex: previous_vertex,
        };

        let permit = Arc::clone(&self.semaphore).acquire_owned().await.unwrap();
        let client = self.client.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let resp = client
                .post(callback_url)
                .json(&callback_payload)
                .send()
                .await
                .map_err(|e| Error::Source(format!("Sending callback request: {e:?}")))
                .unwrap(); //FIXME:

            if !resp.status().is_success() {
                // return Err(Error::Source(format!(
                //     "Received non-OK status for callback request. Status={}, Body: {}",
                //     resp.status(),
                //     resp.text().await.unwrap_or_default()
                // )));
                tracing::error!("Received non-OK status for callback request"); //FIXME: what to do with errors
            }
        });

        Ok(())
    }
}
