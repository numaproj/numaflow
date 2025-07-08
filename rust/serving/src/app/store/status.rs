//! Status tracking for requests using jetstream key-value store.
//!JetStream Status Entry uses the below format:
//! - Status key - `status.{id}`
//! - Status value - JSON serialized [ProcessingStatus]

use crate::config::RequestType;
use async_nats::jetstream::Context;
use async_nats::jetstream::kv::{CreateErrorKind, Store};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::result;
use thiserror::Error;
use tracing::{error, info, warn};

const STATUS_KEY_PREFIX: &str = "status";
const RESPONSE_KEY_PREFIX: &str = "rs";
const START_PROCESSING_MARKER: &str = "start.processing";
const DONE_PROCESSING_MARKER: &str = "done.processing";

#[derive(Error, Debug, Clone)]
pub(crate) enum Error {
    #[error("Invalid status : {0}")]
    InvalidStatus(String),
    #[error("Client cancelled")]
    Cancelled {
        err: String,
        previous_pod_hash: String,
    },
    #[error("Duplicate request : {0}")]
    Duplicate(String),
    #[error("Other : {0}")]
    Other(String),
}

pub(crate) type Result<T> = result::Result<T, Error>;

impl From<Error> for crate::Error {
    fn from(value: Error) -> Self {
        crate::Error::Status(value.to_string())
    }
}

/// Represents the current processing status of a request id in the Jetstream KV Store. It also has the
/// information about the pod hash which has accepted the request.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) enum ProcessingStatus {
    /// InProgress indicates that the request is currently being processed.
    InProgress { pod_hash: String },
    /// Completed indicates that the request has been completed successfully.
    Completed {
        subgraph: String, // subgraph of the completed request
        pod_hash: String,
    },
    /// Failed indicates that the request has failed.
    Failed {
        error: String, // error message of the failed request
        pod_hash: String,
    },
}

impl TryFrom<Bytes> for ProcessingStatus {
    type Error = serde_json::Error;

    fn try_from(value: Bytes) -> result::Result<Self, Self::Error> {
        serde_json::from_slice(&value)
    }
}

impl TryFrom<ProcessingStatus> for Bytes {
    type Error = serde_json::Error;
    fn try_from(status: ProcessingStatus) -> result::Result<Bytes, serde_json::Error> {
        Ok(Bytes::from(serde_json::to_vec(&status)?))
    }
}

/// StatusTracker is responsible for tracking the status of requests in the KV store.
#[derive(Clone)]
pub(crate) struct StatusTracker {
    pod_hash: &'static str,
    status_kv: Store,
    /// Optional response kv store to write start and done processing marker. For user defined store,
    /// this is not used.
    response_kv: Option<Store>,
}

impl StatusTracker {
    pub(crate) async fn new(
        js_context: Context,
        status_bucket: &'static str,
        pod_hash: &'static str,
        response_bucket: Option<String>,
    ) -> Result<Self> {
        let status_kv = js_context.get_key_value(status_bucket).await.map_err(|e| {
            Error::Other(format!(
                "Failed to get status kv store '{status_bucket}': {e:?}"
            ))
        })?;

        // we need to optional response kv store to write start and done processing marker for the
        // response.
        let response_kv = match &response_bucket {
            None => None,
            Some(bucket_name) => {
                let response_kv = js_context.get_key_value(bucket_name).await.map_err(|e| {
                    Error::Other(format!(
                        "Failed to get response kv store '{bucket_name}': {e:?}"
                    ))
                })?;
                Some(response_kv)
            }
        };

        Ok(Self {
            status_kv,
            pod_hash,
            response_kv,
        })
    }

    /// register a request id in the status kv store.
    pub(crate) async fn register(&self, id: &str, request_type: RequestType) -> Result<()> {
        let status_key = format!("{STATUS_KEY_PREFIX}.{id}");
        let initial_status = ProcessingStatus::InProgress {
            pod_hash: self.pod_hash.to_string(),
        };

        match self
            .status_kv
            .create(
                &status_key,
                initial_status
                    .clone()
                    .try_into()
                    .expect("status to bytes failed"),
            )
            .await
        {
            Ok(_) => {
                // if the request is registered successfully, we need to write the start processing marker
                // to signal the response watcher to wait for the responses. We need to do it here, since
                // only the status tracker will know the lifecycle of the request.
                // TODO: This makes `discard` method trickier.
                if let Some(response_kv) = &self.response_kv {
                    let response_key = format!(
                        "{}.{}.{}.{}",
                        RESPONSE_KEY_PREFIX, self.pod_hash, id, START_PROCESSING_MARKER
                    );
                    let request_type_bytes: Bytes = request_type
                        .try_into()
                        .expect("Failed to convert request type");
                    response_kv
                        .put(&response_key, request_type_bytes)
                        .await
                        .map_err(|e| {
                            Error::Other(format!("Failed to create response entry for {id}: {e:?}"))
                        })?;
                }
                Ok(())
            }
            Err(e) if e.kind() == CreateErrorKind::AlreadyExists => {
                warn!(%id, %status_key, "Request ID already exists.");
                let existing_status = self.get_status(&status_key).await?;
                self.handle_existing_status(id, &status_key, existing_status)
            }
            Err(e) => {
                error!(id, status_key, error = ?e, "Failed to create status entry.");
                Err(Error::Other(format!(
                    "Failed to register request id {status_key}: {e:?}"
                )))
            }
        }
    }

    /// Helper to fetch and deserialize the status from the KV store.
    async fn get_status(&self, status_key: &str) -> Result<ProcessingStatus> {
        let status_entry =
            self.status_kv.get(status_key).await.map_err(|e| {
                Error::Other(format!("Failed to get status for {status_key}: {e:?}"))
            })?;
        let status_bytes = status_entry
            .ok_or_else(|| Error::Other(format!("Status entry not found for {status_key}")))?;
        ProcessingStatus::try_from(status_bytes).map_err(|e| {
            Error::Other(format!(
                "Failed to deserialize status for {status_key}: {e:?}"
            ))
        })
    }

    /// Handles scenarios where the status for a request ID already exists in the KV store.
    ///
    /// * [ProcessingStatus::Completed]: Indicates the request has already been processed. In this
    ///   case, a duplicate error is returned.
    /// * [ProcessingStatus::InProgress]: If the request is currently being processed and the pod
    ///   hash matches the current pod, it is treated as a duplicate, and a duplicate error is
    ///   returned. If the pod hash does not match, it means that pod crashed before sending the
    ///   response. A 500 status code is returned and the request cannot be retried.
    /// * [ProcessingStatus::Failed]: Indicates the request failed due to client cancellation or a
    ///   failure in the serving pod (pod that accepted the request). In this case, the request can
    ///   be retried.
    fn handle_existing_status(
        &self,
        id: &str,
        status_key: &str,
        existing_status: ProcessingStatus,
    ) -> Result<()> {
        match existing_status {
            ProcessingStatus::InProgress { pod_hash } => {
                if pod_hash == self.pod_hash {
                    warn!(%id, %status_key, "Request is getting processed");
                    Err(Error::Duplicate(id.to_string()))
                } else {
                    // TODO: There can be a case where the pod terminates without updating the request
                    //  status(SIGKILL). Return with a body asking for retrying with a different ID.
                    warn!(
                        %id,
                        %status_key, "Request is getting processed in another pod"
                    );
                    Err(Error::Other(
                        "Request is getting processed in another pod".to_string(),
                    ))
                }
            }
            ProcessingStatus::Completed { .. } => {
                warn!(
                    %id,
                    %status_key, "Request already completed, returning as duplicate."
                );
                Err(Error::Duplicate(id.to_string()))
            }
            ProcessingStatus::Failed { error, pod_hash } => {
                warn!(%id, %status_key, %error, "Request had failed, retrying...");
                Err(Error::Cancelled {
                    err: error,
                    previous_pod_hash: pod_hash,
                })
            }
        }
    }

    /// returns the status of the request by checking the status kv store.
    pub(crate) async fn status(&mut self, id: &str) -> Result<ProcessingStatus> {
        let key = format!("{STATUS_KEY_PREFIX}.{id}");
        let entry = self.status_kv.get(&key).await.map_err(|e| {
            Error::Other(format!("Failed to get status for request id {id}: {e:?}"))
        })?;
        match entry {
            Some(status) => ProcessingStatus::try_from(status)
                .map_err(|e| Error::Other(format!("Failed to deserialize status for {id}: {e:?}"))),
            None => Err(Error::InvalidStatus(id.to_string())),
        }
    }

    /// Mark the request as failed in the status kv store.
    pub(crate) async fn mark_as_failed(&mut self, id: &str, error: &str) -> Result<()> {
        info!(id, replica_id = ?self.pod_hash, error, "Marking request as failed");
        let failed_status = ProcessingStatus::Failed {
            error: error.to_string(),
            pod_hash: self.pod_hash.to_string(),
        };
        self.update_status(id, failed_status).await
    }

    /// Update the status of a request in the status kv store.
    async fn update_status(&self, id: &str, status: ProcessingStatus) -> Result<()> {
        let key = format!("{STATUS_KEY_PREFIX}.{id}");

        let status_bytes: Bytes = status.try_into().expect("Failed to convert into bytes");

        self.status_kv
            .put(key, status_bytes)
            .await
            .map_err(|e| Error::Other(format!("Failed to update status for {id}: {e:?}")))?;
        Ok(())
    }

    /// Deregister marks the request as completed in the status kv store, it also (unlike registed)
    /// writes a done processing marker to the response kv store (if nats is used) to signal the
    /// response watcher.
    pub(crate) async fn deregister(
        &self,
        id: &str,
        subgraph: &str,
        pod_hash: Option<String>,
    ) -> Result<()> {
        let pod_hash = pod_hash.unwrap_or(self.pod_hash.to_string());

        let completed_status = ProcessingStatus::Completed {
            subgraph: subgraph.to_string(),
            pod_hash: pod_hash.clone(),
        };

        self.update_status(id, completed_status).await?;

        let Some(response_kv) = &self.response_kv else {
            return Ok(());
        };

        // if nats is used, we need to write the done processing marker to the response kv store to
        // let the response watcher know that the processing is done. Since only tracker knows the
        // lifecycle of the request, we need to do it here.
        let done_key = format!("{RESPONSE_KEY_PREFIX}.{pod_hash}.{id}.{DONE_PROCESSING_MARKER}");
        response_kv
            .put(done_key.clone(), Bytes::new())
            .await
            .map_err(|e| {
                Error::Other(format!(
                    "Failed to write done processing marker {done_key}: {e:?}"
                ))
            })?;
        Ok(())
    }

    /// Discard the request from the status kv store. This is used when the request is not accepted
    /// because of back pressure.
    /// It deletes the entry from the status kv store (so ID is removed and can be retried) and also
    /// sends a done processing marker to the response kv store (if nats is used) to signal the end.
    /// This is because we just start processing marker in the register before inserting into the pipeline.
    pub(crate) async fn discard(&self, id: &str) -> Result<()> {
        let key = format!("{STATUS_KEY_PREFIX}.{id}");
        self.status_kv
            .delete(&key)
            .await
            .map_err(|e| Error::Other(format!("Failed to delete status for {id}: {e:?}")))?;

        let Some(response_kv) = &self.response_kv else {
            return Ok(());
        };
        let done_key = format!(
            "{}.{}.{}.{}",
            RESPONSE_KEY_PREFIX, self.pod_hash, id, DONE_PROCESSING_MARKER
        );

        response_kv
            .put(done_key.clone(), Bytes::new())
            .await
            .map_err(|e| {
                Error::Other(format!(
                    "Failed to write done processing marker {done_key}: {e:?}"
                ))
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_nats::jetstream;
    use async_nats::jetstream::kv::Config;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_register_request() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let status_bucket = "test_register_status";
        let response_bucket = "test_register_response";

        // Clean up buckets
        let _ = context.delete_key_value(status_bucket).await;
        let _ = context.delete_key_value(response_bucket).await;

        // Create buckets
        context
            .create_key_value(Config {
                bucket: status_bucket.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        context
            .create_key_value(Config {
                bucket: response_bucket.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut tracker = StatusTracker::new(
            context.clone(),
            status_bucket,
            "test_pod",
            Some(response_bucket.to_string()),
        )
        .await
        .unwrap();

        let request_id = "test_request";
        tracker
            .register(request_id, RequestType::Sync)
            .await
            .expect("Failed to register request");

        // Verify status
        let status = tracker.status(request_id).await.unwrap();
        assert_eq!(
            status,
            ProcessingStatus::InProgress {
                pod_hash: "test_pod".to_string()
            }
        );

        // Verify start processing marker
        let response_kv = context.get_key_value(response_bucket).await.unwrap();
        let start_key = format!(
            "{}.{}.{}.{}",
            RESPONSE_KEY_PREFIX, "test_pod", request_id, START_PROCESSING_MARKER
        );
        let start_marker = response_kv.get(&start_key).await.unwrap();
        assert!(start_marker.is_some());

        // Clean up
        context.delete_key_value(status_bucket).await.unwrap();
        context.delete_key_value(response_bucket).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_handle_existing_status() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let status_bucket = "test_existing_status";

        // Clean up bucket
        let _ = context.delete_key_value(status_bucket).await;

        // Create bucket
        context
            .create_key_value(Config {
                bucket: status_bucket.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let tracker = StatusTracker::new(context.clone(), status_bucket, "test_pod", None)
            .await
            .unwrap();

        let request_id = "test_request";
        tracker
            .register(request_id, RequestType::Sync)
            .await
            .expect("Failed to register request");

        // Attempt to register the same request again
        let result = tracker.register(request_id, RequestType::Sync).await;
        assert!(matches!(result, Err(Error::Duplicate(_))));

        // Clean up
        context.delete_key_value(status_bucket).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_mark_as_failed() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let status_bucket = "test_mark_failed";

        // Clean up bucket
        let _ = context.delete_key_value(status_bucket).await;

        // Create bucket
        context
            .create_key_value(Config {
                bucket: status_bucket.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut tracker = StatusTracker::new(context.clone(), status_bucket, "test_pod", None)
            .await
            .unwrap();

        let request_id = "test_request";
        tracker
            .register(request_id, RequestType::Sync)
            .await
            .expect("Failed to register request");

        tracker
            .mark_as_failed(request_id, "Test error")
            .await
            .expect("Failed to mark request as failed");

        // Verify status
        let status = tracker.status(request_id).await.unwrap();
        assert_eq!(
            status,
            ProcessingStatus::Failed {
                error: "Test error".to_string(),
                pod_hash: "test_pod".to_string()
            }
        );

        // Clean up
        context.delete_key_value(status_bucket).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_deregister_request() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let status_bucket = "test_deregister_status";
        let response_bucket = "test_deregister_response";

        // Clean up buckets
        let _ = context.delete_key_value(status_bucket).await;
        let _ = context.delete_key_value(response_bucket).await;

        // Create buckets
        context
            .create_key_value(Config {
                bucket: status_bucket.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        context
            .create_key_value(Config {
                bucket: response_bucket.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut tracker = StatusTracker::new(
            context.clone(),
            status_bucket,
            "test_pod",
            Some(response_bucket.to_string()),
        )
        .await
        .unwrap();

        let request_id = "test_request";
        tracker
            .register(request_id, RequestType::Sync)
            .await
            .expect("Failed to register request");

        tracker
            .deregister(request_id, "test_subgraph", None)
            .await
            .expect("Failed to deregister request");

        // Verify status
        let status = tracker.status(request_id).await;
        assert!(matches!(status, Ok(ProcessingStatus::Completed { .. })));

        // Verify done processing marker
        let response_kv = context.get_key_value(response_bucket).await.unwrap();
        let done_key = format!(
            "{}.{}.{}.{}",
            RESPONSE_KEY_PREFIX, "test_pod", request_id, DONE_PROCESSING_MARKER
        );
        let done_marker = response_kv.get(&done_key).await.unwrap();
        assert!(done_marker.is_some());

        // Clean up
        context.delete_key_value(status_bucket).await.unwrap();
        context.delete_key_value(response_bucket).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_discard_request() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let status_bucket = "test_discard_status";
        let response_bucket = "test_discard_response";

        // Clean up buckets
        let _ = context.delete_key_value(status_bucket).await;
        let _ = context.delete_key_value(response_bucket).await;

        // Create buckets
        context
            .create_key_value(Config {
                bucket: status_bucket.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        context
            .create_key_value(Config {
                bucket: response_bucket.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut tracker = StatusTracker::new(
            context.clone(),
            status_bucket,
            "test_pod",
            Some(response_bucket.to_string()),
        )
        .await
        .unwrap();

        let request_id = "test_request";
        tracker
            .register(request_id, RequestType::Sync)
            .await
            .expect("Failed to register request");

        tracker
            .discard(request_id)
            .await
            .expect("Failed to discard request");

        // Verify status is removed
        let status = tracker.status(request_id).await;
        assert!(status.is_err());

        // Verify done processing marker is sent so that response watcher can clean up
        let response_kv = context.get_key_value(response_bucket).await.unwrap();
        let done_key = format!(
            "{}.{}.{}.{}",
            RESPONSE_KEY_PREFIX, "test_pod", request_id, DONE_PROCESSING_MARKER
        );
        let done_marker = response_kv.get(&done_key).await.unwrap();
        assert!(done_marker.is_some());

        // Clean up
        context.delete_key_value(status_bucket).await.unwrap();
        context.delete_key_value(response_bucket).await.unwrap();
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_handle_existing_status_cases() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let status_bucket = "test_handle_existing_status_cases";

        // Clean up bucket
        let _ = context.delete_key_value(status_bucket).await;

        // Create bucket
        context
            .create_key_value(Config {
                bucket: status_bucket.to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let tracker = StatusTracker::new(context.clone(), status_bucket, "test_pod", None)
            .await
            .unwrap();

        let request_id = "test_request";

        // Case 1: InProgress (different pod)
        tracker
            .update_status(
                request_id,
                ProcessingStatus::InProgress {
                    pod_hash: "other_pod".to_string(),
                },
            )
            .await
            .unwrap();
        let result = tracker.register(request_id, RequestType::Sync).await;
        assert!(matches!(result, Err(Error::Other(_))));

        // Case 2: Completed
        tracker
            .update_status(
                request_id,
                ProcessingStatus::Completed {
                    subgraph: "test_subgraph".to_string(),
                    pod_hash: "test_pod".to_string(),
                },
            )
            .await
            .unwrap();
        let result = tracker.register(request_id, RequestType::Sync).await;
        assert!(matches!(result, Err(Error::Duplicate(_))));

        // Case 3: Failed
        tracker
            .update_status(
                request_id,
                ProcessingStatus::Failed {
                    error: "Test error".to_string(),
                    pod_hash: "test_pod".to_string(),
                },
            )
            .await
            .unwrap();
        let result = tracker.register(request_id, RequestType::Sync).await;
        assert!(matches!(result, Err(Error::Cancelled { .. })));

        // Clean up
        context.delete_key_value(status_bucket).await.unwrap();
    }
}
