use async_nats::jetstream::kv::{CreateErrorKind, Store};
use async_nats::jetstream::Context;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::result;
use thiserror::Error;
use tracing::{debug, error, info, warn};

const STATUS_KEY_PREFIX: &str = "status";
const RESPONSE_KEY_PREFIX: &str = "rs";
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

/// Represents the current processing status of a request id in the `Store`.
// Redefined ProcessingStatus - Merged Status and Replica Info
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub(crate) enum ProcessingStatus {
    InProgress {
        pod_hash: String,
    },
    Completed {
        subgraph: String, // subgraph of the completed request
        pod_hash: String,
    },
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

#[derive(Clone)]
pub(crate) struct StatusTracker {
    pod_hash: String,
    status_kv: Store,
    response_kv: Option<Store>,
}

impl StatusTracker {
    pub(crate) async fn new(
        js_context: Context,
        status_bucket: &str,
        pod_hash: String,
        response_bucket: Option<String>,
    ) -> Result<Self> {
        let status_kv = js_context.get_key_value(status_bucket).await.map_err(|e| {
            Error::Other(format!(
                "Failed to get status kv store '{status_bucket}': {e:?}"
            ))
        })?;

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
    pub(crate) async fn register(&self, id: &str) -> Result<()> {
        let status_key = format!("{}.{}", STATUS_KEY_PREFIX, id);
        let initial_status = ProcessingStatus::InProgress {
            pod_hash: self.pod_hash.clone(),
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
                info!(id, status_key, "Request registered successfully.");
                Ok(())
            }
            Err(e) if e.kind() == CreateErrorKind::AlreadyExists => {
                warn!(id, status_key, "Request ID already exists.");
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

    /// Handle the case where the status already exists.
    fn handle_existing_status(
        &self,
        id: &str,
        status_key: &str,
        existing_status: ProcessingStatus,
    ) -> Result<()> {
        match existing_status {
            ProcessingStatus::InProgress { pod_hash } if pod_hash == self.pod_hash => {
                warn!(
                    id,
                    status_key, "Request already exists with the same pod hash."
                );
                Err(Error::Duplicate(status_key.to_string()))
            }
            ProcessingStatus::Completed { .. } => {
                warn!(
                    id,
                    status_key, "Request already completed, returning as duplicate."
                );
                Err(Error::Duplicate(status_key.to_string()))
            }
            ProcessingStatus::Failed { error, pod_hash } => {
                warn!(id, status_key, error = ?error, "Request had failed, retrying...");
                Err(Error::Cancelled {
                    err: error,
                    previous_pod_hash: pod_hash,
                })
            }
            _ => {
                error!(id, status_key, "Unexpected status encountered.");
                Err(Error::Other(format!(
                    "Unexpected status for {status_key}: {existing_status:?}"
                )))
            }
        }
    }
    /// returns the status of the request by checking the status kv store.
    pub(crate) async fn status(&mut self, id: &str) -> Result<ProcessingStatus> {
        let key = format!("{}.{}", STATUS_KEY_PREFIX, id);
        let entry = self.status_kv.get(&key).await.map_err(|e| {
            Error::Other(format!("Failed to get status for request id {id}: {e:?}"))
        })?;
        match entry {
            Some(status) => ProcessingStatus::try_from(status)
                .map_err(|e| Error::Other(format!("Failed to deserialize status for {id}: {e:?}"))),
            None => Err(Error::InvalidStatus(id.to_string())),
        }
    }

    /// Mark the request as failed and remove the callback sender from the map, will be invoked
    /// when the request fails.
    pub(crate) async fn mark_as_failed(&mut self, id: &str, error: &str) -> Result<()> {
        info!(id, replica_id = ?self.pod_hash, error, "Marking request as failed");
        let failed_status = ProcessingStatus::Failed {
            error: error.to_string(),
            pod_hash: self.pod_hash.clone(),
        };
        self.update_status(id, failed_status).await
    }

    /// Update the status of a request in the status kv store.
    async fn update_status(&self, id: &str, status: ProcessingStatus) -> Result<()> {
        let key = format!("{}.{}", STATUS_KEY_PREFIX, id);

        let status_bytes: Bytes = status.try_into().expect("Failed to convert into bytes");

        self.status_kv
            .put(key, status_bytes)
            .await
            .map_err(|e| Error::Other(format!("Failed to update status for {id}: {e:?}")))?;
        Ok(())
    }

    /// Deregister the request id from the status kv store, sets the status as completed.
    pub(crate) async fn deregister(&self, id: &str, subgraph: &str) -> Result<()> {
        let completed_status = ProcessingStatus::Completed {
            subgraph: subgraph.to_string(),
            pod_hash: self.pod_hash.clone(),
        };

        self.update_status(id, completed_status).await?;

        // we need to give a done signal for response watcher
        let done_key = format!(
            "{}.{}.{}.{}",
            RESPONSE_KEY_PREFIX, self.pod_hash, id, DONE_PROCESSING_MARKER
        );

        if let Some(response_kv) = &self.response_kv {
            match response_kv.put(done_key.clone(), Bytes::new()).await {
                Ok(_) => {
                    debug!(?id, ?done_key, "Successfully wrote done processing marker.");
                }
                Err(e) => {
                    error!(?id, ?done_key, error = ?e, "Failed to write done processing marker.");
                    return Err(Error::Other(format!(
                        "Failed to write done marker {done_key}: {e:?}"
                    )));
                }
            }
        };

        Ok(())
    }
}
