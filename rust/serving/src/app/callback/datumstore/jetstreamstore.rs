use async_nats::jetstream::kv::Store;
use async_nats::jetstream::Context;
use bytes::Bytes;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::{debug, info};

use crate::app::callback::datumstore::{DatumStore, Error as StoreError, Result as StoreResult};

#[derive(Clone)]
pub(crate) struct JetStreamDatumStore {
    kv_store: Store,
}

impl JetStreamDatumStore {
    pub(crate) async fn new(js_context: Context, datum_store_name: &str) -> StoreResult<Self> {
        let kv_store = js_context
            .get_key_value(datum_store_name)
            .await
            .map_err(|e| StoreError::Connection(format!("Failed to get datum kv store: {e:?}")))?;
        Ok(Self { kv_store })
    }
}

impl DatumStore for JetStreamDatumStore {
    async fn retrieve_datum(&mut self, id: &str) -> StoreResult<Option<Vec<Vec<u8>>>> {
        let id = format!("{id}.response");
        let mut watcher = self
            .kv_store
            .watch_from_revision(id, 1)
            .await
            .map_err(|e| {
                StoreError::StoreRead(format!("Failed to watch request id in kv store: {e:?}"))
            })?;

        let mut results = Vec::new();
        while let Some(watch_event) = watcher.next().await {
            info!(?watch_event, "Received watch event");
            let entry = match watch_event {
                Ok(event) => event,
                Err(e) => {
                    tracing::error!(?e, "Received error from Jetstream KV watcher");
                    continue;
                }
            };

            if entry.operation == async_nats::jetstream::kv::Operation::Delete {
                debug!("Received delete event, stopping watcher");
                break;
            }

            results.push(entry.value.to_vec());
        }

        if results.is_empty() {
            Ok(None)
        } else {
            Ok(Some(results))
        }
    }

    async fn stream_response(
        &mut self,
        id: &str,
    ) -> StoreResult<(ReceiverStream<Arc<Bytes>>, JoinHandle<()>)> {
        let id = format!("{id}.response");
        let mut watcher = self
            .kv_store
            .watch_from_revision(id, 1)
            .await
            .map_err(|e| {
                StoreError::StoreRead(format!("Failed to watch request id in kv store: {e:?}"))
            })?;
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        let handle = tokio::spawn(async move {
            while let Some(watch_event) = watcher.next().await {
                let entry = match watch_event {
                    Ok(event) => event,
                    Err(e) => {
                        tracing::error!(?e, "Received error from Jetstream KV watcher");
                        continue;
                    }
                };

                if entry.operation == async_nats::jetstream::kv::Operation::Delete {
                    break;
                }

                if !entry.value.is_empty() {
                    tx.send(Arc::new(entry.value))
                        .await
                        .expect("Failed to send response");
                }
            }
        });

        Ok((ReceiverStream::new(rx), handle))
    }

    async fn ready(&mut self) -> bool {
        true
    }
}
