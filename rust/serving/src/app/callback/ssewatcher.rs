use std::sync::Arc;

use async_nats::jetstream::kv::Store;
use async_nats::jetstream::Context;
use bytes::Bytes;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::info;

use crate::app::callback::datumstore::{Error as StoreError, Result as StoreResult};

#[derive(Clone)]
pub(crate) struct SSEResponseWatcher {
    kv_store: Store,
}

impl SSEResponseWatcher {
    pub(crate) async fn new(js_context: Context, store_name: &str) -> StoreResult<Self> {
        let kv_store = js_context
            .get_key_value(store_name)
            .await
            .map_err(|e| StoreError::Connection(format!("Failed to get js kv store: {e:?}")))?;
        Ok(Self { kv_store })
    }

    pub(crate) async fn watch_response(
        &mut self,
        id: &str,
    ) -> StoreResult<(ReceiverStream<Arc<Bytes>>, JoinHandle<()>)> {
        let id = format!("{id}=response");
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
                if entry.value == Bytes::from_static(b"deleted") {
                    info!("Received delete event, breaking");
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
}
