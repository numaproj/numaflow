use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::Context;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::app::orchestrator::OrchestratorState as CallbackState;
use crate::app::store::cbstore::jetstreamstore::JetStreamCallbackStore;
use crate::app::store::datastore::jetstream::JetStreamDataStore;
use crate::app::store::datastore::user_defined::UserDefinedStore;
use crate::app::tracker::MessageGraph;
use crate::config::{StoreType, DEFAULT_ID_HEADER};
use crate::{Error, Result};
use crate::{Settings, DEFAULT_CALLBACK_URL_HEADER_KEY};

/// [Message] with a oneshot for notifying when the message has been completed processed.
pub(crate) struct MessageWrapper {
    // TODO: this might be more that saving to ISB.
    pub(crate) confirm_save: oneshot::Sender<()>,
    pub(crate) message: Message,
}

/// Serving payload passed on to Numaflow.
#[derive(Debug)]
pub struct Message {
    pub value: Bytes,
    pub id: String,
    pub headers: HashMap<String, String>,
}

enum ActorMessage {
    Read {
        batch_size: usize,
        timeout_at: Instant,
        reply_to: oneshot::Sender<Result<Vec<Message>>>,
    },
    Ack {
        offsets: Vec<String>,
        reply_to: oneshot::Sender<Result<()>>,
    },
}

pub async fn start(js_context: Context, settings: Arc<Settings>) -> Result<()> {
    // create a callback store for tracking
    let callback_store =
        JetStreamCallbackStore::new(js_context.clone(), &settings.js_store).await?;
    // Create the message graph from the pipeline spec and the redis store
    let msg_graph = MessageGraph::from_pipeline(&settings.pipeline_spec).map_err(|e| {
        Error::InitError(format!(
            "Creating message graph from pipeline spec: {:?}",
            e
        ))
    })?;

    // Create a redis store to store the callbacks and the custom responses
    match &settings.store_type {
        StoreType::Nats => {
            let nats_store =
                JetStreamDataStore::new(js_context.clone(), &settings.js_store).await?;
            let callback_state = CallbackState::new(msg_graph, nats_store, callback_store).await?;
            let app = crate::AppState {
                js_context,
                settings,
                orchestrator_state: callback_state,
            };
            crate::serve(app).await.unwrap();
        }
        StoreType::UserDefined(ud_config) => {
            let ud_store = UserDefinedStore::new(ud_config.clone()).await?;
            let callback_state = CallbackState::new(msg_graph, ud_store, callback_store).await?;
            let app = crate::AppState {
                js_context,
                settings,
                orchestrator_state: callback_state,
            };
            crate::serve(app).await.unwrap();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use async_nats::jetstream;

    use super::ServingSource;
    use crate::Settings;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_serving_source() -> Result<()> {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let store_name = "test_serving_source";

        let _ = context.delete_key_value(store_name).await;
        context
            .create_key_value(jetstream::kv::Config {
                bucket: store_name.to_string(),
                history: 5,
                ..Default::default()
            })
            .await
            .unwrap();

        // Set up the CryptoProvider (controls core cryptography used by rustls) for the process
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let settings = Arc::new(Settings {
            js_store: store_name.to_string(),
            app_listen_port: 2444,
            ..Default::default()
        });

        let serving_source = ServingSource::new(
            context,
            Arc::clone(&settings),
            10,
            Duration::from_millis(1),
            0,
        )
        .await?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();

        // Wait for the server
        for _ in 0..10 {
            let resp = client
                .get(format!(
                    "https://localhost:{}/livez",
                    settings.app_listen_port
                ))
                .send()
                .await;
            if resp.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let mut messages = serving_source.read_messages().await.unwrap();
                if messages.is_empty() {
                    // Server has not received any requests yet
                    continue;
                }
                assert_eq!(messages.len(), 1);
                let msg = messages.remove(0);
                serving_source
                    .ack_messages(vec![format!("{}-0", msg.id)])
                    .await
                    .unwrap();
                break;
            }
        });

        let resp = client
            .post(format!(
                "https://localhost:{}/v1/process/async",
                settings.app_listen_port
            ))
            .json("test-payload")
            .send()
            .await?;

        assert!(resp.status().is_success());
        Ok(())
    }
}
