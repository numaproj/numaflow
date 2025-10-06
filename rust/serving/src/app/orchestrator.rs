//! To process a message in Serving, we need to track the progress of the processing state of the message
//! and then fetch the messages once the results of the processing is stored in [crate::app::store::datastore] by
//! different Sink. The processing state is stored in [crate::app::store::cbstore].

use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{Instrument, error, info, trace, warn};

use crate::Error;
use crate::app::store::cbstore::CallbackStore;
use crate::app::store::datastore::DataStore;
use crate::app::store::status;
use crate::app::store::status::{ProcessingStatus, StatusTracker};
use crate::app::tracker::MessageGraph;
use crate::config::RequestType;
use crate::metrics::serving_metrics;

#[derive(Clone)]
pub(crate) struct OrchestratorState<T, U> {
    msg_graph_generator: Arc<MessageGraph>,
    datum_store: T,
    callback_store: U,
    status_tracker: StatusTracker,
}

impl<T, U> OrchestratorState<T, U>
where
    T: Clone + Send + Sync + DataStore + 'static,
    U: Clone + Send + Sync + CallbackStore + 'static,
{
    /// Create a new State to track connections and callback data
    pub(crate) async fn new(
        msg_graph: MessageGraph,
        datum_store: T,
        callback_store: U,
        status_tracker: StatusTracker,
    ) -> crate::Result<Self> {
        Ok(Self {
            msg_graph_generator: Arc::new(msg_graph),
            datum_store,
            callback_store,
            status_tracker,
        })
    }

    /// register a new connection and spawns a watcher to watch on the callback stream. An oneshot rx is
    /// returned. This oneshot rx will be notified when all callbacks for this connection is received
    /// from the numaflow pipeline.
    pub(crate) async fn process_request(
        &mut self,
        id: &str,
        request_type: RequestType,
    ) -> crate::Result<oneshot::Receiver<Result<String, Error>>> {
        let (tx, rx) = oneshot::channel();
        let sub_graph_generator = Arc::clone(&self.msg_graph_generator);
        let msg_id = id.to_string();

        // register the request id in the status tracker, if the request is already processed
        // or if its getting processed the tracker will return duplicate error. If the request
        // was cancelled before sending the response then the previous pod hash will be returned
        // with cancelled error, and we will fetch the callbacks and responses again.
        let pod_hash = match self.status_tracker.register(id, request_type).await {
            Ok(_) => None,
            Err(e) => match e {
                status::Error::Cancelled {
                    err,
                    previous_pod_hash,
                } => {
                    warn!(
                        error = %err,
                        "Request was cancelled, fetching callbacks and responses again"
                    );
                    Some(previous_pod_hash)
                }
                status::Error::Duplicate(msg) => {
                    warn!(error = %msg, "Request already exists in the store");
                    serving_metrics().request_register_duplicate_count.inc();
                    return Err(Error::Duplicate(msg));
                }
                _ => {
                    error!(?e, "Failed to register request id in status store");
                    serving_metrics().request_register_fail_count.inc();
                    return Err(e.into());
                }
            },
        };

        // start watching for callbacks
        let mut callbacks_stream = self
            .callback_store
            .register_and_watch(id, pod_hash.clone())
            .await?;

        let status_tracker = self.status_tracker.clone();
        let mut cb_store = self.callback_store.clone();
        let span = tracing::Span::current();
        let callback_watcher = async move {
            let mut callbacks = Vec::new();
            while let Some(cb) = callbacks_stream.next().await {
                trace!(?cb, ?msg_id, "Received callback");
                callbacks.push(cb);
                let subgraph = sub_graph_generator
                    .generate_subgraph_from_callbacks(msg_id.clone(), callbacks.clone())
                    .expect("Failed to generate subgraph");

                if let Some(graph) = subgraph {
                    status_tracker
                        .deregister(&msg_id, &graph, pod_hash)
                        .await
                        .expect("Failed to deregister");
                    cb_store
                        .deregister(&msg_id)
                        .await
                        .expect("Failed to deregister");

                    // send can only fail if the request was cancelled by the client or the handler task
                    // was terminated.
                    tx.send(Ok(graph))
                        .expect("receiver was dropped, request is cancelled");
                    break;
                }
            }
        };

        // https://github.com/tokio-rs/tracing/blob/b4868674ba73f3963b125ec38b57efaadc714d90/examples/examples/tokio-spawny-thing.rs#L25
        tokio::spawn(callback_watcher.instrument(span));
        Ok(rx)
    }

    /// Retrieves the output of the processed request after checking whether the processing is complete.
    pub(crate) async fn retrieve_saved(
        &mut self,
        id: &str,
        request_type: RequestType,
    ) -> Result<Option<Vec<Vec<u8>>>, Error> {
        // check the status of the request, if its completed, then retrieve the data
        let status = self.status_tracker.status(id).await?;
        match status {
            ProcessingStatus::InProgress { .. } => {
                info!(?id, "Request is still in progress");
                Ok(None)
            }
            ProcessingStatus::Completed { .. } => {
                let data = self.datum_store.retrieve_data(id, None).await?;
                Ok(Some(data))
            }
            ProcessingStatus::Failed { error, pod_hash } => {
                warn!(
                    %error,
                    "Request was failed, processing and fetching the responses again"
                );
                let notify = self.process_request(id, request_type).await?;
                notify.await.expect("sender was dropped")?;
                Ok(Some(
                    self.datum_store.retrieve_data(id, Some(pod_hash)).await?,
                ))
            }
        }
    }

    /// Listens on watcher events (SSE uses KV watch) and checks with the Graph is complete. Once
    /// Graph is complete, it will deregister and closes the outbound SSE channel. The returned
    /// rx contains the stream of results (result per sink).
    pub(crate) async fn stream_response(
        &mut self,
        id: &str,
        request_type: RequestType,
    ) -> Result<ReceiverStream<Arc<Bytes>>, Error> {
        let (tx, rx) = mpsc::channel(10);
        let sub_graph_generator = Arc::clone(&self.msg_graph_generator);
        let msg_id = id.to_string();

        // register the request id in the status tracker, if the request is already processed
        // or if its getting processed the tracker will return duplicate error. If the request
        // was cancelled before sending the response then the previous pod hash will be returned
        // with cancelled error, and we will fetch the callbacks and responses again.
        let pod_hash = match self.status_tracker.register(id, request_type).await {
            Ok(_) => None,
            Err(e) => match e {
                status::Error::Cancelled {
                    err,
                    previous_pod_hash,
                } => {
                    warn!(
                        error = %err,
                        "Request was cancelled, fetching callbacks and responses again"
                    );
                    Some(previous_pod_hash)
                }
                status::Error::Duplicate(msg) => {
                    warn!(error = %msg, "Request already exists in the tracker");
                    serving_metrics().request_register_duplicate_count.inc();
                    return Err(Error::Duplicate(msg));
                }
                _ => {
                    error!(?e, "Failed to register request id in status tracker");
                    serving_metrics().request_register_fail_count.inc();
                    return Err(e.into());
                }
            },
        };

        // start watching for callbacks
        let mut callbacks_stream = self
            .callback_store
            .register_and_watch(id, pod_hash.clone())
            .await?;

        let mut cb_store = self.callback_store.clone();
        let status_tracker = self.status_tracker.clone();
        let pod_hash_clone = pod_hash.clone();
        tokio::spawn(async move {
            let mut callbacks = Vec::new();
            while let Some(cb) = callbacks_stream.next().await {
                callbacks.push(cb);
                let subgraph = match sub_graph_generator
                    .generate_subgraph_from_callbacks(msg_id.clone(), callbacks.clone())
                {
                    Ok(subgraph) => subgraph,
                    Err(e) => {
                        error!(?e, "Failed to generate subgraph");
                        break;
                    }
                };

                if let Some(graph) = subgraph {
                    status_tracker
                        .deregister(&msg_id, &graph, pod_hash_clone)
                        .await
                        .expect("Failed to deregister in status tracker");
                    cb_store
                        .deregister(&msg_id)
                        .await
                        .expect("Failed to deregister in callback store");
                    break;
                }
            }
        });

        // watch for data stored in the datastore
        let request_id = id.to_string();
        let mut status_tracker = self.status_tracker.clone();
        let mut response_stream = self.datum_store.stream_data(id, pod_hash).await?;
        tokio::spawn(async move {
            while let Some(response) = response_stream.next().await {
                if tx.send(response).await.is_err() {
                    error!("Failed to send response");
                    status_tracker
                        .mark_as_failed(&request_id, "Cancelled")
                        .await
                        .expect("Failed to mark the request has failed");
                }
            }
        });

        Ok(ReceiverStream::new(rx))
    }

    /// Get the subgraph for the given ID from persistent store. This is used querying for the status from the service endpoint even after the
    /// request has been completed.
    pub(crate) async fn retrieve_subgraph_from_storage(
        &mut self,
        id: &str,
    ) -> Result<String, Error> {
        let status = self.status_tracker.status(id).await?;
        match status {
            ProcessingStatus::InProgress { .. } => Ok("Request In Progress".to_string()),
            ProcessingStatus::Completed { subgraph, .. } => Ok(subgraph),
            ProcessingStatus::Failed { error, .. } => {
                error!(?error, "Request failed");
                Err(Error::SubGraphGenerator(error))
            }
        }
    }

    /// marks the request as failed
    pub(crate) async fn mark_as_failed(&mut self, id: &str, error: &str) -> Result<(), Error> {
        self.status_tracker.mark_as_failed(id, error).await?;
        Ok(())
    }

    /// discards the request from the status tracker and callback store. This is used when the
    /// request has not been accepted due to back pressure.
    pub(crate) async fn discard(&mut self, id: &str) -> Result<(), Error> {
        self.status_tracker.discard(id).await?;
        self.callback_store.deregister(id).await?;
        Ok(())
    }

    // Check if the store is ready
    pub(crate) async fn ready(&mut self) -> bool {
        self.datum_store.ready().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::store::cbstore::jetstream_store::JetStreamCallbackStore;
    use crate::app::store::datastore::jetstream::JetStreamDataStore;
    use crate::callback::{Callback, Response};
    use crate::pipeline::PipelineDCG;
    use async_nats::jetstream;
    use axum::body::Bytes;
    use chrono::Utc;
    use std::time::Instant;
    use tokio_util::sync::CancellationToken;

    const PIPELINE_SPEC_ENCODED: &str = "eyJ2ZXJ0aWNlcyI6W3sibmFtZSI6ImluIiwic291cmNlIjp7InNlcnZpbmciOnsiYXV0aCI6bnVsbCwic2VydmljZSI6dHJ1ZSwibXNnSURIZWFkZXJLZXkiOiJYLU51bWFmbG93LUlkIiwic3RvcmUiOnsidXJsIjoicmVkaXM6Ly9yZWRpczo2Mzc5In19fSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIiLCJlbnYiOlt7Im5hbWUiOiJSVVNUX0xPRyIsInZhbHVlIjoiZGVidWcifV19LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6InBsYW5uZXIiLCJ1ZGYiOnsiY29udGFpbmVyIjp7ImltYWdlIjoiYXNjaWk6MC4xIiwiYXJncyI6WyJwbGFubmVyIl0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sImJ1aWx0aW4iOm51bGwsImdyb3VwQnkiOm51bGx9LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6InRpZ2VyIiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsidGlnZXIiXSwicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwiYnVpbHRpbiI6bnVsbCwiZ3JvdXBCeSI6bnVsbH0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiZG9nIiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsiZG9nIl0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sImJ1aWx0aW4iOm51bGwsImdyb3VwQnkiOm51bGx9LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6ImVsZXBoYW50IiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsiZWxlcGhhbnQiXSwicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwiYnVpbHRpbiI6bnVsbCwiZ3JvdXBCeSI6bnVsbH0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiYXNjaWlhcnQiLCJ1ZGYiOnsiY29udGFpbmVyIjp7ImltYWdlIjoiYXNjaWk6MC4xIiwiYXJncyI6WyJhc2NpaWFydCJdLCJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJidWlsdGluIjpudWxsLCJncm91cEJ5IjpudWxsfSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwic2NhbGUiOnsibWluIjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19fSx7Im5hbWUiOiJzZXJ2ZS1zaW5rIiwic2luayI6eyJ1ZHNpbmsiOnsiY29udGFpbmVyIjp7ImltYWdlIjoic2VydmVzaW5rOjAuMSIsImVudiI6W3sibmFtZSI6Ik5VTUFGTE9XX0NBTExCQUNLX1VSTF9LRVkiLCJ2YWx1ZSI6IlgtTnVtYWZsb3ctQ2FsbGJhY2stVXJsIn0seyJuYW1lIjoiTlVNQUZMT1dfTVNHX0lEX0hFQURFUl9LRVkiLCJ2YWx1ZSI6IlgtTnVtYWZsb3ctSWQifV0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn19LCJyZXRyeVN0cmF0ZWd5Ijp7fX0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiZXJyb3Itc2luayIsInNpbmsiOnsidWRzaW5rIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6InNlcnZlc2luazowLjEiLCJlbnYiOlt7Im5hbWUiOiJOVU1BRkxPV19DQUxMQkFDS19VUkxfS0VZIiwidmFsdWUiOiJYLU51bWFmbG93LUNhbGxiYWNrLVVybCJ9LHsibmFtZSI6Ik5VTUFGTE9XX01TR19JRF9IRUFERVJfS0VZIiwidmFsdWUiOiJYLU51bWFmbG93LUlkIn1dLCJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9fSwicmV0cnlTdHJhdGVneSI6e319LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19XSwiZWRnZXMiOlt7ImZyb20iOiJpbiIsInRvIjoicGxhbm5lciIsImNvbmRpdGlvbnMiOm51bGx9LHsiZnJvbSI6InBsYW5uZXIiLCJ0byI6ImFzY2lpYXJ0IiwiY29uZGl0aW9ucyI6eyJ0YWdzIjp7Im9wZXJhdG9yIjoib3IiLCJ2YWx1ZXMiOlsiYXNjaWlhcnQiXX19fSx7ImZyb20iOiJwbGFubmVyIiwidG8iOiJ0aWdlciIsImNvbmRpdGlvbnMiOnsidGFncyI6eyJvcGVyYXRvciI6Im9yIiwidmFsdWVzIjpbInRpZ2VyIl19fX0seyJmcm9tIjoicGxhbm5lciIsInRvIjoiZG9nIiwiY29uZGl0aW9ucyI6eyJ0YWdzIjp7Im9wZXJhdG9yIjoib3IiLCJ2YWx1ZXMiOlsiZG9nIl19fX0seyJmcm9tIjoicGxhbm5lciIsInRvIjoiZWxlcGhhbnQiLCJjb25kaXRpb25zIjp7InRhZ3MiOnsib3BlcmF0b3IiOiJvciIsInZhbHVlcyI6WyJlbGVwaGFudCJdfX19LHsiZnJvbSI6InRpZ2VyIiwidG8iOiJzZXJ2ZS1zaW5rIiwiY29uZGl0aW9ucyI6bnVsbH0seyJmcm9tIjoiZG9nIiwidG8iOiJzZXJ2ZS1zaW5rIiwiY29uZGl0aW9ucyI6bnVsbH0seyJmcm9tIjoiZWxlcGhhbnQiLCJ0byI6InNlcnZlLXNpbmsiLCJjb25kaXRpb25zIjpudWxsfSx7ImZyb20iOiJhc2NpaWFydCIsInRvIjoic2VydmUtc2luayIsImNvbmRpdGlvbnMiOm51bGx9LHsiZnJvbSI6InBsYW5uZXIiLCJ0byI6ImVycm9yLXNpbmsiLCJjb25kaXRpb25zIjp7InRhZ3MiOnsib3BlcmF0b3IiOiJvciIsInZhbHVlcyI6WyJlcnJvciJdfX19XSwibGlmZWN5Y2xlIjp7fSwid2F0ZXJtYXJrIjp7fX0=";

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_process_and_retrieve() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let pod_hash = "0";

        let pipeline_spec: PipelineDCG = PIPELINE_SPEC_ENCODED.parse().unwrap();
        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec).unwrap();

        let store_name = "test_process_and_retrieve";
        let _ = context.delete_key_value(store_name).await;

        let kv_store = context
            .create_key_value(jetstream::kv::Config {
                bucket: store_name.to_string(),
                history: 5,
                ..Default::default()
            })
            .await
            .unwrap();

        let callback_store = JetStreamCallbackStore::new(
            context.clone(),
            pod_hash,
            store_name,
            CancellationToken::new(),
        )
        .await
        .expect("Failed to create callback store");

        let datum_store = JetStreamDataStore::new(
            context.clone(),
            store_name,
            pod_hash,
            CancellationToken::new(),
        )
        .await
        .expect("Failed to create datum store");

        let status_tracker = StatusTracker::new(
            context.clone(),
            store_name,
            pod_hash,
            Some(store_name.to_string()),
        )
        .await
        .unwrap();

        let mut state =
            OrchestratorState::new(msg_graph, datum_store, callback_store, status_tracker)
                .await
                .unwrap();

        let id = "test_id".to_string();

        // Test insert_callback_requests
        let mut cbs = vec![
            Callback {
                id: id.clone(),
                vertex: "in".to_string(),
                cb_time: 12345,
                from_vertex: "in".to_string(),
                responses: vec![Response { tags: None }],
            },
            Callback {
                id: id.clone(),
                vertex: "planner".to_string(),
                cb_time: 12345,
                from_vertex: "in".to_string(),
                responses: vec![Response {
                    tags: Some(vec!["tiger".to_owned(), "asciiart".to_owned()]),
                }],
            },
            Callback {
                id: id.clone(),
                vertex: "tiger".to_string(),
                cb_time: 12345,
                from_vertex: "planner".to_string(),
                responses: vec![Response { tags: None }],
            },
            Callback {
                id: id.clone(),
                vertex: "asciiart".to_string(),
                cb_time: 12345,
                from_vertex: "planner".to_string(),
                responses: vec![Response { tags: None }],
            },
            Callback {
                id: id.clone(),
                vertex: "serve-sink".to_string(),
                cb_time: 12345,
                from_vertex: "tiger".to_string(),
                responses: vec![Response { tags: None }],
            },
            Callback {
                id: id.clone(),
                vertex: "serve-sink".to_string(),
                cb_time: 12345,
                from_vertex: "asciiart".to_string(),
                responses: vec![Response { tags: None }],
            },
        ];

        let request_id = id.clone();
        tokio::spawn(async move {
            // once the start processing rs is present then only write the response
            let rs_key = format!("rs.{pod_hash}.{request_id}.start.processing");
            let start_time = Instant::now();
            loop {
                if let Some(_) = kv_store.get(&rs_key).await.unwrap() {
                    break;
                }

                if start_time.elapsed().as_millis() > 1000 {
                    panic!("Timed out waiting for start processing key");
                }
            }
            // put responses to datum store
            let responses = [
                Bytes::from_static(b"response1"),
                Bytes::from_static(b"response2"),
            ];

            for (i, response) in responses.iter().enumerate() {
                kv_store
                    .put(
                        format!(
                            "rs.{pod_hash}.{request_id}.{}.{}",
                            i,
                            Utc::now().timestamp()
                        ),
                        response.clone(),
                    )
                    .await
                    .unwrap();
            }

            // put the callbacks into the store
            for (i, cb) in cbs.drain(..).enumerate() {
                kv_store
                    .put(
                        format!(
                            "cb.{pod_hash}.{request_id}.{}.{}",
                            i,
                            Utc::now().timestamp_millis()
                        ),
                        cb.try_into().unwrap(),
                    )
                    .await
                    .unwrap();
            }
        });

        // Test process_request
        let result = state.process_request(&id, RequestType::Sync).await.unwrap();
        let subgraph = result.await.unwrap();
        assert!(subgraph.is_ok());
        assert!(!subgraph.unwrap().is_empty());

        // test retrieve_saved
        let result = state
            .retrieve_saved(&id, RequestType::Sync)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], b"response1");
        assert_eq!(result[1], b"response2");
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_stream_response() {
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let pod_hash = "0";

        let store_name = "test_stream_response";
        let _ = context.delete_key_value(store_name).await;

        let kv_store = context
            .create_key_value(jetstream::kv::Config {
                bucket: store_name.to_string(),
                history: 5,
                ..Default::default()
            })
            .await
            .unwrap();

        let callback_store = JetStreamCallbackStore::new(
            context.clone(),
            pod_hash,
            store_name,
            CancellationToken::new(),
        )
        .await
        .expect("Failed to create callback store");

        let datum_store = JetStreamDataStore::new(
            context.clone(),
            store_name,
            pod_hash,
            CancellationToken::new(),
        )
        .await
        .expect("Failed to create datum store");

        let status_tracker = StatusTracker::new(
            context.clone(),
            store_name,
            pod_hash,
            Some(store_name.to_string()),
        )
        .await
        .unwrap();

        let pipeline_spec: PipelineDCG = PIPELINE_SPEC_ENCODED.parse().unwrap();

        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec).unwrap();

        let mut state =
            OrchestratorState::new(msg_graph, datum_store, callback_store, status_tracker)
                .await
                .unwrap();

        let id = "test_id".to_string();

        let mut cbs = vec![
            Callback {
                id: id.clone(),
                vertex: "in".to_string(),
                cb_time: 12346,
                from_vertex: "in".to_string(),
                responses: vec![Response { tags: None }],
            },
            Callback {
                id: id.clone(),
                vertex: "planner".to_string(),
                cb_time: 12346,
                from_vertex: "in".to_string(),
                responses: vec![Response {
                    tags: Some(vec!["tiger".to_owned(), "asciiart".to_owned()]),
                }],
            },
            Callback {
                id: id.clone(),
                vertex: "tiger".to_string(),
                cb_time: 12346,
                from_vertex: "planner".to_string(),
                responses: vec![Response { tags: None }],
            },
            Callback {
                id: id.clone(),
                vertex: "asciiart".to_string(),
                cb_time: 12346,
                from_vertex: "planner".to_string(),
                responses: vec![Response { tags: None }],
            },
            Callback {
                id: id.clone(),
                vertex: "serve-sink".to_string(),
                cb_time: 12346,
                from_vertex: "tiger".to_string(),
                responses: vec![Response { tags: None }],
            },
            Callback {
                id: id.clone(),
                vertex: "serve-sink".to_string(),
                cb_time: 12346,
                from_vertex: "asciiart".to_string(),
                responses: vec![Response { tags: None }],
            },
        ];

        let request_id = id.clone();
        tokio::spawn(async move {
            // once the start processing rs is present then only write the response
            let rs_key = format!("rs.{pod_hash}.{request_id}.start.processing");
            let start_time = Instant::now();
            loop {
                if let Some(_) = kv_store.get(&rs_key).await.unwrap() {
                    break;
                }

                if start_time.elapsed().as_millis() > 1000 {
                    panic!("Timed out waiting for start processing key");
                }
            }
            // put responses to datum store
            let responses = [
                Bytes::from_static(b"response1"),
                Bytes::from_static(b"response2"),
                Bytes::from_static(b"response3"),
                Bytes::from_static(b"response4"),
                Bytes::from_static(b"response5"),
            ];

            for (i, response) in responses.iter().enumerate() {
                kv_store
                    .put(
                        format!(
                            "rs.{pod_hash}.{request_id}.{}.{}",
                            i,
                            Utc::now().timestamp()
                        ),
                        response.clone(),
                    )
                    .await
                    .unwrap();
            }

            // put the callbacks into the store
            for (i, cb) in cbs.drain(..).enumerate() {
                kv_store
                    .put(
                        format!(
                            "cb.{pod_hash}.{request_id}.{}.{}",
                            i,
                            Utc::now().timestamp_millis()
                        ),
                        cb.try_into().unwrap(),
                    )
                    .await
                    .unwrap();
            }
        });

        // stream response
        let response_stream = state.stream_response(&id, RequestType::Sse).await.unwrap();
        let output_responses: Vec<_> = response_stream.collect().await;
        assert_eq!(output_responses.len(), 5);
    }
}
