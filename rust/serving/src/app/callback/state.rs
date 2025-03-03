use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::{error, info};

use super::datumstore::DatumStore;
use crate::app::callback::cbstore::{CallbackStore, ProcessingStatus};
use crate::app::callback::datumstore::Error as StoreError;
use crate::app::callback::datumstore::Result as StoreResult;
use crate::app::tracker::MessageGraph;
use crate::Error;

#[derive(Clone)]
pub(crate) struct State<T, C> {
    // generator to generate subgraph
    msg_graph_generator: Arc<MessageGraph>,
    // conn is to be used while reading and writing to redis.
    datum_store: T,
    callback_store: C,
}

impl<T, C> State<T, C>
where
    T: Clone + Send + Sync + DatumStore + 'static,
    C: Clone + Send + Sync + CallbackStore + 'static,
{
    /// Create a new State to track connections and callback data
    pub(crate) async fn new(
        msg_graph: MessageGraph,
        store: T,
        callback_store: C,
    ) -> crate::Result<Self> {
        Ok(Self {
            msg_graph_generator: Arc::new(msg_graph),
            datum_store: store,
            callback_store,
        })
    }

    /// register a new connection
    /// The oneshot receiver will be notified when all callbacks for this connection is received from
    /// the numaflow pipeline.
    pub(crate) async fn process_request(
        &mut self,
        id: &str,
    ) -> StoreResult<oneshot::Receiver<Result<String, Error>>> {
        let (tx, rx) = oneshot::channel();
        let sub_graph_generator = Arc::clone(&self.msg_graph_generator);
        let msg_id = id.to_string();
        let mut subgraph = None;

        // register the request in the store
        self.callback_store.register(id).await?;

        // start watching for callbacks
        let (mut callbacks_stream, watch_handle) = self.callback_store.watch_callbacks(id).await?;

        let mut cb_store = self.callback_store.clone();
        tokio::spawn(async move {
            let _handle = watch_handle;
            let mut callbacks = Vec::new();

            while let Some(cb) = callbacks_stream.next().await {
                info!(?cb, ?msg_id, "Received callback");
                callbacks.push(cb);
                subgraph = match sub_graph_generator
                    .generate_subgraph_from_callbacks(msg_id.clone(), callbacks.clone())
                {
                    Ok(subgraph) => subgraph,
                    Err(e) => {
                        error!(?e, "Failed to generate subgraph");
                        break;
                    }
                };
                if subgraph.is_some() {
                    break;
                }
            }

            if let Some(subgraph) = subgraph {
                tx.send(Ok(subgraph.clone()))
                    .expect("Failed to send subgraph");

                cb_store
                    .deregister(&msg_id, &subgraph)
                    .await
                    .expect("Failed to deregister");
            } else {
                error!("Subgraph could not be generated for the given ID");

                tx.send(Err(Error::SubGraphNotFound(
                    "Subgraph could not be generated for the given ID",
                )))
                .expect("Failed to send subgraph");

                cb_store
                    .mark_as_failed(&msg_id, "Subgraph could not be generated")
                    .await
                    .expect("Failed to mark as failed");
            }
        });

        Ok(rx)
    }

    /// Retrieves the output of the numaflow pipeline
    pub(crate) async fn retrieve_saved(
        &mut self,
        id: &str,
    ) -> Result<Option<Vec<Vec<u8>>>, StoreError> {
        self.datum_store
            .retrieve_datum(id)
            .await
            .map_err(Into::into)
    }

    /// Listens on watcher events (SSE uses KV watch) and checks with the Graph is complete. Once
    /// Graph is complete, it will deregister and closes the outbound SSE channel.  
    pub(crate) async fn stream_response(
        &mut self,
        id: &str,
    ) -> StoreResult<ReceiverStream<Arc<Bytes>>> {
        let (tx, rx) = mpsc::channel(10);
        let sub_graph_generator = Arc::clone(&self.msg_graph_generator);
        let msg_id = id.to_string();
        let mut subgraph = None;

        // register the request in the store
        self.callback_store.register(id).await?;

        // start watching for callbacks
        let (mut callbacks_stream, watch_handle) = self.callback_store.watch_callbacks(id).await?;

        let mut cb_store = self.callback_store.clone();
        tokio::spawn(async move {
            let _handle = watch_handle;
            let mut callbacks = Vec::new();

            while let Some(cb) = callbacks_stream.next().await {
                info!(?cb, ?msg_id, "Received callback");
                callbacks.push(cb);
                subgraph = match sub_graph_generator
                    .generate_subgraph_from_callbacks(msg_id.clone(), callbacks.clone())
                {
                    Ok(subgraph) => subgraph,
                    Err(e) => {
                        error!(?e, "Failed to generate subgraph");
                        break;
                    }
                };
                if subgraph.is_some() {
                    break;
                }
            }

            if let Some(subgraph) = subgraph {
                cb_store
                    .deregister(&msg_id, &subgraph)
                    .await
                    .expect("Failed to deregister");
            } else {
                error!("Subgraph could not be generated for the given ID");

                cb_store
                    .mark_as_failed(&msg_id, "Subgraph could not be generated")
                    .await
                    .expect("Failed to mark as failed");
            }
        });

        let (mut response_stream, _handle) = self.datum_store.stream_response(id).await?;
        tokio::spawn(async move {
            while let Some(response) = response_stream.next().await {
                tx.send(response).await.expect("Failed to send response");
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
        let status = self.callback_store.status(id).await?;
        match status {
            ProcessingStatus::InProgress => Ok("Request In Progress".to_string()),
            ProcessingStatus::Completed(sub_graph) => Ok(sub_graph),
            ProcessingStatus::Failed(error) => {
                error!(?error, "Request failed");
                Err(Error::SubGraphGenerator(error))
            }
        }
    }

    pub(crate) async fn mark_as_failed(&mut self, id: &str, error: &str) -> Result<(), Error> {
        self.callback_store.mark_as_failed(id, error).await?;
        Ok(())
    }

    // Check if the store is ready
    pub(crate) async fn ready(&mut self) -> bool {
        self.datum_store.ready().await
    }
}

// #[cfg(test)]
// mod tests {
//     use axum::body::Bytes;
//
//     use super::*;
//     use crate::app::callback::datumstore::memstore::InMemoryStore;
//     use crate::callback::Response;
//     use crate::pipeline::PipelineDCG;
//
//     const PIPELINE_SPEC_ENCODED: &str = "eyJ2ZXJ0aWNlcyI6W3sibmFtZSI6ImluIiwic291cmNlIjp7InNlcnZpbmciOnsiYXV0aCI6bnVsbCwic2VydmljZSI6dHJ1ZSwibXNnSURIZWFkZXJLZXkiOiJYLU51bWFmbG93LUlkIiwic3RvcmUiOnsidXJsIjoicmVkaXM6Ly9yZWRpczo2Mzc5In19fSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIiLCJlbnYiOlt7Im5hbWUiOiJSVVNUX0xPRyIsInZhbHVlIjoiZGVidWcifV19LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6InBsYW5uZXIiLCJ1ZGYiOnsiY29udGFpbmVyIjp7ImltYWdlIjoiYXNjaWk6MC4xIiwiYXJncyI6WyJwbGFubmVyIl0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sImJ1aWx0aW4iOm51bGwsImdyb3VwQnkiOm51bGx9LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6InRpZ2VyIiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsidGlnZXIiXSwicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwiYnVpbHRpbiI6bnVsbCwiZ3JvdXBCeSI6bnVsbH0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiZG9nIiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsiZG9nIl0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sImJ1aWx0aW4iOm51bGwsImdyb3VwQnkiOm51bGx9LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6ImVsZXBoYW50IiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsiZWxlcGhhbnQiXSwicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwiYnVpbHRpbiI6bnVsbCwiZ3JvdXBCeSI6bnVsbH0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiYXNjaWlhcnQiLCJ1ZGYiOnsiY29udGFpbmVyIjp7ImltYWdlIjoiYXNjaWk6MC4xIiwiYXJncyI6WyJhc2NpaWFydCJdLCJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJidWlsdGluIjpudWxsLCJncm91cEJ5IjpudWxsfSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwic2NhbGUiOnsibWluIjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19fSx7Im5hbWUiOiJzZXJ2ZS1zaW5rIiwic2luayI6eyJ1ZHNpbmsiOnsiY29udGFpbmVyIjp7ImltYWdlIjoic2VydmVzaW5rOjAuMSIsImVudiI6W3sibmFtZSI6Ik5VTUFGTE9XX0NBTExCQUNLX1VSTF9LRVkiLCJ2YWx1ZSI6IlgtTnVtYWZsb3ctQ2FsbGJhY2stVXJsIn0seyJuYW1lIjoiTlVNQUZMT1dfTVNHX0lEX0hFQURFUl9LRVkiLCJ2YWx1ZSI6IlgtTnVtYWZsb3ctSWQifV0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn19LCJyZXRyeVN0cmF0ZWd5Ijp7fX0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiZXJyb3Itc2luayIsInNpbmsiOnsidWRzaW5rIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6InNlcnZlc2luazowLjEiLCJlbnYiOlt7Im5hbWUiOiJOVU1BRkxPV19DQUxMQkFDS19VUkxfS0VZIiwidmFsdWUiOiJYLU51bWFmbG93LUNhbGxiYWNrLVVybCJ9LHsibmFtZSI6Ik5VTUFGTE9XX01TR19JRF9IRUFERVJfS0VZIiwidmFsdWUiOiJYLU51bWFmbG93LUlkIn1dLCJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9fSwicmV0cnlTdHJhdGVneSI6e319LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19XSwiZWRnZXMiOlt7ImZyb20iOiJpbiIsInRvIjoicGxhbm5lciIsImNvbmRpdGlvbnMiOm51bGx9LHsiZnJvbSI6InBsYW5uZXIiLCJ0byI6ImFzY2lpYXJ0IiwiY29uZGl0aW9ucyI6eyJ0YWdzIjp7Im9wZXJhdG9yIjoib3IiLCJ2YWx1ZXMiOlsiYXNjaWlhcnQiXX19fSx7ImZyb20iOiJwbGFubmVyIiwidG8iOiJ0aWdlciIsImNvbmRpdGlvbnMiOnsidGFncyI6eyJvcGVyYXRvciI6Im9yIiwidmFsdWVzIjpbInRpZ2VyIl19fX0seyJmcm9tIjoicGxhbm5lciIsInRvIjoiZG9nIiwiY29uZGl0aW9ucyI6eyJ0YWdzIjp7Im9wZXJhdG9yIjoib3IiLCJ2YWx1ZXMiOlsiZG9nIl19fX0seyJmcm9tIjoicGxhbm5lciIsInRvIjoiZWxlcGhhbnQiLCJjb25kaXRpb25zIjp7InRhZ3MiOnsib3BlcmF0b3IiOiJvciIsInZhbHVlcyI6WyJlbGVwaGFudCJdfX19LHsiZnJvbSI6InRpZ2VyIiwidG8iOiJzZXJ2ZS1zaW5rIiwiY29uZGl0aW9ucyI6bnVsbH0seyJmcm9tIjoiZG9nIiwidG8iOiJzZXJ2ZS1zaW5rIiwiY29uZGl0aW9ucyI6bnVsbH0seyJmcm9tIjoiZWxlcGhhbnQiLCJ0byI6InNlcnZlLXNpbmsiLCJjb25kaXRpb25zIjpudWxsfSx7ImZyb20iOiJhc2NpaWFydCIsInRvIjoic2VydmUtc2luayIsImNvbmRpdGlvbnMiOm51bGx9LHsiZnJvbSI6InBsYW5uZXIiLCJ0byI6ImVycm9yLXNpbmsiLCJjb25kaXRpb25zIjp7InRhZ3MiOnsib3BlcmF0b3IiOiJvciIsInZhbHVlcyI6WyJlcnJvciJdfX19XSwibGlmZWN5Y2xlIjp7fSwid2F0ZXJtYXJrIjp7fX0=";
//
//     #[tokio::test]
//     async fn test_state() {
//         let pipeline_spec: PipelineDCG = PIPELINE_SPEC_ENCODED.parse().unwrap();
//         let msg_graph = MessageGraph::from_pipeline(&pipeline_spec).unwrap();
//         let datumstore = InMemoryStore::new();
//         let mut state = State::new(msg_graph, datumstore).await.unwrap();
//
//         // Test register
//         let id = "test_id".to_string();
//         let rx = state.register(id.as_str()).await.unwrap();
//
//         let xid = id.clone();
//
//         // spawn a task to listen on the receiver, once we have received all the callbacks for the message
//         // we will get a response from the receiver with the message id
//         let handle = tokio::spawn(async move {
//             let result = rx.await.unwrap();
//             // Tests deregister, and fetching the subgraph from the memory
//             assert_eq!(result.unwrap(), xid);
//         });
//
//         // Test save_response
//         let body = Bytes::from("Test Message");
//         state.save_response(id.clone(), body).await.unwrap();
//
//         // Test retrieve_saved
//         let saved = state.retrieve_saved(&id).await.unwrap();
//         assert_eq!(
//             saved,
//             ProcessingStatus::Completed(vec!["Test Message".as_bytes().to_vec()])
//         );
//
//         // Test insert_callback_requests
//         let cbs = vec![
//             Callback {
//                 id: id.clone(),
//                 vertex: "in".to_string(),
//                 cb_time: 12345,
//                 from_vertex: "in".to_string(),
//                 responses: vec![Response { tags: None }],
//             },
//             Callback {
//                 id: id.clone(),
//                 vertex: "planner".to_string(),
//                 cb_time: 12345,
//                 from_vertex: "in".to_string(),
//                 responses: vec![Response {
//                     tags: Some(vec!["tiger".to_owned(), "asciiart".to_owned()]),
//                 }],
//             },
//             Callback {
//                 id: id.clone(),
//                 vertex: "tiger".to_string(),
//                 cb_time: 12345,
//                 from_vertex: "planner".to_string(),
//                 responses: vec![Response { tags: None }],
//             },
//             Callback {
//                 id: id.clone(),
//                 vertex: "asciiart".to_string(),
//                 cb_time: 12345,
//                 from_vertex: "planner".to_string(),
//                 responses: vec![Response { tags: None }],
//             },
//             Callback {
//                 id: id.clone(),
//                 vertex: "serve-sink".to_string(),
//                 cb_time: 12345,
//                 from_vertex: "tiger".to_string(),
//                 responses: vec![Response { tags: None }],
//             },
//             Callback {
//                 id: id.clone(),
//                 vertex: "serve-sink".to_string(),
//                 cb_time: 12345,
//                 from_vertex: "asciiart".to_string(),
//                 responses: vec![Response { tags: None }],
//             },
//         ];
//         state.insert_callback_requests(cbs).await.unwrap();
//
//         let sub_graph = state.retrieve_subgraph_from_storage(&id).await;
//         assert!(sub_graph.is_ok());
//
//         handle.await.unwrap();
//     }
//
//     #[tokio::test]
//     async fn test_retrieve_saved_no_entry() {
//         let pipeline_spec: PipelineDCG = PIPELINE_SPEC_ENCODED.parse().unwrap();
//         let msg_graph = MessageGraph::from_pipeline(&pipeline_spec).unwrap();
//         let datumstore = InMemoryStore::new();
//         let mut state = State::new(msg_graph, datumstore).await.unwrap();
//
//         let id = "nonexistent_id".to_string();
//
//         // Try to retrieve saved data for an ID that doesn't exist
//         let result = state.retrieve_saved(&id).await;
//
//         // Check that an error is returned
//         assert!(result.is_err());
//     }
//
//     #[tokio::test]
//     async fn test_insert_callback_requests_invalid_id() {
//         let pipeline_spec: PipelineDCG = PIPELINE_SPEC_ENCODED.parse().unwrap();
//         let msg_graph = MessageGraph::from_pipeline(&pipeline_spec).unwrap();
//         let datumstore = InMemoryStore::new();
//         let mut state = State::new(msg_graph, datumstore).await.unwrap();
//
//         let cbs = vec![Callback {
//             id: "nonexistent_id".to_string(),
//             vertex: "in".to_string(),
//             cb_time: 12345,
//             from_vertex: "in".to_string(),
//             responses: vec![Response { tags: None }],
//         }];
//
//         // Try to insert callback requests for an ID that hasn't been registered
//         let result = state.insert_callback_requests(cbs).await;
//
//         // Check that an error is returned
//         assert!(result.is_err());
//     }
//
//     #[tokio::test]
//     async fn test_retrieve_subgraph_from_storage_no_entry() {
//         let pipeline_spec: PipelineDCG = PIPELINE_SPEC_ENCODED.parse().unwrap();
//         let msg_graph = MessageGraph::from_pipeline(&pipeline_spec).unwrap();
//         let datumstore = InMemoryStore::new();
//         let mut state = State::new(msg_graph, datumstore).await.unwrap();
//
//         let id = "nonexistent_id".to_string();
//
//         // Try to retrieve a subgraph for an ID that doesn't exist
//         let result = state.retrieve_subgraph_from_storage(&id).await;
//
//         // Check that an error is returned
//         assert!(result.is_err());
//     }
// }
