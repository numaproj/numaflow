use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::oneshot;

use super::store::{PipelineResult, Store};
use crate::app::callback::{store::PayloadToSave, Callback};
use crate::app::tracker::MessageGraph;
use crate::Error;

struct RequestState {
    // Channel to notify when all callbacks for a message is received
    tx: oneshot::Sender<Result<String, Error>>,
    // CallbackRequest is immutable, while vtx_visited can grow.
    vtx_visited: Vec<Arc<Callback>>,
}

#[derive(Clone)]
pub(crate) struct State<T> {
    // hashmap of vertex infos keyed by ID
    // it also contains tx to trigger to response to the syncHTTP call
    callbacks: Arc<Mutex<HashMap<String, RequestState>>>,
    // generator to generate subgraph
    msg_graph_generator: Arc<MessageGraph>,
    // conn is to be used while reading and writing to redis.
    store: T,
}

impl<T> State<T>
where
    T: Store,
{
    /// Create a new State to track connections and callback data
    pub(crate) async fn new(msg_graph: MessageGraph, store: T) -> crate::Result<Self> {
        Ok(Self {
            callbacks: Arc::new(Mutex::new(HashMap::new())),
            msg_graph_generator: Arc::new(msg_graph),
            store,
        })
    }

    /// register a new connection
    /// The oneshot receiver will be notified when all callbacks for this connection is received from the numaflow pipeline
    pub(crate) async fn register(
        &mut self,
        id: String,
    ) -> oneshot::Receiver<Result<String, Error>> {
        // TODO: add an entry in Redis to note that the entry has been registered

        let (tx, rx) = oneshot::channel();
        {
            let mut guard = self.callbacks.lock().expect("Getting lock on State");
            guard.insert(
                id.clone(),
                RequestState {
                    tx,
                    vtx_visited: Vec::new(),
                },
            );
        }
        self.store.register(id).await.unwrap(); // FIXME:
        rx
    }

    /// Retrieves the output of the numaflow pipeline
    pub(crate) async fn retrieve_saved(&mut self, id: &str) -> Result<PipelineResult, Error> {
        self.store.retrieve_datum(id).await
    }

    pub(crate) async fn save_response(
        &mut self,
        id: String,
        body: axum::body::Bytes,
    ) -> crate::Result<()> {
        // we have to differentiate between the saved responses and the callback requests
        // saved responses are stored in "id_SAVED", callback requests are stored in "id"
        self.store
            .save(vec![PayloadToSave::DatumFromPipeline {
                key: id,
                value: body,
            }])
            .await
    }

    /// insert_callback_requests is used to insert the callback requests.
    pub(crate) async fn insert_callback_requests(
        &mut self,
        cb_requests: Vec<Callback>,
    ) -> Result<(), Error> {
        /*
            TODO: should we consider batching the requests and then processing them?
            that way algorithm can be invoked only once for a batch of requests
            instead of invoking it for each request.
        */
        let cb_requests: Vec<Arc<Callback>> = cb_requests.into_iter().map(Arc::new).collect();
        let redis_payloads: Vec<PayloadToSave> = cb_requests
            .iter()
            .cloned()
            .map(|cbr| PayloadToSave::Callback {
                key: cbr.id.clone(),
                value: Arc::clone(&cbr),
            })
            .collect();

        self.store.save(redis_payloads).await?;

        for cbr in cb_requests {
            let id = cbr.id.clone();
            {
                let mut guard = self.callbacks.lock().expect("Getting lock on State");
                let Some(req_state) = guard.get_mut(&id) else {
                    tracing::debug!(id, "Request is not found in in-memory store");
                    continue;
                };
                req_state.vtx_visited.push(cbr);
            }

            // check if the sub graph can be generated
            match self.get_subgraph_from_memory(&id) {
                Ok(_) => {
                    // if the sub graph is generated, then we can send the response
                    self.deregister(&id).await?
                }
                Err(e) => {
                    match e {
                        Error::SubGraphNotFound(_) => {
                            // if the sub graph is not generated, then we can continue
                            continue;
                        }
                        err => {
                            tracing::error!(?err, "Failed to generate subgraph");
                            // if there is an error, deregister with the error
                            self.deregister(&id).await?
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Get the subgraph for the given ID from in-memory.
    fn get_subgraph_from_memory(&self, id: &str) -> Result<String, Error> {
        let callbacks = self.get_callbacks_from_memory(id).ok_or(Error::IDNotFound(
            "Connection for the received callback is not present in the in-memory store",
        ))?;

        self.get_subgraph(id.to_string(), callbacks)
    }

    /// Get the subgraph for the given ID from persistent store. This is used querying for the status from the service endpoint even after the
    /// request has been completed.
    pub(crate) async fn retrieve_subgraph_from_storage(
        &mut self,
        id: &str,
    ) -> Result<String, Error> {
        // If the id is not found in the in-memory store, fetch from Redis
        let callbacks: Vec<Arc<Callback>> = match self.retrieve_callbacks_from_storage(id).await {
            Ok(callbacks) => callbacks,
            Err(e) => {
                return Err(e);
            }
        };
        // check if the sub graph can be generated
        self.get_subgraph(id.to_string(), callbacks)
    }

    // Generate subgraph from the given callbacks
    fn get_subgraph(&self, id: String, callbacks: Vec<Arc<Callback>>) -> Result<String, Error> {
        match self
            .msg_graph_generator
            .generate_subgraph_from_callbacks(id, callbacks)
        {
            Ok(Some(sub_graph)) => Ok(sub_graph),
            Ok(None) => Err(Error::SubGraphNotFound(
                "Subgraph could not be generated for the given ID",
            )),
            Err(e) => Err(e),
        }
    }

    /// deregister is called to trigger response and delete all the data persisted for that ID
    pub(crate) async fn deregister(&mut self, id: &str) -> Result<(), Error> {
        let state = {
            let mut guard = self.callbacks.lock().expect("Getting lock on State");
            // we do not require the data stored in HashMap anymore
            guard.remove(id)
        };

        let Some(state) = state else {
            return Err(Error::IDNotFound(
                "Connection for the received callback is not present in the in-memory store",
            ));
        };

        self.store.deregister(id.to_string()).await.unwrap(); // FIXME:

        state
            .tx
            .send(Ok(id.to_string()))
            .map_err(|_| Error::Other("Application bug - Receiver is already dropped".to_string()))
    }

    // Get the Callback value for the given ID
    // TODO: Generate json serialized data here itself to avoid cloning.
    fn get_callbacks_from_memory(&self, id: &str) -> Option<Vec<Arc<Callback>>> {
        let guard = self.callbacks.lock().expect("Getting lock on State");
        guard.get(id).map(|state| state.vtx_visited.clone())
    }

    // Get the Callback value for the given ID from persistent store
    async fn retrieve_callbacks_from_storage(
        &mut self,
        id: &str,
    ) -> Result<Vec<Arc<Callback>>, Error> {
        // If the id is not found in the in-memory store, fetch from Redis
        let callbacks: Vec<Arc<Callback>> = match self.store.retrieve_callbacks(id).await {
            Ok(response) => response.into_iter().collect(),
            Err(e) => {
                return Err(e);
            }
        };
        Ok(callbacks)
    }

    // Check if the store is ready
    pub(crate) async fn ready(&mut self) -> bool {
        self.store.ready().await
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Bytes;

    use super::*;
    use crate::app::callback::store::memstore::InMemoryStore;
    use crate::callback::Response;
    use crate::pipeline::PipelineDCG;

    const PIPELINE_SPEC_ENCODED: &str = "eyJ2ZXJ0aWNlcyI6W3sibmFtZSI6ImluIiwic291cmNlIjp7InNlcnZpbmciOnsiYXV0aCI6bnVsbCwic2VydmljZSI6dHJ1ZSwibXNnSURIZWFkZXJLZXkiOiJYLU51bWFmbG93LUlkIiwic3RvcmUiOnsidXJsIjoicmVkaXM6Ly9yZWRpczo2Mzc5In19fSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIiLCJlbnYiOlt7Im5hbWUiOiJSVVNUX0xPRyIsInZhbHVlIjoiZGVidWcifV19LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6InBsYW5uZXIiLCJ1ZGYiOnsiY29udGFpbmVyIjp7ImltYWdlIjoiYXNjaWk6MC4xIiwiYXJncyI6WyJwbGFubmVyIl0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sImJ1aWx0aW4iOm51bGwsImdyb3VwQnkiOm51bGx9LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6InRpZ2VyIiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsidGlnZXIiXSwicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwiYnVpbHRpbiI6bnVsbCwiZ3JvdXBCeSI6bnVsbH0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiZG9nIiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsiZG9nIl0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sImJ1aWx0aW4iOm51bGwsImdyb3VwQnkiOm51bGx9LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6ImVsZXBoYW50IiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsiZWxlcGhhbnQiXSwicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwiYnVpbHRpbiI6bnVsbCwiZ3JvdXBCeSI6bnVsbH0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiYXNjaWlhcnQiLCJ1ZGYiOnsiY29udGFpbmVyIjp7ImltYWdlIjoiYXNjaWk6MC4xIiwiYXJncyI6WyJhc2NpaWFydCJdLCJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJidWlsdGluIjpudWxsLCJncm91cEJ5IjpudWxsfSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwic2NhbGUiOnsibWluIjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19fSx7Im5hbWUiOiJzZXJ2ZS1zaW5rIiwic2luayI6eyJ1ZHNpbmsiOnsiY29udGFpbmVyIjp7ImltYWdlIjoic2VydmVzaW5rOjAuMSIsImVudiI6W3sibmFtZSI6Ik5VTUFGTE9XX0NBTExCQUNLX1VSTF9LRVkiLCJ2YWx1ZSI6IlgtTnVtYWZsb3ctQ2FsbGJhY2stVXJsIn0seyJuYW1lIjoiTlVNQUZMT1dfTVNHX0lEX0hFQURFUl9LRVkiLCJ2YWx1ZSI6IlgtTnVtYWZsb3ctSWQifV0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn19LCJyZXRyeVN0cmF0ZWd5Ijp7fX0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiZXJyb3Itc2luayIsInNpbmsiOnsidWRzaW5rIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6InNlcnZlc2luazowLjEiLCJlbnYiOlt7Im5hbWUiOiJOVU1BRkxPV19DQUxMQkFDS19VUkxfS0VZIiwidmFsdWUiOiJYLU51bWFmbG93LUNhbGxiYWNrLVVybCJ9LHsibmFtZSI6Ik5VTUFGTE9XX01TR19JRF9IRUFERVJfS0VZIiwidmFsdWUiOiJYLU51bWFmbG93LUlkIn1dLCJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9fSwicmV0cnlTdHJhdGVneSI6e319LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19XSwiZWRnZXMiOlt7ImZyb20iOiJpbiIsInRvIjoicGxhbm5lciIsImNvbmRpdGlvbnMiOm51bGx9LHsiZnJvbSI6InBsYW5uZXIiLCJ0byI6ImFzY2lpYXJ0IiwiY29uZGl0aW9ucyI6eyJ0YWdzIjp7Im9wZXJhdG9yIjoib3IiLCJ2YWx1ZXMiOlsiYXNjaWlhcnQiXX19fSx7ImZyb20iOiJwbGFubmVyIiwidG8iOiJ0aWdlciIsImNvbmRpdGlvbnMiOnsidGFncyI6eyJvcGVyYXRvciI6Im9yIiwidmFsdWVzIjpbInRpZ2VyIl19fX0seyJmcm9tIjoicGxhbm5lciIsInRvIjoiZG9nIiwiY29uZGl0aW9ucyI6eyJ0YWdzIjp7Im9wZXJhdG9yIjoib3IiLCJ2YWx1ZXMiOlsiZG9nIl19fX0seyJmcm9tIjoicGxhbm5lciIsInRvIjoiZWxlcGhhbnQiLCJjb25kaXRpb25zIjp7InRhZ3MiOnsib3BlcmF0b3IiOiJvciIsInZhbHVlcyI6WyJlbGVwaGFudCJdfX19LHsiZnJvbSI6InRpZ2VyIiwidG8iOiJzZXJ2ZS1zaW5rIiwiY29uZGl0aW9ucyI6bnVsbH0seyJmcm9tIjoiZG9nIiwidG8iOiJzZXJ2ZS1zaW5rIiwiY29uZGl0aW9ucyI6bnVsbH0seyJmcm9tIjoiZWxlcGhhbnQiLCJ0byI6InNlcnZlLXNpbmsiLCJjb25kaXRpb25zIjpudWxsfSx7ImZyb20iOiJhc2NpaWFydCIsInRvIjoic2VydmUtc2luayIsImNvbmRpdGlvbnMiOm51bGx9LHsiZnJvbSI6InBsYW5uZXIiLCJ0byI6ImVycm9yLXNpbmsiLCJjb25kaXRpb25zIjp7InRhZ3MiOnsib3BlcmF0b3IiOiJvciIsInZhbHVlcyI6WyJlcnJvciJdfX19XSwibGlmZWN5Y2xlIjp7fSwid2F0ZXJtYXJrIjp7fX0=";

    #[tokio::test]
    async fn test_state() {
        let pipeline_spec: PipelineDCG = PIPELINE_SPEC_ENCODED.parse().unwrap();
        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec).unwrap();
        let store = InMemoryStore::new();
        let mut state = State::new(msg_graph, store).await.unwrap();

        // Test register
        let id = "test_id".to_string();
        let rx = state.register(id.clone()).await;

        let xid = id.clone();

        // spawn a task to listen on the receiver, once we have received all the callbacks for the message
        // we will get a response from the receiver with the message id
        let handle = tokio::spawn(async move {
            let result = rx.await.unwrap();
            // Tests deregister, and fetching the subgraph from the memory
            assert_eq!(result.unwrap(), xid);
        });

        // Test save_response
        let body = Bytes::from("Test Message");
        state.save_response(id.clone(), body).await.unwrap();

        // Test retrieve_saved
        let saved = state.retrieve_saved(&id).await.unwrap();
        assert_eq!(
            saved,
            PipelineResult::Completed(vec!["Test Message".as_bytes().to_vec()])
        );

        // Test insert_callback_requests
        let cbs = vec![
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
        state.insert_callback_requests(cbs).await.unwrap();

        let sub_graph = state.retrieve_subgraph_from_storage(&id).await;
        assert!(sub_graph.is_ok());

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_retrieve_saved_no_entry() {
        let pipeline_spec: PipelineDCG = PIPELINE_SPEC_ENCODED.parse().unwrap();
        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec).unwrap();
        let store = InMemoryStore::new();
        let mut state = State::new(msg_graph, store).await.unwrap();

        let id = "nonexistent_id".to_string();

        // Try to retrieve saved data for an ID that doesn't exist
        let result = state.retrieve_saved(&id).await;

        // Check that an error is returned
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_insert_callback_requests_invalid_id() {
        let pipeline_spec: PipelineDCG = PIPELINE_SPEC_ENCODED.parse().unwrap();
        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec).unwrap();
        let store = InMemoryStore::new();
        let mut state = State::new(msg_graph, store).await.unwrap();

        let cbs = vec![Callback {
            id: "nonexistent_id".to_string(),
            vertex: "in".to_string(),
            cb_time: 12345,
            from_vertex: "in".to_string(),
            responses: vec![Response { tags: None }],
        }];

        // Try to insert callback requests for an ID that hasn't been registered
        let result = state.insert_callback_requests(cbs).await;

        // Check that an error is returned
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_retrieve_subgraph_from_storage_no_entry() {
        let pipeline_spec: PipelineDCG = PIPELINE_SPEC_ENCODED.parse().unwrap();
        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec).unwrap();
        let store = InMemoryStore::new();
        let mut state = State::new(msg_graph, store).await.unwrap();

        let id = "nonexistent_id".to_string();

        // Try to retrieve a subgraph for an ID that doesn't exist
        let result = state.retrieve_subgraph_from_storage(&id).await;

        // Check that an error is returned
        assert!(result.is_err());
    }
}
