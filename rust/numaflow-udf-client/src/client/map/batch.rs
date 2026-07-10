use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex};

use numaflow_pb::clients::map::{MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::task::AbortOnDropHandle;
use tonic::Streaming;
use tonic::transport::Channel;

use super::MapRpcStream;
use crate::error::{Result, UdfClientError};
use crate::model::{BatchMapResponse, UdfDatum};
use crate::wire::map::{data_request, decode_results, eot_request};

type ResponseSender = oneshot::Sender<Result<BatchMapResponse>>;
type EotSender = oneshot::Sender<Result<()>>;

/// A cloneable batch map session that owns batch protocol choreography and ID correlation.
#[derive(Clone)]
pub struct BatchMapSession {
    inner: Arc<BatchMapSessionInner>,
}

/// Observable batch boundaries for callers that report protocol progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchMapEvent {
    ClientEotSent,
    ServerEotReceived,
}

struct BatchMapSessionInner {
    request_tx: mpsc::Sender<MapRequest>,
    state: Arc<StdMutex<BatchState>>,
    /// A MapFn stream can have only one batch awaiting its stream-level EOT at a time.
    operation: Mutex<()>,
    _receiver: AbortOnDropHandle<()>,
}

/// Shared state between the batch sender and response pump.
///
/// MapFn is a BiDi gRPC stream, so sending and receiving happen in different tasks. `closed` is
/// stored explicitly because `request_tx.send()` can still return `Ok(())` briefly after the
/// response task has closed the stream.
#[derive(Default)]
struct BatchState {
    entries: HashMap<String, ResponseSender>,
    eot_sender: Option<EotSender>,
    closed: Option<UdfClientError>,
}

impl BatchMapSession {
    /// Opens a handshaken MapFn stream and starts its batch response pump.
    pub async fn open(client: MapClient<Channel>, request_buffer: usize) -> Result<Self> {
        let stream = MapRpcStream::open(client, request_buffer).await?;
        let (request_tx, response_stream) = stream.into_parts();
        Ok(Self::from_stream(request_tx, response_stream))
    }

    fn from_stream(
        request_tx: mpsc::Sender<MapRequest>,
        response_stream: Streaming<MapResponse>,
    ) -> Self {
        let state = Arc::new(StdMutex::new(BatchState::default()));
        let receiver_state = Arc::clone(&state);
        let receiver = tokio::spawn(async move {
            receive_responses(receiver_state, response_stream).await;
        });

        Self {
            inner: Arc::new(BatchMapSessionInner {
                request_tx,
                state,
                operation: Mutex::new(()),
                _receiver: AbortOnDropHandle::new(receiver),
            }),
        }
    }

    /// Sends one batch followed by EOT, then waits for every correlated response and server EOT.
    ///
    /// Responses are returned in request order even when the UDF sends them out of order. Batch
    /// calls on clones of this session are serialized because EOT delimits the entire RPC stream.
    pub async fn batch(&self, data: Vec<UdfDatum>) -> Result<Vec<BatchMapResponse>> {
        self.batch_with_event_handler(data, |_| {}).await
    }

    /// Runs a batch and reports the exact points where client and server EOT are observed.
    ///
    /// The handler is observational only; protocol construction and state transitions remain
    /// owned by this session.
    pub async fn batch_with_event_handler<F>(
        &self,
        data: Vec<UdfDatum>,
        mut handler: F,
    ) -> Result<Vec<BatchMapResponse>>
    where
        F: FnMut(BatchMapEvent),
    {
        let _operation = self.inner.operation.lock().await;
        if data.is_empty() {
            return Ok(Vec::new());
        }

        validate_ids(&data)?;

        let mut receivers = Vec::with_capacity(data.len());
        let eot_rx = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("batch map state lock poisoned");
            if let Some(error) = &state.closed {
                return Err(error.clone());
            }

            // Register every sender before sending data. The UDF can respond faster than the
            // sending task runs, and the response pump must always find the corresponding ID.
            for datum in &data {
                let (response_tx, response_rx) = oneshot::channel();
                state.entries.insert(datum.id.clone(), response_tx);
                receivers.push((datum.id.clone(), response_rx));
            }
            let (eot_tx, eot_rx) = oneshot::channel();
            state.eot_sender = Some(eot_tx);
            eot_rx
        };

        let mut guard = BatchOperationGuard::new(Arc::clone(&self.inner.state));
        for request in data.into_iter().map(data_request) {
            if self.inner.request_tx.send(request).await.is_err() {
                // Clear every registered sender so neither the response receivers nor the state
                // can leak after the request stream has failed.
                close_session(&self.inner.state, UdfClientError::RequestStreamClosed);
                return Err(UdfClientError::RequestStreamClosed);
            }
        }

        if self.inner.request_tx.send(eot_request()).await.is_err() {
            close_session(&self.inner.state, UdfClientError::RequestStreamClosed);
            return Err(UdfClientError::RequestStreamClosed);
        }
        handler(BatchMapEvent::ClientEotSent);

        let mut responses = Vec::with_capacity(receivers.len());
        for (id, receiver) in receivers {
            let response = receiver
                .await
                .map_err(|_| UdfClientError::ResponseChannelClosed(id))??;
            responses.push(response);
        }

        eot_rx
            .await
            .map_err(|_| UdfClientError::ResponseStreamClosed)??;
        handler(BatchMapEvent::ServerEotReceived);
        guard.disarm();
        Ok(responses)
    }
}

fn validate_ids(data: &[UdfDatum]) -> Result<()> {
    let mut ids = HashSet::with_capacity(data.len());
    for datum in data {
        if datum.id.is_empty() {
            return Err(UdfClientError::EmptyRequestId);
        }
        if !ids.insert(&datum.id) {
            return Err(UdfClientError::DuplicateRequestId(datum.id.clone()));
        }
    }
    Ok(())
}

/// Closes a session if a caller drops an in-flight batch before its terminating EOT.
///
/// Unlike unary requests, a partially sent batch cannot safely share the stream with another
/// operation because the missing EOT would merge their boundaries.
struct BatchOperationGuard {
    state: Arc<StdMutex<BatchState>>,
    armed: bool,
}

impl BatchOperationGuard {
    fn new(state: Arc<StdMutex<BatchState>>) -> Self {
        Self { state, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for BatchOperationGuard {
    fn drop(&mut self) {
        if self.armed {
            close_session(&self.state, UdfClientError::BatchOperationAbandoned);
        }
    }
}

async fn receive_responses(
    state: Arc<StdMutex<BatchState>>,
    mut response_stream: Streaming<MapResponse>,
) {
    let terminal_error = loop {
        match response_stream.message().await {
            Ok(Some(response)) => {
                if let Err(error) = process_response(&state, response) {
                    break error;
                }
            }
            Ok(None) => break UdfClientError::ResponseStreamClosed,
            Err(status) => break UdfClientError::Grpc(status),
        }
    };

    close_session(&state, terminal_error);
}

fn process_response(state: &Arc<StdMutex<BatchState>>, response: MapResponse) -> Result<()> {
    if response.handshake.is_some() {
        return Err(UdfClientError::UnexpectedControlFrame(
            "handshake".to_string(),
        ));
    }
    if let Some(status) = response.status {
        if status.eot {
            return process_eot(state);
        }
        return Err(UdfClientError::UnexpectedControlFrame(
            "status(eot=false)".to_string(),
        ));
    }

    let id = response.id.clone();
    let sender = {
        let mut state = state.lock().expect("batch map state lock poisoned");
        if let Some(error) = &state.closed {
            return Err(error.clone());
        }
        state.entries.remove(&id)
    };

    match sender {
        Some(sender) => {
            // A caller can be cancelled after the response pump removes its entry. Failure to
            // deliver across that narrow race is harmless because its operation guard closes the
            // session.
            let _ = sender.send(decode_results(response));
            Ok(())
        }
        None => Err(UdfClientError::UnexpectedResponseId(id)),
    }
}

fn process_eot(state: &Arc<StdMutex<BatchState>>) -> Result<()> {
    let eot_sender = {
        let mut state = state.lock().expect("batch map state lock poisoned");
        if let Some(error) = &state.closed {
            return Err(error.clone());
        }
        if !state.entries.is_empty() {
            let mut pending_ids = state.entries.keys().cloned().collect::<Vec<_>>();
            pending_ids.sort();
            return Err(UdfClientError::PartialBatchResponse { pending_ids });
        }
        state.eot_sender.take()
    };

    let Some(sender) = eot_sender else {
        return Err(UdfClientError::UnexpectedControlFrame(
            "batch EOT without an active batch".to_string(),
        ));
    };
    sender
        .send(Ok(()))
        .map_err(|_| UdfClientError::BatchOperationAbandoned)
}

fn close_session(state: &Arc<StdMutex<BatchState>>, error: UdfClientError) {
    let (entries, eot_sender) = {
        let mut state = state.lock().expect("batch map state lock poisoned");
        if state.closed.is_some() {
            return;
        }
        state.closed = Some(error.clone());
        (std::mem::take(&mut state.entries), state.eot_sender.take())
    };

    for sender in entries.into_values() {
        let _ = sender.send(Err(error.clone()));
    }
    if let Some(sender) = eot_sender {
        let _ = sender.send(Err(error));
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use bytes::Bytes;
    use chrono::Utc;
    use numaflow_pb::clients::map::{TransmissionStatus, map_response};

    use super::*;

    fn datum(id: &str) -> UdfDatum {
        UdfDatum {
            id: id.to_string(),
            keys: vec![format!("key-{id}")],
            value: Bytes::copy_from_slice(id.as_bytes()),
            event_time: Utc::now(),
            watermark: None,
            headers: HashMap::new(),
            metadata: None,
        }
    }

    fn response(id: &str) -> MapResponse {
        MapResponse {
            id: id.to_string(),
            results: vec![map_response::Result {
                keys: vec!["mapped".to_string()],
                value: id.as_bytes().to_vec(),
                tags: vec![],
                metadata: None,
                nack_options: None,
            }],
            handshake: None,
            status: None,
        }
    }

    fn eot_response() -> MapResponse {
        MapResponse {
            id: String::new(),
            results: vec![],
            handshake: None,
            status: Some(TransmissionStatus { eot: true }),
        }
    }

    fn test_session(
        buffer: usize,
    ) -> (
        BatchMapSession,
        mpsc::Receiver<MapRequest>,
        Arc<StdMutex<BatchState>>,
    ) {
        let (request_tx, request_rx) = mpsc::channel(buffer);
        let state = Arc::new(StdMutex::new(BatchState::default()));
        let receiver = tokio::spawn(pending());
        let session = BatchMapSession {
            inner: Arc::new(BatchMapSessionInner {
                request_tx,
                state: Arc::clone(&state),
                operation: Mutex::new(()),
                _receiver: AbortOnDropHandle::new(receiver),
            }),
        };
        (session, request_rx, state)
    }

    #[tokio::test]
    async fn sends_data_then_eot_and_returns_responses_in_request_order() {
        let (session, mut requests, state) = test_session(3);
        let batch = tokio::spawn(async move {
            session
                .batch(vec![datum("A"), datum("B")])
                .await
                .expect("batch")
        });

        assert_eq!(requests.recv().await.expect("request A").id, "A");
        assert_eq!(requests.recv().await.expect("request B").id, "B");
        let eot = requests.recv().await.expect("EOT");
        assert_eq!(eot.status.map(|status| status.eot), Some(true));

        process_response(&state, response("B")).expect("response B");
        process_response(&state, response("A")).expect("response A");
        process_response(&state, eot_response()).expect("server EOT");

        let responses = batch.await.expect("batch task");
        assert_eq!(
            responses
                .iter()
                .map(|response| response.id.as_str())
                .collect::<Vec<_>>(),
            ["A", "B"]
        );
    }

    #[tokio::test]
    async fn eot_before_all_responses_closes_the_session() {
        let (session, mut requests, state) = test_session(3);
        let next_session = session.clone();
        let batch = tokio::spawn(async move { session.batch(vec![datum("A"), datum("B")]).await });
        for _ in 0..3 {
            requests.recv().await.expect("request");
        }

        process_response(&state, response("A")).expect("response A");
        let error = process_response(&state, eot_response()).expect_err("partial EOT");
        assert!(matches!(
            error,
            UdfClientError::PartialBatchResponse { ref pending_ids }
                if pending_ids == &["B".to_string()]
        ));
        close_session(&state, error);

        assert!(matches!(
            batch.await.expect("batch task").expect_err("partial batch"),
            UdfClientError::PartialBatchResponse { .. }
        ));
        assert!(matches!(
            next_session
                .batch(vec![datum("C")])
                .await
                .expect_err("closed"),
            UdfClientError::PartialBatchResponse { .. }
        ));
    }

    #[tokio::test]
    async fn duplicate_ids_are_rejected_before_any_data_is_sent() {
        let (session, mut requests, _) = test_session(1);
        assert!(matches!(
            session
                .batch(vec![datum("same"), datum("same")])
                .await
                .expect_err("duplicate"),
            UdfClientError::DuplicateRequestId(ref id) if id == "same"
        ));
        assert!(requests.try_recv().is_err());
    }

    #[tokio::test]
    async fn request_send_failure_cleans_up_all_registered_senders() {
        let (session, requests, state) = test_session(1);
        drop(requests);

        assert!(matches!(
            session
                .batch(vec![datum("A"), datum("B")])
                .await
                .expect_err("send failure"),
            UdfClientError::RequestStreamClosed
        ));
        let state = state.lock().expect("state");
        assert!(state.entries.is_empty());
        assert!(state.eot_sender.is_none());
        assert!(matches!(
            state.closed,
            Some(UdfClientError::RequestStreamClosed)
        ));
    }
}
