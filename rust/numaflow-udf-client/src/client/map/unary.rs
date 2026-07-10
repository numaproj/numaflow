use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use numaflow_pb::clients::map::{MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::{mpsc, oneshot};
use tokio_util::task::AbortOnDropHandle;
use tonic::Streaming;
use tonic::transport::Channel;

use super::MapRpcStream;
use crate::error::{Result, UdfClientError};
use crate::model::{UdfDatum, UnaryMapResponse};
use crate::wire::map::{data_request, decode_results};

type ResponseSender = oneshot::Sender<Result<UnaryMapResponse>>;

/// A cloneable unary map session with request/response ID correlation.
#[derive(Clone)]
pub struct UnaryMapSession {
    inner: Arc<UnaryMapSessionInner>,
}

struct UnaryMapSessionInner {
    request_tx: mpsc::Sender<MapRequest>,
    state: Arc<Mutex<UnaryState>>,
    _receiver: Arc<AbortOnDropHandle<()>>,
}

#[derive(Default)]
struct UnaryState {
    entries: HashMap<String, PendingEntry>,
    closed: Option<UdfClientError>,
}

enum PendingEntry {
    Waiting(ResponseSender),
    Abandoned,
}

impl UnaryMapSession {
    /// Opens a handshaken MapFn stream and starts its response pump.
    pub async fn open(client: MapClient<Channel>, request_buffer: usize) -> Result<Self> {
        let stream = MapRpcStream::open(client, request_buffer).await?;
        let (request_tx, response_stream) = stream.into_parts();
        Ok(Self::from_stream(request_tx, response_stream))
    }

    fn from_stream(
        request_tx: mpsc::Sender<MapRequest>,
        response_stream: Streaming<MapResponse>,
    ) -> Self {
        let state = Arc::new(Mutex::new(UnaryState::default()));
        let receiver_state = Arc::clone(&state);
        let receiver = tokio::spawn(async move {
            receive_responses(receiver_state, response_stream).await;
        });

        Self {
            inner: Arc::new(UnaryMapSessionInner {
                request_tx,
                state,
                _receiver: Arc::new(AbortOnDropHandle::new(receiver)),
            }),
        }
    }

    /// Sends one datum and waits for the response carrying the same ID.
    pub async fn map(&self, datum: UdfDatum) -> Result<UnaryMapResponse> {
        if datum.id.is_empty() {
            return Err(UdfClientError::EmptyRequestId);
        }

        let id = datum.id.clone();
        let request = data_request(datum);
        let (response_tx, response_rx) = oneshot::channel();

        {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("unary map state lock poisoned");
            if let Some(error) = &state.closed {
                return Err(error.clone());
            }
            if state.entries.contains_key(&id) {
                return Err(UdfClientError::DuplicateRequestId(id));
            }
            state
                .entries
                .insert(id.clone(), PendingEntry::Waiting(response_tx));
        }

        let mut guard = PendingRequestGuard::new(Arc::clone(&self.inner.state), id.clone());
        self.inner
            .request_tx
            .send(request)
            .await
            .map_err(|_| UdfClientError::RequestStreamClosed)?;
        guard.mark_sent();

        let response = response_rx
            .await
            .map_err(|_| UdfClientError::ResponseChannelClosed(id));
        guard.disarm();
        response?
    }
}

struct PendingRequestGuard {
    state: Arc<Mutex<UnaryState>>,
    id: String,
    sent: bool,
    armed: bool,
}

impl PendingRequestGuard {
    fn new(state: Arc<Mutex<UnaryState>>, id: String) -> Self {
        Self {
            state,
            id,
            sent: false,
            armed: true,
        }
    }

    fn mark_sent(&mut self) {
        self.sent = true;
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingRequestGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        let mut state = self.state.lock().expect("unary map state lock poisoned");
        if self.sent {
            if let Some(entry @ PendingEntry::Waiting(_)) = state.entries.get_mut(&self.id) {
                *entry = PendingEntry::Abandoned;
            }
        } else {
            state.entries.remove(&self.id);
        }
    }
}

async fn receive_responses(
    state: Arc<Mutex<UnaryState>>,
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

fn process_response(state: &Arc<Mutex<UnaryState>>, response: MapResponse) -> Result<()> {
    if response.handshake.is_some() {
        return Err(UdfClientError::UnexpectedControlFrame(
            "handshake".to_string(),
        ));
    }
    if let Some(status) = response.status {
        return Err(UdfClientError::UnexpectedControlFrame(format!(
            "status(eot={})",
            status.eot
        )));
    }

    let id = response.id.clone();
    let entry = state
        .lock()
        .expect("unary map state lock poisoned")
        .entries
        .remove(&id);

    match entry {
        Some(PendingEntry::Waiting(sender)) => {
            let decoded = decode_results(response)?;
            let _ = sender.send(Ok(decoded));
            Ok(())
        }
        Some(PendingEntry::Abandoned) => Ok(()),
        None => Err(UdfClientError::UnexpectedResponseId(id)),
    }
}

fn close_session(state: &Arc<Mutex<UnaryState>>, error: UdfClientError) {
    let entries = {
        let mut state = state.lock().expect("unary map state lock poisoned");
        if state.closed.is_some() {
            return;
        }
        state.closed = Some(error.clone());
        std::mem::take(&mut state.entries)
    };

    for entry in entries.into_values() {
        if let PendingEntry::Waiting(sender) = entry {
            let _ = sender.send(Err(error.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::time::Duration;

    use bytes::Bytes;
    use chrono::Utc;
    use numaflow_pb::clients::map::{Handshake, TransmissionStatus, map_response};

    use super::*;

    fn datum(id: &str) -> UdfDatum {
        UdfDatum {
            id: id.to_string(),
            keys: vec!["key".to_string()],
            value: Bytes::from_static(b"value"),
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
                tags: vec!["tag".to_string()],
                metadata: None,
                nack_options: None,
            }],
            handshake: None,
            status: None,
        }
    }

    fn test_session(
        buffer: usize,
    ) -> (
        UnaryMapSession,
        mpsc::Receiver<MapRequest>,
        Arc<Mutex<UnaryState>>,
    ) {
        let (request_tx, request_rx) = mpsc::channel(buffer);
        let state = Arc::new(Mutex::new(UnaryState::default()));
        let receiver = tokio::spawn(pending());
        let session = UnaryMapSession {
            inner: Arc::new(UnaryMapSessionInner {
                request_tx,
                state: Arc::clone(&state),
                _receiver: Arc::new(AbortOnDropHandle::new(receiver)),
            }),
        };
        (session, request_rx, state)
    }

    #[tokio::test]
    async fn correlates_out_of_order_responses() {
        let (session, mut requests, state) = test_session(2);
        let first = tokio::spawn({
            let session = session.clone();
            async move { session.map(datum("A")).await }
        });
        let second = tokio::spawn({
            let session = session.clone();
            async move { session.map(datum("B")).await }
        });

        let mut sent_ids = vec![
            requests.recv().await.expect("first request").id,
            requests.recv().await.expect("second request").id,
        ];
        sent_ids.sort();
        assert_eq!(sent_ids, ["A", "B"]);

        process_response(&state, response("B")).expect("response B");
        process_response(&state, response("A")).expect("response A");

        assert_eq!(first.await.expect("first task").expect("response").id, "A");
        assert_eq!(
            second.await.expect("second task").expect("response").id,
            "B"
        );
    }

    #[tokio::test]
    async fn duplicate_in_flight_id_is_rejected_without_overwrite() {
        let (session, mut requests, state) = test_session(1);
        let first = tokio::spawn({
            let session = session.clone();
            async move { session.map(datum("same")).await }
        });
        requests.recv().await.expect("first request");

        let error = session
            .map(datum("same"))
            .await
            .expect_err("duplicate should fail");
        assert!(matches!(
            error,
            UdfClientError::DuplicateRequestId(ref id) if id == "same"
        ));

        process_response(&state, response("same")).expect("first response");
        assert_eq!(
            first.await.expect("first task").expect("first response").id,
            "same"
        );
    }

    #[tokio::test]
    async fn stream_failure_fans_out_and_closes_session() {
        let (session, mut requests, state) = test_session(2);
        let first = tokio::spawn({
            let session = session.clone();
            async move { session.map(datum("A")).await }
        });
        let second = tokio::spawn({
            let session = session.clone();
            async move { session.map(datum("B")).await }
        });
        requests.recv().await.expect("first request");
        requests.recv().await.expect("second request");

        close_session(
            &state,
            UdfClientError::Grpc(tonic::Status::aborted("failed")),
        );

        for task in [first, second] {
            assert!(matches!(
                task.await.expect("map task").expect_err("stream failure"),
                UdfClientError::Grpc(status) if status.code() == tonic::Code::Aborted
            ));
        }
        assert!(matches!(
            session.map(datum("C")).await.expect_err("closed"),
            UdfClientError::Grpc(_)
        ));
    }

    #[tokio::test]
    async fn request_send_failure_removes_pending_entry() {
        let (session, requests, state) = test_session(1);
        drop(requests);

        assert!(matches!(
            session.map(datum("A")).await.expect_err("send failure"),
            UdfClientError::RequestStreamClosed
        ));
        assert!(state.lock().expect("state").entries.is_empty());
    }

    #[tokio::test]
    async fn cancellation_before_send_removes_pending_entry() {
        let (session, _requests, state) = test_session(1);
        session
            .inner
            .request_tx
            .send(MapRequest {
                request: None,
                id: "block".to_string(),
                handshake: None,
                status: None,
            })
            .await
            .expect("fill request channel");

        let task = tokio::spawn({
            let session = session.clone();
            async move { session.map(datum("A")).await }
        });
        tokio::task::yield_now().await;
        task.abort();
        let _ = task.await;

        assert!(!state.lock().expect("state").entries.contains_key("A"));
    }

    #[tokio::test]
    async fn late_response_for_abandoned_request_is_drained() {
        let (session, mut requests, state) = test_session(1);
        let task = tokio::spawn({
            let session = session.clone();
            async move { session.map(datum("A")).await }
        });
        requests.recv().await.expect("sent request");
        task.abort();
        let _ = task.await;

        assert!(matches!(
            state.lock().expect("state").entries.get("A"),
            Some(PendingEntry::Abandoned)
        ));
        process_response(&state, response("A")).expect("late response is drained");
        assert!(state.lock().expect("state").entries.is_empty());
        assert!(state.lock().expect("state").closed.is_none());
    }

    #[tokio::test]
    async fn unknown_id_fails_waiters_and_future_requests() {
        let (session, mut requests, state) = test_session(1);
        let waiting = tokio::spawn({
            let session = session.clone();
            async move { session.map(datum("A")).await }
        });
        requests.recv().await.expect("sent request");

        let error = process_response(&state, response("unknown")).expect_err("unknown id");
        close_session(&state, error);

        assert!(matches!(
            waiting.await.expect("waiting task").expect_err("closed"),
            UdfClientError::UnexpectedResponseId(ref id) if id == "unknown"
        ));
        assert!(matches!(
            session.map(datum("B")).await.expect_err("closed"),
            UdfClientError::UnexpectedResponseId(ref id) if id == "unknown"
        ));
    }

    #[test]
    fn unary_control_frames_are_rejected() {
        let state = Arc::new(Mutex::new(UnaryState::default()));
        let mut handshake = response("");
        handshake.handshake = Some(Handshake { sot: true });
        assert!(matches!(
            process_response(&state, handshake),
            Err(UdfClientError::UnexpectedControlFrame(ref frame)) if frame == "handshake"
        ));

        let mut eot = response("A");
        eot.status = Some(TransmissionStatus { eot: true });
        assert!(matches!(
            process_response(&state, eot),
            Err(UdfClientError::UnexpectedControlFrame(ref frame)) if frame == "status(eot=true)"
        ));
    }

    #[test]
    fn dropped_oneshot_receiver_does_not_close_session() {
        let state = Arc::new(Mutex::new(UnaryState::default()));
        let (sender, receiver) = oneshot::channel();
        drop(receiver);
        state
            .lock()
            .expect("state")
            .entries
            .insert("A".to_string(), PendingEntry::Waiting(sender));

        process_response(&state, response("A")).expect("delivery race is harmless");
        assert!(state.lock().expect("state").closed.is_none());
    }

    #[tokio::test]
    async fn dropping_last_session_handle_aborts_receiver_task() {
        struct NotifyDrop(Option<oneshot::Sender<()>>);
        impl Drop for NotifyDrop {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let (request_tx, _request_rx) = mpsc::channel(1);
        let state = Arc::new(Mutex::new(UnaryState::default()));
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let receiver = tokio::spawn(async move {
            let _notify = NotifyDrop(Some(dropped_tx));
            pending::<()>().await;
        });
        let session = UnaryMapSession {
            inner: Arc::new(UnaryMapSessionInner {
                request_tx,
                state,
                _receiver: Arc::new(AbortOnDropHandle::new(receiver)),
            }),
        };

        tokio::task::yield_now().await;
        drop(session);
        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("receiver was not aborted")
            .expect("drop notification");
    }

    #[tokio::test]
    async fn empty_request_id_is_rejected_without_sending() {
        let (session, mut requests, _) = test_session(1);
        assert!(matches!(
            session.map(datum("")).await.expect_err("empty id"),
            UdfClientError::EmptyRequestId
        ));
        assert!(requests.try_recv().is_err());
    }
}
