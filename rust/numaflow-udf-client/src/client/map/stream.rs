use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use numaflow_pb::clients::map::{MapRequest, MapResponse, map_client::MapClient};
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;
use tonic::Streaming;
use tonic::transport::Channel;

use super::MapRpcStream;
use crate::error::{Result, UdfClientError};
use crate::model::{StreamMapResponse, UdfDatum};
use crate::wire::map::{data_request, decode_results};

/// Response channel size for each streaming map request.
const STREAMING_MAP_RESP_CHANNEL_SIZE: usize = 10;

/// Receiver for the result chunks produced by one streaming map request.
pub type StreamMapReceiver = mpsc::Receiver<Result<StreamMapResponse>>;

/// A cloneable streaming map session with per-ID response streams.
#[derive(Clone)]
pub struct StreamMapSession {
    inner: Arc<StreamMapSessionInner>,
}

struct StreamMapSessionInner {
    request_tx: mpsc::Sender<MapRequest>,
    state: Arc<Mutex<StreamState>>,
    _receiver: AbortOnDropHandle<()>,
}

/// Shared state between streaming map senders and the response pump.
///
/// MapFn is a BiDi gRPC stream, so sending and receiving happen in different tasks. `closed` is
/// stored explicitly because `request_tx.send()` can still return `Ok(())` briefly after the
/// response task has closed the stream.
#[derive(Default)]
struct StreamState {
    entries: HashMap<String, PendingEntry>,
    closed: Option<UdfClientError>,
}

enum PendingEntry {
    Active(mpsc::Sender<Result<StreamMapResponse>>),
    /// Keep consuming this ID until EOT after its caller stops receiving results.
    Abandoned,
}

impl StreamMapSession {
    /// Opens a handshaken MapFn stream and starts its streaming response pump.
    pub async fn open(client: MapClient<Channel>, request_buffer: usize) -> Result<Self> {
        let stream = MapRpcStream::open(client, request_buffer).await?;
        let (request_tx, response_stream) = stream.into_parts();
        Ok(Self::from_stream(request_tx, response_stream))
    }

    fn from_stream(
        request_tx: mpsc::Sender<MapRequest>,
        response_stream: Streaming<MapResponse>,
    ) -> Self {
        let state = Arc::new(Mutex::new(StreamState::default()));
        let receiver_state = Arc::clone(&state);
        let receiver = tokio::spawn(async move {
            receive_responses(receiver_state, response_stream).await;
        });

        Self {
            inner: Arc::new(StreamMapSessionInner {
                request_tx,
                state,
                _receiver: AbortOnDropHandle::new(receiver),
            }),
        }
    }

    /// Sends one datum and returns its correlated stream of result chunks.
    ///
    /// The returned channel closes after the UDF sends EOT for this datum's ID. No EOT payload is
    /// delivered to the caller.
    pub async fn stream(&self, datum: UdfDatum) -> Result<StreamMapReceiver> {
        if datum.id.is_empty() {
            return Err(UdfClientError::EmptyRequestId);
        }

        let id = datum.id.clone();
        let request = data_request(datum);
        let (response_tx, response_rx) = mpsc::channel(STREAMING_MAP_RESP_CHANNEL_SIZE);

        {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("stream map state lock poisoned");
            if let Some(error) = &state.closed {
                return Err(error.clone());
            }
            if state.entries.contains_key(&id) {
                return Err(UdfClientError::DuplicateRequestId(id));
            }

            // Register the sender before sending data. The UDF can respond faster than the
            // sending task runs, and the response pump must always find the corresponding ID.
            state
                .entries
                .insert(id.clone(), PendingEntry::Active(response_tx));
        }

        let mut guard = PendingRequestGuard::new(Arc::clone(&self.inner.state), id);
        if self.inner.request_tx.send(request).await.is_err() {
            close_session(&self.inner.state, UdfClientError::RequestStreamClosed).await;
            return Err(UdfClientError::RequestStreamClosed);
        }
        guard.mark_sent();
        guard.disarm();
        Ok(response_rx)
    }
}

/// Cleans up correlation state if a caller is cancelled while sending its request.
struct PendingRequestGuard {
    state: Arc<Mutex<StreamState>>,
    id: String,
    sent: bool,
    armed: bool,
}

impl PendingRequestGuard {
    fn new(state: Arc<Mutex<StreamState>>, id: String) -> Self {
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

        let mut state = self.state.lock().expect("stream map state lock poisoned");
        if self.sent {
            if let Some(entry @ PendingEntry::Active(_)) = state.entries.get_mut(&self.id) {
                *entry = PendingEntry::Abandoned;
            }
        } else {
            state.entries.remove(&self.id);
        }
    }
}

async fn receive_responses(
    state: Arc<Mutex<StreamState>>,
    mut response_stream: Streaming<MapResponse>,
) {
    let terminal_error = loop {
        match response_stream.message().await {
            Ok(Some(response)) => match process_response(&state, response).await {
                Ok(()) => {}
                // A cancelled caller does not invalidate the shared MapFn stream. Its remaining
                // frames are drained until EOT by the abandoned entry.
                Err(UdfClientError::ResponseChannelClosed(_)) => {}
                Err(error) => break error,
            },
            Ok(None) => break UdfClientError::ResponseStreamClosed,
            Err(status) => break UdfClientError::Grpc(status),
        }
    };

    close_session(&state, terminal_error).await;
}

/// Processes one response received from the stream map UDF server.
///
/// The reason this method has not been subdivided is that we do not want to yield back to the
/// Tokio runtime between these three operations:
/// * remove the ID from the sender map;
/// * check for EOT;
/// * re-insert the ID into the sender map.
async fn process_response(state: &Arc<Mutex<StreamState>>, response: MapResponse) -> Result<()> {
    if response.handshake.is_some() {
        return Err(UdfClientError::UnexpectedControlFrame(
            "handshake".to_string(),
        ));
    }
    if matches!(response.status, Some(ref status) if !status.eot) {
        return Err(UdfClientError::UnexpectedControlFrame(
            "status(eot=false)".to_string(),
        ));
    }

    let id = response.id.clone();
    let entry = {
        let mut state = state.lock().expect("stream map state lock poisoned");
        if let Some(error) = &state.closed {
            return Err(error.clone());
        }
        state.entries.remove(&id)
    };

    let Some(entry) = entry else {
        return Err(UdfClientError::UnexpectedResponseId(id));
    };

    // Once we get EOT, drop the sender to tell the caller that its response stream is complete.
    if response.status.is_some_and(|status| status.eot) {
        return Ok(());
    }

    let PendingEntry::Active(response_sender) = entry else {
        state
            .lock()
            .expect("stream map state lock poisoned")
            .entries
            .insert(id, PendingEntry::Abandoned);
        return Ok(());
    };

    let decoded = decode_results(response)?;

    // Insert the sender back before awaiting the channel send. This preserves correlation if
    // another response for the same ID is already available to the response task.
    let session_error = {
        let mut state = state.lock().expect("stream map state lock poisoned");
        if let Some(error) = &state.closed {
            Some(error.clone())
        } else {
            state
                .entries
                .insert(id.clone(), PendingEntry::Active(response_sender.clone()));
            None
        }
    };

    if let Some(error) = session_error {
        let _ = response_sender.send(Err(error)).await;
        return Ok(());
    }

    if response_sender.send(Ok(decoded)).await.is_err() {
        let mut state = state.lock().expect("stream map state lock poisoned");
        if state.closed.is_none() && state.entries.contains_key(&id) {
            state.entries.insert(id.clone(), PendingEntry::Abandoned);
        }
        return Err(UdfClientError::ResponseChannelClosed(id));
    }

    Ok(())
}

/// Broadcasts a terminal stream error to every active request.
async fn close_session(state: &Arc<Mutex<StreamState>>, error: UdfClientError) {
    let entries = {
        let mut state = state.lock().expect("stream map state lock poisoned");
        if state.closed.is_some() {
            return;
        }
        state.closed = Some(error.clone());
        std::mem::take(&mut state.entries)
    };

    for entry in entries.into_values() {
        if let PendingEntry::Active(sender) = entry {
            let _ = sender.send(Err(error.clone())).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

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

    fn response(id: &str, value: &[u8]) -> MapResponse {
        MapResponse {
            id: id.to_string(),
            results: vec![map_response::Result {
                keys: vec!["mapped".to_string()],
                value: value.to_vec(),
                tags: vec![],
                metadata: None,
                nack_options: None,
            }],
            handshake: None,
            status: None,
        }
    }

    fn eot_response(id: &str) -> MapResponse {
        MapResponse {
            id: id.to_string(),
            results: vec![],
            handshake: None,
            status: Some(TransmissionStatus { eot: true }),
        }
    }

    fn test_session(
        buffer: usize,
    ) -> (
        StreamMapSession,
        mpsc::Receiver<MapRequest>,
        Arc<Mutex<StreamState>>,
    ) {
        let (request_tx, request_rx) = mpsc::channel(buffer);
        let state = Arc::new(Mutex::new(StreamState::default()));
        let receiver = tokio::spawn(pending());
        let session = StreamMapSession {
            inner: Arc::new(StreamMapSessionInner {
                request_tx,
                state: Arc::clone(&state),
                _receiver: AbortOnDropHandle::new(receiver),
            }),
        };
        (session, request_rx, state)
    }

    #[tokio::test]
    async fn sends_multiple_chunks_then_closes_the_receiver_on_eot() {
        let (session, mut requests, state) = test_session(1);
        let mut receiver = session.stream(datum("A")).await.expect("stream");
        assert_eq!(requests.recv().await.expect("request").id, "A");

        process_response(&state, response("A", b"first"))
            .await
            .expect("first response");
        assert_eq!(
            receiver
                .recv()
                .await
                .expect("first chunk")
                .expect("first response")
                .results
                .first()
                .expect("first result")
                .value,
            "first"
        );

        process_response(&state, response("A", b"second"))
            .await
            .expect("second response");
        assert_eq!(
            receiver
                .recv()
                .await
                .expect("second chunk")
                .expect("second response")
                .results
                .first()
                .expect("second result")
                .value,
            "second"
        );

        process_response(&state, eot_response("A"))
            .await
            .expect("EOT");
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn correlates_interleaved_response_streams_by_id() {
        let (session, mut requests, state) = test_session(2);
        let mut first = session.stream(datum("A")).await.expect("stream A");
        let mut second = session.stream(datum("B")).await.expect("stream B");
        assert_eq!(requests.recv().await.expect("request A").id, "A");
        assert_eq!(requests.recv().await.expect("request B").id, "B");

        process_response(&state, response("B", b"B1"))
            .await
            .expect("response B1");
        process_response(&state, response("A", b"A1"))
            .await
            .expect("response A1");
        process_response(&state, eot_response("B"))
            .await
            .expect("EOT B");
        process_response(&state, response("A", b"A2"))
            .await
            .expect("response A2");
        process_response(&state, eot_response("A"))
            .await
            .expect("EOT A");

        let response_b = second.recv().await.expect("B chunk").expect("B response");
        assert_eq!(response_b.results.first().expect("B result").value, "B1");
        assert!(second.recv().await.is_none());

        let mut values_a = Vec::new();
        while let Some(response) = first.recv().await {
            values_a.push(
                response
                    .expect("A response")
                    .results
                    .first()
                    .expect("A result")
                    .value
                    .clone(),
            );
        }
        assert_eq!(values_a, ["A1", "A2"]);
    }

    #[tokio::test]
    async fn duplicate_in_flight_id_is_rejected_without_overwrite() {
        let (session, mut requests, state) = test_session(1);
        let receiver = session.stream(datum("same")).await.expect("first stream");
        requests.recv().await.expect("first request");

        assert!(matches!(
            session
                .stream(datum("same"))
                .await
                .expect_err("duplicate should fail"),
            UdfClientError::DuplicateRequestId(ref id) if id == "same"
        ));

        process_response(&state, eot_response("same"))
            .await
            .expect("EOT");
        drop(receiver);
    }

    #[tokio::test]
    async fn stream_failure_fans_out_and_closes_session() {
        let (session, mut requests, state) = test_session(2);
        let mut first = session.stream(datum("A")).await.expect("first stream");
        let mut second = session.stream(datum("B")).await.expect("second stream");
        requests.recv().await.expect("first request");
        requests.recv().await.expect("second request");

        close_session(
            &state,
            UdfClientError::Grpc(tonic::Status::aborted("failed")),
        )
        .await;

        for receiver in [&mut first, &mut second] {
            assert!(matches!(
                receiver
                    .recv()
                    .await
                    .expect("terminal error")
                    .expect_err("stream failure"),
                UdfClientError::Grpc(status) if status.code() == tonic::Code::Aborted
            ));
        }
        assert!(matches!(
            session.stream(datum("C")).await.expect_err("closed"),
            UdfClientError::Grpc(_)
        ));
    }

    #[tokio::test]
    async fn dropped_receiver_is_drained_until_eot_without_closing_session() {
        let (session, mut requests, state) = test_session(2);
        let receiver = session.stream(datum("A")).await.expect("stream A");
        requests.recv().await.expect("request A");
        drop(receiver);

        assert!(matches!(
            process_response(&state, response("A", b"ignored")).await,
            Err(UdfClientError::ResponseChannelClosed(ref id)) if id == "A"
        ));
        process_response(&state, response("A", b"also ignored"))
            .await
            .expect("drain response");
        process_response(&state, eot_response("A"))
            .await
            .expect("drain EOT");

        let _receiver = session
            .stream(datum("B"))
            .await
            .expect("session remains open");
        assert_eq!(requests.recv().await.expect("request B").id, "B");
    }

    #[tokio::test]
    async fn request_send_failure_cleans_up_and_closes_session() {
        let (session, requests, state) = test_session(1);
        drop(requests);

        assert!(matches!(
            session.stream(datum("A")).await.expect_err("send failure"),
            UdfClientError::RequestStreamClosed
        ));
        let state = state.lock().expect("state");
        assert!(state.entries.is_empty());
        assert!(matches!(
            state.closed,
            Some(UdfClientError::RequestStreamClosed)
        ));
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
            async move { session.stream(datum("A")).await }
        });
        tokio::task::yield_now().await;
        task.abort();
        let _ = task.await;

        assert!(!state.lock().expect("state").entries.contains_key("A"));
    }

    #[tokio::test]
    async fn unexpected_id_is_rejected() {
        let state = Arc::new(Mutex::new(StreamState::default()));
        assert!(matches!(
            process_response(&state, response("missing", b"value")).await,
            Err(UdfClientError::UnexpectedResponseId(ref id)) if id == "missing"
        ));
        assert!(matches!(
            process_response(&state, eot_response("missing")).await,
            Err(UdfClientError::UnexpectedResponseId(ref id)) if id == "missing"
        ));
    }

    #[tokio::test]
    async fn unexpected_control_frames_are_rejected() {
        let state = Arc::new(Mutex::new(StreamState::default()));
        let mut handshake = response("", b"");
        handshake.handshake = Some(Handshake { sot: true });
        assert!(matches!(
            process_response(&state, handshake).await,
            Err(UdfClientError::UnexpectedControlFrame(ref frame)) if frame == "handshake"
        ));

        let mut status = response("A", b"");
        status.status = Some(TransmissionStatus { eot: false });
        assert!(matches!(
            process_response(&state, status).await,
            Err(UdfClientError::UnexpectedControlFrame(ref frame)) if frame == "status(eot=false)"
        ));
    }

    #[tokio::test]
    async fn empty_request_id_is_rejected_without_sending() {
        let (session, mut requests, _) = test_session(1);
        assert!(matches!(
            session.stream(datum("")).await.expect_err("empty id"),
            UdfClientError::EmptyRequestId
        ));
        assert!(requests.try_recv().is_err());
    }
}
