//! SDK-backed replay source for `transform` (and, in principle, any run that needs the source
//! forwarder to pull CLI-provided events).
//!
//! Transformers only run inside the *source* forwarder, so to test a transformer the facade stands
//! up a tiny [`numaflow::source::Sourcer`] that replays the fed events over a temp-dir UDS socket.
//! The production `create_source` connects to it exactly as it would to any user-defined source
//! (the SDK writes a valid server-info file, so pre-flight works unmodified).
//!
//! This is a minimal standalone implementation. We deliberately do NOT reuse
//! `source/test_utils.rs` / `shared/test_utils` — those are `#[cfg(test)]`-gated and pull test
//! scaffolding; they are reference only.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use numaflow::source::{Message as SdkMessage, Offset as SdkOffset, SourceReadRequest, Sourcer};
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tonic::async_trait;

use crate::local::LocalError;
use crate::local::events::InputEvent;

/// Shared state between the [`Replay`] server handler and the [`ReplayHandle`] the runner drives.
struct ReplayState {
    /// Events waiting to be read, tagged with their assigned sequence/offset.
    queued: Mutex<VecDeque<(u64, InputEvent)>>,
    /// Events handed to `read` but not yet acked (offset → nothing; membership is enough).
    in_flight: Mutex<VecDeque<u64>>,
    /// Total events acked by the source forwarder.
    acked: Arc<AtomicUsize>,
    /// Total events enqueued via `push` (used for the drain condition `acked == fed`).
    fed: Arc<AtomicUsize>,
    /// Monotonic sequence counter for offsets.
    next_seq: Mutex<u64>,
}

/// The `Sourcer` served over UDS. Reads pop from `queued` into `in_flight`; `ack` drains
/// `in_flight` into `acked`.
struct Replay {
    state: Arc<ReplayState>,
}

#[async_trait]
impl Sourcer for Replay {
    async fn read(
        &self,
        request: SourceReadRequest,
        transmitter: tokio::sync::mpsc::Sender<SdkMessage>,
    ) {
        // Emit up to `request.count` currently-queued events; returning fewer (or none) is fine —
        // the forwarder will read again. This is non-blocking beyond what is queued.
        //
        // We must NOT hold the `parking_lot` mutex guards across the `.await` on `send` (they are
        // not `Send` and would make this future non-`Send`). So we pop the whole batch while
        // holding the locks, release them, then stream the messages out.
        let batch: Vec<SdkMessage> = {
            let mut queued = self.state.queued.lock();
            let mut in_flight = self.state.in_flight.lock();
            let mut batch = Vec::new();
            for _ in 0..request.count {
                let Some((seq, ev)) = queued.pop_front() else {
                    break;
                };
                in_flight.push_back(seq);
                batch.push(SdkMessage {
                    value: ev.payload.to_vec(),
                    offset: SdkOffset {
                        offset: seq.to_be_bytes().to_vec(),
                        partition_id: 0,
                    },
                    event_time: ev.event_time,
                    keys: ev.keys.clone(),
                    headers: ev.headers.clone(),
                    user_metadata: None,
                });
            }
            batch
        };

        for msg in batch {
            // The receiver is drained by the SDK for the duration of this batch; a send failure
            // just means the batch was cut short, which the forwarder tolerates.
            if transmitter.send(msg).await.is_err() {
                break;
            }
        }
    }

    async fn ack(&self, offsets: Vec<SdkOffset>) {
        let mut in_flight = self.state.in_flight.lock();
        for off in offsets {
            let seq = u64::from_be_bytes(off.offset.try_into().unwrap_or([0u8; 8]));
            // Remove the matching in-flight entry (there should be exactly one).
            if let Some(pos) = in_flight.iter().position(|s| *s == seq) {
                in_flight.remove(pos);
                self.state.acked.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    async fn nack(
        &self,
        offsets: Vec<SdkOffset>,
        _nack_options: Option<numaflow::shared::NackOptions>,
    ) {
        // In practice the source forwarder acks on a successful downstream write, so this path is
        // rarely hit. We drop nacked entries from in-flight (we no longer retain their payload to
        // re-queue) — nack of a replayed event is not an exercised path for the test tool.
        let mut in_flight = self.state.in_flight.lock();
        for off in offsets {
            let seq = u64::from_be_bytes(off.offset.try_into().unwrap_or([0u8; 8]));
            if let Some(pos) = in_flight.iter().position(|s| *s == seq) {
                in_flight.remove(pos);
            }
        }
    }

    async fn pending(&self) -> Option<usize> {
        let queued = self.state.queued.lock().len();
        let in_flight = self.state.in_flight.lock().len();
        Some(queued + in_flight)
    }

    async fn partitions(&self) -> Option<Vec<i32>> {
        Some(vec![0])
    }
}

/// Handle the runner holds to feed the replay source and observe its drain progress. Owns the
/// temp dir (kept alive so the socket persists) and the server thread.
pub(crate) struct ReplayHandle {
    state: Arc<ReplayState>,
    acked: Arc<AtomicUsize>,
    fed: Arc<AtomicUsize>,
    shutdown: Option<oneshot::Sender<()>>,
    pub(crate) socket_path: PathBuf,
    pub(crate) server_info_path: PathBuf,
    /// Kept alive for the lifetime of the run so the socket/server-info files persist.
    _temp_dir: TempDir,
    server_thread: Option<std::thread::JoinHandle<()>>,
}

impl ReplayHandle {
    /// Start the replay source server on its own thread + runtime (the SDK's `start_with_shutdown`
    /// blocks its thread, matching the production per-container isolation).
    pub(crate) fn start() -> Result<Self, LocalError> {
        let temp_dir = TempDir::new()
            .map_err(|e| LocalError::Internal(format!("failed to create replay temp dir: {e}")))?;
        let socket_path = temp_dir.path().join("source.sock");
        // The server-info basename determines the container type parsed during the source
        // forwarder's `create_source` pre-flight — it MUST be "sourcer-server-info", else the
        // version-compat check fails with "container type: unknown".
        let server_info_path = temp_dir.path().join("sourcer-server-info");

        let state = Arc::new(ReplayState {
            queued: Mutex::new(VecDeque::new()),
            in_flight: Mutex::new(VecDeque::new()),
            acked: Arc::new(AtomicUsize::new(0)),
            fed: Arc::new(AtomicUsize::new(0)),
            next_seq: Mutex::new(0),
        });
        let acked = Arc::clone(&state.acked);
        let fed = Arc::clone(&state.fed);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let sock = socket_path.clone();
        let info = server_info_path.clone();
        let handler_state = Arc::clone(&state);
        let server_thread = std::thread::Builder::new()
            .name("nfcli-replay-source".to_string())
            .spawn(move || {
                use numaflow::shared::ServerExtras;
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build tokio runtime for replay source")
                    .block_on(async move {
                        let replay = Replay {
                            state: handler_state,
                        };
                        if let Err(e) = numaflow::source::Server::new(replay)
                            .with_socket_file(sock)
                            .with_server_info_file(info)
                            .start_with_shutdown(shutdown_rx)
                            .await
                        {
                            tracing::error!("replay source server exited with error: {e}");
                        }
                    });
            })
            .map_err(|e| {
                LocalError::Internal(format!("failed to spawn replay source thread: {e}"))
            })?;

        Ok(Self {
            state,
            acked,
            fed,
            shutdown: Some(shutdown_tx),
            socket_path,
            server_info_path,
            _temp_dir: temp_dir,
            server_thread: Some(server_thread),
        })
    }

    /// Enqueue events for replay. Each event gets a fresh monotonic offset.
    pub(crate) fn push(&self, events: Vec<InputEvent>) {
        let mut queued = self.state.queued.lock();
        let mut next_seq = self.state.next_seq.lock();
        for ev in events {
            let seq = *next_seq;
            *next_seq += 1;
            queued.push_back((seq, ev));
            self.fed.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Drain condition for transform/replay runs: every fed event has been acked.
    pub(crate) fn is_drained(&self) -> bool {
        self.acked.load(Ordering::SeqCst) >= self.fed.load(Ordering::SeqCst)
    }

    /// `(acked, fed)` for drain reporting.
    pub(crate) fn progress(&self) -> (usize, usize) {
        (
            self.acked.load(Ordering::SeqCst),
            self.fed.load(Ordering::SeqCst),
        )
    }

    /// Signal the server to shut down and join its thread.
    pub(crate) fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.server_thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ReplayHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}
