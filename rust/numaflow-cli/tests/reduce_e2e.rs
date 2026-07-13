//! End-to-end tests for the reduce family: aligned reduce, session reduce, accumulator.
//! Each stands up a real SDK server over UDS and runs the compiled `nfcli` binary.

use std::future::pending;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use numaflow::shared::ServerExtras;
use tokio::net::UnixListener;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

mod common;

use common::{run_nfcli, wait_for_socket};

// ---- aligned reduce: counter ----

struct Counter;
struct CounterCreator;

impl numaflow::reduce::ReducerCreator for CounterCreator {
    type R = Counter;
    fn create(&self) -> Counter {
        Counter
    }
}

#[tonic::async_trait]
impl numaflow::reduce::Reducer for Counter {
    async fn reduce(
        &self,
        keys: Vec<String>,
        mut input: mpsc::Receiver<numaflow::reduce::ReduceRequest>,
        _md: &numaflow::reduce::Metadata,
    ) -> Vec<numaflow::reduce::Message> {
        let mut n = 0u32;
        while input.recv().await.is_some() {
            n += 1;
        }
        vec![numaflow::reduce::Message::new(n.to_string().into_bytes()).with_keys(keys)]
    }
}

#[derive(Clone, Copy)]
enum RawReduceBehavior {
    StartupFailure,
    MalformedResponse,
    Stall,
}

struct RawReduceService {
    behavior: RawReduceBehavior,
}

#[tonic::async_trait]
impl numaflow::reduce::proto::reduce_server::Reduce for RawReduceService {
    type ReduceFnStream = ReceiverStream<Result<numaflow::reduce::proto::ReduceResponse, Status>>;

    async fn reduce_fn(
        &self,
        _request: Request<tonic::Streaming<numaflow::reduce::proto::ReduceRequest>>,
    ) -> Result<Response<Self::ReduceFnStream>, Status> {
        match self.behavior {
            RawReduceBehavior::StartupFailure => {
                Err(Status::unavailable("ReduceFn startup rejected"))
            }
            RawReduceBehavior::MalformedResponse => {
                let (response_tx, response_rx) = mpsc::channel(1);
                response_tx
                    .send(Ok(numaflow::reduce::proto::ReduceResponse::default()))
                    .await
                    .expect("send malformed response");
                Ok(Response::new(ReceiverStream::new(response_rx)))
            }
            RawReduceBehavior::Stall => {
                let (response_tx, response_rx) = mpsc::channel(1);
                tokio::spawn(async move {
                    let _response_tx = response_tx;
                    pending::<()>().await;
                });
                Ok(Response::new(ReceiverStream::new(response_rx)))
            }
        }
    }

    async fn is_ready(
        &self,
        _request: Request<()>,
    ) -> Result<Response<numaflow::reduce::proto::ReadyResponse>, Status> {
        Ok(Response::new(numaflow::reduce::proto::ReadyResponse {
            ready: true,
        }))
    }
}

async fn start_raw_reduce_server(
    socket: PathBuf,
    behavior: RawReduceBehavior,
    shutdown_rx: oneshot::Receiver<()>,
) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
    let listener = UnixListener::bind(socket).expect("bind raw reduce server");
    let (incoming_tx, incoming_rx) = mpsc::channel(4);
    let accept_handle = tokio::spawn(async move {
        loop {
            let incoming = listener.accept().await.map(|(stream, _)| stream);
            if incoming_tx.send(incoming).await.is_err() {
                break;
            }
        }
    });
    let server_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(numaflow::reduce::proto::reduce_server::ReduceServer::new(
                RawReduceService { behavior },
            ))
            .serve_with_incoming_shutdown(ReceiverStream::new(incoming_rx), async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("raw reduce server");
    });

    (server_handle, accept_handle)
}

async fn run_raw_reduce_case(
    behavior: RawReduceBehavior,
    extra_args: &[&str],
    yaml: &'static str,
) -> (i32, String, String) {
    let tmp = tempfile::tempdir().unwrap();
    let socket = tmp.path().join("raw-reduce.sock");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (server, accept_handle) =
        start_raw_reduce_server(socket.clone(), behavior, shutdown_rx).await;
    wait_for_socket(&socket).await;

    let mut args = vec![
        "reduce".to_string(),
        "--socket".to_string(),
        socket.to_string_lossy().into_owned(),
        "-f".to_string(),
        "-".to_string(),
        "--window".to_string(),
        "fixed".to_string(),
        "--length".to_string(),
        "60s".to_string(),
    ];
    args.extend(extra_args.iter().map(|arg| (*arg).to_string()));

    let result = tokio::task::spawn_blocking(move || {
        let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
        run_nfcli(&arg_refs, Some(yaml))
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());
    let _ = server.await;
    accept_handle.abort();
    result
}

#[tokio::test]
async fn reduce_fixed_counts_per_key_per_window() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("reduce.sock");
    let info = tmp.path().join("reduce-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::reduce::Server::new(CounterCreator)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("reduce server failed");
    });
    wait_for_socket(&sock).await;

    // 3 blue + 1 red in window [0,60s); 1 red in [60,120s).
    let yaml = r#"
payload: "1"
keys: [blue]
eventTime: "2026-07-06T00:00:01Z"
repeat: 3
---
payload: "1"
keys: [red]
eventTime: "2026-07-06T00:00:05Z"
---
payload: "1"
keys: [red]
eventTime: "2026-07-06T00:01:10Z"
"#;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &[
                "reduce", "--socket", &sock_str, "-f", "-", "--window", "fixed", "--length", "60s",
                "-v",
            ],
            Some(yaml),
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stderr.contains("OPEN"), "stderr={stderr}");
    assert!(stderr.contains("CLOSE (stream close)"), "stderr={stderr}");
    // Two windows.
    assert!(stdout.contains("windows=2"), "stdout={stdout}");
    // blue counted 3, red counted 1 (first window) and 1 (second window).
    // The counter emits the count as payload.
    assert!(
        stdout.contains("payload=3"),
        "expected blue count 3, stdout={stdout}"
    );
    assert!(
        stdout.contains("payload=1"),
        "expected red count 1, stdout={stdout}"
    );
    let first_window = stdout
        .find("2026-07-06T00:00:00+00:00")
        .expect("first fixed window");
    let second_window = stdout
        .find("2026-07-06T00:01:00+00:00")
        .expect("second fixed window");
    assert!(
        first_window < second_window,
        "fixed windows must be reported in order: {stdout}"
    );
}

#[tokio::test]
async fn reduce_sliding_assigns_event_to_multiple_windows() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("reduce.sock");
    let info = tmp.path().join("reduce-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::reduce::Server::new(CounterCreator)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("reduce server failed");
    });
    wait_for_socket(&sock).await;

    // Single event at t=105s, length 60s, slide 10s → 6 windows each containing it.
    let yaml = r#"
payload: "1"
keys: [x]
eventTime: "2026-07-06T00:01:45Z"
"#;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &[
                "reduce", "--socket", &sock_str, "-f", "-", "--window", "sliding", "--length",
                "60s", "--slide", "10s",
            ],
            Some(yaml),
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    // The event lands in 6 sliding windows.
    assert!(
        stdout.contains("windows=6"),
        "expected 6 windows, stdout={stdout}"
    );
    let mut previous = 0;
    for start in [
        "2026-07-06T00:00:50+00:00",
        "2026-07-06T00:01:00+00:00",
        "2026-07-06T00:01:10+00:00",
        "2026-07-06T00:01:20+00:00",
        "2026-07-06T00:01:30+00:00",
        "2026-07-06T00:01:40+00:00",
    ] {
        let position = stdout[previous..]
            .find(start)
            .map(|offset| previous + offset)
            .unwrap_or_else(|| panic!("missing ordered sliding window {start}: {stdout}"));
        previous = position + start.len();
    }
}

#[tokio::test]
async fn reduce_startup_failure_keeps_connect_exit_code_after_pacing() {
    let yaml = r#"
payload: first
keys: [key]
eventTime: "2026-07-06T00:00:01Z"
---
payload: second
keys: [key]
eventTime: "2026-07-06T00:00:02Z"
"#;

    let (code, stdout, stderr) = run_raw_reduce_case(
        RawReduceBehavior::StartupFailure,
        &["--batch-size", "1", "--delay", "100ms", "--timeout", "1s"],
        yaml,
    )
    .await;

    assert_eq!(code, 2, "stdout={stdout}\nstderr={stderr}");
    assert!(
        stderr.contains("ReduceFn startup rejected"),
        "stderr={stderr}"
    );
}

#[tokio::test]
async fn reduce_malformed_response_uses_protocol_exit_code() {
    let yaml = r#"
payload: one
keys: [key]
eventTime: "2026-07-06T00:00:01Z"
"#;

    let (code, stdout, stderr) = run_raw_reduce_case(
        RawReduceBehavior::MalformedResponse,
        &["--timeout", "1s"],
        yaml,
    )
    .await;

    assert_eq!(code, 3, "stdout={stdout}\nstderr={stderr}");
    assert!(
        stderr.contains("invalid reduce response result"),
        "stderr={stderr}"
    );
}

#[tokio::test]
async fn reduce_stalled_response_respects_timeout() {
    let yaml = r#"
payload: one
keys: [key]
eventTime: "2026-07-06T00:00:01Z"
"#;

    let (code, stdout, stderr) =
        run_raw_reduce_case(RawReduceBehavior::Stall, &["--timeout", "100ms"], yaml).await;

    assert_eq!(code, 3, "stdout={stdout}\nstderr={stderr}");
    assert!(
        stderr.contains("timed out waiting for a response"),
        "stderr={stderr}"
    );
}

// ---- session reduce: counter ----

struct SessionCounter {
    count: Arc<AtomicU32>,
}
struct SessionCounterCreator;

impl numaflow::session_reduce::SessionReducerCreator for SessionCounterCreator {
    type R = SessionCounter;
    fn create(&self) -> SessionCounter {
        SessionCounter {
            count: Arc::new(AtomicU32::new(0)),
        }
    }
}

#[tonic::async_trait]
impl numaflow::session_reduce::SessionReducer for SessionCounter {
    async fn session_reduce(
        &self,
        keys: Vec<String>,
        mut input: mpsc::Receiver<numaflow::session_reduce::SessionReduceRequest>,
        output: mpsc::Sender<numaflow::session_reduce::Message>,
    ) {
        while input.recv().await.is_some() {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
        let n = self.count.load(Ordering::Relaxed);
        output
            .send(
                numaflow::session_reduce::Message::new(n.to_string().into_bytes()).with_keys(keys),
            )
            .await
            .unwrap();
    }

    async fn accumulator(&self) -> Vec<u8> {
        self.count.load(Ordering::Relaxed).to_string().into_bytes()
    }

    async fn merge_accumulator(&self, acc: Vec<u8>) {
        if let Ok(s) = String::from_utf8(acc)
            && let Ok(n) = s.parse::<u32>()
        {
            self.count.fetch_add(n, Ordering::Relaxed);
        }
    }
}

#[tokio::test]
async fn session_reduce_counts_sessions() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("sr.sock");
    let info = tmp.path().join("sr-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::session_reduce::Server::new(SessionCounterCreator)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("session server failed");
    });
    wait_for_socket(&sock).await;

    // Two events within 10s gap → same session; third after gap → new session.
    let yaml = r#"
payload: click
keys: [alice]
eventTime: "2026-07-06T00:00:00Z"
---
payload: click
keys: [alice]
eventTime: "2026-07-06T00:00:08Z"
---
payload: click
keys: [alice]
eventTime: "2026-07-06T00:00:30Z"
"#;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &[
                "session-reduce",
                "--socket",
                &sock_str,
                "-f",
                "-",
                "--gap",
                "10s",
            ],
            Some(yaml),
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    // Two sessions: first has 2 clicks, second has 1.
    assert!(stdout.contains("session"), "stdout={stdout}");
    assert!(
        stdout.contains("payload=2"),
        "expected first session count 2, stdout={stdout}"
    );
    assert!(
        stdout.contains("payload=1"),
        "expected second session count 1, stdout={stdout}"
    );
}

#[tokio::test]
async fn session_reduce_merges_bridged_sessions() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("sr.sock");
    let info = tmp.path().join("sr-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::session_reduce::Server::new(SessionCounterCreator)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("session server failed");
    });
    wait_for_socket(&sock).await;

    let yaml = r#"
payload: click
keys: [alice]
eventTime: "2026-07-06T00:00:00Z"
---
payload: click
keys: [alice]
eventTime: "2026-07-06T00:00:18Z"
---
payload: click
keys: [alice]
eventTime: "2026-07-06T00:00:09Z"
"#;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &[
                "session-reduce",
                "--socket",
                &sock_str,
                "-f",
                "-",
                "--gap",
                "10s",
                "-v",
            ],
            Some(yaml),
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stderr.contains("MERGE"), "stderr={stderr}");
    assert!(stdout.contains("payload=3"), "stdout={stdout}");
    assert!(stdout.contains("sessions=1"), "stdout={stdout}");
}

// ---- accumulator: echo each message ----

struct EchoAccumulator;
struct EchoAccumulatorCreator;

impl numaflow::accumulator::AccumulatorCreator for EchoAccumulatorCreator {
    type A = EchoAccumulator;
    fn create(&self) -> EchoAccumulator {
        EchoAccumulator
    }
}

#[tonic::async_trait]
impl numaflow::accumulator::Accumulator for EchoAccumulator {
    async fn accumulate(
        &self,
        mut input: mpsc::Receiver<numaflow::accumulator::AccumulatorRequest>,
        output: mpsc::Sender<numaflow::accumulator::Message>,
    ) {
        while let Some(req) = input.recv().await {
            // Echo the message straight back (id is preserved by from_accumulator_request).
            let msg = numaflow::accumulator::Message::from_accumulator_request(req);
            output.send(msg).await.unwrap();
        }
    }
}

#[tokio::test]
async fn accumulator_echoes_messages() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("acc.sock");
    let info = tmp.path().join("acc-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::accumulator::Server::new(EchoAccumulatorCreator)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("accumulator server failed");
    });
    wait_for_socket(&sock).await;

    let yaml = r#"
payload: first
keys: [k]
eventTime: "2026-07-06T00:00:03Z"
---
payload: second
keys: [k]
eventTime: "2026-07-06T00:00:01Z"
"#;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &["accumulator", "--socket", &sock_str, "-f", "-"],
            Some(yaml),
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("first"), "stdout={stdout}");
    assert!(stdout.contains("second"), "stdout={stdout}");
    // ids echoed back (auto-generated msg-1 / msg-2).
    assert!(
        stdout.contains("msg-1") || stdout.contains("id=msg-"),
        "stdout={stdout}"
    );
}
