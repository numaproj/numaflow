//! End-to-end tests for the reduce family: aligned reduce, session reduce, accumulator.
//! Each stands up a real SDK server over UDS and runs the compiled `nfcli` binary.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use numaflow::shared::ServerExtras;
use tokio::sync::{mpsc, oneshot};

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
            ],
            Some(yaml),
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
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
