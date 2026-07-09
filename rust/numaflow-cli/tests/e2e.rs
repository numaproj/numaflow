//! End-to-end tests: stand up real UDF servers (via the numaflow-rs SDK) over a unix domain
//! socket and run the compiled `nfcli` binary against them, asserting output and exit codes.

use numaflow::shared::ServerExtras;
use tokio::sync::oneshot;

mod common;

use common::{run_nfcli, wait_for_socket};

// ---- map (unary) ----

struct UpperMapper;

#[tonic::async_trait]
impl numaflow::map::Mapper for UpperMapper {
    async fn map(&self, input: numaflow::map::MapRequest) -> Vec<numaflow::map::Message> {
        let upper = input.value.to_ascii_uppercase();
        vec![
            numaflow::map::Message::new(upper)
                .with_keys(input.keys)
                .with_tags(vec!["mapped".to_string()]),
        ]
    }
}

struct NackingMapper;

#[tonic::async_trait]
impl numaflow::map::Mapper for NackingMapper {
    async fn map(&self, _input: numaflow::map::MapRequest) -> Vec<numaflow::map::Message> {
        vec![numaflow::map::Message::message_to_nack(None)]
    }
}

#[tokio::test]
async fn map_unary_echoes_uppercased() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("map.sock");
    let info = tmp.path().join("map-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::map::Server::new(UpperMapper)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("map server failed");
    });
    wait_for_socket(&sock).await;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &[
                "map",
                "--socket",
                &sock_str,
                "--payload",
                "hello",
                "--key",
                "k1",
            ],
            None,
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("handshake ok"), "stdout={stdout}");
    assert!(
        stdout.contains("HELLO"),
        "expected uppercased payload, stdout={stdout}"
    );
    assert!(stdout.contains("mapped"), "expected tag, stdout={stdout}");
}

#[tokio::test]
async fn map_json_output() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("map.sock");
    let info = tmp.path().join("map-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::map::Server::new(UpperMapper)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("map server failed");
    });
    wait_for_socket(&sock).await;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, _stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &[
                "map",
                "--socket",
                &sock_str,
                "--payload",
                "abc",
                "-o",
                "json",
            ],
            None,
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 0);
    // Find the result line and parse it.
    let result_line = stdout
        .lines()
        .find(|l| l.contains("\"type\":\"result\""))
        .expect("a result JSON line");
    let v: serde_json::Value = serde_json::from_str(result_line).unwrap();
    assert_eq!(
        v.get("payload").and_then(serde_json::Value::as_str),
        Some("ABC")
    );
    assert_eq!(
        v.get("tags")
            .and_then(serde_json::Value::as_array)
            .and_then(|tags| tags.first())
            .and_then(serde_json::Value::as_str),
        Some("mapped")
    );
}

#[tokio::test]
async fn map_nack_exits_4() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("map.sock");
    let info = tmp.path().join("map-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::map::Server::new(NackingMapper)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("map server failed");
    });
    wait_for_socket(&sock).await;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &["map", "--socket", &sock_str, "--payload", "nack-me"],
            None,
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 4, "stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("NACKED"), "stdout={stdout}");
    assert!(stdout.contains("nacked=1"), "stdout={stdout}");
}

// ---- sink ----

struct CountingSink;

#[tonic::async_trait]
impl numaflow::sink::Sinker for CountingSink {
    async fn sink(
        &self,
        mut input: tokio::sync::mpsc::Receiver<numaflow::sink::SinkRequest>,
    ) -> Vec<numaflow::sink::Response> {
        let mut responses = Vec::new();
        while let Some(req) = input.recv().await {
            // Fail anything whose payload is "poison", succeed the rest.
            if req.value == b"poison" {
                responses.push(numaflow::sink::Response::failure(
                    req.id,
                    "poisoned".to_string(),
                ));
            } else {
                responses.push(numaflow::sink::Response::ok(req.id));
            }
        }
        responses
    }
}

#[tokio::test]
async fn sink_reports_success() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("sink.sock");
    let info = tmp.path().join("sink-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::sink::Server::new(CountingSink)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("sink server failed");
    });
    wait_for_socket(&sock).await;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &[
                "sink",
                "--socket",
                &sock_str,
                "--payload",
                "ok-msg",
                "--id",
                "m1",
            ],
            None,
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("SUCCESS"), "stdout={stdout}");
    assert!(stdout.contains("success=1"), "stdout={stdout}");
}

#[tokio::test]
async fn sink_failure_exits_4() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("sink.sock");
    let info = tmp.path().join("sink-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::sink::Server::new(CountingSink)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("sink server failed");
    });
    wait_for_socket(&sock).await;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &[
                "sink",
                "--socket",
                &sock_str,
                "--payload",
                "poison",
                "--id",
                "m1",
            ],
            None,
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(
        code, 4,
        "expected exit 4 on sink failure; stdout={stdout}\nstderr={stderr}"
    );
    assert!(stdout.contains("FAILURE"), "stdout={stdout}");
}

#[tokio::test]
async fn map_raw_output_is_payload_only() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("map.sock");
    let info = tmp.path().join("map-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::map::Server::new(UpperMapper)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("map server failed");
    });
    wait_for_socket(&sock).await;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, _stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &[
                "map",
                "--socket",
                &sock_str,
                "--payload",
                "raw-me",
                "-o",
                "raw",
            ],
            None,
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 0);
    // raw mode writes only the response payload bytes to stdout — no headers/summary.
    assert_eq!(stdout, "RAW-ME");
}

// ---- batch map ----

struct EchoBatchMap;

#[tonic::async_trait]
impl numaflow::batchmap::BatchMapper for EchoBatchMap {
    async fn batchmap(
        &self,
        mut input: tokio::sync::mpsc::Receiver<numaflow::batchmap::Datum>,
    ) -> Vec<numaflow::batchmap::BatchResponse> {
        let mut responses = Vec::new();
        while let Some(datum) = input.recv().await {
            let mut r = numaflow::batchmap::BatchResponse::from_id(datum.id);
            r.append(numaflow::batchmap::Message {
                keys: Some(datum.keys),
                value: datum.value.to_ascii_uppercase(),
                tags: None,
                nack_options: None,
            });
            responses.push(r);
        }
        responses
    }
}

#[tokio::test]
async fn map_batch_mode_eot_choreography() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("batchmap.sock");
    let info = tmp.path().join("batchmap-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::batchmap::Server::new(EchoBatchMap)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("batch map server failed");
    });
    wait_for_socket(&sock).await;

    let yaml = "payload: a\n---\npayload: b\n---\npayload: c\n";
    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &[
                "map", "--mode", "batch", "--socket", &sock_str, "-f", "-", "-v",
            ],
            Some(yaml),
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    for expected in ["A", "B", "C"] {
        assert!(
            stdout.contains(expected),
            "missing {expected}; stdout={stdout}"
        );
    }
    // verbose EOT trace goes to stderr
    assert!(
        stderr.contains("EOT"),
        "expected EOT trace; stderr={stderr}"
    );
}

#[tokio::test]
async fn map_mode_mismatch_timeout_mentions_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("batchmap.sock");
    let info = tmp.path().join("batchmap-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::batchmap::Server::new(EchoBatchMap)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("batch map server failed");
    });
    wait_for_socket(&sock).await;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, _stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &[
                "map",
                "--socket",
                &sock_str,
                "--payload",
                "x",
                "--timeout",
                "2s",
            ],
            None,
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 3, "stderr={stderr}");
    assert!(stderr.contains("--mode"), "stderr={stderr}");
}

// ---- stream map ----

struct SplitStream;

#[tonic::async_trait]
impl numaflow::mapstream::MapStreamer for SplitStream {
    async fn map_stream(
        &self,
        input: numaflow::mapstream::MapStreamRequest,
        tx: tokio::sync::mpsc::Sender<numaflow::mapstream::Message>,
    ) {
        let s = String::from_utf8(input.value).unwrap_or_default();
        for part in s.split(',') {
            let msg = numaflow::mapstream::Message::new(part.as_bytes().to_vec())
                .with_keys(input.keys.clone());
            if tx.send(msg).await.is_err() {
                break;
            }
        }
    }
}

#[tokio::test]
async fn map_stream_mode_multiple_results_per_input() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("mapstream.sock");
    let info = tmp.path().join("mapstream-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::mapstream::Server::new(SplitStream)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("mapstream server failed");
    });
    wait_for_socket(&sock).await;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &[
                "map",
                "--mode",
                "stream",
                "--socket",
                &sock_str,
                "--payload",
                "x,y,z",
            ],
            None,
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    for part in ["x", "y", "z"] {
        assert!(
            stdout.contains(&format!("payload={part}")),
            "missing {part}; stdout={stdout}"
        );
    }
}

// ---- transform ----

struct ShiftTransformer;

#[tonic::async_trait]
impl numaflow::sourcetransform::SourceTransformer for ShiftTransformer {
    async fn transform(
        &self,
        input: numaflow::sourcetransform::SourceTransformRequest,
    ) -> Vec<numaflow::sourcetransform::Message> {
        // Re-assign a fixed, well-known event time so we can assert it in output.
        let et = chrono::DateTime::parse_from_rfc3339("2030-01-02T03:04:05Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        vec![numaflow::sourcetransform::Message::new(input.value, et).with_keys(input.keys)]
    }
}

#[tokio::test]
async fn transform_reassigns_event_time() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("st.sock");
    let info = tmp.path().join("st-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::sourcetransform::Server::new(ShiftTransformer)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("transform server failed");
    });
    wait_for_socket(&sock).await;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &["transform", "--socket", &sock_str, "--payload", "x"],
            None,
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("eventTime=2030-01-02"), "stdout={stdout}");
}

// ---- source ----

struct FixedSource;

#[tonic::async_trait]
impl numaflow::source::Sourcer for FixedSource {
    async fn read(
        &self,
        request: numaflow::source::SourceReadRequest,
        transmitter: tokio::sync::mpsc::Sender<numaflow::source::Message>,
    ) {
        for i in 0..request.count {
            let offset = format!("offset-{i}");
            let msg = numaflow::source::Message {
                value: format!("rec-{i}").into_bytes(),
                event_time: chrono::Utc::now(),
                offset: numaflow::source::Offset {
                    offset: offset.into_bytes(),
                    partition_id: 0,
                },
                keys: vec![],
                headers: Default::default(),
                user_metadata: None,
            };
            if transmitter.send(msg).await.is_err() {
                break;
            }
        }
    }

    async fn ack(&self, _offsets: Vec<numaflow::source::Offset>) {}

    async fn nack(&self, _offsets: Vec<numaflow::source::NackOffset>) {}

    async fn pending(&self) -> Option<usize> {
        Some(7)
    }

    async fn partitions(&self) -> Option<Vec<i32>> {
        Some(vec![0])
    }
}

struct PanickingSource;

#[tonic::async_trait]
impl numaflow::source::Sourcer for PanickingSource {
    async fn read(
        &self,
        _request: numaflow::source::SourceReadRequest,
        _transmitter: tokio::sync::mpsc::Sender<numaflow::source::Message>,
    ) {
        panic!("source read exploded");
    }

    async fn ack(&self, _offsets: Vec<numaflow::source::Offset>) {}

    async fn nack(&self, _offsets: Vec<numaflow::source::NackOffset>) {}

    async fn pending(&self) -> Option<usize> {
        Some(0)
    }
}

#[tokio::test]
async fn source_reads_and_acks() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("source.sock");
    let info = tmp.path().join("source-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::source::Server::new(FixedSource)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("source server failed");
    });
    wait_for_socket(&sock).await;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &["source", "--socket", &sock_str, "--count", "3", "--pending"],
            None,
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("pending=7"), "stdout={stdout}");
    assert!(stdout.contains("rec-0"), "stdout={stdout}");
    assert!(
        stdout.contains("3 msg"),
        "expected 3 messages, stdout={stdout}"
    );
    assert!(stdout.contains("acked 3"), "stdout={stdout}");
}

#[tokio::test]
async fn source_uncaught_udf_exception_shows_banner() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("source.sock");
    let info = tmp.path().join("source-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::source::Server::new(PanickingSource)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("source server failed");
    });
    wait_for_socket(&sock).await;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, _stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(&["source", "--socket", &sock_str, "--count", "1"], None)
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 3, "stderr={stderr}");
    assert!(
        stderr.contains("Uncaught Error/Exception from UDF:"),
        "stderr={stderr}"
    );
}

// ---- side-input ----

struct FixedSideInput;

#[tonic::async_trait]
impl numaflow::sideinput::SideInputer for FixedSideInput {
    async fn retrieve_sideinput(&self) -> Option<Vec<u8>> {
        Some(b"side-value".to_vec())
    }
}

#[tokio::test]
async fn side_input_retrieve() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("si.sock");
    let info = tmp.path().join("si-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::sideinput::Server::new(FixedSideInput)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("side input server failed");
    });
    wait_for_socket(&sock).await;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(&["side-input", "--socket", &sock_str], None)
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("broadcast=true"), "stdout={stdout}");
    assert!(stdout.contains("side-value"), "stdout={stdout}");
}

// ---- uncaught UDF exception (gRPC error mid-stream) ----

struct PanickingMapper;

#[tonic::async_trait]
impl numaflow::map::Mapper for PanickingMapper {
    async fn map(&self, _input: numaflow::map::MapRequest) -> Vec<numaflow::map::Message> {
        panic!("poison-pill message rejected");
    }
}

#[tokio::test]
async fn uncaught_udf_exception_shows_banner() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("map.sock");
    let info = tmp.path().join("map-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::map::Server::new(PanickingMapper)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("map server failed");
    });
    wait_for_socket(&sock).await;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, _stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(&["map", "--socket", &sock_str, "--payload", "boom"], None)
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    // Protocol error (mid-stream gRPC failure) → exit 3.
    assert_eq!(code, 3, "stderr={stderr}");
    // Dedicated banner (stderr is piped here, so no ANSI colour codes).
    assert!(
        stderr.contains("Uncaught Error/Exception from UDF:"),
        "stderr={stderr}"
    );
    // No raw gRPC status leaks in non-verbose mode.
    assert!(!stderr.contains("status:"), "stderr={stderr}");
}

#[tokio::test]
async fn uncaught_udf_exception_verbose_shows_status() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("map.sock");
    let info = tmp.path().join("map-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::map::Server::new(PanickingMapper)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("map server failed");
    });
    wait_for_socket(&sock).await;

    let sock_str = sock.to_string_lossy().to_string();
    let (code, _stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &["map", "--socket", &sock_str, "--payload", "boom", "-v"],
            None,
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 3, "stderr={stderr}");
    assert!(
        stderr.contains("Uncaught Error/Exception from UDF:"),
        "stderr={stderr}"
    );
    // Verbose mode surfaces the raw gRPC status.
    assert!(stderr.contains("status:"), "stderr={stderr}");
}

// ---- usage errors ----

#[test]
fn missing_connection_is_usage_error() {
    let (code, _out, err) = run_nfcli(&["map", "--payload", "x"], None);
    assert_eq!(code, 1, "stderr={err}");
    assert!(
        err.contains("--socket") || err.contains("--tcp"),
        "stderr={err}"
    );
}

#[test]
fn missing_payload_is_usage_error() {
    let (code, _out, err) = run_nfcli(&["map", "--socket", "/nonexistent.sock"], None);
    // No payload and no file → usage error (exit 1), evaluated before connecting.
    assert_eq!(code, 1, "stderr={err}");
}

#[test]
fn clap_errors_exit_1_but_help_exits_0() {
    let (code, _out, err) = run_nfcli(&["map", "--bogus-flag"], None);
    assert_eq!(code, 1, "stderr={err}");

    let (code, _out, err) = run_nfcli(&[], None);
    assert_eq!(code, 1, "stderr={err}");

    let (code, _out, err) = run_nfcli(&["--help"], None);
    assert_eq!(code, 0, "stderr={err}");

    let (code, _out, err) = run_nfcli(&["map", "--help"], None);
    assert_eq!(code, 0, "stderr={err}");
}

#[test]
fn both_connections_is_usage_error_from_target_resolve() {
    let (code, _out, err) = run_nfcli(
        &["map", "--socket", "/x", "--tcp", "50051", "--payload", "x"],
        None,
    );
    assert_eq!(code, 1, "stderr={err}");
    assert!(err.contains("not both"), "stderr={err}");
}

#[test]
fn global_flags_parse_before_and_after_subcommand() {
    let (code, _out, err) = run_nfcli(
        &[
            "-v",
            "map",
            "--socket",
            "/tmp/nfcli-no-such.sock",
            "--payload",
            "x",
            "--timeout",
            "1ms",
        ],
        None,
    );
    assert_eq!(code, 2, "stderr={err}");
}

#[test]
fn file_mode_rejects_inline_message_fields_before_connecting() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    writeln!(file, "payload: hi").unwrap();
    let path = file.path().to_string_lossy().to_string();

    let (code, _out, err) = run_nfcli(
        &[
            "map",
            "-f",
            &path,
            "--key",
            "foo",
            "--socket",
            "/tmp/nfcli-no-such.sock",
        ],
        None,
    );
    assert_eq!(code, 1, "stderr={err}");
    assert!(err.contains("--key"), "stderr={err}");
}

#[test]
fn completions_prints_script() {
    let (code, out, err) = run_nfcli(&["completions", "fish"], None);
    assert_eq!(code, 0, "stderr={err}");
    assert!(
        out.contains("complete") || out.contains("nfcli"),
        "stdout={out}"
    );
}

#[tokio::test]
async fn wrong_service_is_ready_fails_fast_with_hint() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("sink.sock");
    let info = tmp.path().join("sink-server-info");
    let (tx, rx) = oneshot::channel();

    let s = sock.clone();
    let i = info.clone();
    let server = tokio::spawn(async move {
        numaflow::sink::Server::new(CountingSink)
            .with_socket_file(s)
            .with_server_info_file(i)
            .start_with_shutdown(rx)
            .await
            .expect("sink server failed");
    });
    wait_for_socket(&sock).await;

    let sock_str = sock.to_string_lossy().to_string();
    let start = std::time::Instant::now();
    let (code, _stdout, stderr) = tokio::task::spawn_blocking(move || {
        run_nfcli(
            &[
                "map",
                "--socket",
                &sock_str,
                "--payload",
                "x",
                "--timeout",
                "10s",
            ],
            None,
        )
    })
    .await
    .unwrap();

    let _ = tx.send(());
    let _ = server.await;

    assert_eq!(code, 2, "stderr={stderr}");
    assert!(
        start.elapsed() < std::time::Duration::from_secs(3),
        "took {:?}; stderr={stderr}",
        start.elapsed()
    );
    assert!(
        stderr.contains("subcommand") || stderr.contains("socket"),
        "stderr={stderr}"
    );
}
