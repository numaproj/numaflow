//! Sink driver: handshake once, then per batch send n requests + an EOT request, read
//! responses (correlated by `Result.id`) until the EOT response.

use std::collections::HashSet;
use std::time::Instant;

use anyhow::anyhow;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use numaflow_pb::clients::sink::sink_client::SinkClient;
use numaflow_pb::clients::sink::{
    Handshake, SinkRequest, Status, TransmissionStatus, sink_request,
};

use crate::cli::{DataArgs, OutputFormat, OutputOpts, ServiceKind, TuningOpts};
use crate::drivers::{
    batches, connect_and_ready, maybe_delay, next_with_timeout, send_with_timeout,
};
use crate::exit::{CliError, CliResult, ExitCode};
use crate::input::{LoadOptions, load_messages, resolve_base_time};
use crate::message::Message;
use crate::output::{Reporter, render_payload};

fn data_request(m: &Message) -> SinkRequest {
    SinkRequest {
        request: Some(sink_request::Request {
            keys: m.keys.clone(),
            value: m.value.clone(),
            event_time: Some(m.event_time_pb()),
            watermark: Some(m.watermark_pb()),
            id: m.id.clone(),
            headers: m.headers.clone(),
            metadata: m.metadata(),
        }),
        status: None,
        handshake: None,
    }
}

fn handshake_request() -> SinkRequest {
    SinkRequest {
        request: None,
        status: None,
        handshake: Some(Handshake { sot: true }),
    }
}

fn eot_request() -> SinkRequest {
    SinkRequest {
        request: None,
        status: Some(TransmissionStatus { eot: true }),
        handshake: None,
    }
}

pub async fn run(args: DataArgs, tuning: &TuningOpts, output: &OutputOpts) -> CliResult<()> {
    let base = resolve_base_time(&args.input, chrono::Utc::now()).map_err(CliError::usage)?;
    let messages =
        load_messages(&args.input, base, LoadOptions::default()).map_err(CliError::usage)?;

    let mut reporter = Reporter::new(output);
    let (target, channel) = connect_and_ready(&args.connect, ServiceKind::Sink, tuning).await?;

    let mut client = SinkClient::new(channel)
        .max_encoding_message_size(tuning.max_message_size)
        .max_decoding_message_size(tuning.max_message_size);

    let (tx, rx) = mpsc::channel::<SinkRequest>(256);
    send_with_timeout(&tx, handshake_request(), tuning.timeout)
        .await
        .map_err(CliError::protocol)?;

    let mut stream = client
        .sink_fn(ReceiverStream::new(rx))
        .await
        .map_err(|e| CliError::new(ExitCode::Connect, anyhow!("SinkFn failed: {e}")))?
        .into_inner();

    let hs = next_with_timeout(&mut stream, tuning.timeout)
        .await
        .map_err(CliError::protocol)?
        .ok_or_else(|| CliError::protocol(anyhow!("stream closed before handshake response")))?;
    if hs.handshake.map(|h| h.sot) != Some(true) {
        return Err(CliError::protocol(anyhow!(
            "expected handshake sot=true, got {hs:?}"
        )));
    }
    reporter.status(format!("✓ ready ({}) · handshake ok", target.label()));

    let start = Instant::now();
    let batch_list = batches(&messages, args.pacing.batch_size);
    let total_batches = batch_list.len();
    let mut success = 0usize;
    let mut fallback = 0usize;
    let mut on_success = 0usize;
    let mut serve = 0usize;

    for (bi, batch) in batch_list.iter().enumerate() {
        let mut pending: HashSet<String> = batch.iter().map(|m| m.id.clone()).collect();
        for m in *batch {
            send_with_timeout(&tx, data_request(m), tuning.timeout)
                .await
                .map_err(CliError::protocol)?;
        }
        send_with_timeout(&tx, eot_request(), tuning.timeout)
            .await
            .map_err(CliError::protocol)?;
        reporter.trace("→ EOT");

        let batch_start = Instant::now();
        reporter.group_header(format!(
            "batch {}/{} ({} msgs)",
            bi + 1,
            total_batches,
            batch.len()
        ));

        loop {
            let resp = next_with_timeout(&mut stream, tuning.timeout)
                .await
                .map_err(CliError::protocol)?
                .ok_or_else(|| CliError::protocol(anyhow!("stream closed before EOT response")))?;
            if resp.status.map(|s| s.eot) == Some(true) {
                reporter.trace("← EOT");
                if !pending.is_empty() {
                    return Err(CliError::protocol(anyhow!(
                        "partial response: EOT arrived with {} id(s) unanswered: {:?}",
                        pending.len(),
                        pending
                    )));
                }
                break;
            }
            for result in &resp.results {
                pending.remove(&result.id);
                let status = Status::try_from(result.status).unwrap_or(Status::Failure);
                match status {
                    Status::Success => success += 1,
                    Status::Failure => reporter.tally.failed += 1,
                    Status::Fallback => fallback += 1,
                    Status::OnSuccess => on_success += 1,
                    Status::Serve => serve += 1,
                    Status::Nack => reporter.tally.nacked += 1,
                }
                emit_result(&reporter, result, status);
            }
        }
        reporter.trace(format!(
            "batch {} done in {}ms",
            bi + 1,
            batch_start.elapsed().as_millis()
        ));
        maybe_delay(args.pacing.delay).await;
    }
    drop(tx);

    reporter.tally.sent = messages.len();
    // For sink, "results" counts successful writes for the summary.
    reporter.tally.results = success;
    let mut extra = vec![
        ("success", success.to_string()),
        ("failure", reporter.tally.failed.to_string()),
    ];
    if fallback > 0 {
        extra.push(("fallback", fallback.to_string()));
    }
    if on_success > 0 {
        extra.push(("onSuccess", on_success.to_string()));
    }
    if serve > 0 {
        extra.push(("serve", serve.to_string()));
    }
    extra.push(("elapsed", format!("{}ms", start.elapsed().as_millis())));
    reporter.summary(&extra);
    reporter.exit_result()
}

fn emit_result(
    reporter: &Reporter,
    result: &numaflow_pb::clients::sink::sink_response::Result,
    status: Status,
) {
    if reporter.format == OutputFormat::Json {
        let mut obj = json!({
            "type": "sinkResult",
            "id": result.id,
            "status": status.as_str_name(),
        });
        let map = obj.as_object_mut().expect("object");
        if !result.err_msg.is_empty() {
            map.insert("errMsg".to_string(), json!(result.err_msg));
        }
        if let Some(m) = &result.on_success_msg {
            map.insert(
                "onSuccessPayloadBase64".to_string(),
                json!(base64_of(&m.value)),
            );
        }
        if let Some(d) = result.nack_options.as_ref().and_then(|n| n.delay) {
            map.insert("nackDelayMs".to_string(), json!(d));
        }
        reporter.json_event(&obj);
        return;
    }
    if reporter.format == OutputFormat::Raw || reporter.quiet {
        return;
    }
    let mut line = format!("  {:<16} {}", result.id, status.as_str_name());
    match status {
        Status::Failure if !result.err_msg.is_empty() => {
            line.push_str(&format!("  err=\"{}\"", result.err_msg));
        }
        Status::Failure => {}
        Status::OnSuccess => {
            if let Some(m) = &result.on_success_msg {
                line.push_str(&format!("  onSuccess={}", render_payload(&m.value)));
            }
        }
        Status::Nack => {
            if let Some(n) = &result.nack_options {
                if let Some(d) = n.delay {
                    line.push_str(&format!("  delay={d}ms"));
                }
                if let Some(r) = &n.reason {
                    line.push_str(&format!("  reason={r}"));
                }
            }
        }
        _ => {}
    }
    println!("{line}");
}

fn base64_of(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}
