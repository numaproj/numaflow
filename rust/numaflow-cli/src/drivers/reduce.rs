//! Aligned reduce driver (fixed & sliding windows). The CLI emulates numa's windower: each
//! message is assigned to its epoch-aligned window(s); each window gets a dedicated `ReduceFn`
//! bidi stream (OPEN + APPEND …), and end-of-input closes every window by closing its request
//! stream (for aligned reduce, stream-close *is* the CLOSE signal).

use std::time::{Duration, Instant};

use anyhow::anyhow;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use numaflow_pb::clients::reduce::reduce_client::ReduceClient;
use numaflow_pb::clients::reduce::reduce_request::window_operation::Event;
use numaflow_pb::clients::reduce::reduce_request::{Payload, WindowOperation};
use numaflow_pb::clients::reduce::{ReduceRequest, Window};

use crate::cli::{OutputOpts, ReduceArgs, ServiceKind, TuningOpts, WindowKind};
use crate::drivers::{Pacer, connect_and_ready, next_with_timeout, send_with_timeout};
use crate::exit::{CliError, CliResult, ExitCode};
use crate::input::{LoadOptions, load_messages, resolve_base_time};
use crate::message::{Message, to_timestamp};
use crate::output::{Reporter, ResultContext, ResultRecord};
use crate::windower::{WindowMs, event_ms, fixed_window, sliding_windows};

/// Build the epoch-truncated fallback base time for the reduce family: now, truncated down to
/// the window boundary so `+0s..+59s` land in one window.
fn reduce_base_fallback(length: Duration) -> chrono::DateTime<chrono::Utc> {
    let now = chrono::Utc::now().timestamp_millis();
    let len = length.as_millis().max(1) as i64;
    let truncated = now.div_euclid(len) * len;
    chrono::DateTime::from_timestamp_millis(truncated).unwrap_or_else(chrono::Utc::now)
}

fn payload_of(m: &Message) -> Payload {
    Payload {
        keys: m.keys.clone(),
        value: m.value.clone(),
        event_time: Some(m.event_time_pb()),
        watermark: Some(m.watermark_pb()),
        headers: m.headers.clone(),
        metadata: m.metadata(),
    }
}

fn window_pb(w: &WindowMs) -> Window {
    Window {
        start: w.start_dt().map(to_timestamp),
        end: w.end_dt().map(to_timestamp),
        slot: "0".to_string(),
    }
}

fn open_request(m: &Message, w: &WindowMs) -> ReduceRequest {
    ReduceRequest {
        payload: Some(payload_of(m)),
        operation: Some(WindowOperation {
            event: Event::Open as i32,
            windows: vec![window_pb(w)],
        }),
    }
}

fn append_request(m: &Message, w: &WindowMs) -> ReduceRequest {
    ReduceRequest {
        payload: Some(payload_of(m)),
        operation: Some(WindowOperation {
            event: Event::Append as i32,
            windows: vec![window_pb(w)],
        }),
    }
}

pub async fn run(args: ReduceArgs, tuning: &TuningOpts, output: &OutputOpts) -> CliResult<()> {
    // Validate window options.
    if args.window == WindowKind::Sliding && args.slide.is_none() {
        return Err(CliError::usage(anyhow!(
            "--slide is required for sliding windows"
        )));
    }
    if args.window == WindowKind::Fixed && args.slide.is_some() {
        return Err(CliError::usage(anyhow!(
            "--slide only applies to sliding windows"
        )));
    }

    let base = resolve_base_time(&args.input, reduce_base_fallback(args.length))
        .map_err(CliError::usage)?;
    let messages = load_messages(&args.input, base, LoadOptions { ignores_id: true })
        .map_err(CliError::usage)?;

    let length_ms = args.length.as_millis() as i64;

    // Assign each message to its window(s), preserving file order per window.
    // BTreeMap keeps windows sorted by (start, end) for deterministic output.
    let mut windows: std::collections::BTreeMap<WindowMs, Vec<Message>> =
        std::collections::BTreeMap::new();
    for m in &messages {
        let et = event_ms(m);
        let assigned = match args.window {
            WindowKind::Fixed => vec![fixed_window(et, length_ms)],
            WindowKind::Sliding => {
                let slide_ms = args.slide.expect("validated").as_millis() as i64;
                sliding_windows(et, length_ms, slide_ms)
            }
        };
        for w in assigned {
            windows.entry(w).or_default().push(m.clone());
        }
    }

    let mut reporter = Reporter::new(output);
    let (target, channel) = connect_and_ready(&args.connect, ServiceKind::Reduce, tuning).await?;
    reporter.status(format!("✓ ready ({})", target.label()));

    let start = Instant::now();
    let mut sent_total = 0usize;
    let mut pacer = Pacer::new(&args.pacing);

    for (w, msgs) in &windows {
        sent_total += drive_window(&channel, tuning, w, msgs, &mut reporter, &mut pacer).await?;
    }

    reporter.tally.sent = sent_total;
    reporter.summary(&[
        ("windows", windows.len().to_string()),
        ("elapsed", format!("{}ms", start.elapsed().as_millis())),
    ]);
    reporter.exit_result()
}

async fn drive_window(
    channel: &Channel,
    tuning: &TuningOpts,
    w: &WindowMs,
    msgs: &[Message],
    reporter: &mut Reporter,
    pacer: &mut Pacer,
) -> CliResult<usize> {
    let mut client = ReduceClient::new(channel.clone())
        .max_encoding_message_size(tuning.max_message_size)
        .max_decoding_message_size(tuning.max_message_size);

    let (tx, rx) = mpsc::channel::<ReduceRequest>(256);
    let mut stream = client
        .reduce_fn(ReceiverStream::new(rx))
        .await
        .map_err(|e| CliError::new(ExitCode::Connect, anyhow!("ReduceFn failed: {e}")))?
        .into_inner();

    let win_label = format!("{w}");
    reporter.trace(format!("window {win_label} OPEN"));

    // Send OPEN for the first message, APPEND for the rest.
    let mut sent = 0usize;
    for (i, m) in msgs.iter().enumerate() {
        let req = if i == 0 {
            open_request(m, w)
        } else {
            append_request(m, w)
        };
        send_with_timeout(&tx, req, tuning.timeout)
            .await
            .map_err(CliError::protocol)?;
        sent += 1;
        pacer.tick().await;
    }
    // End of input for this window: closing the stream is the CLOSE signal (aligned reduce).
    drop(tx);
    reporter.trace(format!("window {win_label} CLOSE (stream close)"));

    // Collect this window's results.
    let mut results: Vec<ResultRecord> = Vec::new();
    loop {
        let resp = next_with_timeout(&mut stream, tuning.timeout)
            .await
            .map_err(CliError::protocol)?;
        let Some(resp) = resp else { break };
        if resp.eof {
            break;
        }
        if let Some(r) = resp.result {
            results.push(ResultRecord {
                keys: r.keys,
                tags: r.tags,
                value: r.value,
                ..Default::default()
            });
        }
    }

    reporter.group_header(format!(
        "window {win_label} · {} msg(s) sent · closed",
        msgs.len()
    ));
    for (i, r) in results.iter().enumerate() {
        reporter.result(r, i + 1, &ResultContext::window(win_label.clone()));
    }
    Ok(sent)
}
