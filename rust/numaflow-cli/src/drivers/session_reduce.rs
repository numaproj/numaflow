//! Session reduce driver: a single multiplexed `SessionReduceFn` stream. The CLI ports numa's
//! session windower, emitting explicit keyed-window operations (OPEN / APPEND / EXPAND / MERGE)
//! per message and an explicit CLOSE for every still-open session at end of input.

use std::time::{Duration, Instant};

use anyhow::anyhow;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use numaflow_pb::clients::sessionreduce::session_reduce_client::SessionReduceClient;
use numaflow_pb::clients::sessionreduce::session_reduce_request::window_operation::Event;
use numaflow_pb::clients::sessionreduce::session_reduce_request::{Payload, WindowOperation};
use numaflow_pb::clients::sessionreduce::{
    KeyedWindow, SessionReduceRequest, SessionReduceResponse,
};

use crate::cli::{DataArgs, OutputOpts, ServiceKind, TuningOpts};
use crate::drivers::{Pacer, connect_and_ready, next_with_timeout, send_with_timeout};
use crate::exit::{CliError, CliResult, ExitCode};
use crate::input::{LoadOptions, load_messages, resolve_base_time};
use crate::message::{Message, to_timestamp};
use crate::output::{Reporter, ResultContext, ResultRecord};
use crate::windower::{WindowMs, event_ms, session_window};

/// The gap comes from the session-reduce args; shared data flags are flattened in `DataArgs`.
pub async fn run(
    args: DataArgs,
    gap: Duration,
    tuning: &TuningOpts,
    output: &OutputOpts,
) -> CliResult<()> {
    let base = resolve_base_time(&args.input, chrono::Utc::now()).map_err(CliError::usage)?;
    let messages = load_messages(&args.input, base, LoadOptions { ignores_id: true })
        .map_err(CliError::usage)?;
    let gap_ms = gap.as_millis() as i64;

    let mut reporter = Reporter::new(output);
    let (target, channel) =
        connect_and_ready(&args.connect, ServiceKind::SessionReduce, tuning).await?;

    let mut client = SessionReduceClient::new(channel)
        .max_encoding_message_size(tuning.max_message_size)
        .max_decoding_message_size(tuning.max_message_size);

    let (tx, rx) = mpsc::channel::<SessionReduceRequest>(256);
    let mut stream = client
        .session_reduce_fn(ReceiverStream::new(rx))
        .await
        .map_err(|e| CliError::new(ExitCode::Connect, anyhow!("SessionReduceFn failed: {e}")))?
        .into_inner();
    reporter.status(format!("✓ ready ({})", target.label()));

    // Active sessions per key. Keys are joined for identity (numa combines keys with a
    // delimiter; for the CLI, the Vec<String> identity is sufficient since we key the map on it).
    let mut sessions: std::collections::HashMap<Vec<String>, Vec<WindowMs>> =
        std::collections::HashMap::new();

    let start = Instant::now();
    let mut sent = 0usize;
    let mut pacer = Pacer::new(&args.pacing);

    for m in &messages {
        let et = event_ms(m);
        let incoming = session_window(et, gap_ms);
        let key = m.keys.clone();
        let active = sessions.entry(key.clone()).or_default();

        // Partition active windows into those overlapping the incoming window and the rest.
        let (old_windows, mut new_active): (Vec<WindowMs>, Vec<WindowMs>) =
            active.iter().partition(|w| w.overlaps(&incoming));

        if old_windows.is_empty() {
            // New session → OPEN.
            new_active.push(incoming);
            *active = new_active;
            send_with_timeout(&tx, open_req(m, &incoming), tuning.timeout)
                .await
                .map_err(CliError::protocol)?;
            reporter.trace(format!("OPEN {:?} {}", key, incoming));
        } else {
            // Merge all overlapping windows (and the incoming) into one.
            let merged = old_windows.iter().fold(incoming, |acc, w| WindowMs {
                start: acc.start.min(w.start),
                end: acc.end.max(w.end),
            });

            if old_windows.len() >= 2 {
                // MERGE the existing overlapping windows first (no payload), then place the msg.
                let merge_windows: Vec<KeyedWindow> =
                    old_windows.iter().map(|w| keyed_window(w, &key)).collect();
                send_with_timeout(&tx, merge_req(merge_windows), tuning.timeout)
                    .await
                    .map_err(CliError::protocol)?;
                reporter.trace(format!("MERGE {:?} {} windows", key, old_windows.len()));
            }

            // Decide APPEND vs EXPAND: if the incoming is fully contained in the (single) existing
            // window and no boundary changed, it's an APPEND; otherwise EXPAND to merged bounds.
            let single_existing = if old_windows.len() == 1 {
                old_windows.first().copied()
            } else {
                None
            };
            let contained = single_existing
                .map(|ex| incoming.start >= ex.start && incoming.end <= ex.end)
                .unwrap_or(false);

            if contained {
                let ex = single_existing.expect("single");
                new_active.push(ex);
                send_with_timeout(&tx, append_req(m, &ex), tuning.timeout)
                    .await
                    .map_err(CliError::protocol)?;
                reporter.trace(format!("APPEND {:?} {}", key, ex));
            } else {
                new_active.push(merged);
                // EXPAND carries [old, new]; after a MERGE the "old" is the merged-from set's union.
                let old_for_expand = single_existing.unwrap_or_else(|| union(&old_windows));
                send_with_timeout(
                    &tx,
                    expand_req(m, &old_for_expand, &merged, &key),
                    tuning.timeout,
                )
                .await
                .map_err(CliError::protocol)?;
                reporter.trace(format!("EXPAND {:?} → {}", key, merged));
            }
            *active = new_active;
        }

        sent += 1;
        pacer.tick().await;
    }

    // End of input: explicit CLOSE for every still-open session (no payload).
    for (key, active) in &sessions {
        for w in active {
            send_with_timeout(&tx, close_req(w, key), tuning.timeout)
                .await
                .map_err(CliError::protocol)?;
            reporter.trace(format!("CLOSE {:?} {}", key, w));
        }
    }
    drop(tx);

    // Read responses until the stream ends, grouping by keyed window.
    read_responses(&mut stream, &mut reporter, tuning.timeout).await?;

    reporter.tally.sent = sent;
    let session_count: usize = sessions.values().map(|v| v.len()).sum();
    reporter.summary(&[
        ("sessions", session_count.to_string()),
        ("elapsed", format!("{}ms", start.elapsed().as_millis())),
    ]);
    reporter.exit_result()
}

fn union(ws: &[WindowMs]) -> WindowMs {
    let start = ws.iter().map(|w| w.start).min().unwrap_or(0);
    let end = ws.iter().map(|w| w.end).max().unwrap_or(0);
    WindowMs { start, end }
}

fn keyed_window(w: &WindowMs, keys: &[String]) -> KeyedWindow {
    KeyedWindow {
        start: w.start_dt().map(to_timestamp),
        end: w.end_dt().map(to_timestamp),
        slot: "0".to_string(),
        keys: keys.to_vec(),
    }
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

fn open_req(m: &Message, w: &WindowMs) -> SessionReduceRequest {
    SessionReduceRequest {
        payload: Some(payload_of(m)),
        operation: Some(WindowOperation {
            event: Event::Open as i32,
            keyed_windows: vec![keyed_window(w, &m.keys)],
        }),
    }
}

fn append_req(m: &Message, w: &WindowMs) -> SessionReduceRequest {
    SessionReduceRequest {
        payload: Some(payload_of(m)),
        operation: Some(WindowOperation {
            event: Event::Append as i32,
            keyed_windows: vec![keyed_window(w, &m.keys)],
        }),
    }
}

fn expand_req(
    m: &Message,
    old: &WindowMs,
    new: &WindowMs,
    keys: &[String],
) -> SessionReduceRequest {
    SessionReduceRequest {
        payload: Some(payload_of(m)),
        operation: Some(WindowOperation {
            event: Event::Expand as i32,
            keyed_windows: vec![keyed_window(old, keys), keyed_window(new, keys)],
        }),
    }
}

fn merge_req(windows: Vec<KeyedWindow>) -> SessionReduceRequest {
    SessionReduceRequest {
        payload: None,
        operation: Some(WindowOperation {
            event: Event::Merge as i32,
            keyed_windows: windows,
        }),
    }
}

fn close_req(w: &WindowMs, keys: &[String]) -> SessionReduceRequest {
    SessionReduceRequest {
        payload: None,
        operation: Some(WindowOperation {
            event: Event::Close as i32,
            keyed_windows: vec![keyed_window(w, keys)],
        }),
    }
}

async fn read_responses(
    stream: &mut tonic::Streaming<SessionReduceResponse>,
    reporter: &mut Reporter,
    timeout: Duration,
) -> CliResult<()> {
    loop {
        let resp = next_with_timeout(stream, timeout)
            .await
            .map_err(CliError::protocol)?;
        let Some(resp) = resp else { break };
        if resp.eof {
            continue;
        }
        let win = resp
            .keyed_window
            .as_ref()
            .map(fmt_keyed)
            .unwrap_or_else(|| "?".to_string());
        if let Some(r) = resp.result {
            let keys = r.keys.clone();
            reporter.group_header(format!("session {} keys={:?}", win, keys));
            let rec = ResultRecord {
                keys: r.keys,
                tags: r.tags,
                value: r.value,
                ..Default::default()
            };
            reporter.result(&rec, 1, &ResultContext::window(win));
        }
    }
    Ok(())
}

fn fmt_keyed(w: &KeyedWindow) -> String {
    let f = |t: &Option<prost_types::Timestamp>| {
        t.as_ref()
            .and_then(crate::message::from_timestamp)
            .map(|d| d.to_rfc3339())
            .unwrap_or_else(|| "?".to_string())
    };
    format!("[{} → {})", f(&w.start), f(&w.end))
}
