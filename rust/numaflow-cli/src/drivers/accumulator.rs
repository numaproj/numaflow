//! Accumulator driver: a single `AccumulateFn` bidi stream. Per key: OPEN (payload +
//! KeyedWindow) for the first message, APPEND for the rest (note: accumulator APPEND=2, not 4),
//! and an explicit CLOSE (no payload) per key at end of input. The window start is the first
//! message's event time and the end is +infinity (numa's unaligned accumulator windower); the
//! server echoes each payload back with its id.

use std::time::{Duration, Instant};

use anyhow::anyhow;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use numaflow_pb::clients::accumulator::accumulator_client::AccumulatorClient;
use numaflow_pb::clients::accumulator::accumulator_request::WindowOperation;
use numaflow_pb::clients::accumulator::accumulator_request::window_operation::Event;
use numaflow_pb::clients::accumulator::{
    AccumulatorRequest, AccumulatorResponse, KeyedWindow, Payload,
};

use crate::cli::{DataArgs, OutputOpts, ServiceKind, TuningOpts};
use crate::drivers::{Pacer, connect_and_ready, next_with_timeout, send_with_timeout};
use crate::exit::{CliError, CliResult, ExitCode};
use crate::input::{LoadOptions, load_messages, resolve_base_time};
use crate::message::{Message, from_timestamp, to_timestamp};
use crate::output::{Reporter, ResultContext, ResultRecord};

/// Far-future window end, standing in for numa's `DateTime::<Utc>::MAX_UTC` at OPEN time.
fn max_end() -> prost_types::Timestamp {
    to_timestamp(chrono::DateTime::<chrono::Utc>::MAX_UTC)
}

fn payload_of(m: &Message) -> Payload {
    Payload {
        keys: m.keys.clone(),
        value: m.value.clone(),
        event_time: Some(m.event_time_pb()),
        watermark: Some(m.watermark_pb()),
        id: m.id.clone(),
        headers: m.headers.clone(),
        metadata: m.metadata(),
    }
}

fn keyed_window(m: &Message) -> KeyedWindow {
    KeyedWindow {
        start: Some(m.event_time_pb()),
        end: Some(max_end()),
        slot: "0".to_string(),
        keys: m.keys.clone(),
    }
}

pub async fn run(args: DataArgs, tuning: &TuningOpts, output: &OutputOpts) -> CliResult<()> {
    let base = resolve_base_time(&args.input, chrono::Utc::now()).map_err(CliError::usage)?;
    let messages =
        load_messages(&args.input, base, LoadOptions::default()).map_err(CliError::usage)?;

    let mut reporter = Reporter::new(output);
    let (target, channel) =
        connect_and_ready(&args.connect, ServiceKind::Accumulator, tuning).await?;

    let mut client = AccumulatorClient::new(channel)
        .max_encoding_message_size(tuning.max_message_size)
        .max_decoding_message_size(tuning.max_message_size);

    let (tx, rx) = mpsc::channel::<AccumulatorRequest>(256);
    let mut stream = client
        .accumulate_fn(ReceiverStream::new(rx))
        .await
        .map_err(|e| CliError::new(ExitCode::Connect, anyhow!("AccumulateFn failed: {e}")))?
        .into_inner();
    reporter.status(format!("✓ ready ({})", target.label()));

    // One global window per key; OPEN on the first message for that key.
    let mut opened: std::collections::HashMap<Vec<String>, KeyedWindow> =
        std::collections::HashMap::new();

    let start = Instant::now();
    let mut sent = 0usize;
    let mut pacer = Pacer::new(&args.pacing);

    for m in &messages {
        let key = m.keys.clone();
        let (event, window) = match opened.entry(key.clone()) {
            std::collections::hash_map::Entry::Vacant(e) => {
                (Event::Open, e.insert(keyed_window(m)).clone())
            }
            std::collections::hash_map::Entry::Occupied(e) => (Event::Append, e.get().clone()),
        };

        send_with_timeout(
            &tx,
            AccumulatorRequest {
                payload: Some(payload_of(m)),
                operation: Some(WindowOperation {
                    event: event as i32,
                    keyed_window: Some(window),
                }),
            },
            tuning.timeout,
        )
        .await
        .map_err(CliError::protocol)?;
        reporter.trace(format!("{:?} {:?}", event, key));

        sent += 1;
        pacer.tick().await;
    }

    // End of input: explicit CLOSE (no payload) per key.
    for (key, window) in &opened {
        send_with_timeout(
            &tx,
            AccumulatorRequest {
                payload: None,
                operation: Some(WindowOperation {
                    event: Event::Close as i32,
                    keyed_window: Some(window.clone()),
                }),
            },
            tuning.timeout,
        )
        .await
        .map_err(CliError::protocol)?;
        reporter.trace(format!("CLOSE {:?}", key));
    }
    drop(tx);

    // Read responses until stream end, printing each payload (incl. id, event time, tags).
    read_responses(&mut stream, &mut reporter, tuning.timeout).await?;

    reporter.tally.sent = sent;
    reporter.summary(&[
        ("keys", opened.len().to_string()),
        ("elapsed", format!("{}ms", start.elapsed().as_millis())),
    ]);
    reporter.exit_result()
}

async fn read_responses(
    stream: &mut tonic::Streaming<AccumulatorResponse>,
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
        if let Some(p) = resp.payload {
            let rec = ResultRecord {
                keys: p.keys.clone(),
                tags: resp.tags.clone(),
                value: p.value,
                event_time: p.event_time.as_ref().and_then(from_timestamp),
                id: Some(p.id.clone()),
                ..Default::default()
            };
            reporter.group_header(format!("accumulated keys={:?}", p.keys));
            reporter.result(&rec, 1, &ResultContext::id(p.id));
        }
    }
    Ok(())
}
