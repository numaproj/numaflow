//! Map driver: unary, batch, and stream modes over the unified `map.v1.Map/MapFn` bidi RPC.

use std::collections::HashSet;
use std::time::Instant;

use anyhow::anyhow;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use numaflow_pb::clients::map::map_client::MapClient;
use numaflow_pb::clients::map::{
    Handshake, MapRequest, MapResponse, TransmissionStatus, map_request,
};

use crate::cli::{MapArgs, MapMode, OutputOpts, ServiceKind, TuningOpts};
use crate::drivers::{
    ResponseTimeout, batches, connect_and_ready, maybe_delay, next_with_timeout, send_with_timeout,
};
use crate::exit::{CliError, CliResult, ExitCode};
use crate::input::{LoadOptions, load_messages, resolve_base_time};
use crate::message::Message;
use crate::output::{Reporter, ResultContext, ResultRecord};

/// Build a MapRequest carrying a message payload.
fn data_request(m: &Message) -> MapRequest {
    MapRequest {
        request: Some(map_request::Request {
            keys: m.keys.clone(),
            value: m.value.clone(),
            event_time: Some(m.event_time_pb()),
            watermark: Some(m.watermark_pb()),
            headers: m.headers.clone(),
            metadata: m.metadata(),
        }),
        id: m.id.clone(),
        handshake: None,
        status: None,
    }
}

fn handshake_request() -> MapRequest {
    MapRequest {
        request: None,
        id: String::new(),
        handshake: Some(Handshake { sot: true }),
        status: None,
    }
}

fn eot_request() -> MapRequest {
    MapRequest {
        request: None,
        id: String::new(),
        handshake: None,
        status: Some(TransmissionStatus { eot: true }),
    }
}

pub async fn run(args: MapArgs, tuning: &TuningOpts, output: &OutputOpts) -> CliResult<()> {
    let base = resolve_base_time(&args.input, chrono::Utc::now()).map_err(CliError::usage)?;
    let messages =
        load_messages(&args.input, base, LoadOptions::default()).map_err(CliError::usage)?;

    let mut reporter = Reporter::new(output);
    let (target, channel) = connect_and_ready(&args.connect, ServiceKind::Map, tuning).await?;

    let mut client = MapClient::new(channel)
        .max_encoding_message_size(tuning.max_message_size)
        .max_decoding_message_size(tuning.max_message_size);

    // Open the bidi stream; the first request is the handshake.
    let (tx, rx) = mpsc::channel::<MapRequest>(256);
    send_with_timeout(&tx, handshake_request(), tuning.timeout)
        .await
        .map_err(CliError::protocol)?;

    let mut resp_stream = client
        .map_fn(ReceiverStream::new(rx))
        .await
        .map_err(|e| CliError::new(ExitCode::Connect, anyhow!("MapFn failed: {e}")))?
        .into_inner();

    // Await the handshake echo.
    let hs = next_with_timeout(&mut resp_stream, tuning.timeout)
        .await
        .map_err(CliError::protocol)?
        .ok_or_else(|| CliError::protocol(anyhow!("stream closed before handshake response")))?;
    if hs.handshake.map(|h| h.sot) != Some(true) {
        return Err(CliError::protocol(anyhow!(
            "expected handshake response with sot=true, got {hs:?}"
        )));
    }
    reporter.status(format!("✓ ready ({}) · handshake ok", target.label()));

    let start = Instant::now();
    let result = match args.mode {
        MapMode::Unary => {
            drive_unary(
                &tx,
                &mut resp_stream,
                &messages,
                &args,
                tuning,
                &mut reporter,
            )
            .await
        }
        MapMode::Batch => {
            drive_batch(
                &tx,
                &mut resp_stream,
                &messages,
                &args,
                tuning,
                &mut reporter,
            )
            .await
        }
        MapMode::Stream => {
            drive_stream(
                &tx,
                &mut resp_stream,
                &messages,
                &args,
                tuning,
                &mut reporter,
            )
            .await
        }
    };
    // Close the request stream.
    drop(tx);

    result?;
    reporter.tally.sent = messages.len();
    reporter.summary(&[("elapsed", format!("{}ms", start.elapsed().as_millis()))]);
    reporter.exit_result()
}

/// Print all results from one MapResponse under a header, returning the number of results.
fn emit_response(reporter: &mut Reporter, resp: &MapResponse) {
    let records = resp
        .results
        .iter()
        .map(|r| ResultRecord {
            keys: r.keys.clone(),
            tags: r.tags.clone(),
            value: r.value.clone(),
            nack_delay_ms: r.nack_options.as_ref().and_then(|n| n.delay),
            nack_reason: r.nack_options.as_ref().and_then(|n| n.reason.clone()),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    reporter.group_header(format!("[{}] {} result(s)", resp.id, records.len()));
    for (i, r) in records.iter().enumerate() {
        reporter.result(r, i + 1, &ResultContext::id(resp.id.clone()));
    }
}

fn response_error(err: anyhow::Error) -> CliError {
    if err.downcast_ref::<ResponseTimeout>().is_some() {
        CliError::protocol(err.context(
            "no response within --timeout; if the server was built as a batch or stream map, \
             pass --mode batch|stream (must match the server)",
        ))
    } else {
        CliError::protocol(err)
    }
}

/// Unary: send n, await n by id, no EOT.
async fn drive_unary(
    tx: &mpsc::Sender<MapRequest>,
    stream: &mut tonic::Streaming<MapResponse>,
    messages: &[Message],
    args: &MapArgs,
    tuning: &TuningOpts,
    reporter: &mut Reporter,
) -> CliResult<()> {
    for batch in batches(messages, args.pacing.batch_size) {
        let mut pending: HashSet<String> = batch.iter().map(|m| m.id.clone()).collect();
        for m in batch {
            send_with_timeout(tx, data_request(m), tuning.timeout)
                .await
                .map_err(CliError::protocol)?;
        }
        while !pending.is_empty() {
            let resp = next_with_timeout(stream, tuning.timeout)
                .await
                .map_err(response_error)?
                .ok_or_else(|| {
                    CliError::protocol(anyhow!(
                        "stream closed with {} response(s) outstanding",
                        pending.len()
                    ))
                })?;
            if resp.status.map(|s| s.eot) == Some(true) {
                continue;
            }
            if !pending.remove(&resp.id) {
                reporter.trace(format!("unexpected/duplicate response id {}", resp.id));
            }
            emit_response(reporter, &resp);
        }
        maybe_delay(args.pacing.delay).await;
    }
    Ok(())
}

/// Batch: send n + EOT, await one response per id plus the terminating EOT response.
async fn drive_batch(
    tx: &mpsc::Sender<MapRequest>,
    stream: &mut tonic::Streaming<MapResponse>,
    messages: &[Message],
    args: &MapArgs,
    tuning: &TuningOpts,
    reporter: &mut Reporter,
) -> CliResult<()> {
    for batch in batches(messages, args.pacing.batch_size) {
        let mut pending: HashSet<String> = batch.iter().map(|m| m.id.clone()).collect();
        for m in batch {
            send_with_timeout(tx, data_request(m), tuning.timeout)
                .await
                .map_err(CliError::protocol)?;
        }
        send_with_timeout(tx, eot_request(), tuning.timeout)
            .await
            .map_err(CliError::protocol)?;
        reporter.trace("→ EOT");

        loop {
            let resp = next_with_timeout(stream, tuning.timeout)
                .await
                .map_err(response_error)?
                .ok_or_else(|| CliError::protocol(anyhow!("stream closed before EOT response")))?;
            if resp.status.map(|s| s.eot) == Some(true) {
                reporter.trace("← EOT");
                if !pending.is_empty() {
                    // numa's UDF_PARTIAL_RESPONSE.
                    return Err(CliError::protocol(anyhow!(
                        "partial response: EOT arrived with {} id(s) unanswered: {:?}",
                        pending.len(),
                        pending
                    )));
                }
                break;
            }
            if !pending.remove(&resp.id) {
                reporter.trace(format!("unexpected/duplicate response id {}", resp.id));
            }
            emit_response(reporter, &resp);
        }
        maybe_delay(args.pacing.delay).await;
    }
    Ok(())
}

/// Stream: send n; each id's response stream is read until its own EOT-marked response.
async fn drive_stream(
    tx: &mpsc::Sender<MapRequest>,
    stream: &mut tonic::Streaming<MapResponse>,
    messages: &[Message],
    args: &MapArgs,
    tuning: &TuningOpts,
    reporter: &mut Reporter,
) -> CliResult<()> {
    for batch in batches(messages, args.pacing.batch_size) {
        let mut pending: HashSet<String> = batch.iter().map(|m| m.id.clone()).collect();
        for m in batch {
            send_with_timeout(tx, data_request(m), tuning.timeout)
                .await
                .map_err(CliError::protocol)?;
        }
        // Read until every id has produced its EOT-marked response.
        while !pending.is_empty() {
            let resp = next_with_timeout(stream, tuning.timeout)
                .await
                .map_err(response_error)?
                .ok_or_else(|| {
                    CliError::protocol(anyhow!(
                        "stream closed with {} id(s) still streaming",
                        pending.len()
                    ))
                })?;
            // In stream mode, the per-id EOT response is marked with status.eot and carries
            // the id it terminates.
            if resp.status.map(|s| s.eot) == Some(true) {
                if !pending.remove(&resp.id) {
                    reporter.trace(format!("EOT for unknown id {}", resp.id));
                } else {
                    reporter.trace(format!("← EOT for {}", resp.id));
                }
                continue;
            }
            emit_response(reporter, &resp);
        }
        maybe_delay(args.pacing.delay).await;
    }
    Ok(())
}
