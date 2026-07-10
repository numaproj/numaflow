//! Map driver: unary, batch, and stream modes over the unified `map.v1.Map/MapFn` bidi RPC.

use std::collections::HashSet;
use std::time::Instant;

use anyhow::anyhow;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tonic::transport::Channel;

use numaflow_pb::clients::map::map_client::MapClient;
use numaflow_pb::clients::map::{MapRequest, MapResponse, TransmissionStatus, map_request};
use numaflow_udf_client::{
    KeyValueGroup, MapRpcStream, UdfClientError, UdfDatum, UdfMetadata, UnaryMapResponse,
    UnaryMapSession,
};

use crate::cli::{MapArgs, MapMode, OutputOpts, ServiceKind, TuningOpts};
use crate::drivers::{
    ResponseTimeout, batches, connect_and_ready, maybe_delay, next_with_timeout, send_with_timeout,
};
use crate::exit::{CliError, CliResult, ExitCode};
use crate::input::{LoadOptions, load_messages, resolve_base_time};
use crate::message::Message;
use crate::output::{Reporter, ResultContext, ResultRecord};

/// Transitional protobuf builder used only by batch and stream modes.
fn data_request(message: &Message) -> MapRequest {
    MapRequest {
        request: Some(map_request::Request {
            keys: message.keys.clone(),
            value: message.value.clone(),
            event_time: Some(message.event_time_pb()),
            watermark: Some(message.watermark_pb()),
            headers: message.headers.clone(),
            metadata: message.metadata(),
        }),
        id: message.id.clone(),
        handshake: None,
        status: None,
    }
}

/// Transitional EOT builder used only by batch mode.
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
    let client = MapClient::new(channel)
        .max_encoding_message_size(tuning.max_message_size)
        .max_decoding_message_size(tuning.max_message_size);

    let start = Instant::now();
    let result = match args.mode {
        MapMode::Unary => {
            let session = open_unary_session(client, tuning).await?;
            reporter.status(format!("✓ ready ({}) · handshake ok", target.label()));
            drive_unary(session, &messages, &args, tuning, &mut reporter).await
        }
        MapMode::Batch => {
            let stream = open_map_rpc_stream(client, tuning).await?;
            reporter.status(format!("✓ ready ({}) · handshake ok", target.label()));
            let (tx, mut response_stream) = stream.into_parts();
            let result = drive_batch(
                &tx,
                &mut response_stream,
                &messages,
                &args,
                tuning,
                &mut reporter,
            )
            .await;
            drop(tx);
            result
        }
        MapMode::Stream => {
            let stream = open_map_rpc_stream(client, tuning).await?;
            reporter.status(format!("✓ ready ({}) · handshake ok", target.label()));
            let (tx, mut response_stream) = stream.into_parts();
            let result = drive_stream(
                &tx,
                &mut response_stream,
                &messages,
                &args,
                tuning,
                &mut reporter,
            )
            .await;
            drop(tx);
            result
        }
    };

    result?;
    reporter.tally.sent = messages.len();
    reporter.summary(&[("elapsed", format!("{}ms", start.elapsed().as_millis()))]);
    reporter.exit_result()
}

async fn open_map_rpc_stream(
    client: MapClient<Channel>,
    tuning: &TuningOpts,
) -> CliResult<MapRpcStream> {
    tokio::time::timeout(tuning.timeout, MapRpcStream::open(client, 256))
        .await
        .map_err(|_| {
            CliError::protocol(
                anyhow::Error::new(ResponseTimeout(tuning.timeout))
                    .context("timed out opening MapFn or waiting for its handshake"),
            )
        })?
        .map_err(open_error)
}

async fn open_unary_session(
    client: MapClient<Channel>,
    tuning: &TuningOpts,
) -> CliResult<UnaryMapSession> {
    tokio::time::timeout(tuning.timeout, UnaryMapSession::open(client, 256))
        .await
        .map_err(|_| {
            CliError::protocol(
                anyhow::Error::new(ResponseTimeout(tuning.timeout))
                    .context("timed out opening MapFn or waiting for its handshake"),
            )
        })?
        .map_err(open_error)
}

fn open_error(error: UdfClientError) -> CliError {
    match error {
        UdfClientError::MapFnStart(status) => CliError::new(
            ExitCode::Connect,
            anyhow::Error::new(status).context("MapFn failed"),
        ),
        UdfClientError::Grpc(status) => CliError::protocol(
            anyhow::Error::new(status).context("gRPC error during MapFn handshake"),
        ),
        other => CliError::protocol(anyhow::Error::new(other)),
    }
}

fn unary_error(error: UdfClientError) -> CliError {
    match error {
        UdfClientError::MapFnStart(status) | UdfClientError::Grpc(status) => {
            CliError::protocol(anyhow::Error::new(status).context("gRPC error mid-stream"))
        }
        other => CliError::protocol(anyhow::Error::new(other)),
    }
}

/// Print protobuf results used by transitional batch and stream modes.
fn emit_response(reporter: &mut Reporter, response: &MapResponse) {
    let records = response
        .results
        .iter()
        .map(|result| ResultRecord {
            keys: result.keys.clone(),
            tags: result.tags.clone(),
            value: result.value.clone(),
            nack_delay_ms: result.nack_options.as_ref().and_then(|nack| nack.delay),
            nack_reason: result
                .nack_options
                .as_ref()
                .and_then(|nack| nack.reason.clone()),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    reporter.group_header(format!("[{}] {} result(s)", response.id, records.len()));
    for (index, record) in records.iter().enumerate() {
        reporter.result(record, index + 1, &ResultContext::id(response.id.clone()));
    }
}

fn emit_unary_response(reporter: &mut Reporter, response: &UnaryMapResponse) {
    let records = response
        .results
        .iter()
        .map(|result| ResultRecord {
            keys: result.keys.clone(),
            tags: result.tags.clone(),
            value: result.value.to_vec(),
            nack_delay_ms: result.nack_options.as_ref().and_then(|nack| nack.delay),
            nack_reason: result
                .nack_options
                .as_ref()
                .and_then(|nack| nack.reason.clone()),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    reporter.group_header(format!("[{}] {} result(s)", response.id, records.len()));
    for (index, record) in records.iter().enumerate() {
        reporter.result(record, index + 1, &ResultContext::id(response.id.clone()));
    }
}

fn response_error(error: anyhow::Error) -> CliError {
    if error.downcast_ref::<ResponseTimeout>().is_some() {
        CliError::protocol(error.context(
            "no response within --timeout; if the server was built as a batch or stream map, \
             pass --mode batch|stream (must match the server)",
        ))
    } else {
        CliError::protocol(error)
    }
}

/// Unary: execute one shared-session future per message, with no EOT.
async fn drive_unary(
    session: UnaryMapSession,
    messages: &[Message],
    args: &MapArgs,
    tuning: &TuningOpts,
    reporter: &mut Reporter,
) -> CliResult<()> {
    for batch in batches(messages, args.pacing.batch_size) {
        let mut pending = FuturesUnordered::new();
        for message in batch {
            let session = session.clone();
            let datum = udf_datum_from_message(message);
            let timeout = tuning.timeout;
            pending.push(async move {
                match tokio::time::timeout(timeout, session.map(datum)).await {
                    Ok(result) => result.map_err(unary_error),
                    Err(_) => Err(response_error(anyhow::Error::new(ResponseTimeout(timeout)))),
                }
            });
        }

        while let Some(response) = pending.next().await {
            emit_unary_response(reporter, &response?);
        }
        maybe_delay(args.pacing.delay).await;
    }
    Ok(())
}

fn udf_datum_from_message(message: &Message) -> UdfDatum {
    let metadata = if message.user_metadata.is_empty() && message.previous_vertex.is_empty() {
        None
    } else {
        Some(UdfMetadata {
            previous_vertex: message.previous_vertex.clone(),
            sys_metadata: Default::default(),
            user_metadata: message
                .user_metadata
                .iter()
                .map(|(group, values)| {
                    (
                        group.clone(),
                        KeyValueGroup {
                            key_value: values
                                .iter()
                                .map(|(key, value)| {
                                    (key.clone(), value.clone().into_bytes().into())
                                })
                                .collect(),
                        },
                    )
                })
                .collect(),
        })
    };

    UdfDatum {
        id: message.id.clone(),
        keys: message.keys.clone(),
        value: message.value.clone().into(),
        event_time: message.event_time,
        watermark: Some(message.watermark),
        headers: message.headers.clone(),
        metadata,
    }
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
        for message in batch {
            send_with_timeout(tx, data_request(message), tuning.timeout)
                .await
                .map_err(CliError::protocol)?;
        }
        send_with_timeout(tx, eot_request(), tuning.timeout)
            .await
            .map_err(CliError::protocol)?;
        reporter.trace("→ EOT");

        loop {
            let response = next_with_timeout(stream, tuning.timeout)
                .await
                .map_err(response_error)?
                .ok_or_else(|| CliError::protocol(anyhow!("stream closed before EOT response")))?;
            if response.status.map(|status| status.eot) == Some(true) {
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
            if !pending.remove(&response.id) {
                reporter.trace(format!("unexpected/duplicate response id {}", response.id));
            }
            emit_response(reporter, &response);
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
        for message in batch {
            send_with_timeout(tx, data_request(message), tuning.timeout)
                .await
                .map_err(CliError::protocol)?;
        }
        while !pending.is_empty() {
            let response = next_with_timeout(stream, tuning.timeout)
                .await
                .map_err(response_error)?
                .ok_or_else(|| {
                    CliError::protocol(anyhow!(
                        "stream closed with {} id(s) still streaming",
                        pending.len()
                    ))
                })?;
            if response.status.map(|status| status.eot) == Some(true) {
                if !pending.remove(&response.id) {
                    reporter.trace(format!("EOT for unknown id {}", response.id));
                } else {
                    reporter.trace(format!("← EOT for {}", response.id));
                }
                continue;
            }
            emit_response(reporter, &response);
        }
        maybe_delay(args.pacing.delay).await;
    }
    Ok(())
}
