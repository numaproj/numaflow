//! Map driver: unary, batch, and stream modes over the unified `map.v1.Map/MapFn` bidi RPC.

use std::time::Instant;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
use tonic::transport::Channel;

use numaflow_pb::clients::map::map_client::MapClient;
use numaflow_udf_client::{
    BatchMapEvent, BatchMapSession, StreamMapSession, UdfClientError, UnaryMapResponse,
    UnaryMapSession,
};

use crate::cli::{MapArgs, MapMode, OutputOpts, ServiceKind, TuningOpts};
use crate::drivers::{ResponseTimeout, batches, connect_and_ready, maybe_delay};
use crate::exit::{CliError, CliResult, ExitCode};
use crate::input::{LoadOptions, load_messages, resolve_base_time};
use crate::message::Message;
use crate::output::{Reporter, ResultContext, ResultRecord};

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
            let session = open_batch_session(client, tuning).await?;
            reporter.status(format!("✓ ready ({}) · handshake ok", target.label()));
            drive_batch(session, &messages, &args, tuning, &mut reporter).await
        }
        MapMode::Stream => {
            let session = open_stream_session(client, tuning).await?;
            reporter.status(format!("✓ ready ({}) · handshake ok", target.label()));
            drive_stream(session, &messages, &args, tuning, &mut reporter).await
        }
    };

    result?;
    reporter.tally.sent = messages.len();
    reporter.summary(&[("elapsed", format!("{}ms", start.elapsed().as_millis()))]);
    reporter.exit_result()
}

async fn open_stream_session(
    client: MapClient<Channel>,
    tuning: &TuningOpts,
) -> CliResult<StreamMapSession> {
    tokio::time::timeout(tuning.timeout, StreamMapSession::open(client, 256))
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

async fn open_batch_session(
    client: MapClient<Channel>,
    tuning: &TuningOpts,
) -> CliResult<BatchMapSession> {
    tokio::time::timeout(tuning.timeout, BatchMapSession::open(client, 256))
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

fn map_session_error(error: UdfClientError) -> CliError {
    match error {
        UdfClientError::MapFnStart(status) | UdfClientError::Grpc(status) => {
            CliError::protocol(anyhow::Error::new(status).context("gRPC error mid-stream"))
        }
        other => CliError::protocol(anyhow::Error::new(other)),
    }
}

fn emit_shared_response(reporter: &mut Reporter, response: &UnaryMapResponse) {
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
            let datum = message.to_udf_datum();
            let timeout = tuning.timeout;
            pending.push(async move {
                match tokio::time::timeout(timeout, session.map(datum)).await {
                    Ok(result) => result.map_err(map_session_error),
                    Err(_) => Err(response_error(anyhow::Error::new(ResponseTimeout(timeout)))),
                }
            });
        }

        while let Some(response) = pending.next().await {
            emit_shared_response(reporter, &response?);
        }
        maybe_delay(args.pacing.delay).await;
    }
    Ok(())
}

/// Batch: send n + EOT, await one response per id plus the terminating EOT response.
async fn drive_batch(
    session: BatchMapSession,
    messages: &[Message],
    args: &MapArgs,
    tuning: &TuningOpts,
    reporter: &mut Reporter,
) -> CliResult<()> {
    for batch in batches(messages, args.pacing.batch_size) {
        let data = batch.iter().map(Message::to_udf_datum).collect();
        let operation = session.batch_with_event_handler(data, |event| match event {
            BatchMapEvent::ClientEotSent => reporter.trace("→ EOT"),
            BatchMapEvent::ServerEotReceived => reporter.trace("← EOT"),
        });

        let responses = match tokio::time::timeout(tuning.timeout, operation).await {
            Ok(result) => result.map_err(map_session_error)?,
            Err(_) => {
                return Err(response_error(anyhow::Error::new(ResponseTimeout(
                    tuning.timeout,
                ))));
            }
        };
        for response in &responses {
            emit_shared_response(reporter, response);
        }
        maybe_delay(args.pacing.delay).await;
    }
    Ok(())
}

enum StreamDriverEvent {
    Response(UnaryMapResponse),
    Eot(String),
}

/// Stream: send n; each id's response stream is read until its own EOT-marked response.
async fn drive_stream(
    session: StreamMapSession,
    messages: &[Message],
    args: &MapArgs,
    tuning: &TuningOpts,
    reporter: &mut Reporter,
) -> CliResult<()> {
    for batch in batches(messages, args.pacing.batch_size) {
        let (event_tx, mut event_rx) = mpsc::channel(1);
        let mut pending = FuturesUnordered::new();
        for message in batch {
            let session = session.clone();
            let datum = message.to_udf_datum();
            let id = datum.id.clone();
            let timeout = tuning.timeout;
            let event_tx = event_tx.clone();
            pending.push(async move {
                let mut receiver = match tokio::time::timeout(timeout, session.stream(datum)).await
                {
                    Ok(result) => result.map_err(map_session_error)?,
                    Err(_) => {
                        return Err(response_error(anyhow::Error::new(ResponseTimeout(timeout))));
                    }
                };

                // Drain every per-ID receiver concurrently. A streaming UDF may interleave result
                // chunks across IDs, so waiting for one ID at a time could apply backpressure to
                // the shared response pump and prevent another ID's EOT from being observed.
                loop {
                    match tokio::time::timeout(timeout, receiver.recv()).await {
                        Ok(Some(Ok(response))) => {
                            event_tx
                                .send(StreamDriverEvent::Response(response))
                                .await
                                .map_err(|_| {
                                    CliError::protocol(anyhow::anyhow!(
                                        "stream result receiver closed"
                                    ))
                                })?;
                        }
                        Ok(Some(Err(error))) => return Err(map_session_error(error)),
                        Ok(None) => {
                            event_tx
                                .send(StreamDriverEvent::Eot(id))
                                .await
                                .map_err(|_| {
                                    CliError::protocol(anyhow::anyhow!(
                                        "stream result receiver closed"
                                    ))
                                })?;
                            return Ok(());
                        }
                        Err(_) => {
                            return Err(response_error(anyhow::Error::new(ResponseTimeout(
                                timeout,
                            ))));
                        }
                    }
                }
            });
        }
        drop(event_tx);

        // Poll the per-ID operations and their output channel together so result chunks are
        // reported as they arrive rather than being buffered until EOT.
        while !pending.is_empty() {
            tokio::select! {
                Some(completed) = pending.next() => completed?,
                Some(event) = event_rx.recv() => emit_stream_event(reporter, event),
            }
        }
        while let Some(event) = event_rx.recv().await {
            emit_stream_event(reporter, event);
        }
        maybe_delay(args.pacing.delay).await;
    }
    Ok(())
}

fn emit_stream_event(reporter: &mut Reporter, event: StreamDriverEvent) {
    match event {
        StreamDriverEvent::Response(response) => emit_shared_response(reporter, &response),
        StreamDriverEvent::Eot(id) => reporter.trace(format!("← EOT for {id}")),
    }
}
