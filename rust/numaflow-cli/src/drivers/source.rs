//! Source driver: plays numa's reader role. Opens the Read and Ack streams (each with its own
//! `sot` handshake), then per round: ReadRequest → collect until eot → AckRequest → AckResponse.

use std::time::{Duration, Instant};

use anyhow::anyhow;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use numaflow_pb::clients::source::source_client::SourceClient;
use numaflow_pb::clients::source::{
    AckRequest, Handshake, Offset, ReadRequest, ReadResponse, ack_request, read_request,
};

use crate::cli::{OutputFormat, OutputOpts, ServiceKind, SourceArgs, TuningOpts};
use crate::drivers::{
    ResponseTimeout, connect_and_ready, maybe_delay, next_with_timeout, send_with_timeout,
};
use crate::exit::{CliError, CliResult, ExitCode};
use crate::message::from_timestamp;
use crate::output::{Reporter, render_payload};

type SourceCli = SourceClient<Channel>;

pub async fn run(args: SourceArgs, tuning: &TuningOpts, output: &OutputOpts) -> CliResult<()> {
    let mut reporter = Reporter::new(output);

    let (target, channel) = connect_and_ready(&args.connect, ServiceKind::Source, tuning).await?;

    let mut client = SourceClient::new(channel)
        .max_encoding_message_size(tuning.max_message_size)
        .max_decoding_message_size(tuning.max_message_size);

    // Open Read stream with handshake.
    let (read_tx, read_rx) = mpsc::channel::<ReadRequest>(16);
    send_with_timeout(
        &read_tx,
        ReadRequest {
            request: None,
            handshake: Some(Handshake { sot: true }),
        },
        tuning.timeout,
    )
    .await
    .map_err(CliError::protocol)?;
    let mut read_stream = client
        .read_fn(ReceiverStream::new(read_rx))
        .await
        .map_err(|e| CliError::new(ExitCode::Connect, anyhow!("ReadFn failed: {e}")))?
        .into_inner();
    let rhs = next_with_timeout(&mut read_stream, tuning.timeout)
        .await
        .map_err(CliError::protocol)?
        .ok_or_else(|| CliError::protocol(anyhow!("read stream closed before handshake")))?;
    if rhs.handshake.map(|h| h.sot) != Some(true) {
        return Err(CliError::protocol(anyhow!(
            "expected read handshake sot=true"
        )));
    }

    // Open Ack stream with handshake (only if we will ack).
    let mut ack_pair = if args.no_ack {
        None
    } else {
        let (ack_tx, ack_rx) = mpsc::channel::<AckRequest>(16);
        send_with_timeout(
            &ack_tx,
            AckRequest {
                request: None,
                handshake: Some(Handshake { sot: true }),
            },
            tuning.timeout,
        )
        .await
        .map_err(CliError::protocol)?;
        let mut ack_stream = client
            .ack_fn(ReceiverStream::new(ack_rx))
            .await
            .map_err(|e| CliError::new(ExitCode::Connect, anyhow!("AckFn failed: {e}")))?
            .into_inner();
        let ahs = next_with_timeout(&mut ack_stream, tuning.timeout)
            .await
            .map_err(CliError::protocol)?
            .ok_or_else(|| CliError::protocol(anyhow!("ack stream closed before handshake")))?;
        if ahs.handshake.map(|h| h.sot) != Some(true) {
            return Err(CliError::protocol(anyhow!(
                "expected ack handshake sot=true"
            )));
        }
        Some((ack_tx, ack_stream))
    };

    reporter.status(format!(
        "✓ ready ({}) · read handshake ok{}",
        target.label(),
        if args.no_ack {
            ""
        } else {
            " · ack handshake ok"
        }
    ));

    if args.partitions {
        match client.partitions_fn(tonic::Request::new(())).await {
            Ok(resp) => {
                let parts = resp
                    .into_inner()
                    .result
                    .map(|r| r.partitions)
                    .unwrap_or_default();
                reporter.status(format!("partitions={parts:?}"));
                reporter.json_event(&json!({"type":"partitions","partitions":parts}));
            }
            Err(e) => reporter.trace(format!("PartitionsFn error: {e}")),
        }
    }

    if args.pending {
        print_pending(&mut client, &reporter, "before").await;
    }

    let start = Instant::now();
    let mut total_read = 0usize;
    let mut total_acked = 0usize;

    for round in 1..=args.rounds {
        send_with_timeout(
            &read_tx,
            ReadRequest {
                request: Some(read_request::Request {
                    num_records: args.count,
                    timeout_in_ms: args.read_timeout.as_millis().min(u32::MAX as u128) as u32,
                }),
                handshake: None,
            },
            tuning.timeout,
        )
        .await
        .map_err(CliError::protocol)?;

        let round_start = Instant::now();
        let mut offsets: Vec<Offset> = Vec::new();
        loop {
            let resp = next_with_timeout(&mut read_stream, tuning.timeout)
                .await
                .map_err(CliError::protocol)?
                .ok_or_else(|| CliError::protocol(anyhow!("read stream closed mid-round")))?;
            if resp.status.as_ref().map(|s| s.eot) == Some(true) {
                break;
            }
            if let Some(result) = resp.result {
                if let Some(off) = &result.offset {
                    offsets.push(off.clone());
                }
                total_read += 1;
                emit_message(&reporter, round, &result);
            }
        }

        reporter.group_header(format!(
            "round {round}: {} msg(s) ({}ms)",
            offsets.len(),
            round_start.elapsed().as_millis()
        ));

        if let Some((ack_tx, ack_stream)) = ack_pair.as_mut() {
            send_with_timeout(
                ack_tx,
                AckRequest {
                    request: Some(ack_request::Request {
                        offsets: offsets.clone(),
                    }),
                    handshake: None,
                },
                tuning.timeout,
            )
            .await
            .map_err(CliError::protocol)?;
            match next_with_timeout(ack_stream, tuning.timeout).await {
                Ok(Some(_)) => {}
                Ok(None) => {
                    return Err(prefer_read_stream_status(
                        &mut read_stream,
                        CliError::protocol(anyhow!("ack stream closed before ack response")),
                    )
                    .await);
                }
                Err(e) => return Err(CliError::protocol(e)),
            }
            total_acked += offsets.len();
            reporter.status(format!("  acked {} offset(s) ✓", offsets.len()));
        }

        if round < args.rounds {
            maybe_delay(args.delay).await;
        }
    }
    drop(read_tx);
    drop(ack_pair);

    if args.pending {
        print_pending(&mut client, &reporter, "after").await;
    }

    reporter.tally.sent = 0;
    reporter.tally.results = total_read;
    reporter.summary(&[
        ("read", total_read.to_string()),
        ("acked", total_acked.to_string()),
        ("elapsed", format!("{}ms", start.elapsed().as_millis())),
    ]);
    Ok(())
}

async fn prefer_read_stream_status(
    read_stream: &mut tonic::Streaming<ReadResponse>,
    fallback: CliError,
) -> CliError {
    match next_with_timeout(read_stream, Duration::from_millis(50)).await {
        Err(e) if e.downcast_ref::<ResponseTimeout>().is_none() => CliError::protocol(e),
        _ => fallback,
    }
}

async fn print_pending(client: &mut SourceCli, reporter: &Reporter, when: &str) {
    match client.pending_fn(tonic::Request::new(())).await {
        Ok(resp) => {
            let count = resp.into_inner().result.map(|r| r.count).unwrap_or(-1);
            reporter.status(format!("pending={count} ({when})"));
            reporter.json_event(&json!({"type":"pending","count":count,"when":when}));
        }
        Err(e) => reporter.trace(format!("PendingFn error: {e}")),
    }
}

fn emit_message(
    reporter: &Reporter,
    round: usize,
    result: &numaflow_pb::clients::source::read_response::Result,
) {
    let offset_b64 = result
        .offset
        .as_ref()
        .map(|o| {
            use base64::Engine;
            format!(
                "{}/p{}",
                base64::engine::general_purpose::STANDARD.encode(&o.offset),
                o.partition_id
            )
        })
        .unwrap_or_default();
    let event_time = result.event_time.as_ref().and_then(from_timestamp);

    if reporter.format == OutputFormat::Json {
        use base64::Engine;
        reporter.json_event(&json!({
            "type": "sourceMessage",
            "round": round,
            "offset": offset_b64,
            "keys": result.keys,
            "eventTime": event_time.map(|t| t.to_rfc3339()),
            "payloadBase64": base64::engine::general_purpose::STANDARD.encode(&result.payload),
        }));
        return;
    }
    if reporter.format == OutputFormat::Raw {
        use std::io::Write;
        let _ = std::io::stdout().lock().write_all(&result.payload);
        return;
    }
    if reporter.quiet {
        return;
    }
    let et = event_time
        .map(|t| t.to_rfc3339())
        .unwrap_or_else(|| "-".to_string());
    println!(
        "  offset={offset_b64} eventTime={et} keys={:?} payload={}",
        result.keys,
        render_payload(&result.payload)
    );
}
