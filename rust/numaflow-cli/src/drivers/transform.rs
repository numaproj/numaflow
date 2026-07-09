//! Source transformer driver: like unary map (id-correlated, no EOT) but each result carries
//! a (possibly re-assigned) event time, which we print.

use std::collections::HashSet;
use std::time::Instant;

use anyhow::anyhow;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use numaflow_pb::clients::sourcetransformer::source_transform_client::SourceTransformClient;
use numaflow_pb::clients::sourcetransformer::{
    Handshake, SourceTransformRequest, SourceTransformResponse, source_transform_request,
};

use crate::cli::{DataArgs, OutputOpts, ServiceKind, TuningOpts};
use crate::drivers::{
    batches, connect_and_ready, maybe_delay, next_with_timeout, send_with_timeout,
};
use crate::exit::{CliError, CliResult, ExitCode};
use crate::input::{LoadOptions, load_messages, resolve_base_time};
use crate::message::{Message, from_timestamp};
use crate::output::{Reporter, ResultContext, ResultRecord};

fn data_request(m: &Message) -> SourceTransformRequest {
    SourceTransformRequest {
        request: Some(source_transform_request::Request {
            keys: m.keys.clone(),
            value: m.value.clone(),
            event_time: Some(m.event_time_pb()),
            watermark: Some(m.watermark_pb()),
            headers: m.headers.clone(),
            id: m.id.clone(),
            metadata: m.metadata(),
        }),
        handshake: None,
    }
}

fn handshake_request() -> SourceTransformRequest {
    SourceTransformRequest {
        request: None,
        handshake: Some(Handshake { sot: true }),
    }
}

pub async fn run(args: DataArgs, tuning: &TuningOpts, output: &OutputOpts) -> CliResult<()> {
    let base = resolve_base_time(&args.input, chrono::Utc::now()).map_err(CliError::usage)?;
    let messages =
        load_messages(&args.input, base, LoadOptions::default()).map_err(CliError::usage)?;

    let mut reporter = Reporter::new(output);
    let (target, channel) =
        connect_and_ready(&args.connect, ServiceKind::Transform, tuning).await?;

    let mut client = SourceTransformClient::new(channel)
        .max_encoding_message_size(tuning.max_message_size)
        .max_decoding_message_size(tuning.max_message_size);

    let (tx, rx) = mpsc::channel::<SourceTransformRequest>(256);
    send_with_timeout(&tx, handshake_request(), tuning.timeout)
        .await
        .map_err(CliError::protocol)?;

    let mut stream = client
        .source_transform_fn(ReceiverStream::new(rx))
        .await
        .map_err(|e| CliError::new(ExitCode::Connect, anyhow!("SourceTransformFn failed: {e}")))?
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
    for batch in batches(&messages, args.pacing.batch_size) {
        let mut pending: HashSet<String> = batch.iter().map(|m| m.id.clone()).collect();
        for m in batch {
            send_with_timeout(&tx, data_request(m), tuning.timeout)
                .await
                .map_err(CliError::protocol)?;
        }
        while !pending.is_empty() {
            let resp = next_with_timeout(&mut stream, tuning.timeout)
                .await
                .map_err(CliError::protocol)?
                .ok_or_else(|| {
                    CliError::protocol(anyhow!(
                        "stream closed with {} response(s) outstanding",
                        pending.len()
                    ))
                })?;
            if !pending.remove(&resp.id) {
                reporter.trace(format!("unexpected/duplicate response id {}", resp.id));
            }
            emit_response(&mut reporter, &resp);
        }
        maybe_delay(args.pacing.delay).await;
    }
    drop(tx);

    reporter.tally.sent = messages.len();
    reporter.summary(&[("elapsed", format!("{}ms", start.elapsed().as_millis()))]);
    reporter.exit_result()
}

fn emit_response(reporter: &mut Reporter, resp: &SourceTransformResponse) {
    reporter.group_header(format!("[{}] {} result(s)", resp.id, resp.results.len()));
    for (i, r) in resp.results.iter().enumerate() {
        let rec = ResultRecord {
            keys: r.keys.clone(),
            tags: r.tags.clone(),
            value: r.value.clone(),
            event_time: r.event_time.as_ref().and_then(from_timestamp),
            nack_delay_ms: r.nack_options.as_ref().and_then(|n| n.delay),
            nack_reason: r.nack_options.as_ref().and_then(|n| n.reason.clone()),
            ..Default::default()
        };
        reporter.result(&rec, i + 1, &ResultContext::id(resp.id.clone()));
    }
}
