//! Aligned reduce driver (fixed & sliding windows). Uses the shared production windower and
//! one `AlignedReduceBook` per window; end-of-input closes every book's request stream (aligned
//! reduce treats stream-close as the CLOSE signal).

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use tokio::task::JoinHandle;

use numaflow_pb::clients::reduce::reduce_client::ReduceClient;
use numaflow_udf_client::{
    AlignedReduceBook, AlignedReduceReceiver, AlignedWindowAction, AlignedWindowManager,
    FixedWindowManager, SlidingWindowManager, UdfClientError, Window,
};

use crate::cli::{OutputOpts, ReduceArgs, ServiceKind, TuningOpts, WindowKind};
use crate::drivers::{Pacer, ResponseTimeout, connect_and_ready};
use crate::exit::{CliError, CliResult, ExitCode};
use crate::input::{LoadOptions, load_messages, resolve_base_time};
use crate::output::{Reporter, ResultContext, ResultRecord};

const REDUCE_REQUEST_BUFFER: usize = 256;

/// Build the epoch-truncated fallback base time for the reduce family: now, truncated down to
/// the window boundary so `+0s..+59s` land in one window.
fn reduce_base_fallback(length: Duration) -> chrono::DateTime<chrono::Utc> {
    let now = chrono::Utc::now().timestamp_millis();
    let len = length.as_millis().max(1) as i64;
    let truncated = now.div_euclid(len) * len;
    chrono::DateTime::from_timestamp_millis(truncated).unwrap_or_else(chrono::Utc::now)
}

struct WindowState {
    book: AlignedReduceBook,
    collector: JoinHandle<CliResult<Vec<ResultRecord>>>,
    sent: usize,
}

type PendingWindowClose = (String, usize, JoinHandle<CliResult<Vec<ResultRecord>>>);

pub async fn run(args: ReduceArgs, tuning: &TuningOpts, output: &OutputOpts) -> CliResult<()> {
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

    let manager = match args.window {
        WindowKind::Fixed => AlignedWindowManager::Fixed(FixedWindowManager::new(args.length)),
        WindowKind::Sliding => AlignedWindowManager::Sliding(SlidingWindowManager::new(
            args.length,
            args.slide.expect("validated"),
        )),
    };

    let mut reporter = Reporter::new(output);
    let (target, channel) = connect_and_ready(&args.connect, ServiceKind::Reduce, tuning).await?;
    reporter.status(format!("✓ ready ({})", target.label()));

    let client = ReduceClient::new(channel)
        .max_encoding_message_size(tuning.max_message_size)
        .max_decoding_message_size(tuning.max_message_size);

    let start = Instant::now();
    let mut sent_total = 0usize;
    let mut pacer = Pacer::new(&args.pacing);
    let mut active: BTreeMap<Window, WindowState> = BTreeMap::new();

    for message in &messages {
        let datum = message.to_udf_datum();
        let actions = manager.assign_windows(message.event_time);
        for action in actions {
            match action {
                AlignedWindowAction::Open { window } => {
                    let win_label = window.to_string();
                    reporter.trace(format!("window {win_label} OPEN"));
                    let (book, receiver) = with_send_timeout(
                        tuning.timeout,
                        AlignedReduceBook::open(
                            client.clone(),
                            window.clone(),
                            datum.clone(),
                            REDUCE_REQUEST_BUFFER,
                        ),
                    )
                    .await?;
                    let collector = spawn_collector(receiver);
                    active.insert(
                        window,
                        WindowState {
                            book,
                            collector,
                            sent: 1,
                        },
                    );
                    sent_total += 1;
                    pacer.tick().await;
                }
                AlignedWindowAction::Append { window } => {
                    let state = active.get_mut(&window).ok_or_else(|| {
                        CliError::protocol(anyhow!(
                            "append for window {window} with no active book"
                        ))
                    })?;
                    append_with_timeout(tuning.timeout, state, datum.clone()).await?;
                    state.sent += 1;
                    sent_total += 1;
                    pacer.tick().await;
                }
                AlignedWindowAction::Close { .. } => {
                    return Err(CliError::protocol(anyhow!(
                        "unexpected CLOSE during input assignment"
                    )));
                }
            }
        }
    }

    let close_actions = manager.close_all();
    let mut closing: BTreeMap<Window, WindowState> = BTreeMap::new();
    for action in close_actions {
        let AlignedWindowAction::Close { window } = action else {
            return Err(CliError::protocol(anyhow!(
                "expected CLOSE at end of input"
            )));
        };
        let state = active.remove(&window).ok_or_else(|| {
            CliError::protocol(anyhow!("close for window {window} with no active book"))
        })?;
        closing.insert(window, state);
    }

    let window_count = closing.len();
    let mut awaiting: Vec<PendingWindowClose> = Vec::with_capacity(window_count);
    for (window, state) in closing {
        let win_label = window.to_string();
        let WindowState {
            book,
            collector,
            sent,
        } = state;
        book.close();
        awaiting.push((win_label, sent, collector));
    }
    for (win_label, _, _) in &awaiting {
        reporter.trace(format!("window {win_label} CLOSE (stream close)"));
    }
    for (win_label, sent, collector) in awaiting {
        let results = match tokio::time::timeout(tuning.timeout, collector).await {
            Ok(join_result) => join_result
                .map_err(|e| CliError::protocol(anyhow!("collector task failed: {e}")))?,
            Err(_) => {
                return Err(CliError::protocol(anyhow::Error::new(ResponseTimeout(
                    tuning.timeout,
                ))));
            }
        }?;

        reporter.group_header(format!(
            "window {win_label} · {} msg(s) sent · closed",
            sent
        ));
        for (i, r) in results.iter().enumerate() {
            reporter.result(r, i + 1, &ResultContext::window(win_label.clone()));
        }
    }

    reporter.tally.sent = sent_total;
    reporter.summary(&[
        ("windows", window_count.to_string()),
        ("elapsed", format!("{}ms", start.elapsed().as_millis())),
    ]);
    reporter.exit_result()
}

async fn with_send_timeout<T, F>(timeout: Duration, future: F) -> CliResult<T>
where
    F: std::future::Future<Output = Result<T, UdfClientError>>,
{
    match tokio::time::timeout(timeout, future).await {
        Err(_) => Err(CliError::protocol(anyhow::Error::new(ResponseTimeout(
            timeout,
        )))),
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(reduce_send_error(error)),
    }
}

async fn append_with_timeout(
    timeout: Duration,
    state: &mut WindowState,
    datum: numaflow_udf_client::UdfDatum,
) -> CliResult<()> {
    match tokio::time::timeout(timeout, state.book.append(datum)).await {
        Err(_) => Err(CliError::protocol(anyhow::Error::new(ResponseTimeout(
            timeout,
        )))),
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(reduce_append_error(error, timeout, &mut state.collector).await),
    }
}

/// A background ReduceFn startup failure closes the request receiver before APPEND observes it.
/// Prefer the collector's typed startup error so the CLI keeps the connect exit code.
async fn reduce_append_error(
    error: UdfClientError,
    timeout: Duration,
    collector: &mut JoinHandle<CliResult<Vec<ResultRecord>>>,
) -> CliError {
    if matches!(&error, UdfClientError::ReduceRequestStreamClosed) {
        match tokio::time::timeout(timeout, collector).await {
            Ok(Ok(Err(collector_error))) => return collector_error,
            Ok(Err(join_error)) => {
                return CliError::protocol(anyhow!("collector task failed: {join_error}"));
            }
            Ok(Ok(Ok(_))) | Err(_) => {}
        }
    }

    reduce_send_error(error)
}

fn spawn_collector(receiver: AlignedReduceReceiver) -> JoinHandle<CliResult<Vec<ResultRecord>>> {
    tokio::spawn(async move {
        let mut receiver = receiver;
        let mut results = Vec::new();
        while let Some(item) = receiver.recv().await {
            let result = item.map_err(reduce_recv_error)?;
            results.push(ResultRecord {
                keys: result.keys,
                tags: result.tags,
                value: result.value.to_vec(),
                ..Default::default()
            });
        }
        Ok(results)
    })
}

fn reduce_send_error(error: UdfClientError) -> CliError {
    match error {
        UdfClientError::ReduceFnStart(status) => CliError::new(
            ExitCode::Connect,
            anyhow::Error::new(status).context("ReduceFn failed"),
        ),
        other => CliError::protocol(anyhow::Error::new(other)),
    }
}

fn reduce_recv_error(error: UdfClientError) -> CliError {
    match error {
        UdfClientError::ReduceFnStart(status) => CliError::new(
            ExitCode::Connect,
            anyhow::Error::new(status).context("ReduceFn failed"),
        ),
        UdfClientError::ReduceGrpc(status) => {
            CliError::protocol(anyhow::Error::new(status).context("gRPC error mid-stream"))
        }
        other => CliError::protocol(anyhow::Error::new(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn closed_append_prefers_collector_startup_error() {
        let mut collector = tokio::spawn(async {
            Err(CliError::new(
                ExitCode::Connect,
                anyhow!("ReduceFn failed during startup"),
            ))
        });

        let error = reduce_append_error(
            UdfClientError::ReduceRequestStreamClosed,
            Duration::from_secs(1),
            &mut collector,
        )
        .await;

        assert_eq!(error.code, ExitCode::Connect);
    }

    #[tokio::test]
    async fn closed_append_without_collector_error_remains_protocol_error() {
        let mut collector = tokio::spawn(async { Ok(Vec::new()) });

        let error = reduce_append_error(
            UdfClientError::ReduceRequestStreamClosed,
            Duration::from_secs(1),
            &mut collector,
        )
        .await;

        assert_eq!(error.code, ExitCode::Protocol);
    }
}
