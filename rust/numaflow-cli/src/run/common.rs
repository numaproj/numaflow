//! Shared feed → drain → read → report engine loop and connection helpers.

use std::path::{Path, PathBuf};
use std::time::Duration;

use numaflow_core::local::{InputEvent, LocalRun, LocalRunOpts, LocalUdf};

use crate::cli::{ConnArgs, FeedArgs, OutputArgs};
use crate::error::CliResult;
use crate::output::{Rendered, render};

/// Standard container mount where SDKs write server-info by convention.
const STANDARD_DIR: &str = "/var/run/numaflow";

/// Resolve the server-info path for a subcommand: explicit `--server-info` wins; else the standard
/// container path when the socket lives under `/var/run/numaflow`; else `<socket dir>/<type>-server-info`.
pub fn resolve_server_info(conn: &ConnArgs, server_info_basename: &str) -> PathBuf {
    if let Some(path) = &conn.server_info {
        return path.clone();
    }
    let socket_dir = conn.socket.parent().unwrap_or_else(|| Path::new("."));
    if socket_dir == Path::new(STANDARD_DIR) {
        PathBuf::from(STANDARD_DIR).join(server_info_basename)
    } else {
        socket_dir.join(server_info_basename)
    }
}

/// Build a `LocalRunOpts` from the connection + feed flags.
pub fn run_opts(conn: &ConnArgs, feed: &FeedArgs) -> LocalRunOpts {
    LocalRunOpts {
        batch_size: feed.batch_size,
        buffer_capacity: feed.buffer_capacity,
        grpc_max_message_size: conn.max_message_size,
        startup_timeout: conn.timeout,
        // A test tool's graceful window can be short.
        graceful_shutdown: Duration::from_secs(5),
    }
}

/// The standard driver for data subcommands: start → feed (paced) → drain → read → report.
///
/// `now` is captured for the elapsed field; the caller has already resolved `events` and `udf`.
pub async fn run_data_subcommand(
    udf: LocalUdf,
    opts: LocalRunOpts,
    events: Vec<InputEvent>,
    feed: &FeedArgs,
    output: &OutputArgs,
) -> CliResult<()> {
    let start = std::time::Instant::now();
    let sent = events.len();

    // Startup error → exit 2 (mapped from LocalError::Startup).
    let mut run = LocalRun::start(udf, opts).await?;

    // Feed in --batch-size chunks, sleeping --delay between them. A forwarder error surfaces here
    // (exit 4) or in drain.
    for chunk in events.chunks(feed.batch_size.max(1)) {
        run.feed(chunk.to_vec()).await?;
        if !feed.delay.is_zero() {
            tokio::time::sleep(feed.delay).await;
        }
    }

    // Drain → DrainTimeout maps to exit 3.
    let drain_result = run.drain(feed.drain_timeout).await;
    let stuck = match &drain_result {
        Ok(report) => report.stuck,
        Err(_) => 0, // reported via the error's exit code path below
    };
    // Surface a drain error before reading outputs (so exit 3/4 wins over a partial read).
    drain_result?;

    let events_out = run.read_outputs().await?;
    run.stop().await?;

    let rendered = Rendered {
        events: events_out,
        sent,
        elapsed: start.elapsed(),
        stuck,
    };
    render(&rendered, output.output);
    Ok(())
}
