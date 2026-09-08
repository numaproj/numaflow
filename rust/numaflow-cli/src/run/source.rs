//! `source` subcommand driver. No input file — read N results (or for a duration) and print.

use std::time::Duration;

use numaflow_core::local::{LocalRun, LocalUdf};

use crate::cli::SourceArgs;
use crate::error::CliResult;
use crate::output::{Rendered, render};
use crate::run::common::{resolve_server_info, run_opts};

/// Poll interval while waiting for source output.
const POLL: Duration = Duration::from_millis(50);

pub async fn run(args: SourceArgs) -> CliResult<()> {
    let server_info = resolve_server_info(&args.conn, "sourcer-server-info");
    // Source has no feed pacing; reuse the connection knobs with defaults for the rest.
    let opts = run_opts(&args.conn, &default_feed());

    let start = std::time::Instant::now();
    let mut run = LocalRun::start(
        LocalUdf::Source {
            socket_path: args.conn.socket.clone(),
            server_info_path: server_info,
        },
        opts,
    )
    .await?;

    let mut collected = Vec::new();
    let duration_deadline = args.duration.map(|d| start + d);

    // Read incrementally: the source forwarder reads → writes to the output buffer → acks the
    // source. We drain the output buffer until we hit --count, --duration elapses, or the forwarder
    // stops producing.
    loop {
        let batch = run.read_outputs().await?;
        collected.extend(batch);

        if collected.len() >= args.count {
            collected.truncate(args.count);
            break;
        }
        if duration_deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            break;
        }
        tokio::time::sleep(POLL).await;
    }

    if args.pending {
        match run.source_pending() {
            Some(p) => eprintln!("pending={p}"),
            None => eprintln!("pending=<unavailable>"),
        }
    }

    run.stop().await?;

    let sent = collected.len();
    let rendered = Rendered {
        events: collected,
        // For source there is no "sent" input; report results==read count.
        sent,
        elapsed: start.elapsed(),
        stuck: 0,
    };
    render(&rendered, args.output.output);
    Ok(())
}

/// Source has no `FeedArgs`; synthesize defaults for the shared `run_opts` helper.
fn default_feed() -> crate::cli::FeedArgs {
    crate::cli::FeedArgs {
        drain_timeout: Duration::from_secs(30),
        batch_size: 500,
        delay: Duration::ZERO,
        buffer_capacity: 30_000,
    }
}
