//! `reduce` / `session-reduce` / `accumulator` subcommand drivers.
//!
//! All three share the data-subcommand loop; only the window shape and its server-info basename
//! differ. The facade handles watermark emulation, so nothing reduce-specific happens here beyond
//! translating flags into a `LocalWindow`.

use numaflow_core::local::{LocalUdf, LocalWindow};

use crate::cli::{AccumulatorArgs, ReduceArgs, SessionReduceArgs, WindowKind};
use crate::error::{CliError, CliResult};
use crate::input::{self, InputContext};
use crate::run::common::{resolve_server_info, run_data_subcommand, run_opts};

pub async fn run_reduce(args: ReduceArgs) -> CliResult<()> {
    let (window, align_to) = match args.window {
        WindowKind::Fixed => {
            if args.slide.is_some() {
                tracing::warn!("--slide is ignored for a fixed window");
            }
            (
                LocalWindow::Fixed {
                    length: args.length,
                },
                Some(args.length),
            )
        }
        WindowKind::Sliding => {
            let slide = args.slide.ok_or_else(|| {
                CliError::Usage("--slide is required when --window sliding".to_string())
            })?;
            (
                LocalWindow::Sliding {
                    length: args.length,
                    slide,
                },
                // Align the base time to the slide so relative event times fall on boundaries.
                Some(slide),
            )
        }
    };

    let server_info = resolve_server_info(&args.conn, reducer_server_info(args.window));
    let events = input::build_events(
        &args.input,
        InputContext {
            reduce_family: true,
            align_to,
        },
    )?;

    let opts = run_opts(&args.conn, &args.feed);
    run_data_subcommand(
        LocalUdf::Reduce {
            socket_path: args.conn.socket.clone(),
            server_info_path: server_info,
            window,
            keyed: true,
            allowed_lateness: args.allowed_lateness,
        },
        opts,
        events,
        &args.feed,
        &args.output,
    )
    .await
}

pub async fn run_session(args: SessionReduceArgs) -> CliResult<()> {
    let server_info = resolve_server_info(&args.conn, "sessionreducer-server-info");
    let events = input::build_events(
        &args.input,
        InputContext {
            reduce_family: true,
            // Session windows are gap-based, not aligned; no base-time flooring.
            align_to: None,
        },
    )?;

    let opts = run_opts(&args.conn, &args.feed);
    run_data_subcommand(
        LocalUdf::Reduce {
            socket_path: args.conn.socket.clone(),
            server_info_path: server_info,
            window: LocalWindow::Session { gap: args.gap },
            keyed: true,
            allowed_lateness: args.allowed_lateness,
        },
        opts,
        events,
        &args.feed,
        &args.output,
    )
    .await
}

pub async fn run_accumulator(args: AccumulatorArgs) -> CliResult<()> {
    let server_info = resolve_server_info(&args.conn, "accumulator-server-info");
    let events = input::build_events(
        &args.input,
        InputContext {
            reduce_family: true,
            align_to: None,
        },
    )?;

    let opts = run_opts(&args.conn, &args.feed);
    run_data_subcommand(
        LocalUdf::Reduce {
            socket_path: args.conn.socket.clone(),
            server_info_path: server_info,
            window: LocalWindow::Accumulator {
                timeout: args.timeout,
            },
            keyed: true,
            allowed_lateness: args.allowed_lateness,
        },
        opts,
        events,
        &args.feed,
        &args.output,
    )
    .await
}

/// Fixed and sliding aligned reducers use the same default server-info basename.
fn reducer_server_info(_window: WindowKind) -> &'static str {
    "reducer-server-info"
}
