//! `sink` subcommand driver.

use std::path::PathBuf;

use numaflow_core::local::LocalUdf;

use crate::cli::SinkArgs;
use crate::error::{CliError, CliResult};
use crate::input::{self, InputContext};
use crate::run::common::{resolve_server_info, run_data_subcommand, run_opts};

pub async fn run(args: SinkArgs) -> CliResult<()> {
    let server_info = resolve_server_info(&args.conn, "sinker-server-info");
    let events = input::build_events(
        &args.input,
        InputContext {
            reduce_family: false,
            align_to: None,
        },
    )?;

    // Each secondary sink needs both its socket and (derived-or-explicit) server-info together.
    let fallback = pair(
        args.fallback_socket.clone(),
        args.fallback_server_info.clone(),
        "fallback",
        "fb-sinker-server-info",
    )?;
    let on_success = pair(
        args.on_success_socket.clone(),
        args.on_success_server_info.clone(),
        "on-success",
        "ons-sinker-server-info",
    )?;

    let opts = run_opts(&args.conn, &args.feed);
    run_data_subcommand(
        LocalUdf::Sink {
            socket_path: args.conn.socket.clone(),
            server_info_path: server_info,
            fallback,
            on_success,
        },
        opts,
        events,
        &args.feed,
        &args.output,
    )
    .await
}

/// Combine a secondary sink's socket + server-info into a pair, deriving the server-info from the
/// socket directory when it is not given. It is a usage error to pass server-info without a socket.
fn pair(
    socket: Option<PathBuf>,
    server_info: Option<PathBuf>,
    label: &str,
    default_basename: &str,
) -> CliResult<Option<(PathBuf, PathBuf)>> {
    match (socket, server_info) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(CliError::Usage(format!(
            "--{label}-server-info requires --{label}-socket"
        ))),
        (Some(sock), info) => {
            let info = info.unwrap_or_else(|| {
                let dir = sock.parent().map(|p| p.to_path_buf()).unwrap_or_default();
                dir.join(default_basename)
            });
            Ok(Some((sock, info)))
        }
    }
}
