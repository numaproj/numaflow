//! `map` subcommand driver.

use numaflow_core::local::LocalUdf;

use crate::cli::MapArgs;
use crate::error::CliResult;
use crate::input::{self, InputContext};
use crate::run::common::{resolve_server_info, run_data_subcommand, run_opts};

pub async fn run(args: MapArgs) -> CliResult<()> {
    let server_info = resolve_server_info(&args.conn, "mapper-server-info");
    let events = input::build_events(
        &args.input,
        InputContext {
            reduce_family: false,
            align_to: None,
        },
    )?;

    // `--mode` is an assertion, not an override: the authoritative map mode comes from server-info
    // inside the production `create_mapper`, and the Phase 1 fix means the configured socket is
    // honored regardless. A true assertion needs the facade to surface the pre-flighted
    // `ServerInfo`, which is future work; for now the flag is accepted and logged.
    if let Some(mode) = args.mode {
        tracing::debug!(
            ?mode,
            "--mode requested; server-info map mode is authoritative"
        );
    }

    let opts = run_opts(&args.conn, &args.feed);
    run_data_subcommand(
        LocalUdf::Map {
            socket_path: args.conn.socket.clone(),
            server_info_path: server_info,
        },
        opts,
        events,
        &args.feed,
        &args.output,
    )
    .await
}
