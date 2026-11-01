//! `transform` subcommand driver. Feed goes to the replay source inside the facade.

use numaflow_core::local::LocalUdf;

use crate::cli::TransformArgs;
use crate::error::CliResult;
use crate::input::{self, InputContext};
use crate::run::common::{resolve_server_info, run_data_subcommand, run_opts};

pub async fn run(args: TransformArgs) -> CliResult<()> {
    let server_info = resolve_server_info(&args.conn, "sourcetransformer-server-info");
    let events = input::build_events(
        &args.input,
        InputContext {
            reduce_family: false,
            align_to: None,
        },
    )?;

    let opts = run_opts(&args.conn, &args.feed);
    run_data_subcommand(
        LocalUdf::Transform {
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
