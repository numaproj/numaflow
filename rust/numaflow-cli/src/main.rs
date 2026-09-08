//! `nfcli` — test Numaflow UDFs locally by running the real vertex forwarder against an in-memory
//! ISB. See `rust/numaflow-cli-v2.md` for the design.

mod cli;
mod conn;
mod error;
mod input;
mod output;
mod run;

use clap::Parser;

use crate::cli::{Cli, Command};
use crate::error::CliResult;

fn main() {
    // Note (G14): production numaflow reads a readable vertex/pipeline name from
    // `NUMAFLOW_VERTEX_NAME` / `NUMAFLOW_PIPELINE_NAME` for metric/tracing labels, via `OnceLock`
    // accessors that freeze on first read. Setting env vars is `unsafe` on edition 2024 and the
    // workspace forbids `unsafe_code`, so we do NOT set them here — the labels default to
    // "default", which is purely cosmetic. The facade's `PipelineConfig` already uses the fixed
    // `nfcli` identity for all functional purposes.

    // Parse manually so clap's usage/parse errors exit with code 1 (v1's contract), not clap's
    // default of 2 (which we reserve for "server not reachable"). `--help`/`--version` still exit 0.
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            let _ = e.print();
            let code = match e.kind() {
                clap::error::ErrorKind::DisplayHelp
                | clap::error::ErrorKind::DisplayVersion
                | clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand => 0,
                _ => 1,
            };
            std::process::exit(code);
        }
    };
    init_tracing(cli.verbose, cli.quiet);

    // Build a tokio runtime explicitly so `main` can map errors to exit codes without `?`.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    let result = runtime.block_on(dispatch(cli.command));

    if let Err(e) = result {
        // Diagnostics to stderr; the code carries the machine-readable outcome.
        eprintln!("error: {e}");
        std::process::exit(e.exit_code());
    }
}

/// Route the parsed subcommand to its driver.
async fn dispatch(command: Command) -> CliResult<()> {
    match command {
        Command::Map(args) => run::map::run(args).await,
        Command::Transform(args) => run::transform::run(args).await,
        Command::Reduce(args) => run::reduce::run_reduce(args).await,
        Command::SessionReduce(args) => run::reduce::run_session(args).await,
        Command::Accumulator(args) => run::reduce::run_accumulator(args).await,
        Command::Sink(args) => run::sink::run(args).await,
        Command::Source(args) => run::source::run(args).await,
        Command::SideInput(args) => run::sideinput::run(args.socket, args.server_info).await,
        Command::Ready(args) => run::ready::run(args).await,
    }
}

/// Initialize tracing. `-v` opens up numaflow-core + nfcli debug logs (the window into wire-level
/// behavior); default is `warn`; `-q` silences everything but errors.
fn init_tracing(verbose: bool, quiet: bool) {
    use tracing_subscriber::EnvFilter;
    let filter = if quiet {
        EnvFilter::new("error")
    } else if verbose {
        EnvFilter::new("numaflow_core=debug,nfcli=debug")
    } else {
        // Respect RUST_LOG if set, else default to warn.
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"))
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        // Logs are diagnostics → stderr, keeping stdout clean for the rendered output.
        .with_writer(std::io::stderr)
        .init();
}
