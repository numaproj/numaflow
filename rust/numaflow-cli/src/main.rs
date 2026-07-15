//! `nfcli` — a standalone, non-interactive CLI that impersonates numa and drives a UDF gRPC
//! server directly so UDF authors can test their code without deploying a pipeline.

mod cli;
mod drivers;
mod exit;
mod input;
mod message;
mod output;
mod ready;
mod transport;
mod windower;

use std::io::IsTerminal;

use clap::{CommandFactory, Parser};

use cli::{Cli, Command};
use exit::{CliResult, ExitCode};

#[tokio::main]
async fn main() {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => match e.kind() {
            clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion => {
                e.exit()
            }
            _ => {
                let _ = e.print();
                std::process::exit(ExitCode::Usage.code());
            }
        },
    };
    let verbose = cli.output.verbose;
    let code = match dispatch(cli).await {
        Ok(()) => ExitCode::Ok,
        Err(e) => {
            e.render(verbose, std::io::stderr().is_terminal());
            e.code
        }
    };
    std::process::exit(code.code());
}

async fn dispatch(cli: Cli) -> CliResult<()> {
    let tuning = &cli.tuning;
    let output = &cli.output;
    match cli.command {
        Command::Ready(args) => ready::run(args, tuning, output).await,
        Command::Map(args) => drivers::map::run(args, tuning, output).await,
        Command::Transform(args) => drivers::transform::run(args, tuning, output).await,
        Command::Sink(args) => drivers::sink::run(args, tuning, output).await,
        Command::Source(args) => drivers::source::run(args, tuning, output).await,
        Command::SideInput(args) => drivers::sideinput::run(args, tuning, output).await,
        Command::Reduce(args) => drivers::reduce::run(args, tuning, output).await,
        Command::Accumulator(args) => drivers::accumulator::run(args, tuning, output).await,
        Command::SessionReduce(args) => {
            drivers::session_reduce::run(args.data, args.gap, tuning, output).await
        }
        Command::Completions { shell } => {
            clap_complete::generate(shell, &mut Cli::command(), "nfcli", &mut std::io::stdout());
            Ok(())
        }
    }
}
