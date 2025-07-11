use clap::{Arg, ArgAction, Command};
use numaflow_sideinput::SideInputMode;
use std::error::Error;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub(super) fn add_sideinput_subcommand() -> Command {
    Command::new("sideinput")
        .about("SideInput System for Numaflow")
        .subcommand_required(true)
        .subcommand(manager_subcmd())
        .subcommand(synchronizer_subcmd())
        .subcommand(initializer_subcmd())
}

fn initializer_subcmd() -> Command {
    Command::new("initializer")
        .about("SideInput Initializer")
        .arg_required_else_help(true)
        .arg(
            Arg::new("side-inputs")
                .long("side-inputs")
                .help("Side inputs it has to synchronize")
                .required(true)
                .num_args(1..)
                .action(ArgAction::Append)
                .value_delimiter(',')
                .value_parser(clap::value_parser!(String)),
        )
        .arg(
            Arg::new("side-inputs-store")
                .long("side-inputs-store")
                .help("Name of the side input store in the ISB")
                .required(true)
                .action(ArgAction::Set)
                .value_parser(clap::value_parser!(String)),
        )
        .arg(
            Arg::new("isbsvc-type")
                .long("isbsvc-type")
                .help("ISB Service type, e.g. jetstream")
                .default_value("jetstream"),
        )
}

fn manager_subcmd() -> Command {
    Command::new("manager")
        .about("SideInput Manager")
        .arg_required_else_help(true)
        .arg(
            Arg::new("side-inputs-store")
                .long("side-inputs-store")
                .help("Name of the side input store in the ISB")
                .required(true)
                .action(ArgAction::Set)
                .value_parser(clap::value_parser!(String)),
        )
        .arg(
            Arg::new("isbsvc-type")
                .long("isbsvc-type")
                .help("ISB Service type, e.g. jetstream")
                .default_value("jetstream"),
        )
}

fn synchronizer_subcmd() -> Command {
    Command::new("synchronize")
        .about("SideInput Synchronizer")
        .arg_required_else_help(true)
        .arg(
            Arg::new("side-inputs")
                .long("side-inputs")
                .help("Side inputs it has to synchronize")
                .required(true)
                .num_args(1..)
                .action(ArgAction::Append)
                .value_delimiter(',')
                .value_parser(clap::value_parser!(String)),
        )
        .arg(
            Arg::new("side-inputs-store")
                .long("side-inputs-store")
                .help("Name of the side input store in the ISB")
                .required(true)
                .action(ArgAction::Set)
                .value_parser(clap::value_parser!(String)),
        )
        .arg(
            Arg::new("isbsvc-type")
                .long("isbsvc-type")
                .help("ISB Service type, e.g. jetstream")
                .default_value("jetstream"),
        )
}

pub(crate) async fn run_sideinput(
    args: &clap::ArgMatches,
    cln_token: CancellationToken,
) -> Result<(), Box<dyn Error>> {
    match args.subcommand() {
        Some(("initializer", args)) => {
            info!("Starting side-input initializer");
            let side_inputs: Vec<&'static str> = args
                .get_many::<String>("side-inputs")
                .unwrap()
                .map(|s| Box::leak(s.clone().into_boxed_str()) as &str)
                .collect();
            let side_input_store = args.get_one::<String>("side-inputs-store").unwrap();
            let side_input_store = Box::leak(side_input_store.clone().into_boxed_str());

            let mode = SideInputMode::Synchronizer {
                side_inputs,
                side_input_store,
                run_once: true,
            };
            Ok(numaflow_sideinput::run(mode, cln_token).await?)
        }
        Some(("synchronizer", args)) => {
            info!("Starting side-input synchronizer");
            let side_inputs: Vec<&str> = args
                .get_many::<String>("side-inputs")
                .expect("side-inputs is required")
                .map(|s| Box::leak(s.clone().into_boxed_str()) as &str)
                .collect();
            let side_input_store = args
                .get_one::<String>("side-inputs-store")
                .expect("side-inputs-store is required");
            let side_input_store = Box::leak(side_input_store.clone().into_boxed_str());

            let mode = SideInputMode::Synchronizer {
                side_inputs,
                side_input_store,
                run_once: false,
            };
            Ok(numaflow_sideinput::run(mode, cln_token).await?)
        }
        Some(("manager", args)) => {
            info!("Starting side-input manager");
            let side_input_store = args
                .get_one::<String>("side-inputs-store")
                .expect("side-inputs-store is required");
            let side_input_store = Box::leak(side_input_store.clone().into_boxed_str());
            let side_input = args
                .get_one::<String>("side-input")
                .expect("side-input is required");
            let side_input = Box::leak(side_input.clone().into_boxed_str());

            let mode = SideInputMode::Manager {
                side_input_store,
                side_input,
            };
            Ok(numaflow_sideinput::run(mode, cln_token).await?)
        }
        other => Err(format!("Unknown side-input {other:?} subcommand").into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_synchronizer_subcmd_cli() {
        add_sideinput_subcommand().debug_assert();

        let match1 = synchronizer_subcmd().try_get_matches_from(vec![
            "synchronizer",
            "--side-inputs",
            "input1",
            "--side-inputs",
            "input2,input3",
            "--side-inputs-store",
            "store1",
        ]);

        assert!(match1.is_ok());
        let matches = match1.unwrap();
        assert_eq!(
            matches
                .get_many::<String>("side-inputs")
                .unwrap()
                .collect::<Vec<_>>(),
            vec!["input1", "input2", "input3"]
        );
        assert_eq!(
            matches.get_one::<String>("side-inputs-store").unwrap(),
            "store1"
        );
        assert_eq!(
            matches.get_one::<String>("isbsvc-type").unwrap(),
            "jetstream"
        );
    }
}
