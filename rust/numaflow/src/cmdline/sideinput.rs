use clap::{Arg, ArgAction, Command};

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
            "input2",
            "--side-inputs-store",
            "store1",
        ]);

        assert!(match1.is_ok());
    }
}
