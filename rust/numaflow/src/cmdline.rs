use clap::{Command, arg};

pub(super) fn root_cli() -> Command {
    Command::new("numaflow")
        .author("Numaflow Authors")
        .about("Numaflow is a stream processing framework")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(add_monitor_subcommand())
        .subcommand(add_serving_subcommand())
        .subcommand(add_processor_subcommand())
}

fn add_processor_subcommand() -> Command {
    Command::new("processor")
        .about("Processor for Numaflow (Pipeline/MonoVertex)")
        .arg(
            arg!(--"type" <TYPE> "Type of the vertex")
                .value_parser(["Sink", "Source", "MapUDF", "ReduceUDF"])
                .required(false),
        )
        .arg(
            arg!(--"isbsvc-type" <ISBSVC_TYPE> "Type of the ISB service")
                .value_parser(["jetstream", "redis", ""])
                .required(false),
        )
}

fn add_monitor_subcommand() -> Command {
    Command::new("monitor").about("Monitor exposes monitoring endpoints")
}

fn add_serving_subcommand() -> Command {
    Command::new("serving").about("Serving System for Numaflow")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_cli() {
        root_cli().debug_assert();
    }
}
