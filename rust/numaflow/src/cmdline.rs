use clap::Command;

pub(super) fn root_cli() -> Command {
    Command::new("numaflow")
        .author("Numaflow Authors")
        .about("Numaflow is a stream processing framework")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(add_monitor_subcommand())
        .subcommand(add_serving_subcommand())
        // TODO: remove after moving e2e to Rust, this is a hack
        .allow_external_subcommands(true)
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
