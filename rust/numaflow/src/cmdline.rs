use clap::Command;

/// SideInput Command Line Interface
mod sideinput;

pub(super) fn root_cli() -> Command {
    Command::new("numaflow")
        .author("Numaflow Authors")
        .about("Numaflow is a stream processing framework for K8s")
        .long_about("https://numaflow.numaproj.io/")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(add_monitor_subcommand())
        .subcommand(add_serving_subcommand())
        .subcommand(sideinput::add_sideinput_subcommand())
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
