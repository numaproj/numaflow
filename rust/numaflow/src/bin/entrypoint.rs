use std::env;
use std::error::Error;
use std::os::unix::process::CommandExt;
use std::process::Command;

const NUMAFLOW_GOLANG: &str = "/bin/numaflow";
const NUMAFLOW_RUST: &str = "/bin/numaflow-rs";

fn start_golang() -> Result<(), Box<dyn Error>> {
    let mut cmd = Command::new(NUMAFLOW_GOLANG);
    Err(cmd.args(env::args()).envs(env::vars()).exec().into())
}

fn start_rust() -> Result<(), Box<dyn Error>> {
    let mut cmd = Command::new(NUMAFLOW_RUST);
    Err(cmd.args(env::args()).envs(env::vars()).exec().into())
}

fn main() -> Result<(), Box<dyn Error>> {
    // start Golang if NUMAFLOW_RUNTIME == Golang.
    // This is a temporary solution until port the entire dataplane to rust.
    let is_golang = env::var("NUMAFLOW_RUNTIME").unwrap_or("golang".to_string());
    if is_golang == "golang" {
        start_golang()
    } else {
        start_rust()
    }
}
