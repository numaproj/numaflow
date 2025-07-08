use std::env;
use std::error::Error;
use std::os::unix::process::CommandExt;
use std::process::Command;

const NUMAFLOW_GOLANG: &str = "/bin/numaflow";
const NUMAFLOW_RUST: &str = "/bin/numaflow-rs";

fn main() -> Result<(), Box<dyn Error>> {
    // Determine the runtime based on the NUMAFLOW_RUNTIME environment variable
    let runtime = env::var("NUMAFLOW_RUNTIME").unwrap_or_else(|_| "golang".to_string());

    // Choose the appropriate binary to execute
    let binary = if runtime == "rust" {
        NUMAFLOW_RUST
    } else {
        NUMAFLOW_GOLANG
    };

    // Collect arguments, skipping the first one (binary name) because we will replace the process
    // with the chosen binary.
    let args: Vec<String> = env::args().skip(1).collect();

    // Replace the current process with the chosen binary
    let mut cmd = Command::new(binary);
    Err(cmd.args(args).envs(env::vars()).exec().into())
}
