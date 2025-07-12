use std::collections::HashMap;
use std::env;
use std::error::Error;
use tracing::{error, info};
mod setup_tracing;

/// Build the command line interface.
mod cmdline;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    setup_tracing::register();

    // Setup the CryptoProvider (controls core cryptography used by rustls) for the process
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Installing default CryptoProvider");

    let cli = cmdline::root_cli();

    if let Err(e) = run(cli).await {
        error!("{e:?}");
        return Err(e);
    }

    info!("Exited.");

    Ok(())
}

async fn run(cli: clap::Command) -> Result<(), Box<dyn Error>> {
    let cli_matches = cli.get_matches();

    match cli_matches.subcommand() {
        Some(("monitor", _)) => {
            info!("Starting monitor");
            numaflow_monitor::run()
                .await
                .map_err(|e| format!("Error running monitor binary: {e:?}"))?;
        }
        Some(("serving", _)) => {
            info!("Starting serving");
            if env::var(serving::ENV_MIN_PIPELINE_SPEC).is_err() {
                return Err(
                    "Environment variable NUMAFLOW_SERVING_MIN_PIPELINE_SPEC is not set".into(),
                );
            }
            let vars: HashMap<String, String> = env::vars().collect();
            let cfg: serving::Settings = vars.try_into().unwrap();
            serving::run(cfg).await?;
        }
        Some(("processor", _)) => {
            info!("Starting processing pipeline");
            numaflow_core::run()
                .await
                .map_err(|e| format!("Error running core binary: {e:?}"))?;
        }
        others => {
            return Err(format!("Invalid subcommand {others:?}").into());
        }
    }

    Ok(())
}
