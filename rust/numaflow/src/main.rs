use std::collections::HashMap;
use std::env;
use std::error::Error;
use tracing::{error, info};
mod setup_tracing;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    setup_tracing::register();

    // Setup the CryptoProvider (controls core cryptography used by rustls) for the process
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Installing default CryptoProvider");

    if let Err(e) = run().await {
        error!("{e:?}");
        return Err(e);
    }
    info!("Exiting...");

    Ok(())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    // Based on the argument, run the appropriate component.
    if args.contains(&"--monitor".to_string()) {
        numaflow_monitor::run()
            .await
            .map_err(|e| format!("Error running monitor binary: {e:?}"))?;
        return Ok(());
    }

    if args.contains(&"--serving".to_string()) {
        if env::var(serving::ENV_MIN_PIPELINE_SPEC).is_ok() {
            let vars: HashMap<String, String> = env::vars().collect();
            let cfg: serving::Settings = vars.try_into().unwrap();
            serving::run(cfg).await?;
        }
        return Ok(());
    }

    info!(?args, "Starting with args");
    numaflow_core::run()
        .await
        .map_err(|e| format!("Error running rust binary: {e:?}"))?;

    Ok(())
}
