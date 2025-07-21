use cmdline::sideinput;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use tokio::signal;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

mod setup_tracing;

/// Build the command line interface.
mod cmdline;

const VERSION_INFO: &str = env!("NUMAFLOW_VERSION_INFO");

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

    let cln_token = CancellationToken::new();
    let shutdown_cln_token = cln_token.clone();

    // wait for SIG{INT,TERM} and invoke cancellation token.
    let shutdown_handle: JoinHandle<Result<(), Box<dyn Send + Sync + Error>>> =
        tokio::spawn(async move {
            shutdown_signal().await;
            shutdown_cln_token.cancel();
            Ok(())
        });

    match cli_matches.subcommand() {
        Some(("monitor", _)) => {
            info!(VERSION_INFO, "Starting monitor");
            numaflow_monitor::run()
                .await
                .map_err(|e| format!("Error running monitor binary: {e:?}"))?;
        }
        Some(("serving", _)) => {
            info!(VERSION_INFO, "Starting serving");
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
            info!(VERSION_INFO, "Starting processing pipeline");
            numaflow_core::run()
                .await
                .map_err(|e| format!("Error running core binary: {e:?}"))?;
        }
        Some(("side-input", args)) => {
            info!(VERSION_INFO, "Starting side input");
            sideinput::run_sideinput(args, cln_token).await?;
        }
        others => {
            info!(VERSION_INFO, "Numaflow");
            return Err(format!("Invalid subcommand {others:?}").into());
        }
    }

    // abort the shutdown handle since we are done processing
    if !shutdown_handle.is_finished() {
        shutdown_handle.abort();
    }

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        info!("Received Ctrl+C signal");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
        info!("Received terminate signal");
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
