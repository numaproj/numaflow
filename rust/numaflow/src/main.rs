use numaflow_sideinput::SideInputMode;
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
        Some(("sideinput", args)) => match args.subcommand() {
            Some(("initializer", args)) => {
                info!("Starting sideinput initializer");
                let side_inputs: Vec<&'static str> = args
                    .get_many::<String>("side-inputs")
                    .unwrap()
                    .map(|s| &*Box::leak(s.clone().into_boxed_str()))
                    .collect();
                let side_input_store = args.get_one::<String>("side-inputs-store").unwrap();
                let side_input_store = Box::leak(side_input_store.clone().into_boxed_str());

                let mode = SideInputMode::Initializer {
                    side_inputs,
                    side_input_store,
                };
                numaflow_sideinput::run(mode, cln_token).await?;
            }
            Some(("synchronizer", args)) => {
                info!("Starting sideinput synchronizer");
                let side_inputs: Vec<&str> = args
                    .get_many::<String>("side-inputs")
                    .expect("side-inputs is required")
                    .map(|s| &*Box::leak(s.clone().into_boxed_str()))
                    .collect();
                let side_input_store = args.get_one::<String>("side-inputs-store").unwrap();
                let side_input_store = Box::leak(side_input_store.clone().into_boxed_str());

                let mode = SideInputMode::Synchronizer {
                    side_inputs,
                    side_input_store,
                };
                numaflow_sideinput::run(mode, cln_token).await?;
            }
            Some(("manager", args)) => {
                info!("Starting sideinput manager");
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
                numaflow_sideinput::run(mode, cln_token).await?;
            }
            _ => {
                return Err("Unknown sideinput subcommand".into());
            }
        },
        _ => {
            info!("Starting processing pipeline");
            numaflow_core::run()
                .await
                .map_err(|e| format!("Error running core binary: {e:?}"))?;
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
