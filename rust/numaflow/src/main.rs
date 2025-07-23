use cmdline::sideinput;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use tokio::task::JoinHandle;
use tokio::{runtime, signal};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

mod setup_tracing;

/// Build the command line interface.
mod cmdline;

const VERSION_INFO: &str = env!("NUMAFLOW_VERSION_INFO");

fn main() {
    setup_tracing::register();
    // Setup the CryptoProvider (controls core cryptography used by rustls) for the process
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Installing default CryptoProvider");

    // Tokio runtime automatically spins up N number of worker threads based on the available cpu cores.
    // In a k8s environment, this value is calculated based on the `resources.limits.cpu` value. If it is not set,
    // the worker thread count will be equivalent to the logical cores available on the host node.
    // On a node with 32 cores, the memory usage of numa container stays at around 120MB to 140MB
    // when `resources.limits.cpu` is not set. If set explicitly to 1 CPU or less, the memory usage
    // stays around 20MB.
    // The `NUMAFLOW_CPU_REQUEST` environment variable is set by the controller as a reference to
    // the `resource.requests.cpu`. We can not use the `NUMAFLOW_CPU_LIMITS` (references
    // `resources.limits.cpu`) value, since its value will be host's logical core count if not set
    // explicitly.
    // User may specify a higher value by setting NUMAFLOW_CPU_REQUEST in `containerTemplate.env`
    // section for the vertex.
    let cpu_core_count = env::var("NUMAFLOW_CPU_REQUEST").unwrap_or_else(|_| "1".into());
    let worker_thread_count = cpu_core_count.parse::<usize>().inspect_err(|e| {
        warn!(integer_conversion_error=?e, "The value of NUMAFLOW_CPU_REQUEST environment variable should be a valid unsigned integer. Worker thread count will be set to 1");
    }).unwrap_or(1).max(1);

    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(worker_thread_count)
        .build()
        .unwrap();

    info!(
        VERSION_INFO,
        tokio_worker_threads = worker_thread_count,
        "Numaflow runtime details"
    );

    let cli = cmdline::root_cli();

    rt.block_on(async move {
        if let Err(e) = run(cli).await {
            error!("{e:?}");
            std::process::exit(1);
        }
    });

    info!("Exited.");
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
        Some(("processor", _)) => {
            info!("Starting processing pipeline");
            numaflow_core::run()
                .await
                .map_err(|e| format!("Error running core binary: {e:?}"))?;
        }
        Some(("side-input", args)) => {
            info!("Starting side input");
            sideinput::run_sideinput(args, cln_token).await?;
        }
        others => {
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
