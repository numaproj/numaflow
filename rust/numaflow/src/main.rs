use std::collections::HashMap;
use std::env;
use std::error::Error;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        // Set up the tracing subscriber. RUST_LOG can be used to set the log level.
        // The default log level is `info`. The `axum::rejection=trace` enables showing
        // rejections from built-in extractors at `TRACE` level.
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    // TODO: add a better default based on entry point invocation
                    //  e.g., serving/monovertex might need a different default
                    .unwrap_or_else(|_| "info".into()),
            )
            .with(tracing_subscriber::fmt::layer().with_ansi(false))
            .init();

        // Setup the CryptoProvider (controls core cryptography used by rustls) for the process
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("Installing default CryptoProvider");

        if let Err(e) = run().await {
            error!("{e:?}");
            std::process::exit(1);
        }

        info!("Exited.");
    })
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
