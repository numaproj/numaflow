use std::env;
use std::error::Error;

use tracing::error;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
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
    if let Err(e) = run().await {
        error!("{e:?}");
        return Err(e);
    }
    Ok(())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    // Based on the argument, run the appropriate component.
    if args.contains(&"--servesink".to_string()) {
        servesink::servesink()
            .await
            .map_err(|e| format!("Error running servesink: {e:?}"))?;
    } else if args.contains(&"--rust".to_string()) {
        numaflow_core::run()
            .await
            .map_err(|e| format!("Error running rust binary: {e:?}"))?;
    } else {
        return Err(format!(
            "Invalid argument. Use --servesink, or --rust. Current args = {:?}",
            args
        )
        .into());
    }
    Ok(())
}
