use std::env;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    // Set up the tracing subscriber. RUST_LOG can be used to set the log level.
    // The default log level is `info`. The `axum::rejection=trace` enables showing
    // rejections from built-in extractors at `TRACE` level.
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                // TODO: add a better default based on entry point invocation
                //   e.g., serving/monovertex might need a different default
                .unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_ansi(false))
        .init();

    // Based on the argument, run the appropriate component.
    if args.contains(&"--serving".to_string()) {
        if let Err(e) = serving::serve().await {
            error!("Error running serving: {}", e);
        }
    } else if args.contains(&"--servesink".to_string()) {
        if let Err(e) = servesink::servesink().await {
            info!("Error running servesink: {}", e);
        }
    } else if args.contains(&"--monovertex".to_string()) {
        monovertex::mono_vertex().await;
    } else {
        error!("Invalid argument. Use --serve, --servesink, or --monovertex.");
    }
}
