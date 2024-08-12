use std::env;
use tracing::{error, info};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    // Based on the argument, run the appropriate component.
    if args.contains(&"--serve".to_string()) {
        serving::serve().await;
    } else if args.contains(&"--servesink".to_string()) {
        if let Err(e) = servesink::servesink().await {
            info!("Error running servesink: {}", e);
        }
    } else if args.contains(&"--mono-vertex".to_string()) {
        mono_vertex::mono_vertex().await;
    } else {
        error!("Invalid argument. Use --serve, --servesink, or --mono-vertex.");
    }
}