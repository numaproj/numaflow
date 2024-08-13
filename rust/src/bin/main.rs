use std::env;
use tracing::{error, info};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    // Based on the argument, run the appropriate component.
    if args.contains(&"--serving".to_string()) {
        serving::serve().await;
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
