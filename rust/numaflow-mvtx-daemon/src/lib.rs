use std::{error::Error, thread};
use tracing::info;

pub async fn run(mvtx_name: String) -> Result<(), Box<dyn Error>> {
    // a dummy async task
    use std::time::Duration;

    info!("MonoVertex name is {}", mvtx_name);

    _ = thread::spawn(|| {
        // Placeholder for the actual implementation
        // Sleep for 15 mins.
        thread::sleep(Duration::from_millis(900));
    });

    Ok(())
}
