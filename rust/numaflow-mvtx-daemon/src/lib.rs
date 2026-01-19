use std::{error::Error, thread};

pub async fn run() -> Result<(), Box<dyn Error>> {
    // a dummy async task
    use std::time::Duration;

    _ = thread::spawn(|| {
        // Placeholder for the actual implementation
        // Sleep for 15 mins.
        thread::sleep(Duration::from_millis(900));
    });

    Ok(())
}
