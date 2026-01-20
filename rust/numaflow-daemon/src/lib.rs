use std::error::Error;
use tracing::info;

pub async fn run(mvtx_name: String) -> Result<(), Box<dyn Error>> {
    info!("MonoVertex name is {}", mvtx_name);

    Ok(())
}
