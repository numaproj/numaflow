//! `side-input` subcommand: a single unary `RetrieveSideInput` call. No facade involved.

use std::path::PathBuf;

use numaflow_pb::clients::sideinput::side_input_client::SideInputClient;

use crate::conn::connect_uds;
use crate::error::{CliError, CliResult};

pub async fn run(socket: PathBuf, _server_info: Option<PathBuf>) -> CliResult<()> {
    let channel = connect_uds(socket)
        .await
        .map_err(|e| CliError::NotReady(format!("failed to connect to side-input server: {e}")))?;
    let mut client = SideInputClient::new(channel);

    let response = client
        .retrieve_side_input(())
        .await
        .map_err(|e| CliError::Command(format!("RetrieveSideInput failed: {e}")))?
        .into_inner();

    // Print the value (UTF-8 if valid, else base64) and the broadcast flag.
    let value = match std::str::from_utf8(&response.value) {
        Ok(s) => s.to_string(),
        Err(_) => {
            use base64::Engine;
            format!(
                "base64:{}",
                base64::engine::general_purpose::STANDARD.encode(&response.value)
            )
        }
    };
    println!("value={value}");
    println!("noBroadcast={}", response.no_broadcast);
    Ok(())
}
