//! Side-input driver: a single unary `RetrieveSideInput` call.

use anyhow::anyhow;
use serde_json::json;

use numaflow_pb::clients::sideinput::side_input_client::SideInputClient;

use crate::cli::{OutputFormat, OutputOpts, ServiceKind, SideInputArgs, TuningOpts};
use crate::drivers::connect_and_ready;
use crate::exit::{CliError, CliResult, ExitCode};
use crate::output::{Reporter, render_payload};

pub async fn run(args: SideInputArgs, tuning: &TuningOpts, output: &OutputOpts) -> CliResult<()> {
    let reporter = Reporter::new(output);

    let (_target, channel) =
        connect_and_ready(&args.connect, ServiceKind::SideInput, tuning).await?;

    let mut client = SideInputClient::new(channel)
        .max_encoding_message_size(tuning.max_message_size)
        .max_decoding_message_size(tuning.max_message_size);

    let resp = client
        .retrieve_side_input(tonic::Request::new(()))
        .await
        .map_err(|e| CliError::new(ExitCode::Protocol, anyhow!("RetrieveSideInput failed: {e}")))?
        .into_inner();

    // no_broadcast=true means "don't update"; broadcast = !no_broadcast.
    let broadcast = !resp.no_broadcast;

    match reporter.format {
        OutputFormat::Json => {
            use base64::Engine;
            reporter.json_event(&json!({
                "type": "sideInput",
                "broadcast": broadcast,
                "payloadBase64": base64::engine::general_purpose::STANDARD.encode(&resp.value),
            }));
        }
        OutputFormat::Raw => {
            use std::io::Write;
            let _ = std::io::stdout().lock().write_all(&resp.value);
        }
        OutputFormat::Text => {
            println!(
                "broadcast={broadcast} payload={}",
                render_payload(&resp.value)
            );
        }
    }
    Ok(())
}
