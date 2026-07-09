//! Protocol drivers: each module drives the gRPC choreography for one UDF family and prints
//! results via the shared [`crate::output::Reporter`].

pub mod accumulator;
pub mod map;
pub mod reduce;
pub mod session_reduce;
pub mod sideinput;
pub mod sink;
pub mod source;
pub mod transform;

use std::time::Duration;

use tonic::transport::Channel;

use crate::cli::{PacingOpts, ServiceKind, TuningOpts};
use crate::exit::{CliError, CliResult};
use crate::message::Message;
use crate::ready::wait_until_ready;
use crate::transport::{ConnectOpts, Target, connect_with_retry};

/// Split messages into batches of at most `batch_size`.
pub fn batches(messages: &[Message], batch_size: usize) -> Vec<&[Message]> {
    if batch_size == 0 {
        return vec![messages];
    }
    messages.chunks(batch_size).collect()
}

/// Sleep for `delay` unless it's zero (avoids a needless await).
pub async fn maybe_delay(delay: Duration) {
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
}

/// Marker error for a response-wait timeout, so callers can attach targeted hints.
#[derive(Debug)]
pub struct ResponseTimeout(pub Duration);

impl std::fmt::Display for ResponseTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "timed out waiting for a response after {:?} (slow UDF? increase --timeout)",
            self.0
        )
    }
}

impl std::error::Error for ResponseTimeout {}

/// Read the next message from a gRPC response stream with a per-response timeout.
/// A mid-stream tonic::Status is preserved in the error chain (drives the UDF banner).
pub async fn next_with_timeout<T>(
    stream: &mut tonic::Streaming<T>,
    timeout: Duration,
) -> anyhow::Result<Option<T>> {
    match tokio::time::timeout(timeout, stream.message()).await {
        Err(_) => Err(anyhow::Error::new(ResponseTimeout(timeout))),
        Ok(Ok(msg)) => Ok(msg),
        Ok(Err(status)) => Err(anyhow::Error::new(status).context("gRPC error mid-stream")),
    }
}

/// Send one request with a timeout so a flow-control stall becomes an error, not a hang.
pub async fn send_with_timeout<T>(
    tx: &tokio::sync::mpsc::Sender<T>,
    req: T,
    timeout: Duration,
) -> anyhow::Result<()> {
    match tokio::time::timeout(timeout, tx.send(req)).await {
        Err(_) => Err(anyhow::anyhow!(
            "timed out sending a request after {timeout:?} - the server stopped reading \
             (stalled UDF? increase --timeout)"
        )),
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(anyhow::anyhow!("send failed: {e}")),
    }
}

/// Resolve the target, connect (with retry), and wait for IsReady. Returns the target
/// (for status labels) and the raw channel; callers build their typed client from it.
pub async fn connect_and_ready(
    connect: &ConnectOpts,
    kind: ServiceKind,
    tuning: &TuningOpts,
) -> CliResult<(Target, Channel)> {
    let target = Target::resolve(connect).map_err(CliError::usage)?;
    let channel = connect_with_retry(&target, tuning.timeout)
        .await
        .map_err(CliError::connect)?;
    wait_until_ready(
        kind,
        channel.clone(),
        tuning.max_message_size,
        tuning.timeout,
    )
    .await
    .map_err(CliError::connect)?;
    Ok((target, channel))
}

/// Applies --batch-size/--delay pacing across a run: call tick() once per message sent.
pub struct Pacer {
    batch_size: usize,
    delay: Duration,
    since: usize,
}

impl Pacer {
    pub fn new(pacing: &PacingOpts) -> Self {
        Self {
            batch_size: pacing.batch_size,
            delay: pacing.delay,
            since: 0,
        }
    }

    pub async fn tick(&mut self) {
        self.since += 1;
        if self.batch_size > 0 && self.since >= self.batch_size {
            self.since = 0;
            maybe_delay(self.delay).await;
        }
    }
}
