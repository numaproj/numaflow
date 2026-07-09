//! The `ready` subcommand and the shared IsReady retry helper.

use std::time::Duration;

use anyhow::anyhow;
use tonic::transport::Channel;

use crate::cli::{OutputOpts, ReadyArgs, ServiceKind, TuningOpts};
use crate::exit::{CliError, CliResult};
use crate::transport::{Target, connect_with_retry};
use numaflow_pb::clients;

/// Poll a service's `IsReady` once per second until it returns ready or `timeout` elapses.
/// The `check` closure performs one IsReady call, returning `Ok(true)` when ready.
async fn wait_ready<F, Fut>(mut check: F, timeout: Duration) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<bool>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let last = match check().await {
            Ok(true) => return Ok(()),
            Ok(false) => anyhow!("server reported not ready"),
            Err(e) => e,
        };
        if has_unimplemented_status(&last) {
            return Err(last);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(last);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Macro to build a typed client with message-size limits and run its IsReady in the retry
/// loop. Each service exposes an identically-shaped `is_ready(())` and `ReadyResponse{ready}`.
macro_rules! ready_for {
    ($client:path, $channel:expr, $max:expr, $timeout:expr) => {{
        let channel = $channel;
        let max = $max;
        wait_ready(
            move || {
                let channel = channel.clone();
                async move {
                    let mut client = <$client>::new(channel)
                        .max_encoding_message_size(max)
                        .max_decoding_message_size(max);
                    let resp = client.is_ready(tonic::Request::new(())).await?;
                    Ok(resp.into_inner().ready)
                }
            },
            $timeout,
        )
        .await
    }};
}

/// Wait until the given service kind is ready over `channel`.
pub async fn wait_until_ready(
    kind: ServiceKind,
    channel: Channel,
    max_message_size: usize,
    timeout: Duration,
) -> anyhow::Result<()> {
    use ServiceKind as K;
    let result = match kind {
        K::Map => ready_for!(
            clients::map::map_client::MapClient<_>,
            channel,
            max_message_size,
            timeout
        ),
        K::Transform => ready_for!(
            clients::sourcetransformer::source_transform_client::SourceTransformClient<_>,
            channel,
            max_message_size,
            timeout
        ),
        K::Reduce => ready_for!(
            clients::reduce::reduce_client::ReduceClient<_>,
            channel,
            max_message_size,
            timeout
        ),
        K::SessionReduce => ready_for!(
            clients::sessionreduce::session_reduce_client::SessionReduceClient<_>,
            channel,
            max_message_size,
            timeout
        ),
        K::Accumulator => ready_for!(
            clients::accumulator::accumulator_client::AccumulatorClient<_>,
            channel,
            max_message_size,
            timeout
        ),
        K::Sink => ready_for!(
            clients::sink::sink_client::SinkClient<_>,
            channel,
            max_message_size,
            timeout
        ),
        K::Source => ready_for!(
            clients::source::source_client::SourceClient<_>,
            channel,
            max_message_size,
            timeout
        ),
        K::SideInput => ready_for!(
            clients::sideinput::side_input_client::SideInputClient<_>,
            channel,
            max_message_size,
            timeout
        ),
    };

    match result {
        Err(e) if has_unimplemented_status(&e) => Err(e.context(format!(
            "the server does not implement {}'s IsReady - is this socket really a {} UDF? (check the subcommand)",
            service_name(kind),
            service_name(kind)
        ))),
        other => other,
    }
}

/// Run the `ready` subcommand.
pub async fn run(args: ReadyArgs, tuning: &TuningOpts, output: &OutputOpts) -> CliResult<()> {
    let target = Target::resolve(&args.connect).map_err(CliError::usage)?;
    let channel = connect_with_retry(&target, tuning.timeout)
        .await
        .map_err(CliError::connect)?;

    wait_until_ready(args.kind, channel, tuning.max_message_size, tuning.timeout)
        .await
        .map_err(CliError::connect)?;

    if !output.quiet {
        println!("✓ ready ({})", target.label());
    }
    Ok(())
}

fn has_unimplemented_status(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<tonic::Status>()
            .is_some_and(|status| status.code() == tonic::Code::Unimplemented)
    })
}

fn service_name(kind: ServiceKind) -> &'static str {
    match kind {
        ServiceKind::Map => "map",
        ServiceKind::Transform => "transform",
        ServiceKind::Reduce => "reduce",
        ServiceKind::SessionReduce => "session-reduce",
        ServiceKind::Accumulator => "accumulator",
        ServiceKind::Sink => "sink",
        ServiceKind::Source => "source",
        ServiceKind::SideInput => "side-input",
    }
}
