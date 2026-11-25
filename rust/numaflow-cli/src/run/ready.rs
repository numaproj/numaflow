//! `ready` subcommand: probe a UDF server's `IsReady` for the given container type.

use std::time::Duration;

use tonic::transport::Channel;

use crate::cli::{ReadyArgs, ReadyType};
use crate::conn::connect_uds;
use crate::error::{CliError, CliResult};

/// How often to retry the readiness probe while waiting for the socket/server.
const RETRY: Duration = Duration::from_millis(200);

pub async fn run(args: ReadyArgs) -> CliResult<()> {
    let deadline = std::time::Instant::now() + args.timeout;

    // Wait for the socket to connect, then probe IsReady, retrying until the deadline.
    loop {
        match connect_uds(args.socket.clone()).await {
            Ok(channel) => {
                if probe(args.kind, channel).await? {
                    println!("ready");
                    return Ok(());
                }
            }
            Err(_) => { /* socket not up yet — retry below */ }
        }

        if std::time::Instant::now() >= deadline {
            return Err(CliError::NotReady(format!(
                "{:?} server at {} not ready within {:?}",
                args.kind,
                args.socket.display(),
                args.timeout
            )));
        }
        tokio::time::sleep(RETRY).await;
    }
}

/// Dispatch to the correct typed client's `is_ready` for the container type. Returns the `ready`
/// bool; a transport error is treated as "not ready yet" (caller retries).
async fn probe(kind: ReadyType, channel: Channel) -> CliResult<bool> {
    use numaflow_pb::clients;
    let ready = match kind {
        ReadyType::Map => clients::map::map_client::MapClient::new(channel)
            .is_ready(())
            .await
            .map(|r| r.into_inner().ready),
        ReadyType::Sink => clients::sink::sink_client::SinkClient::new(channel)
            .is_ready(())
            .await
            .map(|r| r.into_inner().ready),
        ReadyType::Source => clients::source::source_client::SourceClient::new(channel)
            .is_ready(())
            .await
            .map(|r| r.into_inner().ready),
        ReadyType::Transform => {
            clients::sourcetransformer::source_transform_client::SourceTransformClient::new(channel)
                .is_ready(())
                .await
                .map(|r| r.into_inner().ready)
        }
        ReadyType::Reduce => clients::reduce::reduce_client::ReduceClient::new(channel)
            .is_ready(())
            .await
            .map(|r| r.into_inner().ready),
        ReadyType::SessionReduce => {
            clients::sessionreduce::session_reduce_client::SessionReduceClient::new(channel)
                .is_ready(())
                .await
                .map(|r| r.into_inner().ready)
        }
        ReadyType::Accumulator => {
            clients::accumulator::accumulator_client::AccumulatorClient::new(channel)
                .is_ready(())
                .await
                .map(|r| r.into_inner().ready)
        }
        ReadyType::SideInput => {
            clients::sideinput::side_input_client::SideInputClient::new(channel)
                .is_ready(())
                .await
                .map(|r| r.into_inner().ready)
        }
    };
    // A transport-level error means the server isn't answering yet → not ready (retry).
    Ok(ready.unwrap_or(false))
}
