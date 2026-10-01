//! The forwarder for [Pipeline] at its core orchestrates message movement asynchronously using
//! [Stream] over channels between the components. The messages send over this channel using
//! [Actor Pattern].
//!
//! ```text
//! (source) --[c]--> (transformer)* --[c]--> ==> (map)* --[c]--> ===> (reducer)* --[c]--> ===> --[c]--> (sink)
//!    |                   |                       |                      |                                |
//!    |                   |                       |                      |                                |
//!    |                   |                       v                      |                                |
//!    +-------------------+------------------> tracker <-----------------+--------------------------------+
//!
//!
//! ==> - ISB
//! [c] - channel
//!   * - optional
//!  ```
//!
//! Most of the data move forward except for the `ack`, `watermark` which can happen only after the
//! that the tracker has guaranteed that the processing complete. Ack is spawned during the reading.
//! ```text
//! (Read) +-------> (UDF) -------> (Write) +
//!        |                                |
//!        |                                |
//!        +-------> {tracker} <------------
//!                      |
//!          +-----------+-----------+
//!          |           |           |
//!          v           v           v
//!  (track watermark)  (callbacks)   {ack}
//!
//! {} -> Listens on a OneShot
//! () -> Streaming Interface
//! ```
//!
//! [Pipeline]: https://numaflow.numaproj.io/core-concepts/pipeline/
//! [Stream]: https://docs.rs/tokio-stream/latest/tokio_stream/wrappers/struct.ReceiverStream.html
//! [Actor Pattern]: https://ryhl.io/blog/actors-with-tokio/

use crate::config::pipeline::PipelineConfig;
use crate::metrics::MetricsState;
use crate::pipeline::isb::create_isb_factory;
use crate::{Error, config, error};
use futures::future::try_join_all;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{error as log_error, info};

/// Forwarder specific to Sink where reader is ISB, UDF is not present, while
/// the Write is User-defined Sink or builtin.
pub(crate) mod sink_forwarder;

/// Forwarder specific to Mapper where Reader is ISB, UDF is User-defined Mapper,
/// Write is ISB.
pub(crate) mod map_forwarder;

pub(crate) mod reduce_forwarder;
/// Source where the Reader is builtin or User-defined Source, Write is ISB,
/// with an optional Transformer.
pub(crate) mod source_forwarder;

async fn join_forwarder_tasks(
    tasks: Vec<AbortOnDropHandle<error::Result<()>>>,
    cln_token: &CancellationToken,
) -> error::Result<()> {
    let results = try_join_all(tasks).await.map_err(|e| {
        log_error!(?e, "A forwarder task panicked, cancelling token");
        cln_token.cancel();
        Error::Forwarder(e.to_string())
    })?;

    for result in results {
        info!(?result, "Forwarder task completed");
        result?;
    }

    Ok(())
}

/// Starts the appropriate forwarder based on the pipeline configuration.
pub(crate) async fn start_forwarder(
    cln_token: CancellationToken,
    config: PipelineConfig,
    metrics_state: MetricsState,
) -> error::Result<()> {
    let result = run_forwarder(cln_token, config, metrics_state.clone()).await;
    if result.is_err() {
        metrics_state.clear();
    }
    result
}

async fn run_forwarder(
    cln_token: CancellationToken,
    config: PipelineConfig,
    metrics_state: MetricsState,
) -> error::Result<()> {
    let isb_factory = create_isb_factory(&config.isb_client_config, cln_token.clone()).await?;

    match &config.vertex_config {
        config::pipeline::VertexConfig::Source(source) => {
            info!("Starting source forwarder");

            source_forwarder::start_source_forwarder(
                cln_token,
                isb_factory,
                config.clone(),
                source.clone(),
                metrics_state,
            )
            .await?;
        }
        config::pipeline::VertexConfig::Sink(sink) => {
            info!("Starting sink forwarder");
            sink_forwarder::start_sink_forwarder(
                cln_token,
                isb_factory,
                config.clone(),
                (**sink).clone(),
                metrics_state,
            )
            .await?;
        }
        config::pipeline::VertexConfig::Map(map) => {
            info!("Starting map forwarder");
            map_forwarder::start_map_forwarder(
                cln_token,
                isb_factory,
                config.clone(),
                map.clone(),
                metrics_state,
            )
            .await?;
        }
        config::pipeline::VertexConfig::Reduce(reduce) => {
            info!("Starting reduce forwarder");
            reduce_forwarder::start_reduce_forwarder(
                cln_token,
                isb_factory,
                config.clone(),
                reduce.clone(),
                metrics_state,
            )
            .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn join_forwarder_tasks_succeeds_when_all_tasks_succeed() {
        let result = join_forwarder_tasks(
            vec![
                AbortOnDropHandle::new(tokio::spawn(async { Ok(()) })),
                AbortOnDropHandle::new(tokio::spawn(async { Ok(()) })),
            ],
            &CancellationToken::new(),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn join_forwarder_tasks_propagates_task_error() {
        let result = join_forwarder_tasks(
            vec![AbortOnDropHandle::new(tokio::spawn(async {
                Err(Error::Forwarder("forwarder failed".to_string()))
            }))],
            &CancellationToken::new(),
        )
        .await;

        assert!(matches!(result, Err(Error::Forwarder(message)) if message == "forwarder failed"));
    }

    #[tokio::test]
    async fn join_forwarder_tasks_cancels_token_when_task_panics() {
        let token = CancellationToken::new();
        let result = join_forwarder_tasks(
            vec![AbortOnDropHandle::new(tokio::spawn(async {
                panic!("forwarder task panicked")
            }))],
            &token,
        )
        .await;

        assert!(matches!(result, Err(Error::Forwarder(_))));
        assert!(token.is_cancelled());
    }
}
