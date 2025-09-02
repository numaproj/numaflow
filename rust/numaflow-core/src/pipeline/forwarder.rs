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
use crate::config::pipeline::watermark::WatermarkConfig;
use crate::watermark::source::SourceWatermarkHandle;
use crate::{config, error, pipeline};
use tokio_util::sync::CancellationToken;
use tracing::info;

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

/// Starts the appropriate forwarder based on the pipeline configuration.
pub(crate) async fn start_forwarder(
    cln_token: CancellationToken,
    config: PipelineConfig,
) -> error::Result<()> {
    let js_context = pipeline::create_js_context(config.js_client_config.clone()).await?;

    match &config.vertex_config {
        config::pipeline::VertexConfig::Source(source) => {
            info!("Starting source forwarder");

            // create watermark handle, if watermark is enabled
            let source_watermark_handle = match &config.watermark_config {
                Some(WatermarkConfig::Source(source_config)) => Some(
                    SourceWatermarkHandle::new(
                        config.read_timeout,
                        js_context.clone(),
                        &config.to_vertex_config,
                        source_config,
                        cln_token.clone(),
                    )
                    .await?,
                ),
                _ => None,
            };

            source_forwarder::start_source_forwarder(
                cln_token,
                js_context,
                config.clone(),
                source.clone(),
                source_watermark_handle,
            )
            .await?;
        }
        config::pipeline::VertexConfig::Sink(sink) => {
            info!("Starting sink forwarder");
            sink_forwarder::start_sink_forwarder(
                cln_token,
                js_context,
                config.clone(),
                sink.clone(),
            )
            .await?;
        }
        config::pipeline::VertexConfig::Map(map) => {
            info!("Starting map forwarder");
            map_forwarder::start_map_forwarder(cln_token, js_context, config.clone(), map.clone())
                .await?;
        }
        config::pipeline::VertexConfig::Reduce(reduce) => {
            info!("Starting reduce forwarder");
            reduce_forwarder::start_reduce_forwarder(
                cln_token,
                js_context,
                config.clone(),
                reduce.clone(),
            )
            .await?;
        }
    }
    Ok(())
}
