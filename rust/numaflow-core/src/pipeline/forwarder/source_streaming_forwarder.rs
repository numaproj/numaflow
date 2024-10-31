use crate::config::pipeline::PipelineConfig;
use crate::error::Error;
use crate::pipeline::isb::jetstream::StreamingJetstreamWriter;
use crate::source::StreamingSource;
use crate::transformer::StreamingTransformer;
use crate::Result;

use tokio_util::sync::CancellationToken;

/// Simple source forwarder that reads messages from the source, applies transformation if present
/// and writes to the messages to ISB.
pub(crate) struct Forwarder {
    streaming_source: StreamingSource,
    streaming_transformer: Option<StreamingTransformer>,
    writer: StreamingJetstreamWriter,
    cln_token: CancellationToken,
    config: PipelineConfig,
}

pub(crate) struct ForwarderBuilder {
    streaming_source: StreamingSource,
    streaming_transformer: Option<StreamingTransformer>,
    writer: StreamingJetstreamWriter,
    cln_token: CancellationToken,
    config: PipelineConfig,
}

impl ForwarderBuilder {
    pub(crate) fn new(
        streaming_source: StreamingSource,
        streaming_transformer: Option<StreamingTransformer>,
        writer: StreamingJetstreamWriter,
        cln_token: CancellationToken,
        config: PipelineConfig,
    ) -> Self {
        Self {
            streaming_source,
            streaming_transformer,
            writer,
            cln_token,
            config,
        }
    }

    pub(crate) fn build(self) -> Forwarder {
        Forwarder {
            streaming_source: self.streaming_source,
            streaming_transformer: self.streaming_transformer,
            writer: self.writer,
            cln_token: self.cln_token,
            config: self.config,
        }
    }
}

impl Forwarder {
    pub(crate) async fn start(&self) -> Result<()> {
        let (read_messages_rx, reader_handle, ack_handle) =
            self.streaming_source.start_streaming()?;

        let (transformed_messages_rx, transformer_handle) =
            if let Some(transformer) = &self.streaming_transformer {
                let (transformed_messages_rx, transformer_handle) =
                    transformer.start_streaming(read_messages_rx)?;
                (transformed_messages_rx, Some(transformer_handle))
            } else {
                (read_messages_rx, None)
            };

        let writer_handle = self
            .writer
            .start_streaming(transformed_messages_rx, self.cln_token.clone())
            .await?;

        match tokio::try_join!(
            reader_handle,
            transformer_handle.unwrap_or_else(|| tokio::spawn(async { Ok(()) })),
            writer_handle,
            ack_handle
        ) {
            Ok((reader_result, transformer_result, sink_writer_result, ack_result)) => {
                reader_result?;
                transformer_result?;
                sink_writer_result?;
                ack_result?;
                Ok(())
            }
            Err(e) => Err(Error::Forwarder(format!(
                "Error while joining reader, transformer, and sink writer: {:?}",
                e
            ))),
        }
    }
}
