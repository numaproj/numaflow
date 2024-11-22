use tokio_util::sync::CancellationToken;

use crate::config::pipeline::PipelineConfig;
use crate::error;
use crate::error::Error;
use crate::pipeline::isb::jetstream::StreamingJetstreamWriter;
use crate::source::Source;
use crate::transformer::Transformer;

/// Simple source forwarder that reads messages from the source, applies transformation if present
/// and writes to the messages to ISB.
pub(crate) struct Forwarder {
    streaming_source: Source,
    streaming_transformer: Option<Transformer>,
    writer: StreamingJetstreamWriter,
    cln_token: CancellationToken,
    config: PipelineConfig,
}

pub(crate) struct StreamingForwarderBuilder {
    streaming_source: Source,
    streaming_transformer: Option<Transformer>,
    writer: StreamingJetstreamWriter,
    cln_token: CancellationToken,
    config: PipelineConfig,
}

impl StreamingForwarderBuilder {
    pub(crate) fn new(
        streaming_source: Source,
        streaming_transformer: Option<Transformer>,
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
    pub(crate) async fn start(&self) -> error::Result<()> {
        let (read_messages_rx, reader_handle) = self.streaming_source.streaming_read()?;

        let (transformed_messages_rx, transformer_handle) =
            if let Some(transformer) = &self.streaming_transformer {
                let (transformed_messages_rx, transformer_handle) =
                    transformer.transform_stream(read_messages_rx)?;
                (transformed_messages_rx, Some(transformer_handle))
            } else {
                (read_messages_rx, None)
            };

        let writer_handle = self
            .writer
            .streaming_write(transformed_messages_rx, self.cln_token.clone())
            .await?;

        match tokio::try_join!(
            reader_handle,
            transformer_handle.unwrap_or_else(|| tokio::spawn(async { Ok(()) })),
            writer_handle,
        ) {
            Ok((reader_result, transformer_result, sink_writer_result)) => {
                reader_result?;
                transformer_result?;
                sink_writer_result?;
                Ok(())
            }
            Err(e) => Err(Error::Forwarder(format!(
                "Error while joining reader, transformer, and sink writer: {:?}",
                e
            ))),
        }
    }
}
