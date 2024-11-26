use tokio_util::sync::CancellationToken;

use crate::error;
use crate::error::Error;
use crate::pipeline::isb::jetstream::ISBWriter;
use crate::source::Source;
use crate::transformer::Transformer;

/// Source forwarder is the orchestrator which starts streaming source, a transformer, and an isb writer
/// and manages the lifecycle of these components.
pub(crate) struct SourceForwarder {
    source: Source,
    transformer: Option<Transformer>,
    writer: ISBWriter,
    cln_token: CancellationToken,
}

/// ForwarderBuilder is a builder for Forwarder.
pub(crate) struct SourceForwarderBuilder {
    streaming_source: Source,
    transformer: Option<Transformer>,
    writer: ISBWriter,
    cln_token: CancellationToken,
}

impl SourceForwarderBuilder {
    pub(crate) fn new(
        streaming_source: Source,
        writer: ISBWriter,
        cln_token: CancellationToken,
    ) -> Self {
        Self {
            streaming_source,
            transformer: None,
            writer,
            cln_token,
        }
    }

    pub(crate) fn with_transformer(mut self, transformer: Transformer) -> Self {
        self.transformer = Some(transformer);
        self
    }

    pub(crate) fn build(self) -> SourceForwarder {
        SourceForwarder {
            source: self.streaming_source,
            transformer: self.transformer,
            writer: self.writer,
            cln_token: self.cln_token,
        }
    }
}

impl SourceForwarder {
    /// Start the forwarder by starting the streaming source, transformer, and writer.
    pub(crate) async fn start(&self) -> error::Result<()> {
        // RETHINK: only source should stop when the token is cancelled, transformer and writer should drain the streams
        // and then stop.
        let (read_messages_rx, reader_handle) =
            self.source.streaming_read(self.cln_token.clone())?;

        // start the transformer if it is present
        let (transformed_messages_rx, transformer_handle) =
            if let Some(transformer) = &self.transformer {
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
