use crate::config::monovertex::MonovertexConfig;
use crate::error::Error;
use crate::sink::StreamingSink;
use crate::source::StreamingSource;
use crate::transformer::StreamingTransformer;
use crate::Result;
use tokio_util::sync::CancellationToken;

/// StreamingForwarder reads messages from the source, applies transformation if present, and writes to the sink.
pub(crate) struct StreamingForwarder {
    streaming_source: StreamingSource,
    streaming_transformer: Option<StreamingTransformer>,
    streaming_sink: StreamingSink,
    cln_token: CancellationToken,
    #[allow(dead_code)]
    mvtx_config: MonovertexConfig,
}

pub(crate) struct StreamingForwarderBuilder {
    streaming_source: StreamingSource,
    streaming_sink: StreamingSink,
    cln_token: CancellationToken,
    streaming_transformer: Option<StreamingTransformer>,
    mvtx_config: MonovertexConfig,
}

impl StreamingForwarderBuilder {
    /// Create a new builder with mandatory fields
    pub(crate) fn new(
        streaming_source: StreamingSource,
        streaming_sink: StreamingSink,
        mvtx_config: MonovertexConfig,
        cln_token: CancellationToken,
    ) -> Self {
        Self {
            streaming_source,
            streaming_sink,
            cln_token,
            streaming_transformer: None,
            mvtx_config,
        }
    }

    /// Set the optional transformer client
    pub(crate) fn streaming_transformer(
        mut self,
        transformer_client: StreamingTransformer,
    ) -> Self {
        self.streaming_transformer = Some(transformer_client);
        self
    }

    /// Build the StreamingForwarder instance
    #[must_use]
    pub(crate) fn build(self) -> StreamingForwarder {
        StreamingForwarder {
            streaming_source: self.streaming_source,
            streaming_sink: self.streaming_sink,
            streaming_transformer: self.streaming_transformer,
            cln_token: self.cln_token,
            mvtx_config: self.mvtx_config,
        }
    }
}

impl StreamingForwarder {
    pub(crate) async fn start(&self) -> Result<()> {
        let (read_messages_rx, reader_handle) = self.streaming_source.start()?;

        let (transformed_messages_rx, transformer_handle) =
            if let Some(transformer) = &self.streaming_transformer {
                let (transformed_messages_rx, transformer_handle) =
                    transformer.start_streaming(read_messages_rx)?;
                (transformed_messages_rx, Some(transformer_handle))
            } else {
                (read_messages_rx, None)
            };

        let sink_writer_handle = self
            .streaming_sink
            .start(transformed_messages_rx, self.cln_token.clone())
            .await?;

        match tokio::try_join!(
            reader_handle,
            transformer_handle.unwrap_or_else(|| tokio::spawn(async { Ok(()) })),
            sink_writer_handle,
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
