use tokio_util::sync::CancellationToken;

use crate::config::pipeline::PipelineConfig;
use crate::error::Error;
use crate::pipeline::isb::jetstream::reader::JetstreamReader;
use crate::sink::SinkWriter;
use crate::Result;

/// Sink forwarder reads messages from the jetstream and writes to the sink.
pub(crate) struct SinkForwarder {
    jetstream_reader: JetstreamReader,
    sink_writer: SinkWriter,
    cln_token: CancellationToken,
}

impl SinkForwarder {
    pub(crate) async fn new(
        jetstream_reader: JetstreamReader,
        sink_writer: SinkWriter,
        cln_token: CancellationToken,
    ) -> Self {
        Self {
            jetstream_reader,
            sink_writer,
            cln_token,
        }
    }

    pub(crate) async fn start(&self, pipeline_config: PipelineConfig) -> Result<()> {
        // Create a child cancellation token only for the reader so that we can stop the reader first
        let reader_cancellation_token = self.cln_token.child_token();
        let (read_messages_rx, reader_handle) = self
            .jetstream_reader
            .start(reader_cancellation_token.clone(), &pipeline_config)
            .await?;

        let sink_writer_handle = self
            .sink_writer
            .start(read_messages_rx, self.cln_token.clone())
            .await?;

        // Join the reader and sink writer
        match tokio::try_join!(reader_handle, sink_writer_handle) {
            Ok((reader_result, sink_writer_result)) => {
                reader_result?;
                sink_writer_result?;
                Ok(())
            }
            Err(e) => Err(Error::Forwarder(format!(
                "Error while joining reader and sink writer: {:?}",
                e
            ))),
        }
    }
}
