use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::pipeline::isb::jetstream::reader::JetstreamReader;
use crate::sink::SinkWriter;
use crate::Result;

/// Sink forwarder is a component which starts a streaming reader and a sink writer
/// and manages the lifecycle of these components.
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

    pub(crate) async fn start(&self) -> Result<()> {
        // Create a child cancellation token only for the reader so that we can stop the reader first
        let reader_cancellation_token = self.cln_token.child_token();
        let (read_messages_stream, reader_handle) = self
            .jetstream_reader
            .streaming_read(reader_cancellation_token.clone())
            .await?;

        let sink_writer_handle = self
            .sink_writer
            .streaming_write(read_messages_stream, self.cln_token.clone())
            .await?;

        // Join the reader and sink writer
        match tokio::try_join!(reader_handle, sink_writer_handle) {
            Ok((reader_result, sink_writer_result)) => {
                sink_writer_result?;
                reader_result?;
                Ok(())
            }
            Err(e) => Err(Error::Forwarder(format!(
                "Error while joining reader and sink writer: {:?}",
                e
            ))),
        }
    }
}
