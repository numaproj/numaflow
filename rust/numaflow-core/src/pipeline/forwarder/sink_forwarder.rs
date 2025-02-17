use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::pipeline::isb::jetstream::reader::JetStreamReader;
use crate::sink::SinkWriter;
use crate::Result;

/// Sink forwarder is a component which starts a streaming reader and a sink writer
/// and manages the lifecycle of these components.
pub(crate) struct SinkForwarder {
    jetstream_reader: JetStreamReader,
    sink_writer: SinkWriter,
    cln_token: CancellationToken,
}

impl SinkForwarder {
    pub(crate) async fn new(
        jetstream_reader: JetStreamReader,
        sink_writer: SinkWriter,
        cln_token: CancellationToken,
    ) -> Self {
        Self {
            jetstream_reader,
            sink_writer,
            cln_token,
        }
    }

    pub(crate) async fn start(self) -> Result<()> {
        // only the reader need to listen on the cancellation token, if the reader stops all
        // other components will stop gracefully because they are chained using tokio streams.
        let (read_messages_stream, reader_handle) = self
            .jetstream_reader
            .streaming_read(self.cln_token.clone())
            .await?;

        let sink_writer_handle = self
            .sink_writer
            .streaming_write(read_messages_stream)
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
