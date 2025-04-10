use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::Result;
use crate::error::Error;
use crate::pipeline::isb::jetstream::reader::JetStreamReader;
use crate::sink::SinkWriter;

/// Sink forwarder is a component which starts a streaming reader and a sink writer
/// and manages the lifecycle of these components.
pub(crate) struct SinkForwarder {
    jetstream_reader: JetStreamReader,
    sink_writer: SinkWriter,
}

impl SinkForwarder {
    pub(crate) async fn new(jetstream_reader: JetStreamReader, sink_writer: SinkWriter) -> Self {
        Self {
            jetstream_reader,
            sink_writer,
        }
    }

    pub(crate) async fn start(self, cln_token: CancellationToken) -> Result<()> {
        let child_token = cln_token.child_token();
        // only the reader need to listen on the cancellation token, if the reader stops all
        // other components will stop gracefully because they are chained using tokio streams.
        let (read_messages_stream, reader_handle) = self
            .jetstream_reader
            .streaming_read(child_token.clone())
            .await?;

        let sink_writer_handle = self
            .sink_writer
            .streaming_write(read_messages_stream, child_token)
            .await?;

        // Join the reader and sink writer
        let (reader_result, sink_writer_result) =
            tokio::try_join!(reader_handle, sink_writer_handle).map_err(|e| {
                error!(?e, "Error while joining reader and sink writer");
                Error::Forwarder(format!(
                    "Error while joining reader and sink writer: {:?}",
                    e
                ))
            })?;

        sink_writer_result.inspect_err(|e| {
            error!(?e, "Error while writing messages");
            cln_token.cancel();
        })?;

        reader_result.inspect_err(|e| {
            error!(?e, "Error while reading messages");
            cln_token.cancel();
        })?;

        Ok(())
    }
}
