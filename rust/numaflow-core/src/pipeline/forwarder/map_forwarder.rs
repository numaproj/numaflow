use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::error::Error;
use crate::mapper::map::MapHandle;
use crate::pipeline::isb::jetstream::reader::JetStreamReader;
use crate::pipeline::isb::jetstream::writer::JetstreamWriter;
use crate::Result;

/// Map forwarder is a component which starts a streaming reader, a mapper, and a writer
/// and manages the lifecycle of these components.
pub(crate) struct MapForwarder {
    jetstream_reader: JetStreamReader,
    mapper: MapHandle,
    jetstream_writer: JetstreamWriter,
}

impl MapForwarder {
    pub(crate) async fn new(
        jetstream_reader: JetStreamReader,
        mapper: MapHandle,
        jetstream_writer: JetstreamWriter,
    ) -> Self {
        Self {
            jetstream_reader,
            mapper,
            jetstream_writer,
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

        let (mapped_messages_stream, mapper_handle) = self
            .mapper
            .streaming_map(read_messages_stream, child_token.clone())
            .await?;

        let writer_handle = self
            .jetstream_writer
            .streaming_write(mapped_messages_stream, child_token)
            .await?;

        // Join the reader, mapper, and writer
        let (reader_result, mapper_result, writer_result) =
            tokio::try_join!(reader_handle, mapper_handle, writer_handle).map_err(|e| {
                error!(?e, "Error while joining reader, mapper, and writer");
                Error::Forwarder(format!(
                    "Error while joining reader, mapper, and writer: {:?}",
                    e
                ))
            })?;

        writer_result.inspect_err(|e| {
            error!(?e, "Error while writing messages");
            cln_token.cancel();
        })?;

        mapper_result.inspect_err(|e| {
            error!(?e, "Error while mapping messages");
            cln_token.cancel();
        })?;

        reader_result.inspect_err(|e| {
            error!(?e, "Error while reading messages");
            cln_token.cancel();
        })?;

        Ok(())
    }
}
