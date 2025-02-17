use crate::error::Error;
use crate::mapper::map::MapHandle;
use crate::pipeline::isb::jetstream::reader::JetStreamReader;
use crate::pipeline::isb::jetstream::writer::JetstreamWriter;
use crate::Result;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Map forwarder is a component which starts a streaming reader, a mapper, and a writer
/// and manages the lifecycle of these components.
pub(crate) struct MapForwarder {
    jetstream_reader: JetStreamReader,
    mapper: MapHandle,
    jetstream_writer: JetstreamWriter,
    cln_token: CancellationToken,
}

impl MapForwarder {
    pub(crate) async fn new(
        jetstream_reader: JetStreamReader,
        mapper: MapHandle,
        jetstream_writer: JetstreamWriter,
        cln_token: CancellationToken,
    ) -> Self {
        Self {
            jetstream_reader,
            mapper,
            jetstream_writer,
            cln_token,
        }
    }

    pub(crate) async fn start(self) -> Result<()> {
        // only the reader need to listen on the cancellation token, if the reader stops all
        // other components will stop gracefully because they are chained using tokio streams.
        let (read_messages_stream, reader_handle) =
            self.jetstream_reader.streaming_read(self.cln_token).await?;

        let (mapped_messages_stream, mapper_handle) =
            self.mapper.streaming_map(read_messages_stream).await?;

        let writer_handle = self
            .jetstream_writer
            .streaming_write(mapped_messages_stream)
            .await?;

        // Join the reader, mapper, and writer
        match tokio::try_join!(reader_handle, mapper_handle, writer_handle) {
            Ok((reader_result, mapper_result, writer_result)) => {
                info!("All components in map forwarder returned");
                writer_result?;
                mapper_result?;
                reader_result?;
                Ok(())
            }
            Err(e) => Err(Error::Forwarder(format!(
                "Error while joining reader, mapper, and writer: {:?}",
                e
            ))),
        }
    }
}
