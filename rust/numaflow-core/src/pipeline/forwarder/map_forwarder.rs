use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::mapper::Mapper;
use crate::pipeline::isb::jetstream::reader::JetstreamReader;
use crate::pipeline::isb::jetstream::writer::JetstreamWriter;
use crate::Result;

/// Map forwarder is a component which starts a streaming reader, a mapper, and a writer
/// and manages the lifecycle of these components.
pub(crate) struct MapForwarder {
    jetstream_reader: JetstreamReader,
    mapper: Mapper,
    jetstream_writer: JetstreamWriter,
    cln_token: CancellationToken,
}

impl MapForwarder {
    pub(crate) async fn new(
        jetstream_reader: JetstreamReader,
        mapper: Mapper,
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

    pub(crate) async fn start(&self) -> Result<()> {
        // Create a child cancellation token only for the reader so that we can stop the reader first
        let reader_cancellation_token = self.cln_token.child_token();
        let (read_messages_stream, reader_handle) = self
            .jetstream_reader
            .streaming_read(reader_cancellation_token.clone())
            .await?;

        let (mapped_messages_stream, mapper_handle) =
            self.mapper.map_stream(read_messages_stream).await?;

        let writer_handle = self
            .jetstream_writer
            .streaming_write(mapped_messages_stream)
            .await?;

        // Join the reader, mapper, and writer
        match tokio::try_join!(reader_handle, mapper_handle, writer_handle) {
            Ok((reader_result, mapper_result, writer_result)) => {
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
