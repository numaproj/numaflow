use crate::error::Error;
use crate::pipeline::isb::jetstream::reader::JetstreamReader;
use crate::sink::SinkWriter;
use crate::Result;
use tokio_util::sync::CancellationToken;

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
        // create a child cancellation token only for reader so that we can exit the reader first
        let reader_cancellation_token = self.cln_token.child_token();
        let (read_messages_rx, reader_handle) = self
            .jetstream_reader
            .start(reader_cancellation_token.clone())
            .await;

        let sink_writer_handle = self.sink_writer.start(read_messages_rx).await?;

        // join the reader and sink writer
        // TODO:: handle the error
        tokio::try_join!(reader_handle, sink_writer_handle).map_err(|e| {
            Error::Forwarder(format!(
                "Error while joining reader and sink writer: {:?}",
                e
            ))
        })?;

        Ok(())
    }
}
