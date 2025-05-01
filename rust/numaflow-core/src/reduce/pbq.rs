//! PBQ is a persistent buffer queue. It is used to store the data that is not yet processed and
//! forwarded. It exposes a ReceiverStream that can be used to read the data from the PBQ.
//! Reading in PBQ is done as follows:
//! - Before it can read from ISB (during bootup)
//!   - it will read from WAL and replay the data.
//! - After it has replayed from the WAL (after bootup)
//!   - Read the data from the ISB
//!   - Write the data to WAL

use crate::error::Result;
use crate::message::Message;
use crate::pipeline::isb::jetstream::reader::JetStreamReader;
use crate::reduce::wal::segment::append::AppendOnlyWal;
use crate::reduce::wal::segment::replay::ReplayWal;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

/// WAL for storing the data. If None, we will not persist the data.
pub(crate) struct WAL {
    append_only_wal: AppendOnlyWal,
    replay_handle: ReplayWal,
}

/// PBQ is a persistent buffer queue.
pub(crate) struct PBQ {
    /// JetStream Reader.
    isb_reader: JetStreamReader,
    wal: Option<WAL>,
}

impl PBQ {
    /// Creates a new PBQ.
    pub(crate) fn new(isb_reader: JetStreamReader, wal: Option<WAL>) -> Self {
        Self { isb_reader, wal }
    }

    /// Starts the PBQ and returns a ReceiverStream that can be used to read the data from the PBQ.
    /// It replays data from WAL and then starts reading from ISB.
    pub(crate) async fn streaming_read(
        self,
        cancellation_token: CancellationToken,
    ) -> Result<(ReceiverStream<Message>, JoinHandle<Result<()>>)> {
        if let Some(wal) = self.wal {
            _ = wal;
            // Replay from WAL

            todo!()
        } else {
            self.isb_reader.streaming_read(cancellation_token).await
        }
    }
}
