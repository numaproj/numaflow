use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::Result;
use crate::error::Error;
use crate::reduce::pbq::PBQ;
use crate::reduce::reducer::aligned::reducer::AlignedReducer;

/// ReduceForwarder is a component which starts a PBQ reader and a reducer
/// and manages the lifecycle of these components.
pub(crate) struct ReduceForwarder {
    pbq: PBQ,
    reducer: AlignedReducer,
}

impl ReduceForwarder {
    pub(crate) fn new(pbq: PBQ, reducer: AlignedReducer) -> Self {
        Self { pbq, reducer }
    }

    pub(crate) async fn start(self, cln_token: CancellationToken) -> Result<()> {
        let child_token = cln_token.child_token();

        // Start the PBQ reader
        let (read_messages_stream, pbq_handle) =
            self.pbq.streaming_read(child_token.clone()).await?;

        // Start the reducer
        let processor_handle = self
            .reducer
            .start(read_messages_stream, child_token)
            .await?;

        // Join the pbq and reducer
        let (pbq_result, processor_result) = tokio::try_join!(pbq_handle, processor_handle)
            .map_err(|e| {
                error!(?e, "Error while joining PBQ reader and reducer");
                Error::Forwarder(format!(
                    "Error while joining PBQ reader and reducer: {:?}",
                    e
                ))
            })?;

        // TODO(vigith):
        processor_result.inspect_err(|e| {
            error!(?e, "Error in reducer");
            cln_token.cancel();
        })?;

        pbq_result.inspect_err(|e| {
            error!(?e, "Error in PBQ reader");
            cln_token.cancel();
        })?;

        info!("Reduce forwarder completed successfully");
        Ok(())
    }
}
