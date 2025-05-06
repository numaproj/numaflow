use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::Result;
use crate::error::Error;
use crate::reduce::pbq::PBQ;
use crate::reduce::pnf::aligned::reducer::ProcessAndForward;
use crate::reduce::pnf::aligned::windower::Windower;

/// ReduceForwarder is a component which starts a PBQ reader and a ProcessAndForward processor
/// and manages the lifecycle of these components.
pub(crate) struct ReduceForwarder<W: Windower + Send + Sync + Clone + 'static> {
    pbq: PBQ,
    processor: ProcessAndForward<W>,
}

impl<W: Windower + Send + Sync + Clone + 'static> ReduceForwarder<W> {
    pub(crate) fn new(pbq: PBQ, processor: ProcessAndForward<W>) -> Self {
        Self { pbq, processor }
    }

    pub(crate) async fn start(self, cln_token: CancellationToken) -> Result<()> {
        let child_token = cln_token.child_token();

        // Start the PBQ reader
        let (read_messages_stream, pbq_handle) =
            self.pbq.streaming_read(child_token.clone()).await?;

        // Start the ProcessAndForward
        let processor_handle = self
            .processor
            .start(read_messages_stream, child_token)
            .await?;

        // Join the pbq and pnf
        let (pbq_result, processor_result) = tokio::try_join!(pbq_handle, processor_handle)
            .map_err(|e| {
                error!(
                    ?e,
                    "Error while joining PBQ reader and ProcessAndForward processor"
                );
                Error::Forwarder(format!(
                    "Error while joining PBQ reader and ProcessAndForward processor: {:?}",
                    e
                ))
            })?;

        processor_result.inspect_err(|e| {
            error!(?e, "Error in ProcessAndForward processor");
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
