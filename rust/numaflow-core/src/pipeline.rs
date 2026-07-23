use std::marker::PhantomData;

use tokio_util::sync::CancellationToken;

use crate::config::pipeline::PipelineConfig;
use crate::pipeline::isb::ISBFactory;
use crate::tracker::Tracker;
use crate::typ::NumaflowTypeConfig;

pub(crate) mod forwarder;
pub(crate) mod isb;

/// PipelineContext contains the common context for all the forwarders.
///
/// This struct is generic over the `NumaflowTypeConfig` to support different
/// rate-limiter implementations. The ISB backend is selected at runtime via
/// a type-erased factory handle.
pub(crate) struct PipelineContext<'a, C>
where
    C: NumaflowTypeConfig,
{
    pub(crate) cln_token: CancellationToken,
    pub(crate) isb_factory: &'a dyn ISBFactory,
    pub(crate) config: &'a PipelineConfig,
    pub(crate) tracker: Tracker,
    _phantom: PhantomData<C>,
}

impl<'a, C> PipelineContext<'a, C>
where
    C: NumaflowTypeConfig,
{
    /// Creates a new PipelineContext with the given components.
    pub(crate) fn new(
        cln_token: CancellationToken,
        isb_factory: &'a dyn ISBFactory,
        config: &'a PipelineConfig,
        tracker: Tracker,
    ) -> Self {
        Self {
            cln_token,
            isb_factory,
            config,
            tracker,
            _phantom: PhantomData,
        }
    }

    /// Returns a reference to the ISB factory.
    pub(crate) fn factory(&self) -> &dyn ISBFactory {
        self.isb_factory
    }
}
