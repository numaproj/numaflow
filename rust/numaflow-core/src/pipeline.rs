use std::marker::PhantomData;
use std::time::Duration;

use async_nats::jetstream::Context;
use async_nats::{ConnectOptions, jetstream};
use tokio_util::sync::CancellationToken;

use crate::config::pipeline;
use crate::config::pipeline::PipelineConfig;
use crate::pipeline::isb::ISBFactory;
use crate::tracker::Tracker;
use crate::typ::NumaflowTypeConfig;
use crate::{Result, error};

pub(crate) mod forwarder;
pub(crate) mod isb;

/// PipelineContext contains the common context for all the forwarders.
///
/// This struct is generic over the `NumaflowTypeConfig` to support different
/// ISB implementations (JetStream, SimpleBuffer, etc.) through the factory pattern.
/// The factory is passed as a separate generic parameter to allow flexibility in
/// choosing the ISB backend implementation.
pub(crate) struct PipelineContext<'a, C, F>
where
    C: NumaflowTypeConfig,
    F: ISBFactory<Reader = C::ISBReader, Writer = C::ISBWriter>,
{
    pub(crate) cln_token: CancellationToken,
    pub(crate) isb_factory: &'a F,
    pub(crate) config: &'a PipelineConfig,
    pub(crate) tracker: Tracker,
    _phantom: PhantomData<C>,
}

impl<'a, C, F> PipelineContext<'a, C, F>
where
    C: NumaflowTypeConfig,
    F: ISBFactory<Reader = C::ISBReader, Writer = C::ISBWriter>,
{
    /// Creates a new PipelineContext with the given components.
    pub(crate) fn new(
        cln_token: CancellationToken,
        isb_factory: &'a F,
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
    pub(crate) fn factory(&self) -> &F {
        self.isb_factory
    }
}

/// Creates a jetstream context based on the provided configuration
pub(crate) async fn create_js_context(
    config: pipeline::isb::jetstream::ClientConfig,
) -> Result<Context> {
    // TODO: make these configurable. today this is hardcoded on Golang code too.
    let mut opts = ConnectOptions::new()
        .max_reconnects(None) // unlimited reconnects
        .ping_interval(Duration::from_secs(3))
        .retry_on_initial_connect();

    if let (Some(user), Some(password)) = (config.user, config.password) {
        opts = opts.user_and_password(user, password);
    }

    let js_client = async_nats::connect_with_options(&config.url, opts)
        .await
        .map_err(|e| error::Error::Connection(e.to_string()))?;

    Ok(jetstream::new(js_client))
}
