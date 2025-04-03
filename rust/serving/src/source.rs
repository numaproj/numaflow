use std::collections::HashMap;
use std::sync::Arc;

use async_nats::jetstream::Context;
use bytes::Bytes;

use crate::app::orchestrator::OrchestratorState as CallbackState;
use crate::app::store::cbstore::jetstreamstore::JetStreamCallbackStore;
use crate::app::store::datastore::jetstream::JetStreamDataStore;
use crate::app::store::datastore::user_defined::UserDefinedStore;
use crate::app::tracker::MessageGraph;
use crate::config::StoreType;
use crate::Settings;
use crate::{Error, Result};

/// Serving payload passed on to Numaflow.
#[derive(Debug)]
pub struct Message {
    pub value: Bytes,
    pub id: String,
    pub headers: HashMap<String, String>,
}
