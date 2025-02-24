use axum::extract::State;
use axum::routing;
use axum::Router;
use bytes::Bytes;
use http::HeaderMap;
use state::State as CallbackState;
use tracing::error;

use crate::app::callback::cbstore::CallbackStore;
use crate::app::callback::datumstore::DatumStore;
use crate::app::response::ApiError;
use crate::callback::Callback;

/// in-memory state store including connection tracking
pub(crate) mod state;

pub(crate) mod cbstore;
/// datumstore for storing the state
pub(crate) mod datumstore;
