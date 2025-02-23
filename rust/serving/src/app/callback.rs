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

#[derive(Clone)]
struct CallbackAppState<T: Clone, C: Clone> {
    tid_header: String,
    callback_state: CallbackState<T, C>,
}

pub fn callback_handler<
    T: Send + Sync + Clone + DatumStore + 'static,
    C: Send + Sync + Clone + CallbackStore + 'static,
>(
    tid_header: String,
    callback_state: CallbackState<T, C>,
) -> Router {
    let app_state = CallbackAppState {
        tid_header,
        callback_state,
    };
    Router::new()
        // .route("/callback", routing::post(callback))
        .route("/callback_save", routing::post(callback_save))
        .with_state(app_state)
}

async fn callback_save<
    T: Clone + Send + Sync + DatumStore + 'static,
    C: Clone + Send + Sync + CallbackStore + 'static,
>(
    State(app_state): State<CallbackAppState<T, C>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(), ApiError> {
    let id = headers
        .get(&app_state.tid_header)
        .map(|id| String::from_utf8_lossy(id.as_bytes()).to_string())
        .ok_or_else(|| ApiError::BadRequest("Missing id header".to_string()))?;

    app_state
        .callback_state
        .clone()
        .save_response(id, body)
        .await
        .map_err(|e| {
            error!(error=?e, "Saving body from callback save request");
            ApiError::InternalServerError(
                "Failed to save body from callback save request".to_string(),
            )
        })?;

    Ok(())
}
//
// async fn callback<T: Send + Sync + Clone + Store>(
//     State(app_state): State<CallbackAppState<T>>,
//     Json(payload): Json<Vec<Callback>>,
// ) -> Result<(), ApiError> {
//     info!(?payload, "Received callback request");
//     app_state
//         .callback_state
//         .clone()
//         .insert_callback_requests(payload)
//         .await
//         .map_err(|e| {
//             error!(error=?e, "Inserting callback requests");
//             ApiError::InternalServerError("Failed to insert callback requests".to_string())
//         })?;
//
//     Ok(())
// }
