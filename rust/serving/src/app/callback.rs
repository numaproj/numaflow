use axum::{body::Bytes, extract::State, http::HeaderMap, routing, Json, Router};
use serde::{Deserialize, Serialize};
use tracing::error;

use self::store::Store;
use crate::app::response::ApiError;

/// in-memory state store including connection tracking
pub(crate) mod state;
use state::State as CallbackState;

/// store for storing the state
pub(crate) mod store;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct CallbackRequest {
    pub(crate) id: String,
    pub(crate) vertex: String,
    pub(crate) cb_time: u64,
    pub(crate) from_vertex: String,
    pub(crate) tags: Option<Vec<String>>,
}

#[derive(Clone)]
struct CallbackAppState<T: Clone> {
    tid_header: String,
    callback_state: CallbackState<T>,
}

pub fn callback_handler<T: Send + Sync + Clone + Store + 'static>(
    tid_header: String,
    callback_state: CallbackState<T>,
) -> Router {
    let app_state = CallbackAppState {
        tid_header,
        callback_state,
    };
    Router::new()
        .route("/callback", routing::post(callback))
        .route("/callback_save", routing::post(callback_save))
        .with_state(app_state)
}

async fn callback_save<T: Send + Sync + Clone + Store>(
    State(app_state): State<CallbackAppState<T>>,
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

async fn callback<T: Send + Sync + Clone + Store>(
    State(app_state): State<CallbackAppState<T>>,
    Json(payload): Json<Vec<CallbackRequest>>,
) -> Result<(), ApiError> {
    app_state
        .callback_state
        .clone()
        .insert_callback_requests(payload)
        .await
        .map_err(|e| {
            error!(error=?e, "Inserting callback requests");
            ApiError::InternalServerError("Failed to insert callback requests".to_string())
        })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::extract::Request;
    use axum::http::header::CONTENT_TYPE;
    use axum::http::StatusCode;
    use tower::ServiceExt;

    use super::*;
    use crate::app::callback::state::State as CallbackState;
    use crate::app::callback::store::memstore::InMemoryStore;
    use crate::app::tracker::MessageGraph;
    use crate::pipeline::min_pipeline_spec;

    #[tokio::test]
    async fn test_callback_failure() {
        let store = InMemoryStore::new();
        let msg_graph = MessageGraph::from_pipeline(min_pipeline_spec()).unwrap();
        let state = CallbackState::new(msg_graph, store).await.unwrap();
        let app = callback_handler("ID".to_owned(), state);

        let payload = vec![CallbackRequest {
            id: "test_id".to_string(),
            vertex: "in".to_string(),
            cb_time: 12345,
            from_vertex: "in".to_string(),
            tags: None,
        }];

        let res = Request::builder()
            .method("POST")
            .uri("/callback")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let resp = app.oneshot(res).await.unwrap();
        // id is not registered, so it should return 500
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_callback_success() {
        let store = InMemoryStore::new();
        let msg_graph = MessageGraph::from_pipeline(min_pipeline_spec()).unwrap();
        let mut state = CallbackState::new(msg_graph, store).await.unwrap();

        let x = state.register("test_id".to_string());
        // spawn a task which will be awaited later
        let handle = tokio::spawn(async move {
            let _ = x.await.unwrap();
        });

        let app = callback_handler("ID".to_owned(), state);

        let payload = vec![
            CallbackRequest {
                id: "test_id".to_string(),
                vertex: "in".to_string(),
                cb_time: 12345,
                from_vertex: "in".to_string(),
                tags: None,
            },
            CallbackRequest {
                id: "test_id".to_string(),
                vertex: "cat".to_string(),
                cb_time: 12345,
                from_vertex: "in".to_string(),
                tags: None,
            },
            CallbackRequest {
                id: "test_id".to_string(),
                vertex: "out".to_string(),
                cb_time: 12345,
                from_vertex: "cat".to_string(),
                tags: None,
            },
        ];

        let res = Request::builder()
            .method("POST")
            .uri("/callback")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let resp = app.oneshot(res).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_callback_save() {
        let store = InMemoryStore::new();
        let msg_graph = MessageGraph::from_pipeline(min_pipeline_spec()).unwrap();
        let state = CallbackState::new(msg_graph, store).await.unwrap();
        let app = callback_handler("ID".to_owned(), state);

        let res = Request::builder()
            .method("POST")
            .uri("/callback_save")
            .header(CONTENT_TYPE, "application/json")
            .header("id", "test_id")
            .body(Body::from("test_body"))
            .unwrap();

        let resp = app.oneshot(res).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_without_id_header() {
        let store = InMemoryStore::new();
        let msg_graph = MessageGraph::from_pipeline(min_pipeline_spec()).unwrap();
        let state = CallbackState::new(msg_graph, store).await.unwrap();
        let app = callback_handler("ID".to_owned(), state);

        let res = Request::builder()
            .method("POST")
            .uri("/callback_save")
            .body(Body::from("test_body"))
            .unwrap();

        let resp = app.oneshot(res).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
