use axum::{
    extract::{Query, State},
    routing, Router,
};

use crate::app::response::ApiError;

use super::callback::state::State as CallbackState;
use super::callback::store::Store;

pub fn get_message_path<T: Send + Sync + Clone + Store + 'static>(
    callback_store: CallbackState<T>,
) -> Router {
    Router::new()
        .route("/message", routing::get(message_path))
        .with_state(callback_store)
}

#[derive(serde::Deserialize)]
struct MessagePath {
    id: String,
}

async fn message_path<T: Send + Sync + Clone + Store>(
    State(mut store): State<CallbackState<T>>,
    Query(MessagePath { id }): Query<MessagePath>,
) -> Result<String, ApiError> {
    match store.retrieve_subgraph_from_storage(&id).await {
        Ok(subgraph) => Ok(subgraph),
        Err(e) => {
            tracing::error!(error=?e);
            Err(ApiError::InternalServerError(
                "Failed to track message".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::extract::Request;
    use axum::http::header::CONTENT_TYPE;
    use axum::http::StatusCode;
    use tower::ServiceExt;

    use crate::app::callback::store::memstore::InMemoryStore;
    use crate::app::tracker::MessageGraph;
    use crate::pipeline::pipeline_spec;

    use super::*;

    #[tokio::test]
    async fn test_message_path_not_present() {
        let store = InMemoryStore::new();
        let msg_graph = MessageGraph::from_pipeline(pipeline_spec()).unwrap();
        let state = CallbackState::new(msg_graph, store).await.unwrap();
        let app = get_message_path(state);

        let res = Request::builder()
            .method("GET")
            .uri("/message?id=test_id")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(res).await.unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
