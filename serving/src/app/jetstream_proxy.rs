use std::borrow::Borrow;

use async_nats::{jetstream::Context, HeaderMap as JSHeaderMap};
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::post,
    Json, Router,
};
use tracing::error;
use uuid::Uuid;

use crate::app::callback::state;
use crate::app::response::{ApiError, ServeResponse};
use crate::config;

use super::callback::{state::State as CallbackState, store::Store};

// TODO:
// - [ ] better health check
// - [ ] jetstream connection pooling
// - [ ] make use of proper url capture! perhaps we have to rewrite the nesting at app level
//       *async*
//       curl -H 'Content-Type: text/plain' -X POST -d "test-$(date +'%s')" -v http://localhost:3000/v1/process/async | jq
//       *sync*
//       curl -H 'ID: foobar' -H 'Content-Type: text/plain' -X POST -d "test-$(date +'%s')" http://localhost:3000/v1/process/sync
//       curl -H 'Content-Type: application/json' -X POST -d '{"id": "foobar"}' http://localhost:3000/v1/process/callback
// {
//   "id": "foobar",
//   "vertex": "b",
//   "cb_time": 12345,
//   "from_vertex": "a"
// }

const ID_HEADER_KEY: &str = "X-Numaflow-Id";
const CALLBACK_URL_KEY: &str = "X-Numaflow-Callback-Url";

const NUMAFLOW_RESP_ARRAY_LEN: &str = "Numaflow-Array-Len";
const NUMAFLOW_RESP_ARRAY_IDX_LEN: &str = "Numaflow-Array-Index-Len";

#[derive(Clone)]
struct ProxyState<T> {
    context: Context,
    callback: state::State<T>,
    stream: &'static str,
    callback_url: String,
}

pub(crate) async fn jetstream_proxy<T: Clone + Send + Sync + Store + 'static>(
    context: Context,
    callback_store: CallbackState<T>,
) -> crate::Result<Router> {
    let proxy_state = ProxyState {
        context,
        callback: callback_store,
        stream: &config().jetstream.stream,
        callback_url: format!(
            "http://{}:{}/v1/process/callback",
            config().host_ip,
            config().app_listen_port
        ),
    };

    let router = Router::new()
        .route("/async", post(async_publish))
        .route("/sync", post(sync_publish))
        .route("/sync_serve", post(sync_publish_serve))
        .with_state(proxy_state);
    Ok(router)
}

async fn sync_publish_serve<T: Send + Sync + Clone + Store>(
    State(mut proxy_state): State<ProxyState<T>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let id = extract_id_from_headers(&headers);

    // Register the ID in the callback proxy state
    let notify = proxy_state.callback.register(id.clone());

    if let Err(e) = publish_to_jetstream(
        proxy_state.stream,
        &proxy_state.callback_url,
        headers,
        body,
        proxy_state.context,
        id.clone(),
    )
    .await
    {
        // Deregister the ID in the callback proxy state if writing to Jetstream fails
        let _ = proxy_state.callback.deregister(&id).await;
        error!(error = ?e, "Publishing message to Jetstream for sync serve request");
        return Err(ApiError::BadGateway(
            "Failed to write message to Jetstream".to_string(),
        ));
    }

    // TODO: add a timeout for waiting on rx. make sure deregister is called if timeout branch is invoked.
    if let Err(e) = notify.await {
        error!(error = ?e, "Waiting for the pipeline output");
        return Err(ApiError::InternalServerError(
            "Failed while waiting on pipeline output".to_string(),
        ));
    }

    let result = match proxy_state.callback.retrieve_saved(&id).await {
        Ok(result) => result,
        Err(e) => {
            error!(error = ?e, "Failed to retrieve from redis");
            return Err(ApiError::InternalServerError(
                "Failed to retrieve from redis".to_string(),
            ));
        }
    };

    // The reponse can be a binary array of elements as single chunk. For the user to process the blob, we return the array len and
    // length of each element in the array. This will help the user to decomponse the binary response chunk into individual
    // elements.
    let mut header_map = HeaderMap::new();
    let response_arr_len: String = result
        .iter()
        .map(|x| x.len().to_string())
        .collect::<Vec<_>>()
        .join(",");
    header_map.insert(NUMAFLOW_RESP_ARRAY_LEN, result.len().into());
    header_map.insert(
        NUMAFLOW_RESP_ARRAY_IDX_LEN,
        HeaderValue::from_str(response_arr_len.as_str()).map_err(|e| {
            ApiError::InternalServerError(format!("Encoding response array len failed: {}", e))
        })?,
    );

    // concatenate as single result, should use header values to decompose based on the len
    let body = result.concat();

    Ok((header_map, body))
}

async fn sync_publish<T: Send + Sync + Clone + Store>(
    State(mut proxy_state): State<ProxyState<T>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<ServeResponse>, ApiError> {
    let id = extract_id_from_headers(&headers);

    // Register the ID in the callback proxy state
    let notify = proxy_state.callback.register(id.clone());

    if let Err(e) = publish_to_jetstream(
        proxy_state.stream,
        &proxy_state.callback_url,
        headers,
        body,
        proxy_state.context,
        id.clone(),
    )
    .await
    {
        // Deregister the ID in the callback proxy state if writing to Jetstream fails
        let _ = proxy_state.callback.deregister(&id).await;
        error!(error = ?e, "Publishing message to Jetstream for sync request");
        return Err(ApiError::BadGateway(
            "Failed to write message to Jetstream".to_string(),
        ));
    }

    // TODO: add a timeout for waiting on rx. make sure deregister is called if timeout branch is invoked.
    match notify.await {
        Ok(result) => match result {
            Ok(value) => Ok(Json(ServeResponse::new(
                "Successfully processed the message".to_string(),
                value,
                StatusCode::OK,
            ))),
            Err(_) => Err(ApiError::InternalServerError(
                "Failed while waiting for the pipeline output".to_string(),
            )),
        },
        Err(e) => {
            error!(error=?e, "Waiting for the pipeline output");
            Err(ApiError::BadGateway(
                "Failed while waiting for the pipeline output".to_string(),
            ))
        }
    }
}

async fn async_publish<T: Send + Sync + Clone + Store>(
    State(proxy_state): State<ProxyState<T>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<ServeResponse>, ApiError> {
    let id = extract_id_from_headers(&headers);

    let result = publish_to_jetstream(
        proxy_state.stream,
        &proxy_state.callback_url,
        headers,
        body,
        proxy_state.context,
        id.clone(),
    )
    .await;

    match result {
        Ok(_) => Ok(Json(ServeResponse::new(
            "Successfully published message".to_string(),
            id,
            StatusCode::OK,
        ))),
        Err(e) => {
            error!(error = ?e, "Publishing message to Jetstream");
            Err(ApiError::InternalServerError(
                "Failed to publish message to Jetstream".to_string(),
            ))
        }
    }
}

/// Write to JetStream and return the metadata. It is responsible for getting the ID from the header.
async fn publish_to_jetstream(
    stream: &'static str,
    callback_url: &str,
    headers: HeaderMap,
    body: Bytes,
    js_context: Context,
    id: String, // Added ID as a parameter
) -> Result<(), async_nats::Error> {
    let mut js_headers = JSHeaderMap::new();

    // pass in the HTTP headers as jetstream headers
    for (k, v) in headers.iter() {
        js_headers.append(k.as_ref(), String::from_utf8_lossy(v.as_bytes()).borrow())
    }

    js_headers.append(ID_HEADER_KEY, id.as_str()); // Use the passed ID
    js_headers.append(CALLBACK_URL_KEY, callback_url);

    js_context
        .publish_with_headers(stream, js_headers, body)
        .await
        .inspect_err(|e| error!(stream, error=?e, "Publishing message to stream"))?
        .await
        .inspect_err(
            |e| error!(stream, error=?e, "Waiting for acknowledgement of published message"),
        )?;

    Ok(())
}

// extracts the ID from the headers, if not found, generates a new UUID
fn extract_id_from_headers(headers: &HeaderMap) -> String {
    headers.get(&config().tid_header).map_or_else(
        || Uuid::new_v4().to_string(),
        |v| String::from_utf8_lossy(v.as_bytes()).to_string(),
    )
}

#[cfg(feature = "nats-tests")]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_nats::jetstream;
    use async_nats::jetstream::stream;
    use axum::body::{to_bytes, Body};
    use axum::extract::Request;
    use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::app::callback::store::memstore::InMemoryStore;
    use crate::app::callback::store::PayloadToSave;
    use crate::app::callback::CallbackRequest;
    use crate::app::tracker::MessageGraph;
    use crate::pipeline::pipeline_spec;
    use crate::Error;

    use super::*;

    #[derive(Clone)]
    struct MockStore;

    impl Store for MockStore {
        async fn save(&mut self, _messages: Vec<PayloadToSave>) -> crate::Result<()> {
            Ok(())
        }
        async fn retrieve_callbacks(
            &mut self,
            _id: &str,
        ) -> Result<Vec<Arc<CallbackRequest>>, Error> {
            Ok(vec![])
        }
        async fn retrieve_datum(&mut self, _id: &str) -> Result<Vec<Vec<u8>>, Error> {
            Ok(vec![])
        }
        async fn ready(&mut self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn test_async_publish() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = async_nats::connect(&config().jetstream.url)
            .await
            .map_err(|e| format!("Connecting to Jetstream: {:?}", e))?;

        let context = jetstream::new(client);
        let id = "foobar";
        let stream_name = "default";

        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await
            .map_err(|e| format!("creating stream {}: {}", &config().jetstream.url, e))?;

        let mock_store = MockStore {};
        let msg_graph = MessageGraph::from_pipeline(pipeline_spec())
            .map_err(|e| format!("Failed to create message graph from pipeline spec: {:?}", e))?;

        let callback_state = CallbackState::new(msg_graph, mock_store).await?;
        let app = jetstream_proxy(context, callback_state).await?;
        let res = Request::builder()
            .method("POST")
            .uri("/async")
            .header(CONTENT_TYPE, "text/plain")
            .header("id", id)
            .body(Body::from("Test Message"))
            .unwrap();

        let response = app.oneshot(res).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let result = extract_response_from_body(response.into_body()).await;
        assert_eq!(
            result,
            json!({
                "message": "Successfully published message",
                "id": id,
                "code": 200
            })
        );
        Ok(())
    }

    async fn extract_response_from_body(body: Body) -> Value {
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        let mut resp: Value = serde_json::from_slice(&bytes).unwrap();
        let _ = resp.as_object_mut().unwrap().remove("timestamp").unwrap();
        resp
    }

    fn create_default_callbacks(id: &str) -> Vec<CallbackRequest> {
        vec![
            CallbackRequest {
                id: id.to_string(),
                vertex: "in".to_string(),
                cb_time: 12345,
                from_vertex: "in".to_string(),
                tags: None,
            },
            CallbackRequest {
                id: id.to_string(),
                vertex: "cat".to_string(),
                cb_time: 12345,
                from_vertex: "in".to_string(),
                tags: None,
            },
            CallbackRequest {
                id: id.to_string(),
                vertex: "out".to_string(),
                cb_time: 12345,
                from_vertex: "cat".to_string(),
                tags: None,
            },
        ]
    }

    #[tokio::test]
    async fn test_sync_publish() {
        let client = async_nats::connect(&config().jetstream.url).await.unwrap();
        let context = jetstream::new(client);
        let id = "foobar";
        let stream_name = "sync_pub";

        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await
            .map_err(|e| format!("creating stream {}: {}", &config().jetstream.url, e));

        let mem_store = InMemoryStore::new();
        let msg_graph = MessageGraph::from_pipeline(pipeline_spec()).unwrap();

        let mut callback_state = CallbackState::new(msg_graph, mem_store).await.unwrap();

        let app = jetstream_proxy(context, callback_state.clone())
            .await
            .unwrap();

        tokio::spawn(async move {
            let cbs = create_default_callbacks(id);
            let mut retries = 0;
            loop {
                match callback_state.insert_callback_requests(cbs.clone()).await {
                    Ok(_) => break,
                    Err(e) => {
                        retries += 1;
                        if retries > 10 {
                            panic!("Failed to insert callback requests: {:?}", e);
                        }
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                }
            }
        });

        let res = Request::builder()
            .method("POST")
            .uri("/sync")
            .header("Content-Type", "text/plain")
            .header("id", id)
            .body(Body::from("Test Message"))
            .unwrap();

        let response = app.clone().oneshot(res).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let result = extract_response_from_body(response.into_body()).await;
        assert_eq!(
            result,
            json!({
                "message": "Successfully processed the message",
                "id": id,
                "code": 200
            })
        );
    }

    #[tokio::test]
    async fn test_sync_publish_serve() {
        let client = async_nats::connect(&config().jetstream.url).await.unwrap();
        let context = jetstream::new(client);
        let id = "foobar";
        let stream_name = "sync_serve_pub";

        let _stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.into(),
                subjects: vec![stream_name.into()],
                ..Default::default()
            })
            .await
            .map_err(|e| format!("creating stream {}: {}", &config().jetstream.url, e));

        let mem_store = InMemoryStore::new();
        let msg_graph = MessageGraph::from_pipeline(pipeline_spec()).unwrap();

        let mut callback_state = CallbackState::new(msg_graph, mem_store).await.unwrap();

        let app = jetstream_proxy(context, callback_state.clone())
            .await
            .unwrap();

        // pipeline is in -> cat -> out, so we will have 3 callback requests
        let cbs = create_default_callbacks(id);

        // spawn a tokio task which will insert the callback requests to the callback state
        // if it fails, sleep for 10ms and retry
        tokio::spawn(async move {
            let mut retries = 0;
            loop {
                match callback_state.insert_callback_requests(cbs.clone()).await {
                    Ok(_) => {
                        // save a test message, we should get this message when serve is invoked
                        // with foobar id
                        callback_state
                            .save_response("foobar".to_string(), Bytes::from("Test Message 1"))
                            .await
                            .unwrap();
                        callback_state
                            .save_response(
                                "foobar".to_string(),
                                Bytes::from("Another Test Message 2"),
                            )
                            .await
                            .unwrap();
                        break;
                    }
                    Err(e) => {
                        retries += 1;
                        if retries > 10 {
                            panic!("Failed to insert callback requests: {:?}", e);
                        }
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                }
            }
        });

        let res = Request::builder()
            .method("POST")
            .uri("/sync_serve")
            .header("Content-Type", "text/plain")
            .header("id", id)
            .body(Body::from("Test Message"))
            .unwrap();

        let response = app.oneshot(res).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let content_len = response.headers().get(CONTENT_LENGTH).unwrap();
        assert_eq!(content_len.as_bytes(), b"36");

        let hval_response_len = response.headers().get(NUMAFLOW_RESP_ARRAY_LEN).unwrap();
        assert_eq!(hval_response_len.as_bytes(), b"2");

        let hval_response_array_len = response.headers().get(NUMAFLOW_RESP_ARRAY_IDX_LEN).unwrap();
        assert_eq!(hval_response_array_len.as_bytes(), b"14,22");

        let result = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(result, "Test Message 1Another Test Message 2".as_bytes());
    }
}
