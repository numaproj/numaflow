use std::{str::FromStr, sync::Arc};

use super::{orchestrator, store::datastore::DataStore, AppState};
use super::{FetchQueryParams, Tid};
use crate::app::response::{ApiError, ServeResponse};
use crate::app::store::cbstore::CallbackStore;
use crate::app::store::datastore::Error as StoreError;
use crate::metrics::serving_metrics;
use crate::Error;
use async_nats::jetstream::Context;
use axum::response::sse::Event;
use axum::response::Sse;
use axum::Extension;
use axum::{
    body::{Body, Bytes},
    extract::{Query, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use tokio::time::Instant;
use tokio_stream::{Stream, StreamExt};
use tracing::{error, Instrument};

const NUMAFLOW_RESP_ARRAY_LEN: &str = "Numaflow-Array-Len";
const NUMAFLOW_RESP_ARRAY_IDX_LEN: &str = "Numaflow-Array-Index-Len";

struct ProxyState<T, U> {
    js_context: Context,
    stream: String,
    tid_header: String,
    orchestrator: orchestrator::OrchestratorState<T, U>,
}

pub(crate) async fn jetstream_proxy<
    T: Clone + Send + Sync + DataStore + 'static,
    U: Clone + Send + Sync + CallbackStore + 'static,
>(
    state: AppState<T, U>,
) -> crate::Result<Router> {
    let proxy_state = Arc::new(ProxyState {
        js_context: state.js_context.clone(),
        stream: state.settings.js_message_stream.clone(),
        tid_header: state.settings.tid_header.clone(),
        orchestrator: state.orchestrator_state.clone(),
    });

    let router = Router::new()
        .route("/async", post(async_publish))
        .route("/sync", post(sync_publish))
        .route("/fetch", get(fetch))
        .route("/sse", get(sse_handler))
        .with_state(proxy_state);
    Ok(router)
}

async fn fetch<
    T: Send + Sync + Clone + DataStore + 'static,
    U: Send + Sync + Clone + CallbackStore + 'static,
>(
    State(proxy_state): State<Arc<ProxyState<T, U>>>,
    Query(FetchQueryParams { id }): Query<FetchQueryParams>,
) -> Response {
    let datum_retrieve_start = Instant::now();
    let pipeline_result = match proxy_state.orchestrator.clone().retrieve_saved(&id).await {
        Ok(result) => {
            serving_metrics()
                .datum_retrive_duration
                .observe(datum_retrieve_start.elapsed().as_micros() as f64);
            result
        }
        Err(e) => {
            if let Error::Store(e) = e {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::new(format!(
                        "Failed to fetch response for {id} from store: {e}"
                    )))
                    .expect("creating response");
            }
            error!(error = ?e, "Failed to retrieve data");
            return ApiError::InternalServerError("Failed to retrieve data".to_string())
                .into_response();
        }
    };

    let Some(result) = pipeline_result else {
        return Json(json!({"status": "in-progress"})).into_response();
    };

    // The response can be a binary array of elements as single chunk. For the user to process the blob, we return the array len and
    // length of each element in the array. This will help the user to decompose the binary response chunk into individual
    // elements.
    let mut header_map = HeaderMap::new();
    let response_arr_len: String = result
        .iter()
        .map(|x| x.len().to_string())
        .collect::<Vec<_>>()
        .join(",");

    let arr_idx_header_val = match HeaderValue::from_str(response_arr_len.as_str()) {
        Ok(val) => val,
        Err(e) => {
            error!(?e, "Encoding response array length");
            return ApiError::InternalServerError(format!(
                "Encoding response array len failed: {}",
                e
            ))
            .into_response();
        }
    };
    header_map.insert(NUMAFLOW_RESP_ARRAY_LEN, result.len().into());
    header_map.insert(NUMAFLOW_RESP_ARRAY_IDX_LEN, arr_idx_header_val);

    // concatenate as single result, should use header values to decompose based on the len
    let body = result.concat();

    (header_map, body).into_response()
}

async fn sse_handler<T, U>(
    State(proxy_state): State<Arc<ProxyState<T, U>>>,
    Extension(Tid(id)): Extension<Tid>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError>
where
    T: Send + Sync + Clone + DataStore + 'static,
    U: Send + Sync + Clone + CallbackStore + 'static,
{
    let mut msg_headers = async_nats::HeaderMap::new();
    for (key, value) in headers.iter() {
        msg_headers.insert(
            key.to_string(),
            String::from_utf8_lossy(value.as_bytes()).to_string(),
        );
    }
    msg_headers.insert(proxy_state.tid_header.clone(), id.clone());

    let mut orchestrator_state = proxy_state.orchestrator.clone();
    let response_stream = orchestrator_state.stream_response(&id).await.unwrap();

    let save_start = Instant::now();
    proxy_state
        .js_context
        .publish_with_headers(proxy_state.stream.clone(), msg_headers, body)
        .await
        .inspect_err(|err| {
            tracing::error!(
                ?err,
                id,
                stream = proxy_state.stream,
                "Saving message to Jetstream"
            )
        })
        .map_err(|_| ApiError::BadGateway("Failed to write message to Jetstream".to_string()))?
        .await
        .inspect_err(|err| {
            tracing::error!(
                ?err,
                id,
                stream = proxy_state.stream,
                "Waiting for message save confirmation from Jetstream"
            )
        })
        .map_err(|_| ApiError::BadGateway("Failed to write message to Jetstream".to_string()))?;
    serving_metrics()
        .payload_save_duration
        .observe(save_start.elapsed().as_micros() as f64);

    let stream = response_stream
        .map(|response| Ok(Event::default().data(String::from_utf8_lossy(&response))));

    Ok(Sse::new(stream))
}

async fn sync_publish<
    T: Send + Sync + Clone + DataStore + 'static,
    U: Send + Sync + Clone + CallbackStore + 'static,
>(
    State(proxy_state): State<Arc<ProxyState<T, U>>>,
    Extension(Tid(id)): Extension<Tid>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let mut msg_headers = async_nats::HeaderMap::new();
    for (key, value) in headers.iter() {
        msg_headers.insert(
            key.to_string(),
            String::from_utf8_lossy(value.as_bytes()).to_string(),
        );
    }
    msg_headers.insert(proxy_state.tid_header.clone(), id.clone());

    let processing_start = Instant::now();
    // Register the ID in the callback proxy state
    let notify = match proxy_state
        .orchestrator
        .clone()
        .process_request(id.as_str())
        .await
    {
        Ok(result) => result,
        Err(e) => {
            error!(error = ?e, "Registering request in data store");
            if let StoreError::DuplicateRequest(id) = e {
                return Err(ApiError::Conflict(format!(
                    "Specified request id {id} already exists in the store"
                )));
            }
            return Err(ApiError::InternalServerError(
                "Failed to register message".to_string(),
            ));
        }
    };

    let save_start = Instant::now();
    proxy_state
        .js_context
        .publish_with_headers(proxy_state.stream.clone(), msg_headers, body)
        .await
        .inspect_err(|err| {
            tracing::error!(
                ?err,
                id,
                stream = proxy_state.stream,
                "Saving message to Jetstream"
            )
        })
        .map_err(|_| ApiError::BadGateway("Failed to write message to Jetstream".to_string()))?
        .await
        .inspect_err(|err| {
            tracing::error!(
                ?err,
                id,
                stream = proxy_state.stream,
                "Waiting for message save confirmation from Jetstream"
            )
        })
        .map_err(|_| ApiError::BadGateway("Failed to write message to Jetstream".to_string()))?;
    serving_metrics()
        .payload_save_duration
        .observe(save_start.elapsed().as_micros() as f64);

    let processing_result = match notify.await {
        Ok(processing_result) => {
            serving_metrics()
                .processing_time
                .observe(processing_start.elapsed().as_micros() as f64);
            processing_result
        }
        Err(e) => {
            error!(error = ?e, "Waiting for the pipeline output");
            return Err(ApiError::InternalServerError(
                "Failed while waiting on pipeline output".to_string(),
            ));
        }
    };

    if let Err(err) = processing_result {
        tracing::error!(
            ?err,
            "The request processing task failed. Returning error to the client"
        );
        return Err(ApiError::InternalServerError(
            "Failed to collect results of processing from all vertices".to_string(),
        ));
    }

    let datum_retrieve_start = Instant::now();
    let result = match proxy_state.orchestrator.clone().retrieve_saved(&id).await {
        Ok(result) => {
            serving_metrics()
                .datum_retrive_duration
                .observe(datum_retrieve_start.elapsed().as_micros() as f64);
            result
        }
        Err(e) => {
            error!(error = ?e, "Failed to retrieve from store");
            return Err(ApiError::InternalServerError(
                "Failed to retrieve result from Datum store".to_string(),
            ));
        }
    };

    let mut header_map = HeaderMap::new();
    header_map.insert(
        HeaderName::from_str(&proxy_state.tid_header).unwrap(),
        HeaderValue::from_str(&id).unwrap(),
    );

    let Some(result) = result else {
        return Ok(Json(json!({"status": "processing"})).into_response());
    };

    // The response can be a binary array of elements as single chunk. For the user to process the blob, we return the array len and
    // length of each element in the array. This will help the user to decompose the binary response chunk into individual
    // elements.
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

    Ok((header_map, body).into_response())
}

async fn async_publish<
    T: Send + Sync + Clone + DataStore + 'static,
    U: Send + Sync + Clone + CallbackStore + 'static,
>(
    State(proxy_state): State<Arc<ProxyState<T, U>>>,
    Extension(Tid(id)): Extension<Tid>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<ServeResponse>, ApiError> {
    let mut msg_headers = async_nats::HeaderMap::new();
    for (key, value) in headers.iter() {
        msg_headers.insert(
            key.to_string(),
            String::from_utf8_lossy(value.as_bytes()).to_string(),
        );
    }
    msg_headers.insert(proxy_state.tid_header.clone(), id.clone());

    let processing_start = Instant::now();
    // Register request in Redis
    let notify = match proxy_state
        .orchestrator
        .clone()
        .process_request(id.as_str())
        .await
    {
        Ok(result) => result,
        Err(e) => {
            error!(error = ?e, "Registering request in data store");
            if let StoreError::DuplicateRequest(id) = e {
                return Err(ApiError::Conflict(format!(
                    "Specified request id {id} already exists in the store"
                )));
            }
            return Err(ApiError::InternalServerError(
                "Failed to register message".to_string(),
            ));
        }
    };

    {
        let span = tracing::Span::current();
        let mut orchestrator = proxy_state.orchestrator.clone();
        let id = id.clone();
        // A send operation happens at the sender end of the notify channel when all callbacks are received.
        // We keep the receiver alive to avoid send failure.
        let wait_for_callbacks = async move {
            match notify.await {
                Ok(val) => {
                    match val {
                        Ok(_) => {}
                        Err(e) => {
                            _ = orchestrator
                                .mark_as_failed(id.as_str(), e.to_string().as_str())
                                .await
                                .inspect_err(|e| error!(error=?e, "Failed to mark as failed"));
                        }
                    }
                    serving_metrics()
                        .processing_time
                        .observe(processing_start.elapsed().as_micros() as f64);
                }
                Err(e) => {
                    error!(error = ?e, "Waiting for the pipeline output");
                }
            };
        };
        tokio::spawn(wait_for_callbacks.instrument(span));
    }

    let save_start = Instant::now();
    proxy_state
        .js_context
        .publish_with_headers(proxy_state.stream.clone(), msg_headers, body)
        .await
        .inspect_err(|err| {
            tracing::error!(
                ?err,
                id,
                stream = proxy_state.stream,
                "Saving message to Jetstream"
            )
        })
        .map_err(|_| ApiError::BadGateway("Failed to write message to Jetstream".to_string()))?
        .await
        .inspect_err(|err| {
            tracing::error!(
                ?err,
                id,
                stream = proxy_state.stream,
                "Waiting for message save confirmation from Jetstream"
            )
        })
        .map_err(|_| ApiError::BadGateway("Failed to write message to Jetstream".to_string()))?;
    serving_metrics()
        .payload_save_duration
        .observe(save_start.elapsed().as_micros() as f64);

    Ok(Json(ServeResponse::new(
        "Successfully published message".to_string(),
        id,
        StatusCode::OK,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::orchestrator::OrchestratorState;
    use crate::app::router_with_auth;
    use crate::app::store::cbstore::jetstreamstore::JetStreamCallbackStore;
    use crate::app::store::datastore::jetstream::JetStreamDataStore;
    use crate::app::tracker::MessageGraph;
    use crate::callback::{Callback, Response};
    use crate::config::DEFAULT_ID_HEADER;
    use crate::pipeline::PipelineDCG;
    use crate::Settings;
    use async_nats::jetstream;
    use axum::body::{to_bytes, Body};
    use axum::extract::Request;
    use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE};
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    const PIPELINE_SPEC_ENCODED: &str = "eyJ2ZXJ0aWNlcyI6W3sibmFtZSI6ImluIiwic291cmNlIjp7InNlcnZpbmciOnsiYXV0aCI6bnVsbCwic2VydmljZSI6dHJ1ZSwibXNnSURIZWFkZXJLZXkiOiJYLU51bWFmbG93LUlkIiwic3RvcmUiOnsidXJsIjoicmVkaXM6Ly9yZWRpczo2Mzc5In19fSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIiLCJlbnYiOlt7Im5hbWUiOiJSVVNUX0xPRyIsInZhbHVlIjoiZGVidWcifV19LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6InBsYW5uZXIiLCJ1ZGYiOnsiY29udGFpbmVyIjp7ImltYWdlIjoiYXNjaWk6MC4xIiwiYXJncyI6WyJwbGFubmVyIl0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sImJ1aWx0aW4iOm51bGwsImdyb3VwQnkiOm51bGx9LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6InRpZ2VyIiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsidGlnZXIiXSwicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwiYnVpbHRpbiI6bnVsbCwiZ3JvdXBCeSI6bnVsbH0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiZG9nIiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsiZG9nIl0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sImJ1aWx0aW4iOm51bGwsImdyb3VwQnkiOm51bGx9LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19LHsibmFtZSI6ImVsZXBoYW50IiwidWRmIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6ImFzY2lpOjAuMSIsImFyZ3MiOlsiZWxlcGhhbnQiXSwicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwiYnVpbHRpbiI6bnVsbCwiZ3JvdXBCeSI6bnVsbH0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiYXNjaWlhcnQiLCJ1ZGYiOnsiY29udGFpbmVyIjp7ImltYWdlIjoiYXNjaWk6MC4xIiwiYXJncyI6WyJhc2NpaWFydCJdLCJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJidWlsdGluIjpudWxsLCJncm91cEJ5IjpudWxsfSwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7fSwiaW1hZ2VQdWxsUG9saWN5IjoiTmV2ZXIifSwic2NhbGUiOnsibWluIjoxfSwidXBkYXRlU3RyYXRlZ3kiOnsidHlwZSI6IlJvbGxpbmdVcGRhdGUiLCJyb2xsaW5nVXBkYXRlIjp7Im1heFVuYXZhaWxhYmxlIjoiMjUlIn19fSx7Im5hbWUiOiJzZXJ2ZS1zaW5rIiwic2luayI6eyJ1ZHNpbmsiOnsiY29udGFpbmVyIjp7ImltYWdlIjoic2VydmVzaW5rOjAuMSIsImVudiI6W3sibmFtZSI6Ik5VTUFGTE9XX0NBTExCQUNLX1VSTF9LRVkiLCJ2YWx1ZSI6IlgtTnVtYWZsb3ctQ2FsbGJhY2stVXJsIn0seyJuYW1lIjoiTlVNQUZMT1dfTVNHX0lEX0hFQURFUl9LRVkiLCJ2YWx1ZSI6IlgtTnVtYWZsb3ctSWQifV0sInJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn19LCJyZXRyeVN0cmF0ZWd5Ijp7fX0sImNvbnRhaW5lclRlbXBsYXRlIjp7InJlc291cmNlcyI6e30sImltYWdlUHVsbFBvbGljeSI6Ik5ldmVyIn0sInNjYWxlIjp7Im1pbiI6MX0sInVwZGF0ZVN0cmF0ZWd5Ijp7InR5cGUiOiJSb2xsaW5nVXBkYXRlIiwicm9sbGluZ1VwZGF0ZSI6eyJtYXhVbmF2YWlsYWJsZSI6IjI1JSJ9fX0seyJuYW1lIjoiZXJyb3Itc2luayIsInNpbmsiOnsidWRzaW5rIjp7ImNvbnRhaW5lciI6eyJpbWFnZSI6InNlcnZlc2luazowLjEiLCJlbnYiOlt7Im5hbWUiOiJOVU1BRkxPV19DQUxMQkFDS19VUkxfS0VZIiwidmFsdWUiOiJYLU51bWFmbG93LUNhbGxiYWNrLVVybCJ9LHsibmFtZSI6Ik5VTUFGTE9XX01TR19JRF9IRUFERVJfS0VZIiwidmFsdWUiOiJYLU51bWFmbG93LUlkIn1dLCJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9fSwicmV0cnlTdHJhdGVneSI6e319LCJjb250YWluZXJUZW1wbGF0ZSI6eyJyZXNvdXJjZXMiOnt9LCJpbWFnZVB1bGxQb2xpY3kiOiJOZXZlciJ9LCJzY2FsZSI6eyJtaW4iOjF9LCJ1cGRhdGVTdHJhdGVneSI6eyJ0eXBlIjoiUm9sbGluZ1VwZGF0ZSIsInJvbGxpbmdVcGRhdGUiOnsibWF4VW5hdmFpbGFibGUiOiIyNSUifX19XSwiZWRnZXMiOlt7ImZyb20iOiJpbiIsInRvIjoicGxhbm5lciIsImNvbmRpdGlvbnMiOm51bGx9LHsiZnJvbSI6InBsYW5uZXIiLCJ0byI6ImFzY2lpYXJ0IiwiY29uZGl0aW9ucyI6eyJ0YWdzIjp7Im9wZXJhdG9yIjoib3IiLCJ2YWx1ZXMiOlsiYXNjaWlhcnQiXX19fSx7ImZyb20iOiJwbGFubmVyIiwidG8iOiJ0aWdlciIsImNvbmRpdGlvbnMiOnsidGFncyI6eyJvcGVyYXRvciI6Im9yIiwidmFsdWVzIjpbInRpZ2VyIl19fX0seyJmcm9tIjoicGxhbm5lciIsInRvIjoiZG9nIiwiY29uZGl0aW9ucyI6eyJ0YWdzIjp7Im9wZXJhdG9yIjoib3IiLCJ2YWx1ZXMiOlsiZG9nIl19fX0seyJmcm9tIjoicGxhbm5lciIsInRvIjoiZWxlcGhhbnQiLCJjb25kaXRpb25zIjp7InRhZ3MiOnsib3BlcmF0b3IiOiJvciIsInZhbHVlcyI6WyJlbGVwaGFudCJdfX19LHsiZnJvbSI6InRpZ2VyIiwidG8iOiJzZXJ2ZS1zaW5rIiwiY29uZGl0aW9ucyI6bnVsbH0seyJmcm9tIjoiZG9nIiwidG8iOiJzZXJ2ZS1zaW5rIiwiY29uZGl0aW9ucyI6bnVsbH0seyJmcm9tIjoiZWxlcGhhbnQiLCJ0byI6InNlcnZlLXNpbmsiLCJjb25kaXRpb25zIjpudWxsfSx7ImZyb20iOiJhc2NpaWFydCIsInRvIjoic2VydmUtc2luayIsImNvbmRpdGlvbnMiOm51bGx9LHsiZnJvbSI6InBsYW5uZXIiLCJ0byI6ImVycm9yLXNpbmsiLCJjb25kaXRpb25zIjp7InRhZ3MiOnsib3BlcmF0b3IiOiJvciIsInZhbHVlcyI6WyJlcnJvciJdfX19XSwibGlmZWN5Y2xlIjp7fSwid2F0ZXJtYXJrIjp7fX0=";

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_async_publish() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        const ID_HEADER: &str = "X-Numaflow-Id";
        const ID_VALUE: &str = "foobar";

        let store_name = "test_async_publish";
        let message_stream_name = "test_async_publish_messages";
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let _ = context.delete_key_value(store_name).await;
        let _ = context.delete_stream(message_stream_name).await;

        let _ = context
            .create_key_value(jetstream::kv::Config {
                bucket: store_name.to_string(),
                history: 5,
                ..Default::default()
            })
            .await
            .unwrap();

        context
            .create_stream(async_nats::jetstream::stream::Config {
                name: message_stream_name.into(),
                ..Default::default()
            })
            .await
            .unwrap();

        let settings = Settings {
            tid_header: ID_HEADER.into(),
            js_callback_store: store_name.to_string(),
            js_message_stream: message_stream_name.to_string(),
            ..Default::default()
        };

        let callback_store = JetStreamCallbackStore::new(context.clone(), store_name)
            .await
            .expect("Failed to create callback store");

        let datum_store = JetStreamDataStore::new(context.clone(), store_name)
            .await
            .expect("Failed to create datum store");

        let pipeline_spec = PIPELINE_SPEC_ENCODED.parse().unwrap();
        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec)?;
        let orchestrator_state =
            OrchestratorState::new(msg_graph, datum_store, callback_store).await?;

        let app_state = AppState {
            js_context: context,
            settings: Arc::new(settings),
            orchestrator_state,
        };

        let app = jetstream_proxy(app_state).await?;
        let mut req = Request::builder()
            .method("POST")
            .uri("/async")
            .header(CONTENT_TYPE, "text/plain")
            .header(ID_HEADER, ID_VALUE)
            .body(Body::from("Test Message"))
            .unwrap();

        req.extensions_mut().insert(Tid(ID_VALUE.to_string()));

        let response = app.oneshot(req).await.unwrap();
        // assert_eq!(message.id, ID_VALUE);
        assert_eq!(response.status(), StatusCode::OK);

        let result = extract_response_from_body(response.into_body()).await;
        assert_eq!(
            result,
            json!({
                "message": "Successfully published message",
                "id": ID_VALUE,
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

    fn create_default_callbacks(id: &str) -> Vec<Callback> {
        vec![
            Callback {
                id: id.to_string(),
                vertex: "in".to_string(),
                cb_time: 12345,
                from_vertex: "in".to_string(),
                responses: vec![Response { tags: None }],
            },
            Callback {
                id: id.to_string(),
                vertex: "planner".to_string(),
                cb_time: 12345,
                from_vertex: "in".to_string(),
                responses: vec![Response {
                    tags: Some(vec!["tiger".into()]),
                }],
            },
            Callback {
                id: id.to_string(),
                vertex: "tiger".to_string(),
                cb_time: 12345,
                from_vertex: "planner".to_string(),
                responses: vec![Response { tags: None }],
            },
            Callback {
                id: id.to_string(),
                vertex: "serve-sink".to_string(),
                cb_time: 12345,
                from_vertex: "tiger".to_string(),
                responses: vec![Response { tags: None }],
            },
        ]
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_sync_publish() {
        const ID_HEADER: &str = "X-Numaflow-ID";
        const ID_VALUE: &str = "foobar";

        let store_name = "test_sync_publish";
        let messages_stream_name = "test_sync_publish_messages";
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let _ = context.delete_key_value(store_name).await;

        let kv_store = context
            .create_key_value(jetstream::kv::Config {
                bucket: store_name.to_string(),
                history: 5,
                ..Default::default()
            })
            .await
            .unwrap();
        context
            .create_stream(async_nats::jetstream::stream::Config {
                name: messages_stream_name.into(),
                ..Default::default()
            })
            .await
            .unwrap();

        let settings = Settings {
            tid_header: ID_HEADER.into(),
            js_callback_store: store_name.to_string(),
            js_message_stream: messages_stream_name.to_string(),
            ..Default::default()
        };

        let callback_store = JetStreamCallbackStore::new(context.clone(), store_name)
            .await
            .expect("Failed to create callback store");

        let datum_store = JetStreamDataStore::new(context.clone(), store_name)
            .await
            .expect("Failed to create datum store");

        let pipeline_spec: PipelineDCG = PIPELINE_SPEC_ENCODED.parse().unwrap();
        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec).unwrap();

        let orchestrator_state = OrchestratorState::new(msg_graph, datum_store, callback_store)
            .await
            .unwrap();

        let app_state = AppState {
            js_context: context,
            settings: Arc::new(settings),
            orchestrator_state,
        };

        let app = jetstream_proxy(app_state).await.unwrap();

        tokio::spawn(async move {
            let rs_key = format!("rs.{ID_VALUE}.temp.12345");
            kv_store
                .put(&rs_key, Bytes::from("test-output"))
                .await
                .unwrap();
            let cbs = create_default_callbacks(ID_VALUE);
            for cb in cbs {
                let cb_key = format!("cb.{ID_VALUE}.temp.{}", cb.cb_time);
                kv_store.put(&cb_key, cb.try_into().unwrap()).await.unwrap();
            }
        });

        let mut req = Request::builder()
            .method("POST")
            .uri("/sync")
            .header("Content-Type", "text/plain")
            .header(ID_HEADER, ID_VALUE)
            .body(Body::from("Test Message"))
            .unwrap();

        req.extensions_mut().insert(Tid(ID_VALUE.to_string()));

        let response = app.clone().oneshot(req).await.unwrap();
        // assert_eq!(message.id, ID_VALUE);
        assert_eq!(response.status(), StatusCode::OK);

        let result = to_bytes(response.into_body(), 10 * 1024).await.unwrap();
        assert_eq!(result, Bytes::from_static(b"test-output"));
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_sync_publish_serve() {
        const ID_VALUE: &str = "foobar";

        let store_name = "test_sync_publish_serve";
        let messages_stream_name = "test_sync_publish_serve_messages";
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let _ = context.delete_key_value(store_name).await;

        let kv_store = context
            .create_key_value(jetstream::kv::Config {
                bucket: store_name.to_string(),
                history: 5,
                ..Default::default()
            })
            .await
            .unwrap();

        context
            .create_stream(async_nats::jetstream::stream::Config {
                name: messages_stream_name.into(),
                ..Default::default()
            })
            .await
            .unwrap();

        let settings = Settings {
            js_callback_store: store_name.to_string(),
            js_message_stream: messages_stream_name.to_string(),
            ..Default::default()
        };

        let callback_store = JetStreamCallbackStore::new(context.clone(), store_name)
            .await
            .expect("Failed to create callback store");

        let datum_store = JetStreamDataStore::new(context.clone(), store_name)
            .await
            .expect("Failed to create datum store");

        let pipeline_spec: PipelineDCG = PIPELINE_SPEC_ENCODED.parse().unwrap();
        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec).unwrap();

        let orchestrator_state = OrchestratorState::new(msg_graph, datum_store, callback_store)
            .await
            .unwrap();

        let app_state = AppState {
            js_context: context,
            settings: Arc::new(settings),
            orchestrator_state,
        };

        let app = jetstream_proxy(app_state).await.unwrap();

        // pipeline is in -> cat -> out, so we will have 3 callback requests
        // spawn a tokio task which will insert the callback requests to the callback state
        tokio::spawn(async move {
            let rs_key_one = format!("rs.{ID_VALUE}.temp.12345");
            kv_store
                .put(&rs_key_one, Bytes::from("Test Message 1"))
                .await
                .unwrap();
            let rs_key_two = format!("rs.{ID_VALUE}.temp.12346");
            kv_store
                .put(&rs_key_two, Bytes::from("Another Test Message 2"))
                .await
                .unwrap();

            let cbs = create_default_callbacks(ID_VALUE);
            for cb in cbs {
                let cb_key = format!("cb.{ID_VALUE}.temp.{}", cb.cb_time);
                kv_store.put(&cb_key, cb.try_into().unwrap()).await.unwrap();
            }
        });

        let mut req = Request::builder()
            .method("POST")
            .uri("/sync")
            .header("Content-Type", "text/plain")
            .header(DEFAULT_ID_HEADER, ID_VALUE)
            .body(Body::from("Test Message"))
            .unwrap();

        req.extensions_mut().insert(Tid(ID_VALUE.to_string()));

        let response = app.clone().oneshot(req).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let content_len = response.headers().get(CONTENT_LENGTH).unwrap();
        assert_eq!(content_len.as_bytes(), b"36");

        let hval_response_len = response.headers().get(NUMAFLOW_RESP_ARRAY_LEN).unwrap();
        assert_eq!(hval_response_len.as_bytes(), b"2");

        let hval_response_array_len = response.headers().get(NUMAFLOW_RESP_ARRAY_IDX_LEN).unwrap();
        assert_eq!(hval_response_array_len.as_bytes(), b"14,22");

        let result = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(result, "Test Message 1Another Test Message 2".as_bytes());

        // Get result for the request id using /fetch endpoint
        let req = Request::builder()
            .method("GET")
            .uri(format!("/fetch?id={ID_VALUE}"))
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let serve_resp = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serve_resp,
            Bytes::from_static(b"Test Message 1Another Test Message 2")
        );
    }

    #[cfg(feature = "nats-tests")]
    #[tokio::test]
    async fn test_sse_handler() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        const ID_HEADER: &str = "X-Numaflow-Id";
        const ID_VALUE: &str = "foobar";

        let store_name = "test_sse_handler";
        let messages_stream_name = "test_sse_handler_messages";
        let js_url = "localhost:4222";
        let client = async_nats::connect(js_url).await.unwrap();
        let context = jetstream::new(client);
        let _ = context.delete_key_value(store_name).await;

        let kv_store = context
            .create_key_value(jetstream::kv::Config {
                bucket: store_name.to_string(),
                history: 5,
                ..Default::default()
            })
            .await
            .unwrap();

        context
            .create_stream(async_nats::jetstream::stream::Config {
                name: messages_stream_name.into(),
                ..Default::default()
            })
            .await
            .unwrap();

        let settings = Settings {
            tid_header: ID_HEADER.into(),
            js_callback_store: store_name.to_string(),
            js_message_stream: messages_stream_name.to_string(),
            ..Default::default()
        };

        let callback_store = JetStreamCallbackStore::new(context.clone(), store_name)
            .await
            .expect("Failed to create callback store");

        let datum_store = JetStreamDataStore::new(context.clone(), store_name)
            .await
            .expect("Failed to create datum store");

        let pipeline_spec = PIPELINE_SPEC_ENCODED.parse().unwrap();
        let msg_graph = MessageGraph::from_pipeline(&pipeline_spec)?;
        let orchestrator_state =
            OrchestratorState::new(msg_graph, datum_store, callback_store).await?;

        let app_state = AppState {
            js_context: context,
            settings: Arc::new(settings),
            orchestrator_state,
        };

        let app = router_with_auth(app_state).await?;

        let host = "127.0.0.1";
        // Bind to localhost at the port 0, which will let the OS assign an available port to us
        let listener = TcpListener::bind(format!("{}:0", host)).await.unwrap();
        // Retrieve the port assigned to us by the OS
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async {
            axum::serve(listener, app).await.unwrap();
        });

        // Returns address (e.g. http://127.0.0.1{random_port})
        let listening_url = format!("http://{}:{}", host, port);

        // pipeline is in -> cat -> out, so we will have 3 callback requests
        // spawn a tokio task which will insert the callback requests to the callback state
        tokio::spawn(async move {
            let rs_key_one = format!("rs.{ID_VALUE}.test.12345");
            kv_store
                .put(&rs_key_one, Bytes::from("Test Message 1"))
                .await
                .unwrap();
            let rs_key_two = format!("rs.{ID_VALUE}.test.12346");
            kv_store
                .put(&rs_key_two, Bytes::from("Another Test Message 2"))
                .await
                .unwrap();

            let cbs = create_default_callbacks(ID_VALUE);
            for cb in cbs {
                let cb_key = format!("cb.{ID_VALUE}.test.{}", cb.cb_time);
                kv_store.put(&cb_key, cb.try_into().unwrap()).await.unwrap();
            }
        });

        let mut event_stream = reqwest::Client::new()
            .get(format!("{}/v1/process/sse", listening_url))
            .header("Content-Type", "text/plain")
            .header(DEFAULT_ID_HEADER, ID_VALUE)
            .send()
            .await
            .unwrap()
            .bytes_stream();

        let mut event_data: Vec<Bytes> = vec![];
        while let Some(event) = event_stream.next().await {
            match event {
                Ok(event) => {
                    event_data.push(event);
                }
                Err(_) => {
                    panic!("Error in event stream");
                }
            }
        }

        assert_eq!(event_data.len(), 2);
        assert_eq!(event_data[0], Bytes::from("data: Test Message 1\n\n"));
        assert_eq!(
            event_data[1],
            Bytes::from("data: Another Test Message 2\n\n")
        );
        Ok(())
    }
}
