use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Serialize;

// Response sent by the serve handler sync/async to the client(user).
#[derive(Serialize)]
pub(crate) struct ServeResponse {
    pub(crate) message: String,
    pub(crate) id: String,
    pub(crate) code: u16,
    pub(crate) timestamp: DateTime<Utc>,
}

impl ServeResponse {
    pub(crate) fn new(message: String, id: String, status: StatusCode) -> Self {
        Self {
            code: status.as_u16(),
            message,
            id,
            timestamp: Utc::now(),
        }
    }
}

// Error response sent by all the handlers to the client(user).
#[derive(Debug, Serialize)]
pub enum ApiError {
    BadRequest(String),
    InternalServerError(String),
    BadGateway(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct ErrorBody {
            message: String,
            code: u16,
            timestamp: DateTime<Utc>,
        }

        let (status, message) = match self {
            ApiError::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            ApiError::InternalServerError(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
            ApiError::BadGateway(message) => (StatusCode::BAD_GATEWAY, message),
        };

        (
            status,
            Json(ErrorBody {
                code: status.as_u16(),
                message,
                timestamp: Utc::now(),
            }),
        )
            .into_response()
    }
}
