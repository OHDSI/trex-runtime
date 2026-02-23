use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

#[derive(Debug)]
pub enum AppError {
    NotFound(String),
    BadRequest(String),
    Conflict(String),
    Gone(String),
    Internal(String),
    Timeout(String),
}

impl AppError {
    fn status_code(&self) -> StatusCode {
        match self {
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::Conflict(_) => StatusCode::CONFLICT,
            AppError::Gone(_) => StatusCode::GONE,
            AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Timeout(_) => StatusCode::REQUEST_TIMEOUT,
        }
    }

    fn issue_code(&self) -> &str {
        match self {
            AppError::NotFound(_) => "not-found",
            AppError::BadRequest(_) => "invalid",
            AppError::Conflict(_) => "conflict",
            AppError::Gone(_) => "deleted",
            AppError::Internal(_) => "exception",
            AppError::Timeout(_) => "timeout",
        }
    }

    fn diagnostics(&self) -> &str {
        match self {
            AppError::NotFound(msg)
            | AppError::BadRequest(msg)
            | AppError::Conflict(msg)
            | AppError::Gone(msg)
            | AppError::Internal(msg)
            | AppError::Timeout(msg) => msg,
        }
    }

    pub fn operation_outcome(&self) -> Value {
        json!({
            "resourceType": "OperationOutcome",
            "issue": [{
                "severity": "error",
                "code": self.issue_code(),
                "diagnostics": self.diagnostics()
            }]
        })
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = self.operation_outcome();
        (status, [("content-type", "application/fhir+json")], Json(body)).into_response()
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.diagnostics())
    }
}

impl std::error::Error for AppError {}
