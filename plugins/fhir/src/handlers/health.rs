use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::state::AppState;

static REQUEST_COUNT: AtomicU64 = AtomicU64::new(0);
static ERROR_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn increment_request_count() {
    REQUEST_COUNT.fetch_add(1, Ordering::Relaxed);
}

pub fn increment_error_count() {
    ERROR_COUNT.fetch_add(1, Ordering::Relaxed);
}

pub async fn health_check(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let db_ok = match state.executor.submit("SELECT 1".to_string()).await {
        crate::query_executor::QueryResult::Select { .. } => true,
        crate::query_executor::QueryResult::Execute { .. } => true,
        _ => false,
    };

    if db_ok {
        (
            StatusCode::OK,
            Json(json!({"status": "healthy", "database": "connected"})),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "unhealthy"})),
        )
    }
}

pub async fn metrics() -> impl IntoResponse {
    let requests = REQUEST_COUNT.load(Ordering::Relaxed);
    let errors = ERROR_COUNT.load(Ordering::Relaxed);

    let body = format!(
        "# HELP fhir_requests_total Total FHIR requests\n\
         # TYPE fhir_requests_total counter\n\
         fhir_requests_total {}\n\
         # HELP fhir_errors_total Total FHIR errors\n\
         # TYPE fhir_errors_total counter\n\
         fhir_errors_total {}\n",
        requests, errors
    );

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}
