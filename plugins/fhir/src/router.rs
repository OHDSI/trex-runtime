use axum::body::Body;
use axum::extract::MatchedPath;
use axum::http::{header, Request, Response};
use axum::middleware::{self, Next};
use axum::routing::{delete, get, post, put};
use axum::Router;
use std::sync::Arc;
use std::time::Instant;

use crate::handlers;
use crate::state::AppState;

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(handlers::health::health_check))
        .route("/metrics", get(handlers::health::metrics))
        .route("/datasets", post(handlers::dataset::create_dataset))
        .route("/datasets", get(handlers::dataset::list_datasets))
        .route("/datasets/{dataset_id}", get(handlers::dataset::get_dataset))
        .route(
            "/datasets/{dataset_id}",
            delete(handlers::dataset::delete_dataset),
        )
        .route(
            "/datasets/{dataset_id}",
            put(handlers::dataset::update_dataset),
        )
        .route(
            "/{dataset_id}/metadata",
            get(handlers::metadata::get_metadata),
        )
        .route(
            "/{dataset_id}/{resource_type}",
            get(handlers::search::search_resources),
        )
        .route(
            "/{dataset_id}/{resource_type}",
            post(handlers::crud::create_resource),
        )
        .route(
            "/{dataset_id}/{resource_type}/{resource_id}",
            get(handlers::crud::read_resource),
        )
        .route(
            "/{dataset_id}/{resource_type}/{resource_id}",
            put(handlers::crud::update_resource),
        )
        .route(
            "/{dataset_id}/{resource_type}/{resource_id}",
            delete(handlers::crud::delete_resource),
        )
        .route(
            "/{dataset_id}/{resource_type}/{resource_id}/_history",
            get(handlers::history::resource_history),
        )
        .route(
            "/{dataset_id}/{resource_type}/{resource_id}/_history/{version_id}",
            get(handlers::history::read_resource_version),
        )
        .route("/{dataset_id}", post(handlers::bundle::process_bundle))
        .route(
            "/{dataset_id}/Measure/$evaluate-measure",
            get(handlers::measure::evaluate_measure),
        )
        .route(
            "/{dataset_id}/Measure/$evaluate-measure",
            post(handlers::measure::evaluate_measure),
        )
        .route(
            "/{dataset_id}/Measure/{measure_id}/$evaluate-measure",
            get(handlers::measure::evaluate_measure_instance),
        )
        .route(
            "/{dataset_id}/Measure/{measure_id}/$evaluate-measure",
            post(handlers::measure::evaluate_measure_instance),
        )
        .route("/{dataset_id}/$cql", post(handlers::cql::evaluate_cql))
        .route(
            "/{dataset_id}/$export",
            get(handlers::export::system_export),
        )
        .route(
            "/{dataset_id}/$export/status/{job_id}",
            get(handlers::export::export_status),
        )
        .route(
            "/{dataset_id}/{resource_type}/$export",
            get(handlers::export::type_export),
        )
        .layer(middleware::from_fn(logging_middleware))
        .layer(middleware::from_fn(fhir_content_type_middleware))
        .with_state(state)
}

async fn logging_middleware(request: Request<Body>, next: Next) -> Response<Body> {
    let start = Instant::now();
    let method = request.method().clone();
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let uri = request.uri().to_string();

    let response = next.run(request).await;

    let duration_ms = start.elapsed().as_millis();
    let status = response.status().as_u16();

    eprintln!(
        "{{\"level\":\"info\",\"method\":\"{}\",\"path\":\"{}\",\"uri\":\"{}\",\"status\":{},\"duration_ms\":{}}}",
        method, path, uri, status, duration_ms
    );

    response
}

async fn fhir_content_type_middleware(request: Request<Body>, next: Next) -> Response<Body> {
    let path = request.uri().path().to_string();
    let mut response = next.run(request).await;

    let is_fhir_endpoint = !path.starts_with("/health")
        && !path.starts_with("/metrics")
        && !path.starts_with("/datasets");

    if is_fhir_endpoint {
        if let Some(ct) = response.headers().get(header::CONTENT_TYPE) {
            if ct.to_str().unwrap_or("").contains("application/json") {
                response.headers_mut().insert(
                    header::CONTENT_TYPE,
                    "application/fhir+json; charset=utf-8".parse().unwrap(),
                );
            }
        }
    }

    response
}
