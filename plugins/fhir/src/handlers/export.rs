use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::AppError;
use crate::export::ndjson;
use crate::sql_safety::{validate_dataset_id, validate_resource_type, validate_uuid};
use crate::state::AppState;

pub async fn system_export(
    State(state): State<Arc<AppState>>,
    Path(dataset_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;

    let resource_types: Vec<String> = if let Some(types) = params.get("_type") {
        types.split(',').map(|s| s.trim().to_string()).collect()
    } else {
        state.registry.resource_type_names()
    };

    let job_id = ndjson::create_export_job(
        &state.executor,
        &dataset_id,
        Some(&resource_types),
    )
    .await
    .map_err(|e| {
        eprintln!("[fhir] Failed to create export job: {}", e);
        AppError::Internal("Failed to create export job".to_string())
    })?;

    let executor = state.executor.clone();
    let ds_id = dataset_id.clone();
    let jid = job_id.clone();
    let types = resource_types.clone();
    tokio::spawn(async move {
        if let Err(e) = ndjson::execute_export(executor, &ds_id, &jid, &types).await {
            eprintln!("[fhir] Export job {} failed: {}", jid, e);
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        [(
            "Content-Location",
            format!("/{}/$export/status/{}", dataset_id, job_id),
        )],
        Json(json!({"status": "accepted", "jobId": job_id})),
    ))
}

pub async fn type_export(
    State(state): State<Arc<AppState>>,
    Path((dataset_id, resource_type)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;
    validate_resource_type(&resource_type, &state.registry)?;

    let resource_types = vec![resource_type.clone()];

    let job_id = ndjson::create_export_job(
        &state.executor,
        &dataset_id,
        Some(&resource_types),
    )
    .await
    .map_err(|e| {
        eprintln!("[fhir] Failed to create export job: {}", e);
        AppError::Internal("Failed to create export job".to_string())
    })?;

    let executor = state.executor.clone();
    let ds_id = dataset_id.clone();
    let jid = job_id.clone();
    let types = resource_types.clone();
    tokio::spawn(async move {
        if let Err(e) = ndjson::execute_export(executor, &ds_id, &jid, &types).await {
            eprintln!("[fhir] Export job {} failed: {}", jid, e);
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        [(
            "Content-Location",
            format!("/{}/$export/status/{}", dataset_id, job_id),
        )],
        Json(json!({"status": "accepted", "jobId": job_id})),
    ))
}

pub async fn export_status(
    State(state): State<Arc<AppState>>,
    Path((_dataset_id, job_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    validate_uuid(&job_id)?;

    let job = ndjson::get_export_job(&state.executor, &job_id)
        .await
        .map_err(|e| {
            eprintln!("[fhir] Failed to get export job: {}", e);
            AppError::Internal("Failed to get export job".to_string())
        })?
        .ok_or_else(|| AppError::NotFound(format!("Export job not found: {}", job_id)))?;

    let status = job
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    match status {
        "in-progress" | "accepted" => Ok((
            StatusCode::ACCEPTED,
            Json(json!({
                "status": status,
                "jobId": job_id
            })),
        )),
        "complete" => {
            let output_files: Vec<Value> = job
                .get("output_files")
                .and_then(|v| v.as_str())
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();

            Ok((
                StatusCode::OK,
                Json(json!({
                    "transactionTime": job.get("completed_at").and_then(|v| v.as_str()).unwrap_or(""),
                    "request": format!("/{}/$export", _dataset_id),
                    "requiresAccessToken": false,
                    "output": output_files,
                    "error": []
                })),
            ))
        }
        "error" => {
            let error_msg = job
                .get("error_message")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error");

            eprintln!("[fhir] Export failed: {}", error_msg);
            Err(AppError::Internal("Export failed".to_string()))
        }
        _ => {
            eprintln!("[fhir] Unknown job status: {}", status);
            Err(AppError::Internal("Unknown job status".to_string()))
        }
    }
}
