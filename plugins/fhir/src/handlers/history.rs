use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::Json;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::error::AppError;
use crate::query_executor::QueryResult;
use crate::sql_safety::{validate_dataset_id, validate_fhir_id, validate_resource_type, validate_version_id};
use crate::state::AppState;

pub async fn resource_history(
    State(state): State<Arc<AppState>>,
    Path((dataset_id, resource_type, resource_id)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;
    validate_resource_type(&resource_type, &state.registry)?;
    validate_fhir_id(&resource_id)?;

    let schema_name = dataset_id.replace('-', "_");

    let sql = format!(
        "SELECT _version_id::VARCHAR, _last_updated::VARCHAR, _raw, _is_deleted::VARCHAR FROM \"{schema}\"._history \
         WHERE _id = '{id}' AND _resource_type = '{rtype}' \
         ORDER BY _version_id DESC",
        schema = schema_name,
        id = resource_id.replace('\'', "''"),
        rtype = resource_type.replace('\'', "''")
    );

    let current_sql = format!(
        "SELECT _version_id::VARCHAR, _last_updated::VARCHAR, _raw, _is_deleted::VARCHAR FROM \"{schema}\".\"{table}\" WHERE _id = '{id}'",
        schema = schema_name,
        table = resource_type.to_lowercase(),
        id = resource_id.replace('\'', "''")
    );

    let mut entries = Vec::new();

    if let QueryResult::Select { rows, .. } = state.executor.submit(current_sql).await {
        for row in &rows {
            let raw = row.get(2).and_then(|v| v.as_str()).unwrap_or("{}");
            if let Ok(resource) = serde_json::from_str::<Value>(raw) {
                let version = row.get(0).and_then(|v| v.as_str()).unwrap_or("1");
                let is_deleted = row.get(3).and_then(|v| v.as_str()).map(|s| s == "true").unwrap_or(false);
                let method = if is_deleted { "DELETE" } else { "PUT" };
                entries.push(json!({
                    "fullUrl": format!("/{}/{}/{}", dataset_id, resource_type, resource_id),
                    "resource": resource,
                    "request": {
                        "method": method,
                        "url": format!("{}/{}", resource_type, resource_id)
                    },
                    "response": {
                        "status": "200",
                        "etag": format!("W/\"{}\"", version)
                    }
                }));
            }
        }
    }

    if let QueryResult::Select { rows, .. } = state.executor.submit(sql).await {
        for row in &rows {
            let raw = row.get(2).and_then(|v| v.as_str()).unwrap_or("{}");
            if let Ok(resource) = serde_json::from_str::<Value>(raw) {
                let version = row.get(0).and_then(|v| v.as_str()).unwrap_or("1");
                entries.push(json!({
                    "fullUrl": format!("/{}/{}/{}", dataset_id, resource_type, resource_id),
                    "resource": resource,
                    "request": {
                        "method": "PUT",
                        "url": format!("{}/{}", resource_type, resource_id)
                    },
                    "response": {
                        "status": "200",
                        "etag": format!("W/\"{}\"", version)
                    }
                }));
            }
        }
    }

    Ok(Json(json!({
        "resourceType": "Bundle",
        "type": "history",
        "total": entries.len(),
        "entry": entries
    })))
}

pub async fn read_resource_version(
    State(state): State<Arc<AppState>>,
    Path((dataset_id, resource_type, resource_id, version_id)): Path<(String, String, String, String)>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;
    validate_resource_type(&resource_type, &state.registry)?;
    validate_fhir_id(&resource_id)?;
    validate_version_id(&version_id)?;

    let schema_name = dataset_id.replace('-', "_");

    let sql = format!(
        "SELECT _raw FROM \"{schema}\"._history \
         WHERE _id = '{id}' AND _resource_type = '{rtype}' AND _version_id = {version}",
        schema = schema_name,
        id = resource_id.replace('\'', "''"),
        rtype = resource_type.replace('\'', "''"),
        version = version_id
    );

    match state.executor.submit(sql).await {
        QueryResult::Select { rows, .. } => {
            if rows.is_empty() {
                let current_sql = format!(
                    "SELECT _raw FROM \"{schema}\".\"{table}\" WHERE _id = '{id}' AND _version_id = {version}",
                    schema = schema_name,
                    table = resource_type.to_lowercase(),
                    id = resource_id.replace('\'', "''"),
                    version = version_id
                );
                match state.executor.submit(current_sql).await {
                    QueryResult::Select { rows, .. } if !rows.is_empty() => {
                        let raw = rows[0].first().and_then(|v| v.as_str()).unwrap_or("{}");
                        let resource: Value = serde_json::from_str(raw)
                            .map_err(|e| AppError::Internal(format!("JSON parse: {}", e)))?;
                        Ok(Json(resource))
                    }
                    _ => Err(AppError::NotFound(format!(
                        "Version {} of {}/{} not found",
                        version_id, resource_type, resource_id
                    ))),
                }
            } else {
                let raw = rows[0].first().and_then(|v| v.as_str()).unwrap_or("{}");
                let resource: Value = serde_json::from_str(raw)
                    .map_err(|e| AppError::Internal(format!("JSON parse: {}", e)))?;
                Ok(Json(resource))
            }
        }
        QueryResult::Error(e) => {
            eprintln!("[fhir] Failed to read version: {}", e);
            Err(AppError::Internal("Failed to read version".to_string()))
        }
        _ => Err(AppError::NotFound(format!(
            "Version {} of {}/{} not found",
            version_id, resource_type, resource_id
        ))),
    }
}
