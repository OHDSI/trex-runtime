use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::error::AppError;
use crate::fhir::bundle_processor;
use crate::query_executor::QueryResult;
use crate::sql_safety::validate_dataset_id;
use crate::state::AppState;

const MAX_BUNDLE_ENTRIES: usize = 10_000;

pub async fn process_bundle(
    State(state): State<Arc<AppState>>,
    Path(dataset_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;

    let rt = body
        .get("resourceType")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if rt != "Bundle" {
        return Err(AppError::BadRequest(
            "Expected a FHIR Bundle resource".to_string(),
        ));
    }

    let bundle_type = body
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    match bundle_type {
        "transaction" => process_transaction(state, &dataset_id, &body).await,
        "batch" => process_batch(state, &dataset_id, &body).await,
        _ => Err(AppError::BadRequest(format!(
            "Unsupported Bundle type: '{}'. Must be 'transaction' or 'batch'",
            bundle_type
        ))),
    }
}

async fn process_transaction(
    state: Arc<AppState>,
    dataset_id: &str,
    bundle: &Value,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let entries = bundle_processor::process_bundle_entries(bundle, MAX_BUNDLE_ENTRIES)
        .map_err(|e| AppError::BadRequest(e))?;

    if entries.is_empty() {
        return Ok((
            StatusCode::OK,
            Json(json!({
                "resourceType": "Bundle",
                "type": "transaction-response",
                "entry": []
            })),
        ));
    }

    let schema_name = dataset_id.replace('-', "_");

    if let QueryResult::Error(e) = state.executor.submit("BEGIN TRANSACTION".to_string()).await {
        eprintln!("[fhir] Failed to begin transaction: {}", e);
        return Err(AppError::Internal(
            "Failed to begin transaction".to_string(),
        ));
    }

    let mut response_entries = Vec::new();

    for entry in &entries {
        match process_single_entry(&state, &schema_name, dataset_id, entry).await {
            Ok(resp_entry) => {
                response_entries.push(resp_entry);
            }
            Err(e) => {
                let _ = state.executor.submit("ROLLBACK".to_string()).await;
                return Err(AppError::BadRequest(format!(
                    "Transaction failed on {}/{}: {}",
                    entry.resource_type, entry.server_id, e
                )));
            }
        }
    }

    if let QueryResult::Error(e) = state.executor.submit("COMMIT".to_string()).await {
        eprintln!("[fhir] Failed to commit transaction: {}", e);
        return Err(AppError::Internal(
            "Failed to commit transaction".to_string(),
        ));
    }

    Ok((
        StatusCode::OK,
        Json(json!({
            "resourceType": "Bundle",
            "type": "transaction-response",
            "entry": response_entries
        })),
    ))
}

async fn process_batch(
    state: Arc<AppState>,
    dataset_id: &str,
    bundle: &Value,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let entries = bundle_processor::process_bundle_entries(bundle, MAX_BUNDLE_ENTRIES)
        .map_err(|e| AppError::BadRequest(e))?;

    if entries.is_empty() {
        return Ok((
            StatusCode::OK,
            Json(json!({
                "resourceType": "Bundle",
                "type": "batch-response",
                "entry": []
            })),
        ));
    }

    let schema_name = dataset_id.replace('-', "_");
    let mut response_entries = Vec::new();

    for entry in &entries {
        match process_single_entry(&state, &schema_name, dataset_id, entry).await {
            Ok(resp_entry) => {
                response_entries.push(resp_entry);
            }
            Err(e) => {
                response_entries.push(json!({
                    "response": {
                        "status": "400 Bad Request",
                        "outcome": {
                            "resourceType": "OperationOutcome",
                            "issue": [{
                                "severity": "error",
                                "code": "processing",
                                "diagnostics": e.to_string()
                            }]
                        }
                    }
                }));
            }
        }
    }

    Ok((
        StatusCode::OK,
        Json(json!({
            "resourceType": "Bundle",
            "type": "batch-response",
            "entry": response_entries
        })),
    ))
}

async fn process_single_entry(
    state: &AppState,
    schema_name: &str,
    dataset_id: &str,
    entry: &bundle_processor::ProcessedEntry,
) -> Result<Value, String> {
    let table_name = entry.resource_type.to_lowercase();
    let now = chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();

    let mut resource = entry.resource.clone();
    if let Some(obj) = resource.as_object_mut() {
        obj.insert("id".to_string(), Value::String(entry.server_id.clone()));
        obj.insert(
            "meta".to_string(),
            json!({
                "versionId": "1",
                "lastUpdated": now
            }),
        );
    }

    let raw_json = serde_json::to_string(&resource)
        .map_err(|e| format!("JSON serialize: {}", e))?;

    match entry.method.as_str() {
        "POST" => {
            let insert_sql = format!(
                "INSERT INTO \"{schema}\".\"{table}\" (_id, _version_id, _last_updated, _is_deleted, _raw) \
                 VALUES ($1, 1, CURRENT_TIMESTAMP, false, $2)",
                schema = schema_name,
                table = table_name,
            );

            match state.executor.submit_params(insert_sql, vec![entry.server_id.clone(), raw_json]).await {
                QueryResult::Error(e) => Err(format!("Insert failed: {}", e)),
                _ => Ok(json!({
                    "response": {
                        "status": "201 Created",
                        "location": format!("/{}/{}/{}", dataset_id, entry.resource_type, entry.server_id),
                        "etag": "W/\"1\""
                    }
                })),
            }
        }
        "PUT" => {
            let resource_id = entry
                .resource
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or(&entry.server_id);

            let upsert_sql = format!(
                "INSERT OR REPLACE INTO \"{schema}\".\"{table}\" (_id, _version_id, _last_updated, _is_deleted, _raw) \
                 VALUES ($1, 1, CURRENT_TIMESTAMP, false, $2)",
                schema = schema_name,
                table = table_name,
            );

            match state.executor.submit_params(upsert_sql, vec![resource_id.to_string(), raw_json]).await {
                QueryResult::Error(e) => Err(format!("Upsert failed: {}", e)),
                _ => Ok(json!({
                    "response": {
                        "status": "200 OK",
                        "location": format!("/{}/{}/{}", dataset_id, entry.resource_type, resource_id),
                        "etag": "W/\"1\""
                    }
                })),
            }
        }
        "DELETE" => {
            let resource_id = entry
                .resource
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            if resource_id.is_empty() {
                return Err("DELETE entry missing resource id".to_string());
            }

            let delete_sql = format!(
                "UPDATE \"{schema}\".\"{table}\" SET _is_deleted = true, \
                 _version_id = _version_id + 1, _last_updated = CURRENT_TIMESTAMP \
                 WHERE _id = $1",
                schema = schema_name,
                table = table_name,
            );

            match state.executor.submit_params(delete_sql, vec![resource_id.to_string()]).await {
                QueryResult::Error(e) => Err(format!("Delete failed: {}", e)),
                _ => Ok(json!({
                    "response": {
                        "status": "204 No Content"
                    }
                })),
            }
        }
        _ => Err(format!("Unsupported method: {}", entry.method)),
    }
}
