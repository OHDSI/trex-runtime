use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use serde_json::Value;
use std::sync::Arc;

use crate::error::AppError;
use crate::fhir::validation;
use crate::query_executor::QueryResult;
use crate::sql_safety::{to_schema_name, validate_dataset_id, validate_fhir_id, validate_resource_type};
use crate::state::AppState;

pub async fn create_resource(
    State(state): State<Arc<AppState>>,
    Path((dataset_id, resource_type)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;
    validate_resource_type(&resource_type, &state.registry)?;

    let validation_result = validation::validate_resource(&body, &resource_type, &state.registry);
    if !validation_result.is_valid() {
        return Err(AppError::BadRequest(
            serde_json::to_string(&validation_result.to_operation_outcome())
                .unwrap_or_else(|_| "Validation failed".to_string()),
        ));
    }

    let id = uuid::Uuid::new_v4().to_string();
    let schema_name = to_schema_name(&dataset_id);
    let table_name = resource_type.to_lowercase();
    let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let mut resource = body.clone();
    if let Some(obj) = resource.as_object_mut() {
        obj.insert("id".to_string(), Value::String(id.clone()));
        obj.insert(
            "meta".to_string(),
            serde_json::json!({
                "versionId": "1",
                "lastUpdated": now
            }),
        );
    }

    let raw_json = serde_json::to_string(&resource)
        .map_err(|e| AppError::Internal(format!("JSON serialize: {}", e)))?;

    let insert_sql = format!(
        "INSERT INTO {schema}.\"{table}\" (_id, _version_id, _last_updated, _is_deleted, _raw) \
         VALUES ($1, 1, CURRENT_TIMESTAMP, false, $2)",
        schema = schema_name,
        table = table_name,
    );

    if let QueryResult::Error(e) = state.executor.submit_params(insert_sql, vec![id.clone(), raw_json]).await {
        eprintln!("[fhir] INSERT error for {}.{}: {}", dataset_id, resource_type, e);
        if e.contains("does not exist") || e.contains("Table") {
            return Err(AppError::NotFound(format!(
                "Resource type '{}' not found in dataset '{}'",
                resource_type, dataset_id
            )));
        }
        return Err(AppError::Internal(
            "Failed to create resource".to_string(),
        ));
    }

    let location = format!("/{}/{}/{}", dataset_id, resource_type, id);
    let mut headers = HeaderMap::new();
    if let Ok(v) = location.parse() {
        headers.insert("Location", v);
    }
    if let Ok(v) = "W/\"1\"".parse() {
        headers.insert("ETag", v);
    }
    if let Ok(v) = "application/fhir+json".parse() {
        headers.insert("Content-Type", v);
    }

    Ok((StatusCode::CREATED, headers, Json(resource)))
}

pub async fn read_resource(
    State(state): State<Arc<AppState>>,
    Path((dataset_id, resource_type, resource_id)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;
    validate_resource_type(&resource_type, &state.registry)?;
    validate_fhir_id(&resource_id)?;

    let schema_name = to_schema_name(&dataset_id);
    let table_name = resource_type.to_lowercase();

    let sql = format!(
        "SELECT _raw, _is_deleted::VARCHAR, _version_id::VARCHAR FROM {schema}.\"{table}\" WHERE _id = '{id}'",
        schema = schema_name,
        table = table_name,
        id = resource_id.replace('\'', "''")
    );

    match state.executor.submit(sql).await {
        QueryResult::Select { rows, .. } => {
            if rows.is_empty() {
                return Err(AppError::NotFound(format!(
                    "{}/{} not found",
                    resource_type, resource_id
                )));
            }

            let row = &rows[0];
            let is_deleted = row
                .get(1)
                .and_then(|v| v.as_str())
                .map(|s| s == "true")
                .unwrap_or(false);

            if is_deleted {
                return Err(AppError::Gone(format!(
                    "{}/{} has been deleted",
                    resource_type, resource_id
                )));
            }

            let raw_json = row.get(0).and_then(|v| v.as_str()).unwrap_or("{}");

            let resource: Value = serde_json::from_str(raw_json)
                .map_err(|e| AppError::Internal(format!("JSON parse: {}", e)))?;

            let version_id = row.get(2).and_then(|v| v.as_str()).unwrap_or("1");

            let mut headers = HeaderMap::new();
            if let Ok(v) = format!("W/\"{}\"", version_id).parse() {
                headers.insert("ETag", v);
            }
            if let Ok(v) = "application/fhir+json".parse() {
                headers.insert("Content-Type", v);
            }

            Ok((headers, Json(resource)))
        }
        QueryResult::Error(e) => {
            if e.contains("does not exist") || e.contains("not found") || e.contains("Table") {
                return Err(AppError::NotFound(format!(
                    "Resource type '{}' not found in dataset '{}'",
                    resource_type, dataset_id
                )));
            }
            eprintln!("[fhir] Failed to read resource: {}", e);
            Err(AppError::Internal(
                "Failed to read resource".to_string(),
            ))
        }
        _ => Err(AppError::NotFound(format!(
            "{}/{} not found",
            resource_type, resource_id
        ))),
    }
}

pub async fn update_resource(
    State(state): State<Arc<AppState>>,
    Path((dataset_id, resource_type, resource_id)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;
    validate_resource_type(&resource_type, &state.registry)?;
    validate_fhir_id(&resource_id)?;

    let validation_result =
        validation::validate_resource_update(&body, &resource_type, &resource_id, &state.registry);
    if !validation_result.is_valid() {
        return Err(AppError::BadRequest(
            serde_json::to_string(&validation_result.to_operation_outcome())
                .unwrap_or_else(|_| "Validation failed".to_string()),
        ));
    }

    let schema_name = to_schema_name(&dataset_id);
    let table_name = resource_type.to_lowercase();

    let check_sql = format!(
        "SELECT _version_id::VARCHAR, _raw FROM {schema}.\"{table}\" WHERE _id = '{id}'",
        schema = schema_name,
        table = table_name,
        id = resource_id.replace('\'', "''")
    );

    let (current_version, is_new, current_raw) = match state.executor.submit(check_sql).await {
        QueryResult::Select { rows, .. } => {
            if rows.is_empty() {
                (0i64, true, String::new())
            } else {
                let v = rows[0]
                    .get(0)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(1);
                let raw = rows[0]
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("{}")
                    .to_string();
                (v, false, raw)
            }
        }
        QueryResult::Error(e) => {
            if e.contains("does not exist") || e.contains("Table") {
                return Err(AppError::NotFound(format!(
                    "Resource type '{}' not found in dataset '{}'",
                    resource_type, dataset_id
                )));
            }
            eprintln!("[fhir] Failed to check resource: {}", e);
            return Err(AppError::Internal(
                "Failed to check resource".to_string(),
            ));
        }
        _ => (0, true, String::new()),
    };

    if let Some(if_match) = headers.get("If-Match") {
        if let Ok(etag) = if_match.to_str() {
            let expected_version = etag
                .trim_matches('"')
                .trim_start_matches("W/\"")
                .trim_end_matches('"');
            if let Ok(expected) = expected_version.parse::<i64>() {
                if !is_new && expected != current_version {
                    return Err(AppError::Conflict(format!(
                        "Version conflict: expected {}, current {}",
                        expected, current_version
                    )));
                }
            }
        }
    }

    let new_version = current_version + 1;
    let now = chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();

    let mut resource = body.clone();
    if let Some(obj) = resource.as_object_mut() {
        obj.insert("id".to_string(), Value::String(resource_id.clone()));
        obj.insert(
            "meta".to_string(),
            serde_json::json!({
                "versionId": new_version.to_string(),
                "lastUpdated": now
            }),
        );
    }

    let raw_json = serde_json::to_string(&resource)
        .map_err(|e| AppError::Internal(format!("JSON serialize: {}", e)))?;

    if !is_new {
        let history_sql = format!(
            "INSERT INTO {schema}._history (_id, _resource_type, _version_id, _last_updated, _raw, _is_deleted) \
             VALUES ($1, $2, {version}, CURRENT_TIMESTAMP, $3, false)",
            schema = schema_name,
            version = current_version,
        );
        let _ = state.executor.submit_params(history_sql, vec![
            resource_id.clone(),
            resource_type.clone(),
            current_raw,
        ]).await;
    }

    let sql = if is_new {
        format!(
            "INSERT INTO {schema}.\"{table}\" (_id, _version_id, _last_updated, _is_deleted, _raw) \
             VALUES ($1, {version}, CURRENT_TIMESTAMP, false, $2)",
            schema = schema_name,
            table = table_name,
            version = new_version,
        )
    } else {
        format!(
            "UPDATE {schema}.\"{table}\" SET _version_id = {version}, _last_updated = CURRENT_TIMESTAMP, \
             _is_deleted = false, _raw = $2 WHERE _id = $1",
            schema = schema_name,
            table = table_name,
            version = new_version,
        )
    };

    if let QueryResult::Error(e) = state.executor.submit_params(sql, vec![resource_id.clone(), raw_json]).await {
        eprintln!("[fhir] Failed to update resource: {}", e);
        return Err(AppError::Internal(
            "Failed to update resource".to_string(),
        ));
    }

    let status = if is_new {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };

    let mut resp_headers = HeaderMap::new();
    if let Ok(v) = format!("W/\"{}\"", new_version).parse() {
        resp_headers.insert("ETag", v);
    }
    if let Ok(v) = "application/fhir+json".parse() {
        resp_headers.insert("Content-Type", v);
    }

    Ok((status, resp_headers, Json(resource)))
}

pub async fn delete_resource(
    State(state): State<Arc<AppState>>,
    Path((dataset_id, resource_type, resource_id)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;
    validate_resource_type(&resource_type, &state.registry)?;
    validate_fhir_id(&resource_id)?;

    let schema_name = to_schema_name(&dataset_id);
    let table_name = resource_type.to_lowercase();

    let check_sql = format!(
        "SELECT _version_id::VARCHAR, _raw FROM {schema}.\"{table}\" WHERE _id = '{id}' AND NOT _is_deleted",
        schema = schema_name,
        table = table_name,
        id = resource_id.replace('\'', "''")
    );

    let (current_version, current_raw) = match state.executor.submit(check_sql).await {
        QueryResult::Select { rows, .. } => {
            if rows.is_empty() {
                return Err(AppError::NotFound(format!(
                    "{}/{} not found",
                    resource_type, resource_id
                )));
            }
            let v = rows[0]
                .get(0)
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(1);
            let raw = rows[0]
                .get(1)
                .and_then(|v| v.as_str())
                .unwrap_or("{}")
                .to_string();
            (v, raw)
        }
        QueryResult::Error(e) => {
            if e.contains("does not exist") || e.contains("Table") {
                return Err(AppError::NotFound(format!(
                    "Resource type '{}' not found in dataset '{}'",
                    resource_type, dataset_id
                )));
            }
            eprintln!("[fhir] Failed to check resource: {}", e);
            return Err(AppError::Internal(
                "Failed to check resource".to_string(),
            ));
        }
        _ => {
            return Err(AppError::NotFound(format!(
                "{}/{} not found",
                resource_type, resource_id
            )));
        }
    };

    let new_version = current_version + 1;

    let history_sql = format!(
        "INSERT INTO {schema}._history (_id, _resource_type, _version_id, _last_updated, _raw, _is_deleted) \
         VALUES ($1, $2, {version}, CURRENT_TIMESTAMP, $3, false)",
        schema = schema_name,
        version = current_version,
    );
    let _ = state.executor.submit_params(history_sql, vec![
        resource_id.clone(),
        resource_type.clone(),
        current_raw,
    ]).await;

    let delete_sql = format!(
        "UPDATE {schema}.\"{table}\" SET _is_deleted = true, _version_id = {version}, \
         _last_updated = CURRENT_TIMESTAMP WHERE _id = $1",
        schema = schema_name,
        table = table_name,
        version = new_version
    );

    if let QueryResult::Error(e) = state.executor.submit_params(delete_sql, vec![resource_id.clone()]).await {
        eprintln!("[fhir] Failed to delete resource: {}", e);
        return Err(AppError::Internal(
            "Failed to delete resource".to_string(),
        ));
    }

    Ok(StatusCode::NO_CONTENT)
}
