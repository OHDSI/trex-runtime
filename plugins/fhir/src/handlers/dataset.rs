use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::error::AppError;
use crate::fhir::structure_definition::DefinitionRegistry;
use crate::query_executor::QueryResult;
use crate::sql_safety::validate_dataset_id;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct CreateDatasetRequest {
    pub id: String,
    pub name: String,
    pub structure_definitions: Option<Value>,
}

pub async fn create_dataset(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateDatasetRequest>,
) -> Result<impl IntoResponse, AppError> {
    if !body
        .id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-')
        || body.id.is_empty()
    {
        return Err(AppError::BadRequest(
            "Dataset ID must contain only alphanumeric characters and hyphens".to_string(),
        ));
    }

    let schema_name = body.id.replace('-', "_");

    let (resource_type_names, custom_definitions) = if let Some(ref sd_bundle) = body.structure_definitions {
        parse_custom_definitions(sd_bundle)?
    } else {
        let names = state.registry.resource_type_names();
        if names.is_empty() {
            return Err(AppError::Internal(
                "No FHIR definitions loaded on server".to_string(),
            ));
        }
        (names, None)
    };

    // Pin all DDL for this dataset to the same worker to avoid schema visibility races.
    let worker_id = state.executor.next_worker_id();

    let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", schema_name);
    if let QueryResult::Error(e) = state.executor.submit_on(worker_id, create_schema_sql).await {
        eprintln!("[fhir] Failed to create schema: {}", e);
        return Err(AppError::Internal("Failed to create schema".to_string()));
    }

    let history_ddl = format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\"._history (
            _id VARCHAR NOT NULL,
            _resource_type VARCHAR NOT NULL,
            _version_id INTEGER NOT NULL,
            _last_updated TIMESTAMP NOT NULL,
            _raw JSON NOT NULL,
            _is_deleted BOOLEAN NOT NULL DEFAULT false,
            PRIMARY KEY (_id, _version_id)
        )",
        schema = schema_name
    );
    if let QueryResult::Error(e) = state.executor.submit_on(worker_id, history_ddl).await {
        eprintln!("[fhir] Failed to create _history table: {}", e);
        return Err(AppError::Internal(
            "Failed to create _history table".to_string(),
        ));
    }

    let vs_ddl = format!(
        "CREATE TABLE IF NOT EXISTS \"{}\"._valueset_expansion (
            valueset_url VARCHAR NOT NULL,
            valueset_version VARCHAR,
            code VARCHAR NOT NULL,
            system VARCHAR NOT NULL,
            display VARCHAR
        )",
        schema_name
    );
    if let QueryResult::Error(e) = state.executor.submit_on(worker_id, vs_ddl).await {
        eprintln!("[fhir] Failed to create _valueset_expansion table: {}", e);
        return Err(AppError::Internal(
            "Failed to create _valueset_expansion table".to_string(),
        ));
    }

    let mut created_types = Vec::new();
    let mut errors = Vec::new();

    if let Some(ref custom_defs) = custom_definitions {
        for type_name in &resource_type_names {
            match crate::schema::generator::generate_ddl(custom_defs, type_name, &schema_name) {
                Ok(ddl) => match state.executor.submit_on(worker_id, ddl).await {
                    QueryResult::Error(e) => {
                        errors.push(format!("{}: {}", type_name, e));
                    }
                    _ => {
                        created_types.push(type_name.clone());
                    }
                },
                Err(e) => {
                    errors.push(format!("{}: {}", type_name, e));
                }
            }
        }
    } else {
        for type_name in &resource_type_names {
            match state.registry.get_ddl(type_name, &schema_name) {
                Ok(ddl) => match state.executor.submit_on(worker_id, ddl).await {
                    QueryResult::Error(e) => {
                        errors.push(format!("{}: {}", type_name, e));
                    }
                    _ => {
                        created_types.push(type_name.clone());
                    }
                },
                Err(e) => {
                    errors.push(format!("{}: {}", type_name, e));
                }
            }
        }
    }

    if created_types.is_empty() {
        let _ = state
            .executor
            .submit_on(worker_id, format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema_name))
            .await;
        eprintln!("[fhir] Failed to create any resource tables: {}", errors.join("; "));
        return Err(AppError::Internal(
            "Failed to create any resource tables".to_string(),
        ));
    }

    // Force all workers to refresh their catalog after DDL changes.
    state.executor.sync_all().await;

    let resource_types_sql = created_types
        .iter()
        .map(|t| format!("'{}'", t.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");

    let insert_sql = format!(
        "INSERT INTO _fhir_meta._datasets (id, name, status, resource_types) VALUES ($1, $2, 'active', [{}])",
        resource_types_sql
    );

    if let QueryResult::Error(e) = state.executor.submit_params(insert_sql, vec![body.id.clone(), body.name.clone()]).await {
        if e.contains("Duplicate") || e.contains("duplicate") || e.contains("UNIQUE") {
            return Err(AppError::BadRequest(format!(
                "Dataset '{}' already exists",
                body.id
            )));
        }
        eprintln!("[fhir] Failed to register dataset: {}", e);
        return Err(AppError::Internal(
            "Failed to register dataset".to_string(),
        ));
    }

    let mut response = json!({
        "id": body.id,
        "name": body.name,
        "status": "active",
        "resource_types": created_types,
        "resource_count": created_types.len()
    });

    if !errors.is_empty() {
        response["warnings"] = json!(errors);
    }

    Ok((StatusCode::CREATED, Json(response)))
}

fn parse_custom_definitions(
    bundle: &Value,
) -> Result<(Vec<String>, Option<DefinitionRegistry>), AppError> {
    let resource_type = bundle
        .get("resourceType")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if resource_type != "Bundle" {
        return Err(AppError::BadRequest(
            "structure_definitions must be a FHIR Bundle".to_string(),
        ));
    }

    let entries = bundle
        .get("entry")
        .and_then(|v| v.as_array())
        .ok_or_else(|| AppError::BadRequest("Bundle missing 'entry' array".to_string()))?;

    if entries.is_empty() {
        return Err(AppError::BadRequest(
            "structure_definitions Bundle is empty".to_string(),
        ));
    }

    let bundle_str = serde_json::to_string(bundle).map_err(|e| {
        eprintln!("[fhir] Failed to serialize custom definitions: {}", e);
        AppError::Internal("Failed to serialize custom definitions".to_string())
    })?;

    // Default type definitions handle complex types.
    let empty_types = r#"{"resourceType":"Bundle","type":"collection","entry":[]}"#;

    let registry = DefinitionRegistry::load_from_json(&bundle_str, empty_types)
        .map_err(|e| AppError::BadRequest(format!("Invalid StructureDefinitions: {}", e)))?;

    let names = registry.resource_type_names();
    if names.is_empty() {
        return Err(AppError::BadRequest(
            "No valid resource StructureDefinitions found in Bundle".to_string(),
        ));
    }

    Ok((names, Some(registry)))
}

pub async fn list_datasets(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    let result = state
        .executor
        .submit(
            "SELECT id, name, status, created_at, resource_types FROM _fhir_meta._datasets"
                .to_string(),
        )
        .await;

    match result {
        QueryResult::Select { rows, columns } => {
            let datasets: Vec<Value> = rows
                .iter()
                .map(|row| {
                    let mut obj = serde_json::Map::new();
                    for (i, col) in columns.iter().enumerate() {
                        if let Some(val) = row.get(i) {
                            obj.insert(col.clone(), val.clone());
                        }
                    }
                    Value::Object(obj)
                })
                .collect();
            Ok(Json(Value::Array(datasets)))
        }
        QueryResult::Error(e) => {
            eprintln!("[fhir] Failed to list datasets: {}", e);
            Err(AppError::Internal("Failed to list datasets".to_string()))
        }
        _ => Ok(Json(json!([]))),
    }
}

pub async fn get_dataset(
    State(state): State<Arc<AppState>>,
    Path(dataset_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;

    let sql = format!(
        "SELECT id, name, status, created_at, resource_types FROM _fhir_meta._datasets WHERE id = '{}'",
        dataset_id.replace('\'', "''")
    );

    let result = state.executor.submit(sql).await;

    match result {
        QueryResult::Select { rows, columns } => {
            if rows.is_empty() {
                return Err(AppError::NotFound(format!(
                    "Dataset '{}' not found",
                    dataset_id
                )));
            }
            let row = &rows[0];
            let mut obj = serde_json::Map::new();
            for (i, col) in columns.iter().enumerate() {
                if let Some(val) = row.get(i) {
                    obj.insert(col.clone(), val.clone());
                }
            }
            Ok(Json(Value::Object(obj)))
        }
        QueryResult::Error(e) => {
            eprintln!("[fhir] Failed to get dataset: {}", e);
            Err(AppError::Internal("Failed to get dataset".to_string()))
        }
        _ => Err(AppError::NotFound(format!(
            "Dataset '{}' not found",
            dataset_id
        ))),
    }
}

pub async fn delete_dataset(
    State(state): State<Arc<AppState>>,
    Path(dataset_id): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;

    let check_sql = format!(
        "SELECT status FROM _fhir_meta._datasets WHERE id = '{}'",
        dataset_id.replace('\'', "''")
    );

    match state.executor.submit(check_sql).await {
        QueryResult::Select { rows, columns } => {
            if rows.is_empty() {
                return Err(AppError::NotFound(format!(
                    "Dataset '{}' not found",
                    dataset_id
                )));
            }
            if let Some(status_idx) = columns.iter().position(|c| c == "status") {
                if let Some(status) = rows[0].get(status_idx).and_then(|v| v.as_str()) {
                    if status == "deleting" || status == "exporting" {
                        return Err(AppError::Conflict(format!(
                            "Dataset '{}' has active operations (status: {})",
                            dataset_id, status
                        )));
                    }
                }
            }
        }
        QueryResult::Error(e) => {
            eprintln!("[fhir] Failed to check dataset: {}", e);
            return Err(AppError::Internal(
                "Failed to check dataset".to_string(),
            ));
        }
        _ => {
            return Err(AppError::NotFound(format!(
                "Dataset '{}' not found",
                dataset_id
            )));
        }
    }

    let mark_sql = format!(
        "UPDATE _fhir_meta._datasets SET status = 'deleting' WHERE id = '{}'",
        dataset_id.replace('\'', "''")
    );
    let _ = state.executor.submit(mark_sql).await;

    let schema_name = dataset_id.replace('-', "_");
    let drop_sql = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema_name);
    if let QueryResult::Error(e) = state.executor.submit(drop_sql).await {
        eprintln!("[fhir] Failed to drop schema: {}", e);
        return Err(AppError::Internal(
            "Failed to drop schema".to_string(),
        ));
    }

    let delete_sql = format!(
        "DELETE FROM _fhir_meta._datasets WHERE id = '{}'",
        dataset_id.replace('\'', "''")
    );
    if let QueryResult::Error(e) = state.executor.submit(delete_sql).await {
        eprintln!("[fhir] Failed to delete dataset record: {}", e);
        return Err(AppError::Internal(
            "Failed to delete dataset record".to_string(),
        ));
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn update_dataset(
    State(state): State<Arc<AppState>>,
    Path(dataset_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;

    let check_sql = format!(
        "SELECT id FROM _fhir_meta._datasets WHERE id = '{}'",
        dataset_id.replace('\'', "''")
    );
    match state.executor.submit(check_sql).await {
        QueryResult::Select { rows, .. } if !rows.is_empty() => {}
        _ => {
            return Err(AppError::NotFound(format!(
                "Dataset '{}' not found",
                dataset_id
            )));
        }
    }

    let sd_bundle = body
        .get("structure_definitions")
        .ok_or_else(|| AppError::BadRequest("Missing 'structure_definitions' field".to_string()))?;

    let (new_types, custom_defs) = parse_custom_definitions(sd_bundle)?;
    let schema_name = dataset_id.replace('-', "_");

    let mut added = Vec::new();
    let registry = custom_defs.as_ref().ok_or_else(|| {
        AppError::Internal("Expected custom definitions from parsed bundle".to_string())
    })?;

    for type_name in &new_types {
        // generate_ddl uses CREATE TABLE IF NOT EXISTS, so concurrent calls are safe
        match crate::schema::generator::generate_ddl(registry, type_name, &schema_name) {
            Ok(ddl) => {
                match state.executor.submit(ddl).await {
                    QueryResult::Error(e) => {
                        eprintln!("[fhir] Failed to create table for {}: {}", type_name, e);
                        return Err(AppError::Internal(format!(
                            "Failed to create table for {}",
                            type_name
                        )));
                    }
                    _ => added.push(type_name.clone()),
                }
            }
            Err(e) => {
                eprintln!("[fhir] Failed to generate DDL for {}: {}", type_name, e);
                return Err(AppError::Internal(format!(
                    "Failed to generate DDL for {}",
                    type_name
                )));
            }
        }
    }

    if !added.is_empty() {
        let new_types_sql = added
            .iter()
            .map(|t| format!("'{}'", t.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        let update_sql = format!(
            "UPDATE _fhir_meta._datasets SET resource_types = list_concat(resource_types, [{}]) WHERE id = '{}'",
            new_types_sql,
            dataset_id.replace('\'', "''")
        );
        let _ = state.executor.submit(update_sql).await;
    }

    Ok(Json(json!({
        "id": dataset_id,
        "added_types": added,
        "skipped": new_types.len() - added.len()
    })))
}
