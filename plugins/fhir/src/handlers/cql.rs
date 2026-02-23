use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::Json;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::cql::compiler;
use crate::cql::elm_types::ElmLibrary;
use crate::error::AppError;
use crate::query_executor::QueryResult;
use crate::sql_safety::validate_dataset_id;
use crate::state::AppState;

pub async fn evaluate_cql(
    State(state): State<Arc<AppState>>,
    Path(dataset_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;

    let schema_name = dataset_id.replace('-', "_");

    let elm = if let Some(library) = body.get("library") {
        library.clone()
    } else if let Some(cql_text) = body.get("cql").and_then(|v| v.as_str()) {
        translate_cql_to_elm(&state, cql_text).await?
    } else if let Some(library_url) = body.get("libraryUrl").and_then(|v| v.as_str()) {
        load_library_elm(&state, &schema_name, library_url).await?
    } else {
        return Err(AppError::BadRequest(
            "Request must include 'library' (ELM JSON), 'cql' (CQL text), or 'libraryUrl'".to_string(),
        ));
    };

    let elm_library: ElmLibrary = serde_json::from_value(elm)
        .map_err(|e| AppError::BadRequest(format!("Invalid ELM JSON: {}", e)))?;

    let sql = compiler::compile_library(&elm_library, &schema_name)
        .map_err(|e| AppError::BadRequest(format!("CQL compilation error: {}", e)))?;

    match state.executor.submit(sql).await {
        QueryResult::Select { columns, rows } => {
            let parameters = build_parameters_response(&elm_library, &columns, &rows);
            Ok(Json(parameters))
        }
        QueryResult::Error(e) => {
            eprintln!("[fhir] CQL execution error: {}", e);
            Err(AppError::Internal("CQL execution failed".to_string()))
        }
        _ => Ok(Json(json!({
            "resourceType": "Parameters",
            "parameter": []
        }))),
    }
}

async fn translate_cql_to_elm(state: &AppState, cql_text: &str) -> Result<Value, AppError> {
    let escaped = cql_text.replace('\'', "''");
    let sql = format!("SELECT cql_to_elm('{}')", escaped);
    match state.executor.submit(sql).await {
        QueryResult::Select { rows, .. } => {
            let elm_str = rows
                .first()
                .and_then(|r| r.first())
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    AppError::Internal("CQL translation returned no result".to_string())
                })?;

            let elm: Value = serde_json::from_str(elm_str).map_err(|e| {
                AppError::Internal(format!("Invalid ELM JSON from translator: {}", e))
            })?;

            if let Some(library) = elm.get("library") {
                Ok(library.clone())
            } else {
                Ok(elm)
            }
        }
        QueryResult::Error(e) if e.contains("does not exist") || e.contains("cql_to_elm") => {
            Err(AppError::BadRequest(
                "CQL text translation requires the cql2elm extension to be loaded. \
                 Provide pre-compiled ELM JSON via the 'library' field instead."
                    .to_string(),
            ))
        }
        QueryResult::Error(e) => {
            eprintln!("[fhir] CQL translation error: {}", e);
            Err(AppError::BadRequest(format!("CQL translation failed: {}", e)))
        }
        _ => Err(AppError::Internal(
            "Unexpected result from CQL translation".to_string(),
        )),
    }
}

async fn load_library_elm(
    state: &AppState,
    schema_name: &str,
    library_url: &str,
) -> Result<Value, AppError> {
    let sql = format!(
        "SELECT _raw FROM \"{}\".\"library\" WHERE json_extract_string(_raw, '$.url') = '{}' AND NOT _is_deleted ORDER BY json_extract_string(_raw, '$.version') DESC LIMIT 1",
        schema_name,
        library_url.replace('\'', "''")
    );

    match state.executor.submit(sql).await {
        QueryResult::Select { rows, .. } => {
            let raw = rows
                .first()
                .and_then(|r| r.first())
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    AppError::NotFound(format!("Library not found: {}", library_url))
                })?;

            let library: Value = serde_json::from_str(raw)
                .map_err(|e| {
                    eprintln!("[fhir] Invalid Library JSON: {}", e);
                    AppError::Internal("Invalid Library JSON".to_string())
                })?;

            let content = library
                .get("content")
                .and_then(|c| c.as_array())
                .ok_or_else(|| {
                    AppError::BadRequest("Library has no content".to_string())
                })?;

            for item in content {
                let content_type = item
                    .get("contentType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if content_type == "application/elm+json" {
                    if let Some(data) = item.get("data").and_then(|v| v.as_str()) {
                        let decoded = base64_decode(data).map_err(|e| {
                            AppError::BadRequest(format!("Invalid base64 in Library content: {}", e))
                        })?;
                        let elm: Value = serde_json::from_str(&decoded).map_err(|e| {
                            AppError::BadRequest(format!("Invalid ELM JSON in Library: {}", e))
                        })?;
                        return Ok(elm);
                    }
                }
            }

            Err(AppError::BadRequest(
                "Library has no application/elm+json content".to_string(),
            ))
        }
        QueryResult::Error(e) => {
            if e.contains("does not exist") || e.contains("Table") {
                Err(AppError::NotFound(
                    "Library resource type not available in this dataset".to_string(),
                ))
            } else {
                eprintln!("[fhir] Failed to query Library: {}", e);
                Err(AppError::Internal(
                    "Failed to query Library".to_string(),
                ))
            }
        }
        _ => Err(AppError::NotFound(format!(
            "Library not found: {}",
            library_url
        ))),
    }
}

fn base64_decode(input: &str) -> Result<String, String> {
    let chars: Vec<u8> = input.bytes().filter(|b| *b != b'\n' && *b != b'\r' && *b != b' ').collect();
    let mut output = Vec::new();
    let table = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    fn decode_char(c: u8, table: &[u8]) -> Result<u8, String> {
        table
            .iter()
            .position(|&t| t == c)
            .map(|p| p as u8)
            .ok_or_else(|| format!("Invalid base64 character: {}", c as char))
    }

    let mut i = 0;
    while i < chars.len() {
        let a = if chars[i] == b'=' { 0 } else { decode_char(chars[i], table)? };
        let b = if i + 1 < chars.len() && chars[i + 1] != b'=' { decode_char(chars[i + 1], table)? } else { 0 };
        let c = if i + 2 < chars.len() && chars[i + 2] != b'=' { decode_char(chars[i + 2], table)? } else { 0 };
        let d = if i + 3 < chars.len() && chars[i + 3] != b'=' { decode_char(chars[i + 3], table)? } else { 0 };

        output.push((a << 2) | (b >> 4));
        if i + 2 < chars.len() && chars[i + 2] != b'=' {
            output.push(((b & 0x0f) << 4) | (c >> 2));
        }
        if i + 3 < chars.len() && chars[i + 3] != b'=' {
            output.push(((c & 0x03) << 6) | d);
        }

        i += 4;
    }

    String::from_utf8(output).map_err(|e| format!("Invalid UTF-8 in decoded base64: {}", e))
}

fn build_parameters_response(
    library: &ElmLibrary,
    columns: &[String],
    rows: &[Vec<Value>],
) -> Value {
    // Result name comes from the last non-Patient expression.
    let result_name = library
        .statements
        .as_ref()
        .and_then(|s| {
            s.defs
                .iter()
                .rev()
                .find(|d| d.name != "Patient")
                .map(|d| d.name.as_str())
        })
        .unwrap_or("result");

    let mut parameters = Vec::new();

    if !rows.is_empty() && !columns.is_empty() {
        // Prefer _raw column for resource results; fall back to column 0
        let value_col_idx = columns
            .iter()
            .position(|c| c == "_raw")
            .unwrap_or(0);

        let values: Vec<&Value> = rows
            .iter()
            .filter_map(|row| row.get(value_col_idx))
            .collect();

        if values.len() == 1 {
            parameters.push(json!({
                "name": result_name,
                "valueString": values[0].to_string()
            }));
        } else if !values.is_empty() {
            let parts: Vec<Value> = values
                .iter()
                .map(|v| {
                    json!({
                        "name": "result",
                        "valueString": v.to_string()
                    })
                })
                .collect();
            parameters.push(json!({
                "name": result_name,
                "part": parts
            }));
        }
    }

    json!({
        "resourceType": "Parameters",
        "parameter": parameters
    })
}
