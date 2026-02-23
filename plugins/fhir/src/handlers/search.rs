use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::Json;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::AppError;
use crate::fhir::search_parameter;
use crate::query_executor::QueryResult;
use crate::sql_safety::{validate_dataset_id, validate_resource_type};
use crate::state::AppState;

pub async fn search_resources(
    State(state): State<Arc<AppState>>,
    Path((dataset_id, resource_type)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, AppError> {
    validate_dataset_id(&dataset_id)?;
    validate_resource_type(&resource_type, &state.registry)?;

    let schema_name = dataset_id.replace('-', "_");
    let table_name = resource_type.to_lowercase();

    let count: usize = params
        .get("_count")
        .and_then(|v| v.parse().ok())
        .unwrap_or(100)
        .min(1000);
    let offset: usize = params
        .get("_offset")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let search_where = search_parameter::generate_search_sql(
        &state.search_params,
        &state.registry,
        &resource_type,
        &params,
    )
    .map_err(|e| AppError::BadRequest(e))?;

    let where_clause = if search_where.is_empty() {
        "NOT _is_deleted".to_string()
    } else {
        format!("NOT _is_deleted AND ({})", search_where)
    };

    let sql = format!(
        "SELECT _raw FROM \"{schema}\".\"{table}\" WHERE {where_clause} LIMIT {limit} OFFSET {offset}",
        schema = schema_name,
        table = table_name,
        where_clause = where_clause,
        limit = count + 1,
        offset = offset
    );

    let count_sql = format!(
        "SELECT COUNT(*)::VARCHAR as cnt FROM \"{schema}\".\"{table}\" WHERE {where_clause}",
        schema = schema_name,
        table = table_name,
        where_clause = where_clause
    );

    let total = match state.executor.submit(count_sql).await {
        QueryResult::Select { rows, .. } => rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0),
        _ => 0,
    };

    match state.executor.submit(sql).await {
        QueryResult::Select { rows, .. } => {
            let has_more = rows.len() > count;
            let entries: Vec<Value> = rows
                .iter()
                .take(count)
                .filter_map(|row| {
                    row.first()
                        .and_then(|v| v.as_str())
                        .and_then(|s| serde_json::from_str(s).ok())
                })
                .map(|resource: Value| {
                    json!({
                        "fullUrl": format!("{}/{}/{}",
                            dataset_id,
                            resource_type,
                            resource.get("id").and_then(|v| v.as_str()).unwrap_or("")
                        ),
                        "resource": resource,
                        "search": {"mode": "match"}
                    })
                })
                .collect();

            let search_query: String = params
                .iter()
                .filter(|(k, _)| !k.starts_with('_'))
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("&");
            let search_suffix = if search_query.is_empty() {
                String::new()
            } else {
                format!("&{}", search_query)
            };

            let mut link = vec![json!({
                "relation": "self",
                "url": format!("/{}/{}?_count={}&_offset={}{}", dataset_id, resource_type, count, offset, search_suffix)
            })];

            if has_more {
                link.push(json!({
                    "relation": "next",
                    "url": format!("/{}/{}?_count={}&_offset={}{}", dataset_id, resource_type, count, offset + count, search_suffix)
                }));
            }

            if offset > 0 {
                let prev_offset = if offset > count { offset - count } else { 0 };
                link.push(json!({
                    "relation": "previous",
                    "url": format!("/{}/{}?_count={}&_offset={}{}", dataset_id, resource_type, count, prev_offset, search_suffix)
                }));
            }

            let bundle = json!({
                "resourceType": "Bundle",
                "type": "searchset",
                "total": total,
                "link": link,
                "entry": entries
            });

            Ok(Json(bundle))
        }
        QueryResult::Error(e) => {
            if e.contains("does not exist") || e.contains("Table") {
                return Err(AppError::NotFound(format!(
                    "Resource type '{}' not found in dataset '{}'",
                    resource_type, dataset_id
                )));
            }
            eprintln!("[fhir] Search failed: {}", e);
            Err(AppError::Internal("Search failed".to_string()))
        }
        _ => Ok(Json(json!({
            "resourceType": "Bundle",
            "type": "searchset",
            "total": 0,
            "entry": []
        }))),
    }
}
