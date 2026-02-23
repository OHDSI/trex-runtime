use serde_json::{json, Value};
use std::sync::Arc;

use crate::query_executor::{QueryExecutor, QueryResult};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExportStatus {
    Accepted,
    InProgress,
    Complete,
    Error,
}

impl ExportStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExportStatus::Accepted => "accepted",
            ExportStatus::InProgress => "in-progress",
            ExportStatus::Complete => "complete",
            ExportStatus::Error => "error",
        }
    }
}

pub async fn create_export_job(
    executor: &QueryExecutor,
    dataset_id: &str,
    resource_types: Option<&[String]>,
) -> Result<String, String> {
    let job_id = uuid::Uuid::new_v4().to_string();
    let types_str = resource_types
        .map(|ts| ts.join(","))
        .unwrap_or_default();

    let sql = format!(
        "INSERT INTO _fhir_meta._export_jobs (id, dataset_id, status, resource_types, created_at) \
         VALUES ('{id}', '{ds}', 'accepted', '{types}', CURRENT_TIMESTAMP)",
        id = job_id,
        ds = dataset_id.replace('\'', "''"),
        types = types_str.replace('\'', "''")
    );

    match executor.submit(sql).await {
        QueryResult::Error(e) => Err(format!("Failed to create export job: {}", e)),
        _ => Ok(job_id),
    }
}

pub async fn get_export_job(
    executor: &QueryExecutor,
    job_id: &str,
) -> Result<Option<Value>, String> {
    let sql = format!(
        "SELECT id, dataset_id, status, resource_types, created_at, completed_at, output_files, error_message \
         FROM _fhir_meta._export_jobs WHERE id = '{}'",
        job_id.replace('\'', "''")
    );

    match executor.submit(sql).await {
        QueryResult::Select { columns, rows } => {
            if rows.is_empty() {
                return Ok(None);
            }
            let row = &rows[0];
            let mut job = serde_json::Map::new();
            for (i, col) in columns.iter().enumerate() {
                if let Some(val) = row.get(i) {
                    job.insert(col.clone(), val.clone());
                }
            }
            Ok(Some(Value::Object(job)))
        }
        QueryResult::Error(e) => Err(format!("Failed to query export job: {}", e)),
        _ => Ok(None),
    }
}

pub async fn update_export_job_status(
    executor: &QueryExecutor,
    job_id: &str,
    status: ExportStatus,
    output_files: Option<&str>,
    error_message: Option<&str>,
) -> Result<(), String> {
    let mut updates = vec![format!("status = '{}'", status.as_str())];

    if status == ExportStatus::Complete || status == ExportStatus::Error {
        updates.push("completed_at = CURRENT_TIMESTAMP".to_string());
    }

    if let Some(files) = output_files {
        updates.push(format!(
            "output_files = '{}'",
            files.replace('\'', "''")
        ));
    }

    if let Some(err) = error_message {
        updates.push(format!(
            "error_message = '{}'",
            err.replace('\'', "''")
        ));
    }

    let sql = format!(
        "UPDATE _fhir_meta._export_jobs SET {} WHERE id = '{}'",
        updates.join(", "),
        job_id.replace('\'', "''")
    );

    match executor.submit(sql).await {
        QueryResult::Error(e) => Err(format!("Failed to update export job: {}", e)),
        _ => Ok(()),
    }
}

pub async fn execute_export(
    executor: Arc<QueryExecutor>,
    dataset_id: &str,
    job_id: &str,
    resource_types: &[String],
) -> Result<Vec<(String, usize)>, String> {
    let schema_name = dataset_id.replace('-', "_");
    let mut results = Vec::new();

    update_export_job_status(&executor, job_id, ExportStatus::InProgress, None, None).await?;

    for rt in resource_types {
        let table_name = rt.to_lowercase();
        let sql = format!(
            "SELECT _raw FROM \"{}\".\"{}\" WHERE NOT _is_deleted",
            schema_name, table_name
        );

        match executor.submit(sql).await {
            QueryResult::Select { rows, .. } => {
                // TODO: write to NDJSON files
                results.push((rt.clone(), rows.len()));
            }
            QueryResult::Error(e) => {
                if !e.contains("does not exist") {
                    return Err(format!("Export failed for {}: {}", rt, e));
                }
            }
            _ => {}
        }
    }

    let output: Vec<Value> = results
        .iter()
        .filter(|(_, count)| *count > 0)
        .map(|(rt, count)| {
            json!({
                "type": rt,
                "url": format!("/{}/{}/$export/{}/{}.ndjson", dataset_id, rt, job_id, rt.to_lowercase()),
                "count": count
            })
        })
        .collect();

    let output_json = serde_json::to_string(&output).unwrap_or_default();
    update_export_job_status(
        &executor,
        job_id,
        ExportStatus::Complete,
        Some(&output_json),
        None,
    )
    .await?;

    Ok(results)
}
