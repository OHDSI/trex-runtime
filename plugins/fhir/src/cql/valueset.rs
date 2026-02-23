use serde_json::Value;

use crate::query_executor::{QueryExecutor, QueryResult};

pub async fn expand_valueset(
    executor: &QueryExecutor,
    schema_name: &str,
    valueset: &Value,
) -> Result<usize, String> {
    let url = valueset
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or("ValueSet missing 'url'")?;

    let version = valueset
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let contains = valueset
        .get("expansion")
        .and_then(|e| e.get("contains"))
        .and_then(|c| c.as_array());

    let entries = match contains {
        Some(arr) => arr,
        None => return Ok(0),
    };

    let delete_sql = format!(
        "DELETE FROM \"{schema}\"._valueset_expansion WHERE valueset_url = '{url}'",
        schema = schema_name,
        url = url.replace('\'', "''")
    );
    let _ = executor.submit(delete_sql).await;

    let mut count = 0;
    for entry in entries {
        count += insert_expansion_entry(executor, schema_name, url, version, entry).await?;
    }

    Ok(count)
}

fn insert_expansion_entry<'a>(
    executor: &'a QueryExecutor,
    schema_name: &'a str,
    valueset_url: &'a str,
    valueset_version: &'a str,
    entry: &'a Value,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<usize, String>> + Send + 'a>> {
    Box::pin(async move {
    let code = entry.get("code").and_then(|v| v.as_str()).unwrap_or("");
    let system = entry.get("system").and_then(|v| v.as_str()).unwrap_or("");
    let display = entry.get("display").and_then(|v| v.as_str()).unwrap_or("");

    if code.is_empty() {
        return Ok(0);
    }

    let sql = format!(
        "INSERT INTO \"{schema}\"._valueset_expansion (valueset_url, valueset_version, code, system, display) \
         VALUES ('{url}', '{ver}', '{code}', '{sys}', '{disp}')",
        schema = schema_name,
        url = valueset_url.replace('\'', "''"),
        ver = valueset_version.replace('\'', "''"),
        code = code.replace('\'', "''"),
        sys = system.replace('\'', "''"),
        disp = display.replace('\'', "''")
    );

    match executor.submit(sql).await {
        QueryResult::Error(e) => Err(format!("Failed to insert expansion entry: {}", e)),
        _ => {
            let mut total = 1;
            if let Some(children) = entry.get("contains").and_then(|c| c.as_array()) {
                for child in children {
                    total += insert_expansion_entry(
                        executor,
                        schema_name,
                        valueset_url,
                        valueset_version,
                        child,
                    )
                    .await?;
                }
            }
            Ok(total)
        }
    }
    })
}

pub async fn code_in_valueset(
    executor: &QueryExecutor,
    schema_name: &str,
    valueset_url: &str,
    system: &str,
    code: &str,
) -> Result<bool, String> {
    let sql = format!(
        "SELECT 1 FROM \"{schema}\"._valueset_expansion \
         WHERE valueset_url = '{url}' AND system = '{sys}' AND code = '{code}' LIMIT 1",
        schema = schema_name,
        url = valueset_url.replace('\'', "''"),
        sys = system.replace('\'', "''"),
        code = code.replace('\'', "''")
    );

    match executor.submit(sql).await {
        QueryResult::Select { rows, .. } => Ok(!rows.is_empty()),
        QueryResult::Error(e) => Err(format!("ValueSet lookup failed: {}", e)),
        _ => Ok(false),
    }
}
