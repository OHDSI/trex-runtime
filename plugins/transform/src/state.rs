use crate::{escape_sql_ident, escape_sql_str, execute_sql, query_sql};
use std::collections::HashMap;
use std::error::Error;

#[derive(Debug, Clone)]
pub struct ModelState {
    pub model_name: String,
    pub materialized: String,
    pub checksum: String,
    #[allow(dead_code)]
    pub deployed_at: String,
    #[allow(dead_code)]
    pub incremental_strategy: Option<String>,
    pub last_watermark: Option<String>,
}

pub fn ensure_state_table(schema: &str) -> Result<(), Box<dyn Error>> {
    let esc = escape_sql_ident(schema);
    execute_sql(&format!(
        "CREATE TABLE IF NOT EXISTS \"{esc}\".\"_transform_state\" (\
            model_name VARCHAR PRIMARY KEY,\
            materialized VARCHAR NOT NULL,\
            checksum VARCHAR NOT NULL,\
            deployed_at VARCHAR NOT NULL\
        );"
    ))?;
    execute_sql(&format!(
        "ALTER TABLE \"{esc}\".\"_transform_state\" ADD COLUMN IF NOT EXISTS incremental_strategy VARCHAR"
    ))?;
    execute_sql(&format!(
        "ALTER TABLE \"{esc}\".\"_transform_state\" ADD COLUMN IF NOT EXISTS last_watermark VARCHAR"
    ))?;
    Ok(())
}

pub fn query_state(schema: &str) -> Result<HashMap<String, ModelState>, Box<dyn Error>> {
    let rows = query_sql(&format!(
        "SELECT model_name, materialized, checksum, deployed_at, \
         incremental_strategy, last_watermark \
         FROM \"{}\".\"_transform_state\" ORDER BY model_name",
        escape_sql_ident(schema)
    ))
    .unwrap_or_default();

    let mut map = HashMap::new();
    for row in rows {
        if row.columns.len() < 4 {
            continue;
        }
        let incremental_strategy = row
            .columns
            .get(4)
            .filter(|s| !s.is_empty())
            .cloned();
        let last_watermark = row
            .columns
            .get(5)
            .filter(|s| !s.is_empty())
            .cloned();
        let state = ModelState {
            model_name: row.columns[0].clone(),
            materialized: row.columns[1].clone(),
            checksum: row.columns[2].clone(),
            deployed_at: row.columns[3].clone(),
            incremental_strategy,
            last_watermark,
        };
        map.insert(state.model_name.clone(), state);
    }
    Ok(map)
}

pub fn upsert_state(
    schema: &str,
    model_name: &str,
    materialized: &str,
    checksum: &str,
    deployed_at: &str,
    incremental_strategy: Option<&str>,
    last_watermark: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    let strategy_val = match incremental_strategy {
        Some(s) => format!("'{}'", escape_sql_str(s)),
        None => "NULL".to_string(),
    };
    let watermark_val = match last_watermark {
        Some(w) => format!("'{}'", escape_sql_str(w)),
        None => "NULL".to_string(),
    };
    execute_sql(&format!(
        "INSERT OR REPLACE INTO \"{schema}\".\"_transform_state\" \
         (model_name, materialized, checksum, deployed_at, incremental_strategy, last_watermark) \
         VALUES ('{name}', '{mat}', '{cksum}', '{deployed}', {strategy}, {watermark})",
        schema = escape_sql_ident(schema),
        name = escape_sql_str(model_name),
        mat = escape_sql_str(materialized),
        cksum = escape_sql_str(checksum),
        deployed = escape_sql_str(deployed_at),
        strategy = strategy_val,
        watermark = watermark_val,
    ))
}

pub fn delete_state(schema: &str, model_name: &str) -> Result<(), Box<dyn Error>> {
    execute_sql(&format!(
        "DELETE FROM \"{schema}\".\"_transform_state\" WHERE model_name = '{name}'",
        schema = escape_sql_ident(schema),
        name = escape_sql_str(model_name),
    ))
}
