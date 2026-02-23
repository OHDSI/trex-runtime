use crate::compile::compile_project;
use crate::dag::transitive_dependents;
use crate::parser::{extract_dependencies, rewrite_table_references, rewrite_table_references_dual};
use crate::project::{load_project, BatchSize, IncrementalStrategy, Materialization, SnapshotStrategy};
use crate::state::{delete_state, ensure_state_table, query_state, upsert_state};
use crate::{escape_sql_ident, escape_sql_str, execute_sql, query_sql};
use chrono::Utc;
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use siphasher::sip::SipHasher13;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

struct RunResult {
    name: String,
    action: String,
    materialized: String,
    duration_ms: i64,
    message: String,
}

fn compute_model_checksum(name: &str, sql: &str, yaml: Option<&str>) -> String {
    let mut hasher = SipHasher13::new();
    name.hash(&mut hasher);
    sql.hash(&mut hasher);
    if let Some(y) = yaml {
        y.hash(&mut hasher);
    }
    hasher.finish().to_string()
}

pub struct IncrementalConfig {
    pub strategy: IncrementalStrategy,
    pub unique_key: Option<Vec<String>>,
    pub updated_at: Option<String>,
    pub batch_size: Option<BatchSize>,
    pub lookback: Option<u32>,
    pub merge_update_columns: Option<Vec<String>>,
    pub merge_exclude_columns: Option<Vec<String>>,
    pub last_watermark: Option<String>,
}

fn process_incremental_markers(sql: &str, schema: &str, model_name: &str, is_new: bool) -> String {
    let start_marker = "-- __is_incremental__";
    let end_marker = "-- __end_incremental__";

    let this_ref = format!(
        "\"{}\".\"{}\"",
        escape_sql_ident(schema),
        escape_sql_ident(model_name)
    );

    let mut result = String::new();
    let mut remaining = sql;

    loop {
        let start_pos = match remaining.find(start_marker) {
            Some(pos) => pos,
            None => {
                result.push_str(remaining);
                break;
            }
        };

        let after_start = &remaining[start_pos + start_marker.len()..];
        let end_pos = match after_start.find(end_marker) {
            Some(pos) => pos,
            None => {
                    result.push_str(remaining);
                break;
            }
        };

        let before = &remaining[..start_pos];
        let block = &after_start[..end_pos];
        remaining = &after_start[end_pos + end_marker.len()..];

        if is_new {
            result.push_str(before.trim_end());
        } else {
            result.push_str(before.trim_end());
            result.push_str(&block.replace("__this__", &this_ref));
        }
    }

    result
}

fn materialize_append(
    schema: &str,
    model_name: &str,
    rewritten: &str,
) -> Result<(), Box<dyn Error>> {
    let esc_schema = escape_sql_ident(schema);
    let esc_name = escape_sql_ident(model_name);
    execute_sql(&format!(
        "INSERT INTO \"{esc_schema}\".\"{esc_name}\" {rewritten}"
    ))
}

fn materialize_delete_insert(
    schema: &str,
    model_name: &str,
    rewritten: &str,
    unique_key: &[String],
) -> Result<(), Box<dyn Error>> {
    let esc_schema = escape_sql_ident(schema);
    let esc_name = escape_sql_ident(model_name);

    if unique_key.len() == 1 {
        let esc_key = escape_sql_ident(&unique_key[0]);
        execute_sql(&format!(
            "DELETE FROM \"{esc_schema}\".\"{esc_name}\" \
             WHERE \"{esc_key}\" IN (SELECT \"{esc_key}\" FROM ({rewritten}))"
        ))?;
    } else {
        let where_clause: Vec<String> = unique_key
            .iter()
            .map(|k| {
                let ek = escape_sql_ident(k);
                format!("\"{esc_schema}\".\"{esc_name}\".\"{ek}\" = __src__.\"{ek}\"")
            })
            .collect();
        execute_sql(&format!(
            "DELETE FROM \"{esc_schema}\".\"{esc_name}\" WHERE EXISTS (\
             SELECT 1 FROM ({rewritten}) AS __src__ WHERE {})",
            where_clause.join(" AND ")
        ))?;
    }

    execute_sql(&format!(
        "INSERT INTO \"{esc_schema}\".\"{esc_name}\" {rewritten}"
    ))
}

fn materialize_merge(
    schema: &str,
    model_name: &str,
    rewritten: &str,
    unique_key: &[String],
    merge_update_columns: Option<&Vec<String>>,
    merge_exclude_columns: Option<&Vec<String>>,
) -> Result<(), Box<dyn Error>> {
    let staging = format!("__staging_{model_name}__");
    let esc_staging = escape_sql_ident(&staging);

    execute_sql(&format!(
        "CREATE TEMPORARY TABLE \"{esc_staging}\" AS {rewritten}"
    ))?;

    let result = materialize_merge_inner(
        schema,
        model_name,
        &staging,
        &esc_staging,
        unique_key,
        merge_update_columns,
        merge_exclude_columns,
    );

    let _ = execute_sql(&format!("DROP TABLE IF EXISTS \"{esc_staging}\""));

    result
}

fn materialize_merge_inner(
    schema: &str,
    model_name: &str,
    staging: &str,
    esc_staging: &str,
    unique_key: &[String],
    merge_update_columns: Option<&Vec<String>>,
    merge_exclude_columns: Option<&Vec<String>>,
) -> Result<(), Box<dyn Error>> {
    let esc_schema = escape_sql_ident(schema);
    let esc_name = escape_sql_ident(model_name);

    // Temporary tables live in the 'temp' schema
    let col_rows = query_sql(&format!(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = 'temp' AND table_name = '{}'",
        escape_sql_str(staging)
    ))?;

    let all_columns: Vec<String> = col_rows.iter().map(|r| r.columns[0].clone()).collect();
    let key_set: HashSet<&str> = unique_key.iter().map(|s| s.as_str()).collect();

    let update_cols: Vec<&String> = if let Some(whitelist) = merge_update_columns {
        whitelist.iter().filter(|c| !key_set.contains(c.as_str())).collect()
    } else {
        let exclude_set: HashSet<&str> = merge_exclude_columns
            .map(|v| v.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default();
        all_columns
            .iter()
            .filter(|c| !key_set.contains(c.as_str()) && !exclude_set.contains(c.as_str()))
            .collect()
    };

    if !update_cols.is_empty() {
        let set_clause: Vec<String> = update_cols
            .iter()
            .map(|c| {
                let ec = escape_sql_ident(c);
                format!("\"{ec}\" = __stg__.\"{ec}\"")
            })
            .collect();

        let join_clause: Vec<String> = unique_key
            .iter()
            .map(|k| {
                let ek = escape_sql_ident(k);
                format!(
                    "\"{esc_schema}\".\"{esc_name}\".\"{ek}\" = __stg__.\"{ek}\""
                )
            })
            .collect();

        execute_sql(&format!(
            "UPDATE \"{esc_schema}\".\"{esc_name}\" SET {} \
             FROM \"{esc_staging}\" AS __stg__ WHERE {}",
            set_clause.join(", "),
            join_clause.join(" AND ")
        ))?;
    }

    let insert_join: Vec<String> = unique_key
        .iter()
        .map(|k| {
            let ek = escape_sql_ident(k);
            format!(
                "\"{esc_schema}\".\"{esc_name}\".\"{ek}\" = \"{esc_staging}\".\"{ek}\""
            )
        })
        .collect();

    let col_list: Vec<String> = all_columns
        .iter()
        .map(|c| format!("\"{}\"", escape_sql_ident(c)))
        .collect();

    execute_sql(&format!(
        "INSERT INTO \"{esc_schema}\".\"{esc_name}\" ({cols}) \
         SELECT {cols} FROM \"{esc_staging}\" \
         WHERE NOT EXISTS (\
             SELECT 1 FROM \"{esc_schema}\".\"{esc_name}\" WHERE {join}\
         )",
        cols = col_list.join(", "),
        join = insert_join.join(" AND ")
    ))?;

    Ok(())
}

fn materialize_microbatch(
    schema: &str,
    model_name: &str,
    rewritten: &str,
    updated_at: &str,
    batch_size: BatchSize,
    lookback: u32,
    last_watermark: Option<&str>,
) -> Result<Option<String>, Box<dyn Error>> {
    let esc_schema = escape_sql_ident(schema);
    let esc_name = escape_sql_ident(model_name);
    let esc_updated_at = escape_sql_ident(updated_at);
    let trunc = batch_size.as_trunc();
    let interval = batch_size.as_interval();

    let batch_end_rows = query_sql(&format!(
        "SELECT date_trunc('{trunc}', CURRENT_TIMESTAMP)::VARCHAR"
    ))?;
    let batch_end = batch_end_rows
        .first()
        .map(|r| r.columns[0].clone())
        .ok_or("Failed to compute batch_end")?;

    let batch_start = if let Some(watermark) = last_watermark {
        let start_rows = query_sql(&format!(
            "SELECT ('{watermark}'::TIMESTAMP - {lookback} * INTERVAL '{interval}')::VARCHAR",
            watermark = escape_sql_str(watermark),
            lookback = lookback,
            interval = interval,
        ))?;
        start_rows
            .first()
            .map(|r| r.columns[0].clone())
            .ok_or_else(|| "Failed to compute batch_start".to_string())?
    } else {
        let min_rows = query_sql(&format!(
            "SELECT MIN(\"{esc_updated_at}\")::VARCHAR FROM \"{esc_schema}\".\"{esc_name}\""
        ))?;
        let min_val = min_rows
            .first()
            .and_then(|r| {
                let v = &r.columns[0];
                if v.is_empty() { None } else { Some(v.clone()) }
            });
        match min_val {
            Some(v) => v,
            None => {
                execute_sql(&format!(
                    "INSERT INTO \"{esc_schema}\".\"{esc_name}\" {rewritten}"
                ))?;
                return Ok(Some(batch_end));
            }
        }
    };

    execute_sql(&format!(
        "DELETE FROM \"{esc_schema}\".\"{esc_name}\" \
         WHERE \"{esc_updated_at}\" >= '{batch_start}'::TIMESTAMP \
         AND \"{esc_updated_at}\" < '{batch_end}'::TIMESTAMP",
        batch_start = escape_sql_str(&batch_start),
        batch_end = escape_sql_str(&batch_end),
    ))?;

    execute_sql(&format!(
        "INSERT INTO \"{esc_schema}\".\"{esc_name}\" \
         SELECT * FROM ({rewritten}) AS __batch__ \
         WHERE \"{esc_updated_at}\" >= '{batch_start}'::TIMESTAMP \
         AND \"{esc_updated_at}\" < '{batch_end}'::TIMESTAMP",
        batch_start = escape_sql_str(&batch_start),
        batch_end = escape_sql_str(&batch_end),
    ))?;

    Ok(Some(batch_end))
}

fn execute_hooks(
    hooks: &[String],
    schema: &str,
    model_name: &str,
) -> Result<(), Box<dyn Error>> {
    let this_ref = format!(
        "\"{}\".\"{}\"",
        escape_sql_ident(schema),
        escape_sql_ident(model_name)
    );
    for hook in hooks {
        let resolved = hook.replace("{{this}}", &this_ref);
        execute_sql(&resolved)?;
    }
    Ok(())
}

fn inline_ephemeral_models(
    sql: &str,
    ephemeral_models: &HashMap<String, String>,
    schema: &str,
    known_names: &HashSet<String>,
    source_names: Option<&HashSet<String>>,
    source_schema: Option<&str>,
) -> String {
    if ephemeral_models.is_empty() {
        return sql.to_string();
    }

    let referenced: Vec<String> = match extract_dependencies(sql) {
        Ok(deps) => deps
            .into_iter()
            .filter(|d| ephemeral_models.contains_key(d))
            .collect(),
        Err(_) => return sql.to_string(),
    };

    if referenced.is_empty() {
        return sql.to_string();
    }

    let mut all_needed: HashSet<String> = HashSet::new();
    let mut queue: std::collections::VecDeque<String> = referenced.into_iter().collect();
    while let Some(name) = queue.pop_front() {
        if !all_needed.insert(name.clone()) {
            continue;
        }
        if let Some(eph_sql) = ephemeral_models.get(&name) {
            if let Ok(deps) = extract_dependencies(eph_sql) {
                for dep in deps {
                    if ephemeral_models.contains_key(&dep) && !all_needed.contains(&dep) {
                        queue.push_back(dep);
                    }
                }
            }
        }
    }

    let mut edges: HashMap<String, HashSet<String>> = HashMap::new();
    for name in &all_needed {
        let mut deps = HashSet::new();
        if let Some(eph_sql) = ephemeral_models.get(name) {
            if let Ok(all_deps) = extract_dependencies(eph_sql) {
                for d in all_deps {
                    if all_needed.contains(&d) {
                        deps.insert(d);
                    }
                }
            }
        }
        edges.insert(name.clone(), deps);
    }

    let nodes: Vec<String> = all_needed.iter().cloned().collect();
    let sorted = match crate::dag::topological_sort(&nodes, &edges) {
        Ok(s) => s,
        Err(_) => return sql.to_string(),
    };

    let mut cte_parts: Vec<String> = Vec::new();
    for name in &sorted {
        if let Some(eph_sql) = ephemeral_models.get(name) {
            let non_ephemeral_names: HashSet<String> = known_names
                .iter()
                .filter(|n| !ephemeral_models.contains_key(*n))
                .cloned()
                .collect();
            let rewritten = if let (Some(src_names), Some(src_schema)) = (source_names, source_schema) {
                match rewrite_table_references_dual(eph_sql, &non_ephemeral_names, src_names, schema, src_schema) {
                    Ok(r) => r,
                    Err(_) => eph_sql.clone(),
                }
            } else {
                match rewrite_table_references(eph_sql, &non_ephemeral_names, schema) {
                    Ok(r) => r,
                    Err(_) => eph_sql.clone(),
                }
            };
            cte_parts.push(format!("{} AS ({})", name, rewritten.trim()));
        }
    }

    if cte_parts.is_empty() {
        return sql.to_string();
    }

    let trimmed = sql.trim_start();
    let cte_prefix = cte_parts.join(",\n     ");

    if let Some(rest) = trimmed.strip_prefix("WITH") {
        format!("WITH {},\n     {}", cte_prefix, rest.trim_start())
    } else {
        format!("WITH {}\n{}", cte_prefix, sql)
    }
}

fn materialize_snapshot(
    schema: &str,
    model_name: &str,
    rewritten: &str,
    is_new: bool,
    unique_key: &[String],
    strategy: SnapshotStrategy,
    updated_at: Option<&str>,
    check_cols: Option<&Vec<String>>,
) -> Result<(), Box<dyn Error>> {
    let esc_schema = escape_sql_ident(schema);
    let esc_name = escape_sql_ident(model_name);
    let staging = format!("__snap_staging_{model_name}__");
    let esc_staging = escape_sql_ident(&staging);

    let hash_expr = match strategy {
        SnapshotStrategy::Timestamp => {
            let col = escape_sql_ident(updated_at.unwrap());
            format!("hash(\"{col}\"::VARCHAR)")
        }
        SnapshotStrategy::Check => {
            let cols = check_cols.unwrap();
            let parts: Vec<String> = cols
                .iter()
                .map(|c| format!("\"{}\"::VARCHAR", escape_sql_ident(c)))
                .collect();
            format!("hash({})", parts.join(" || '|' || "))
        }
    };

    execute_sql(&format!(
        "CREATE TEMPORARY TABLE \"{esc_staging}\" AS \
         SELECT *, {hash_expr} AS _stg_hash FROM ({rewritten})"
    ))?;

    let result = if is_new {
        execute_sql(&format!(
            "CREATE TABLE \"{esc_schema}\".\"{esc_name}\" AS \
             SELECT *, CURRENT_TIMESTAMP AS _snapshot_valid_from, \
             NULL::TIMESTAMP AS _snapshot_valid_to, \
             _stg_hash AS _snapshot_hash \
             FROM \"{esc_staging}\""
        ))
    } else {
        let key_match_target_staging: Vec<String> = unique_key
            .iter()
            .map(|k| {
                let ek = escape_sql_ident(k);
                format!(
                    "\"{esc_schema}\".\"{esc_name}\".\"{ek}\" = \"{esc_staging}\".\"{ek}\""
                )
            })
            .collect();
        let key_match_str = key_match_target_staging.join(" AND ");

        let key_match_staging_tgt: Vec<String> = unique_key
            .iter()
            .map(|k| {
                let ek = escape_sql_ident(k);
                format!("\"{esc_staging}\".\"{ek}\" = __tgt__.\"{ek}\"")
            })
            .collect();
        let key_match_staging_tgt_str = key_match_staging_tgt.join(" AND ");

        execute_sql(&format!(
            "UPDATE \"{esc_schema}\".\"{esc_name}\" SET _snapshot_valid_to = CURRENT_TIMESTAMP \
             WHERE _snapshot_valid_to IS NULL \
             AND (NOT EXISTS (SELECT 1 FROM \"{esc_staging}\" WHERE {key_match_str}) \
                  OR _snapshot_hash != (SELECT _stg_hash FROM \"{esc_staging}\" WHERE {key_match_str}))"
        ))?;

        execute_sql(&format!(
            "INSERT INTO \"{esc_schema}\".\"{esc_name}\" \
             SELECT \"{esc_staging}\".*, CURRENT_TIMESTAMP, NULL, \"{esc_staging}\"._stg_hash \
             FROM \"{esc_staging}\" \
             WHERE NOT EXISTS (\
                 SELECT 1 FROM \"{esc_schema}\".\"{esc_name}\" AS __tgt__ \
                 WHERE {key_match_staging_tgt_str} \
                 AND __tgt__._snapshot_valid_to IS NULL \
                 AND __tgt__._snapshot_hash = \"{esc_staging}\"._stg_hash\
             )"
        ))
    };

    let _ = execute_sql(&format!("DROP TABLE IF EXISTS \"{esc_staging}\""));
    result
}

fn materialize_model(
    model_name: &str,
    sql: &str,
    materialization: Materialization,
    schema: &str,
    known_names: &HashSet<String>,
    is_new: bool,
    incremental_config: Option<&IncrementalConfig>,
    ephemeral_models: &HashMap<String, String>,
    snapshot_strategy: Option<SnapshotStrategy>,
    snapshot_updated_at: Option<&str>,
    snapshot_check_cols: Option<&Vec<String>>,
    unique_key: Option<&Vec<String>>,
    source_names: Option<&HashSet<String>>,
    source_schema: Option<&str>,
) -> Result<Option<String>, Box<dyn Error>> {
    let with_ephemerals = inline_ephemeral_models(sql, ephemeral_models, schema, known_names, source_names, source_schema);
    // Must process incremental markers before SQL rewriting because sqlparser strips comments
    let processed = process_incremental_markers(&with_ephemerals, schema, model_name, is_new);
    let rewritten = if let (Some(src_names), Some(src_schema)) = (source_names, source_schema) {
        rewrite_table_references_dual(&processed, known_names, src_names, schema, src_schema)?
    } else {
        rewrite_table_references(&processed, known_names, schema)?
    };
    let esc_schema = escape_sql_ident(schema);
    let esc_name = escape_sql_ident(model_name);

    match materialization {
        Materialization::Ephemeral => {
            Ok(None)
        }
        Materialization::View => {
            execute_sql(&format!(
                "CREATE OR REPLACE VIEW \"{esc_schema}\".\"{esc_name}\" AS {rewritten}"
            ))?;
            Ok(None)
        }
        Materialization::Table => {
            execute_sql(&format!(
                "DROP TABLE IF EXISTS \"{esc_schema}\".\"{esc_name}\""
            ))?;
            execute_sql(&format!(
                "CREATE TABLE \"{esc_schema}\".\"{esc_name}\" AS {rewritten}"
            ))?;
            Ok(None)
        }
        Materialization::Snapshot => {
            let strategy = snapshot_strategy.unwrap_or(SnapshotStrategy::Timestamp);
            let uk = unique_key.ok_or_else(|| {
                format!("Model '{}': snapshot requires unique_key", model_name)
            })?;
            materialize_snapshot(
                schema,
                model_name,
                &rewritten,
                is_new,
                uk,
                strategy,
                snapshot_updated_at,
                snapshot_check_cols,
            )?;
            Ok(None)
        }
        Materialization::Incremental => {
            if is_new {
                execute_sql(&format!(
                    "CREATE TABLE \"{esc_schema}\".\"{esc_name}\" AS {rewritten}"
                ))?;
                if let Some(config) = incremental_config {
                    if config.strategy == IncrementalStrategy::Microbatch {
                        if let Some(batch_size) = config.batch_size {
                            let trunc = batch_size.as_trunc();
                            let rows = query_sql(&format!(
                                "SELECT date_trunc('{trunc}', CURRENT_TIMESTAMP)::VARCHAR"
                            ))?;
                            return Ok(rows.first().map(|r| r.columns[0].clone()));
                        }
                    }
                }
                return Ok(None);
            }

            let config = incremental_config;
            let strategy = config
                .map(|c| c.strategy)
                .unwrap_or(IncrementalStrategy::DeleteInsert);

            match strategy {
                IncrementalStrategy::Append => {
                    materialize_append(schema, model_name, &rewritten)?;
                    Ok(None)
                }
                IncrementalStrategy::DeleteInsert => {
                    if let Some(keys) = config.and_then(|c| c.unique_key.as_ref()) {
                        materialize_delete_insert(schema, model_name, &rewritten, keys)?;
                    } else {
                        execute_sql(&format!(
                            "DROP TABLE IF EXISTS \"{esc_schema}\".\"{esc_name}\""
                        ))?;
                        execute_sql(&format!(
                            "CREATE TABLE \"{esc_schema}\".\"{esc_name}\" AS {rewritten}"
                        ))?;
                    }
                    Ok(None)
                }
                IncrementalStrategy::Merge => {
                    let keys = config
                        .and_then(|c| c.unique_key.as_ref())
                        .ok_or_else(|| {
                            format!("Model '{}': merge strategy requires unique_key", model_name)
                        })?;
                    materialize_merge(
                        schema,
                        model_name,
                        &rewritten,
                        keys,
                        config.and_then(|c| c.merge_update_columns.as_ref()),
                        config.and_then(|c| c.merge_exclude_columns.as_ref()),
                    )?;
                    Ok(None)
                }
                IncrementalStrategy::Microbatch => {
                    let cfg = config.ok_or_else(|| {
                        format!("Model '{}': microbatch requires config", model_name)
                    })?;
                    let updated_at = cfg.updated_at.as_ref().ok_or_else(|| {
                        format!("Model '{}': microbatch requires updated_at", model_name)
                    })?;
                    let batch_size = cfg.batch_size.ok_or_else(|| {
                        format!("Model '{}': microbatch requires batch_size", model_name)
                    })?;
                    let lookback = cfg.lookback.unwrap_or(0);
                    materialize_microbatch(
                        schema,
                        model_name,
                        &rewritten,
                        updated_at,
                        batch_size,
                        lookback,
                        cfg.last_watermark.as_deref(),
                    )
                }
            }
        }
    }
}

fn run_project(path: &str, schema: &str, source_schema: Option<&str>) -> Result<Vec<RunResult>, Box<dyn Error>> {
    let project = load_project(path)?;
    let compiled = compile_project(&project)?;

    execute_sql(&format!(
        "CREATE SCHEMA IF NOT EXISTS \"{}\"",
        escape_sql_ident(schema)
    ))?;
    ensure_state_table(schema)?;

    let existing_state = query_state(schema)?;

    let known_names: HashSet<String> = project
        .models
        .iter()
        .map(|m| m.name.clone())
        .chain(project.seeds.iter().map(|s| s.name.clone()))
        .collect();

    let src_names: Option<HashSet<String>> = source_schema.map(|_| {
        project.source_tables.iter().cloned().collect()
    });

    let mut edges: HashMap<String, HashSet<String>> = HashMap::new();
    for model in &project.models {
        if let Ok(all_refs) = extract_dependencies(&model.sql) {
            let deps: HashSet<String> = all_refs
                .into_iter()
                .filter(|r| known_names.contains(r) && *r != model.name)
                .collect();
            edges.insert(model.name.clone(), deps);
        }
    }

    let mut directly_changed: HashSet<String> = HashSet::new();
    let mut checksums: HashMap<String, String> = HashMap::new();
    for model in &project.models {
        let checksum =
            compute_model_checksum(&model.name, &model.sql, model.yaml_content.as_deref());
        checksums.insert(model.name.clone(), checksum.clone());
        if model.materialization == Materialization::Ephemeral {
            continue;
        }
        match existing_state.get(&model.name) {
            Some(state) => {
                if state.checksum != checksum {
                    directly_changed.insert(model.name.clone());
                }
            }
            None => {
                directly_changed.insert(model.name.clone());
            }
        }
    }

    let all_model_names: Vec<String> = project.models.iter().map(|m| m.name.clone()).collect();
    let affected = transitive_dependents(&directly_changed, &all_model_names, &edges);

    let project_names: HashSet<String> = known_names.clone();
    let mut results = Vec::new();

    for (name, state) in &existing_state {
        if !project_names.contains(name) && state.materialized != "seed" {
            let start = Instant::now();
            let drop_sql = if state.materialized == "view" {
                format!(
                    "DROP VIEW IF EXISTS \"{}\".\"{}\"",
                    escape_sql_ident(schema),
                    escape_sql_ident(name)
                )
            } else {
                format!(
                    "DROP TABLE IF EXISTS \"{}\".\"{}\"",
                    escape_sql_ident(schema),
                    escape_sql_ident(name)
                )
            };

            match execute_sql(&drop_sql) {
                Ok(_) => {
                    delete_state(schema, name)?;
                    results.push(RunResult {
                        name: name.clone(),
                        action: "drop".to_string(),
                        materialized: state.materialized.clone(),
                        duration_ms: start.elapsed().as_millis() as i64,
                        message: String::new(),
                    });
                }
                Err(e) => {
                    return Err(format!("Failed to drop {}: {}", name, e).into());
                }
            }
        }
    }

    let ephemeral_models: HashMap<String, String> = project
        .models
        .iter()
        .filter(|m| m.materialization == Materialization::Ephemeral)
        .map(|m| (m.name.clone(), m.sql.clone()))
        .collect();

    for cr in &compiled {
        if cr.materialized == "seed" || cr.materialized == "ephemeral" {
            continue;
        }

        let model = project
            .models
            .iter()
            .find(|m| m.name == cr.name)
            .unwrap();

        let is_new = !existing_state.contains_key(&cr.name);
        let needs_rebuild = affected.contains(&cr.name);

        // Force rebuild when incremental strategy changes to avoid incompatible state
        let strategy_changed = if model.materialization == Materialization::Incremental {
            let current_strategy = model
                .incremental_strategy
                .unwrap_or(IncrementalStrategy::DeleteInsert);
            existing_state.get(&cr.name).map_or(false, |s| {
                s.incremental_strategy
                    .as_deref()
                    .map_or(false, |stored| stored != current_strategy.as_str())
            })
        } else {
            false
        };

        // These strategies process new data each run regardless of definition changes
        let always_run = match model.materialization {
            Materialization::Snapshot => true,
            Materialization::Incremental => {
                let strategy = model
                    .incremental_strategy
                    .unwrap_or(IncrementalStrategy::DeleteInsert);
                strategy == IncrementalStrategy::Append
                    || strategy == IncrementalStrategy::Microbatch
            }
            _ => false,
        };

        if !is_new && !needs_rebuild && !always_run && !strategy_changed {
            results.push(RunResult {
                name: cr.name.clone(),
                action: "no_change".to_string(),
                materialized: cr.materialized.clone(),
                duration_ms: 0,
                message: String::new(),
            });
            continue;
        }

        let inc_config = if model.materialization == Materialization::Incremental {
            let strategy = model
                .incremental_strategy
                .unwrap_or(IncrementalStrategy::DeleteInsert);
            let last_watermark = if strategy_changed {
                None
            } else {
                existing_state
                    .get(&cr.name)
                    .and_then(|s| s.last_watermark.clone())
            };
            Some(IncrementalConfig {
                strategy,
                unique_key: model.unique_key.clone(),
                updated_at: model.updated_at.clone(),
                batch_size: model.batch_size,
                lookback: model.lookback,
                merge_update_columns: model.merge_update_columns.clone(),
                merge_exclude_columns: model.merge_exclude_columns.clone(),
                last_watermark,
            })
        } else {
            None
        };

        let action = if is_new { "create" } else { "update" };
        let start = Instant::now();

        execute_sql("BEGIN TRANSACTION;")?;

        if let Some(hooks) = &model.pre_hooks {
            if let Err(e) = execute_hooks(hooks, schema, &model.name) {
                let _ = execute_sql("ROLLBACK;");
                return Err(format!("Pre-hook failed for {}: {}", model.name, e).into());
            }
        }

        match materialize_model(
            &model.name,
            &model.sql,
            model.materialization,
            schema,
            &known_names,
            is_new,
            inc_config.as_ref(),
            &ephemeral_models,
            model.strategy,
            model.updated_at.as_deref(),
            model.check_cols.as_ref(),
            model.unique_key.as_ref(),
            src_names.as_ref(),
            source_schema,
        ) {
            Ok(new_watermark) => {
                if let Some(hooks) = &model.post_hooks {
                    if let Err(e) = execute_hooks(hooks, schema, &model.name) {
                        let _ = execute_sql("ROLLBACK;");
                        return Err(
                            format!("Post-hook failed for {}: {}", model.name, e).into()
                        );
                    }
                }

                let checksum = checksums
                    .get(&model.name)
                    .cloned()
                    .unwrap_or_default();
                let deployed_at = Utc::now().to_rfc3339();
                let strategy_str = inc_config.as_ref().map(|c| c.strategy.as_str());
                upsert_state(
                    schema,
                    &model.name,
                    model.materialization.as_str(),
                    &checksum,
                    &deployed_at,
                    strategy_str,
                    new_watermark.as_deref(),
                )?;
                execute_sql("COMMIT;")?;
                results.push(RunResult {
                    name: cr.name.clone(),
                    action: action.to_string(),
                    materialized: cr.materialized.clone(),
                    duration_ms: start.elapsed().as_millis() as i64,
                    message: String::new(),
                });
            }
            Err(e) => {
                let _ = execute_sql("ROLLBACK;");
                return Err(format!("Failed to materialize {}: {}", model.name, e).into());
            }
        }
    }

    Ok(results)
}

#[repr(C)]
pub struct RunBindData {
    path: String,
    schema: String,
    source_schema: Option<String>,
}

#[repr(C)]
pub struct RunInitData {
    results: Vec<RunResult>,
    index: AtomicUsize,
}

pub struct RunVTab;

impl VTab for RunVTab {
    type InitData = RunInitData;
    type BindData = RunBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("action", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "materialized",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "duration_ms",
            LogicalTypeHandle::from(LogicalTypeId::Bigint),
        );
        bind.add_result_column("message", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let path = bind.get_parameter(0).to_string();
        let schema = bind.get_parameter(1).to_string();
        let source_schema = bind
            .get_named_parameter("source_schema")
            .map(|v| v.to_string())
            .filter(|s| !s.is_empty());
        Ok(RunBindData { path, schema, source_schema })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = init.get_bind_data::<Self::BindData>();
        if bind_data.is_null() {
            return Err("Bind data is null".into());
        }
        let (path, schema, source_schema) = unsafe {
            (
                (*bind_data).path.clone(),
                (*bind_data).schema.clone(),
                (*bind_data).source_schema.clone(),
            )
        };

        let results = run_project(&path, &schema, source_schema.as_deref())?;

        Ok(RunInitData {
            results,
            index: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let current_index = init_data.index.fetch_add(1, Ordering::Relaxed);

        if current_index >= init_data.results.len() {
            output.set_len(0);
            return Ok(());
        }

        let result = &init_data.results[current_index];

        let name_vector = output.flat_vector(0);
        name_vector.insert(0, result.name.as_str());

        let action_vector = output.flat_vector(1);
        action_vector.insert(0, result.action.as_str());

        let mat_vector = output.flat_vector(2);
        mat_vector.insert(0, result.materialized.as_str());

        let mut dur_vector = output.flat_vector(3);
        dur_vector.as_mut_slice::<i64>()[0] = result.duration_ms;

        let msg_vector = output.flat_vector(4);
        msg_vector.insert(0, result.message.as_str());

        output.set_len(1);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            ("source_schema".to_string(), LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ])
    }
}
