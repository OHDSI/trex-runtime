//! Distributed table catalog: resolves table names to node endpoints via gossip.

use duckdb::arrow::array::Array as _;
use duckdb::arrow::array::RecordBatch as DuckRecordBatch;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use crate::gossip::{GossipRegistry, NodeKeyValueInfo};
use crate::logging::SwarmLogger;

/// Escape a SQL identifier by doubling internal double-quotes.
pub fn escape_identifier(name: &str) -> String {
    name.replace('"', "\"\"")
}

/// Validate that an extension name contains only alphanumeric, `_`, or `-` characters.
pub fn is_valid_extension_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-')
}

/// One table on one node; a replicated table produces one entry per node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    pub node_name: String,
    pub node_id: String,
    pub table_name: String,
    pub approx_rows: u64,
    /// Matching hashes across nodes implies compatible schemas.
    pub schema_hash: u64,
    pub flight_endpoint: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CatalogValue {
    rows: u64,
    schema_hash: u64,
}

#[derive(Debug, Deserialize)]
struct FlightServiceValue {
    host: String,
    port: u16,
    status: String,
}

fn parse_flight_endpoint(json_value: &str) -> Option<String> {
    let svc: FlightServiceValue = serde_json::from_str(json_value).ok()?;
    if svc.status == "running" {
        Some(format!("http://{}:{}", svc.host, svc.port))
    } else {
        None
    }
}

fn parse_catalog_value(json_value: &str) -> Option<CatalogValue> {
    serde_json::from_str(json_value).ok()
}

fn resolve_table_from_states(
    table_name: &str,
    nodes: &[NodeKeyValueInfo],
) -> Vec<CatalogEntry> {
    let catalog_key = format!("catalog:{table_name}");
    let mut entries = Vec::new();

    for node in nodes {
        let catalog_json = node
            .key_values
            .iter()
            .find(|(k, _)| k == &catalog_key)
            .map(|(_, v)| v.as_str());

        let catalog_json = match catalog_json {
            Some(v) => v,
            None => continue,
        };

        let catalog_val = match parse_catalog_value(catalog_json) {
            Some(v) => v,
            None => {
                SwarmLogger::warn(
                    "catalog",
                    &format!(
                        "Failed to parse catalog value for table '{}' on node '{}': {}",
                        table_name, node.node_name, catalog_json,
                    ),
                );
                continue;
            }
        };

        let flight_endpoint = node
            .key_values
            .iter()
            .find(|(k, _)| k == "service:flight")
            .and_then(|(_, v)| parse_flight_endpoint(v));

        entries.push(CatalogEntry {
            node_name: node.node_name.clone(),
            node_id: node.node_id.clone(),
            table_name: table_name.to_string(),
            approx_rows: catalog_val.rows,
            schema_hash: catalog_val.schema_hash,
            flight_endpoint,
        });
    }

    entries
}

fn get_all_tables_from_states(nodes: &[NodeKeyValueInfo]) -> Vec<CatalogEntry> {
    let mut entries = Vec::new();

    for node in nodes {
        let flight_endpoint = node
            .key_values
            .iter()
            .find(|(k, _)| k == "service:flight")
            .and_then(|(_, v)| parse_flight_endpoint(v));

        for (key, value) in &node.key_values {
            let table_name = match key.strip_prefix("catalog:") {
                Some(name) if !name.is_empty() => name,
                _ => continue,
            };

            let catalog_val = match parse_catalog_value(value) {
                Some(v) => v,
                None => {
                    SwarmLogger::warn(
                        "catalog",
                        &format!(
                            "Failed to parse catalog value for table '{}' on node '{}': {}",
                            table_name, node.node_name, value,
                        ),
                    );
                    continue;
                }
            };

            entries.push(CatalogEntry {
                node_name: node.node_name.clone(),
                node_id: node.node_id.clone(),
                table_name: table_name.to_string(),
                approx_rows: catalog_val.rows,
                schema_hash: catalog_val.schema_hash,
                flight_endpoint: flight_endpoint.clone(),
            });
        }
    }

    entries
}

fn list_table_names_from_states(nodes: &[NodeKeyValueInfo]) -> Vec<String> {
    let mut names: Vec<String> = nodes
        .iter()
        .flat_map(|n| n.key_values.iter())
        .filter_map(|(k, _)| k.strip_prefix("catalog:").map(String::from))
        .filter(|name| !name.is_empty())
        .collect();

    names.sort();
    names.dedup();
    names
}

fn fetch_node_key_values() -> Result<Vec<NodeKeyValueInfo>, String> {
    GossipRegistry::instance().get_node_key_values()
}

/// Resolve a table name to the set of nodes that hold it.
pub fn resolve_table(table_name: &str) -> Result<Vec<CatalogEntry>, String> {
    let nodes = fetch_node_key_values()?;

    SwarmLogger::debug(
        "catalog",
        &format!(
            "Resolving table '{}' across {} node(s)",
            table_name,
            nodes.len(),
        ),
    );

    let entries = resolve_table_from_states(table_name, &nodes);

    SwarmLogger::debug(
        "catalog",
        &format!(
            "Table '{}' found on {} node(s)",
            table_name,
            entries.len(),
        ),
    );

    Ok(entries)
}

/// Return catalog entries for every table in the cluster.
pub fn get_all_tables() -> Result<Vec<CatalogEntry>, String> {
    let nodes = fetch_node_key_values()?;

    SwarmLogger::debug(
        "catalog",
        &format!("Scanning catalog across {} node(s)", nodes.len()),
    );

    let entries = get_all_tables_from_states(&nodes);

    SwarmLogger::debug(
        "catalog",
        &format!(
            "Catalog scan complete: {} entry/entries across {} unique table(s)",
            entries.len(),
            {
                let mut names: Vec<&str> =
                    entries.iter().map(|e| e.table_name.as_str()).collect();
                names.sort();
                names.dedup();
                names.len()
            },
        ),
    );

    Ok(entries)
}

/// Return sorted, deduplicated table names across the cluster.
pub fn list_tables() -> Result<Vec<String>, String> {
    let nodes = fetch_node_key_values()?;
    Ok(list_table_names_from_states(&nodes))
}

pub fn tables_by_name() -> Result<HashMap<String, Vec<CatalogEntry>>, String> {
    let entries = get_all_tables()?;
    let mut map: HashMap<String, Vec<CatalogEntry>> = HashMap::new();
    for entry in entries {
        map.entry(entry.table_name.clone()).or_default().push(entry);
    }
    Ok(map)
}

/// FNV-1a hash of field names and types. Stable across processes (unlike DefaultHasher).
fn compute_schema_hash(schema: &arrow::datatypes::SchemaRef) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for field in schema.fields() {
        for byte in field.name().bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        for byte in format!("{:?}", field.data_type()).bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash
}

/// FNV-1a schema hash for duckdb-reexported arrow types.
fn compute_schema_hash_duckdb(schema: &duckdb::arrow::datatypes::SchemaRef) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for field in schema.fields() {
        for byte in field.name().bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        for byte in format!("{:?}", field.data_type()).bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash
}

/// Publish `catalog:{table}` gossip keys for all local tables.
pub fn advertise_local_tables() -> Result<usize, String> {
    let conn_arc = crate::get_shared_connection()
        .ok_or_else(|| "Shared DuckDB connection not available".to_string())?;

    let conn = conn_arc
        .lock()
        .map_err(|e| format!("Failed to lock shared connection: {e}"))?;

    let table_names: Vec<String> = {
        let mut stmt = conn
            .prepare("SHOW TABLES")
            .map_err(|e| format!("Failed to prepare SHOW TABLES: {e}"))?;

        let batches: Vec<DuckRecordBatch> = stmt
            .query_arrow([])
            .map_err(|e| format!("Failed to execute SHOW TABLES: {e}"))?
            .collect();

        let mut names = Vec::new();
        for batch in &batches {
            if batch.num_columns() == 0 {
                continue;
            }
            let col = batch.column(0);
            let string_array = col
                .as_any()
                .downcast_ref::<duckdb::arrow::array::StringArray>()
                .ok_or_else(|| "SHOW TABLES did not return string column".to_string())?;
            for i in 0..string_array.len() {
                if !string_array.is_null(i) {
                    names.push(string_array.value(i).to_string());
                }
            }
        }
        names
    };

    if table_names.is_empty() {
        SwarmLogger::debug("catalog", "No local tables to advertise");
        return Ok(0);
    }

    let gossip = GossipRegistry::instance();
    let mut count = 0;

    for table in &table_names {
        let row_count: u64 = {
            let count_sql = format!("SELECT COUNT(*) FROM \"{}\"", escape_identifier(table));
            let mut stmt = conn
                .prepare(&count_sql)
                .map_err(|e| format!("Failed to prepare COUNT for table '{}': {e}", table))?;

            let batches: Vec<DuckRecordBatch> = stmt
                .query_arrow([])
                .map_err(|e| format!("Failed to execute COUNT for table '{}': {e}", table))?
                .collect();

            if let Some(batch) = batches.first() {
                if batch.num_columns() > 0 && batch.num_rows() > 0 {
                    let col = batch.column(0);
                    if let Some(arr) =
                        col.as_any().downcast_ref::<duckdb::arrow::array::Int64Array>()
                    {
                        arr.value(0) as u64
                    } else {
                        let s =
                            duckdb::arrow::util::display::array_value_to_string(col, 0)
                                .unwrap_or_default();
                        s.parse::<u64>().unwrap_or(0)
                    }
                } else {
                    0
                }
            } else {
                0
            }
        };

        let schema_hash: u64 = {
            let schema_sql = format!("SELECT * FROM \"{}\" LIMIT 0", escape_identifier(table));
            let mut stmt = conn
                .prepare(&schema_sql)
                .map_err(|e| format!("Failed to prepare schema query for '{}': {e}", table))?;

            let batches: Vec<DuckRecordBatch> = stmt
                .query_arrow([])
                .map_err(|e| format!("Failed to execute schema query for '{}': {e}", table))?
                .collect();

            if let Some(batch) = batches.first() {
                compute_schema_hash_duckdb(&batch.schema())
            } else {
                0
            }
        };

        let key = format!("catalog:{}", table);
        let value = format!(r#"{{"rows": {}, "schema_hash": {}}}"#, row_count, schema_hash);

        match gossip.set_key(&key, &value) {
            Ok(()) => {
                SwarmLogger::debug(
                    "catalog",
                    &format!(
                        "Advertised table '{}': rows={}, schema_hash=0x{:X}",
                        table, row_count, schema_hash,
                    ),
                );
                count += 1;
            }
            Err(e) => {
                SwarmLogger::warn(
                    "catalog",
                    &format!("Failed to advertise table '{}': {}", table, e),
                );
            }
        }
    }

    SwarmLogger::info(
        "catalog",
        &format!("Advertised {} local table(s)", count),
    );

    Ok(count)
}

/// Remove all `catalog:*` gossip keys from this node.
pub fn remove_catalog_keys() -> Result<usize, String> {
    let gossip = GossipRegistry::instance();

    let keys = gossip.list_keys_with_prefix("catalog:")?;
    let count = keys.len();

    for key in &keys {
        if let Err(e) = gossip.delete_key(key) {
            SwarmLogger::warn(
                "catalog",
                &format!("Failed to delete catalog key '{}': {}", key, e),
            );
        }
    }

    if count > 0 {
        SwarmLogger::info(
            "catalog",
            &format!("Removed {} catalog key(s) from gossip", count),
        );
    }

    Ok(count)
}

struct CatalogRefreshHandle {
    stop_flag: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

static CATALOG_REFRESH: OnceLock<std::sync::Mutex<Option<CatalogRefreshHandle>>> = OnceLock::new();

fn catalog_refresh_lock() -> &'static std::sync::Mutex<Option<CatalogRefreshHandle>> {
    CATALOG_REFRESH.get_or_init(|| std::sync::Mutex::new(None))
}

/// Spawn a background thread that re-advertises local tables every `SWARM_CATALOG_INTERVAL` seconds (default 30). No-op if already running.
pub fn start_catalog_refresh() -> Result<(), String> {
    let mut guard = catalog_refresh_lock()
        .lock()
        .map_err(|e| format!("Failed to lock catalog refresh handle: {e}"))?;

    if guard.is_some() {
        SwarmLogger::debug("catalog", "Catalog refresh is already running");
        return Ok(());
    }

    let interval_secs: u64 = std::env::var("SWARM_CATALOG_INTERVAL")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);

    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop_flag);

    let thread = std::thread::Builder::new()
        .name("swarm-catalog-refresh".to_string())
        .spawn(move || {
            SwarmLogger::info(
                "catalog",
                &format!(
                    "Catalog refresh started (interval={}s)",
                    interval_secs,
                ),
            );

            loop {
                std::thread::sleep(std::time::Duration::from_secs(interval_secs));

                if stop_clone.load(Ordering::Acquire) {
                    break;
                }

                match advertise_local_tables() {
                    Ok(n) => {
                        SwarmLogger::debug(
                            "catalog",
                            &format!("Catalog refresh: advertised {} table(s)", n),
                        );
                    }
                    Err(e) => {
                        SwarmLogger::warn(
                            "catalog",
                            &format!("Catalog refresh failed: {}", e),
                        );
                    }
                }
            }

            SwarmLogger::info("catalog", "Catalog refresh stopped");
        })
        .map_err(|e| format!("Failed to spawn catalog refresh thread: {e}"))?;

    *guard = Some(CatalogRefreshHandle {
        stop_flag,
        thread: Some(thread),
    });

    Ok(())
}

/// Stop the background catalog refresh thread and wait for it to exit.
pub fn stop_catalog_refresh() {
    let mut guard = match catalog_refresh_lock().lock() {
        Ok(g) => g,
        Err(e) => {
            SwarmLogger::warn(
                "catalog",
                &format!("Failed to lock catalog refresh handle for stop: {e}"),
            );
            return;
        }
    };

    if let Some(mut handle) = guard.take() {
        handle.stop_flag.store(true, Ordering::Release);
        SwarmLogger::info("catalog", "Catalog refresh stop requested");
        if let Some(thread) = handle.thread.take() {
            let _ = thread.join();
        }
    } else {
        SwarmLogger::debug("catalog", "Catalog refresh is not running");
    }
}

/// Register gossip-discovered tables in a DataFusion session. `local_only` restricts to this node.
pub fn register_tables_in_datafusion(
    ctx: &datafusion::execution::context::SessionContext,
    local_only: bool,
) -> Result<usize, String> {
    use std::sync::Arc;
    use crate::duckdb_table_provider::DuckDBTableProvider;
    use crate::duckdb_sql_executor::DuckDBSQLExecutor;

    let entries = get_all_tables()?;

    let filtered: Vec<CatalogEntry> = if local_only {
        let self_node_id = get_self_node_id();
        match self_node_id {
            Some(id) => entries.into_iter().filter(|e| e.node_id == id).collect(),
            None => entries,
        }
    } else {
        entries
    };

    let mut seen = std::collections::HashSet::new();
    let unique_tables: Vec<CatalogEntry> = filtered
        .into_iter()
        .filter(|e| seen.insert(e.table_name.clone()))
        .collect();

    let executor = Arc::new(DuckDBSQLExecutor);
    let mut count = 0;

    for entry in &unique_tables {
        let provider = match DuckDBTableProvider::new(&entry.table_name, Arc::clone(&executor)) {
            Ok(p) => p,
            Err(e) => {
                SwarmLogger::warn(
                    "catalog",
                    &format!(
                        "Failed to create TableProvider for '{}': {}",
                        entry.table_name, e
                    ),
                );
                continue;
            }
        };

        if let Err(e) = ctx.register_table(&entry.table_name, Arc::new(provider)) {
            SwarmLogger::warn(
                "catalog",
                &format!(
                    "Failed to register '{}' in DataFusion: {}",
                    entry.table_name, e
                ),
            );
            continue;
        }

        count += 1;
    }

    SwarmLogger::debug(
        "catalog",
        &format!("Registered {} table(s) in DataFusion session", count),
    );

    Ok(count)
}

/// Resolve via gossip catalog, falling back to local tables.
pub fn resolve_table_with_fallback(table_name: &str) -> Result<Vec<CatalogEntry>, String> {
    match resolve_table(table_name) {
        Ok(entries) if !entries.is_empty() => return Ok(entries),
        Ok(_) => {}
        Err(e) => return Err(e),
    }

    let conn_arc = crate::get_shared_connection()
        .ok_or_else(|| format!(
            "Table '{}' not found in distributed catalog or local database",
            table_name,
        ))?;

    let conn = conn_arc
        .lock()
        .map_err(|e| format!("Failed to lock shared connection: {e}"))?;

    let check_sql = format!("SELECT COUNT(*) FROM \"{}\"", escape_identifier(table_name));
    match conn.prepare(&check_sql) {
        Ok(mut stmt) => {
            let batches: Vec<DuckRecordBatch> = stmt
                .query_arrow([])
                .map_err(|e| format!(
                    "Table '{}' not found in distributed catalog or local database: {e}",
                    table_name,
                ))?
                .collect();

            let approx_rows = batches.first().and_then(|batch| {
                if batch.num_columns() > 0 && batch.num_rows() > 0 {
                    let col = batch.column(0);
                    col.as_any()
                        .downcast_ref::<duckdb::arrow::array::Int64Array>()
                        .map(|arr| arr.value(0) as u64)
                } else {
                    None
                }
            }).unwrap_or(0);

            SwarmLogger::debug(
                "catalog",
                &format!(
                    "Table '{}' resolved via local fallback (rows={})",
                    table_name, approx_rows,
                ),
            );

            Ok(vec![CatalogEntry {
                node_name: "local".to_string(),
                node_id: "local".to_string(),
                table_name: table_name.to_string(),
                approx_rows,
                schema_hash: 0,
                flight_endpoint: None,
            }])
        }
        Err(_) => Err(format!(
            "Table '{}' not found in distributed catalog or local database",
            table_name,
        )),
    }
}

/// Verify schema hash consistency across nodes for the given tables.
pub fn validate_join_key_types(table_names: &[String]) -> Result<(), String> {
    let nodes = fetch_node_key_values()?;
    let all_entries = get_all_tables_from_states(&nodes);

    let mut by_table: HashMap<&str, Vec<&CatalogEntry>> = HashMap::new();
    for entry in &all_entries {
        by_table.entry(&entry.table_name).or_default().push(entry);
    }

    for table_name in table_names {
        let entries = match by_table.get(table_name.as_str()) {
            Some(entries) => entries,
            None => continue,
        };

        if entries.is_empty() {
            continue;
        }

        let expected_hash = entries[0].schema_hash;
        let mismatched: Vec<&str> = entries
            .iter()
            .filter(|e| e.schema_hash != expected_hash)
            .map(|e| e.node_name.as_str())
            .collect();

        if !mismatched.is_empty() {
            let first_node = &entries[0].node_name;
            return Err(format!(
                "Schema mismatch for table '{}': node '{}' has schema_hash 0x{:X} \
                 but node(s) {} have different hashes",
                table_name,
                first_node,
                expected_hash,
                mismatched
                    .iter()
                    .map(|n| format!("'{}'", n))
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }
    }

    Ok(())
}

/// Look up the approximate row count for a table from the gossip catalog.
/// Returns `None` if the table is not found or gossip is unavailable.
pub fn get_table_row_count(table_name: &str) -> Option<u64> {
    let entries = resolve_table(table_name).ok()?;
    // Sum across all nodes (for sharded tables) or return the single entry.
    if entries.is_empty() {
        return None;
    }
    Some(entries.iter().map(|e| e.approx_rows).sum())
}

pub fn get_self_node_id() -> Option<String> {
    let config = GossipRegistry::instance().get_self_config().ok()?;
    config
        .into_iter()
        .find(|(k, _)| k == "node_id")
        .map(|(_, v)| v)
}

/// One shard of a distributed table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardInfo {
    pub node_name: String,
    pub flight_endpoint: String,
}

/// How a table is routed in the distributed session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableClassification {
    Local,
    RemoteUnique {
        node_name: String,
        flight_endpoint: String,
    },
    Sharded { shards: Vec<ShardInfo> },
}

/// Classify all cluster tables relative to the local node.
pub fn classify_tables() -> Result<HashMap<String, TableClassification>, String> {
    let nodes = fetch_node_key_values()?;
    let self_node_id = get_self_node_id();
    Ok(classify_tables_from_states(&nodes, self_node_id.as_deref()))
}

pub fn classify_tables_from_states(
    nodes: &[NodeKeyValueInfo],
    self_node_id: Option<&str>,
) -> HashMap<String, TableClassification> {
    let entries = get_all_tables_from_states(nodes);

    let mut by_table: HashMap<String, Vec<&CatalogEntry>> = HashMap::new();
    for entry in &entries {
        by_table.entry(entry.table_name.clone()).or_default().push(entry);
    }

    let mut result = HashMap::new();

    for (table_name, table_entries) in by_table {
        if table_entries.len() == 1 {
            let entry = &table_entries[0];
            let is_local = self_node_id
                .map(|id| entry.node_id == id)
                .unwrap_or(true);

            if is_local {
                result.insert(table_name, TableClassification::Local);
            } else if let Some(ref ep) = entry.flight_endpoint {
                result.insert(
                    table_name,
                    TableClassification::RemoteUnique {
                        node_name: entry.node_name.clone(),
                        flight_endpoint: ep.clone(),
                    },
                );
            } else {
                // Remote node without Flight — treat as local fallback
                result.insert(table_name, TableClassification::Local);
            }
        } else {
            let shards: Vec<ShardInfo> = table_entries
                .iter()
                .filter_map(|e| {
                    e.flight_endpoint.as_ref().map(|ep| ShardInfo {
                        node_name: e.node_name.clone(),
                        flight_endpoint: ep.clone(),
                    })
                })
                .collect();

            if shards.len() > 1 {
                result.insert(table_name, TableClassification::Sharded { shards });
            } else if shards.len() == 1 {
                // Only one node has Flight — treat as remote unique
                let shard = &shards[0];
                let is_local = self_node_id
                    .map(|id| {
                        table_entries
                            .iter()
                            .any(|e| e.node_id == id && e.flight_endpoint.is_some())
                    })
                    .unwrap_or(false);

                if is_local {
                    result.insert(table_name, TableClassification::Local);
                } else {
                    result.insert(
                        table_name,
                        TableClassification::RemoteUnique {
                            node_name: shard.node_name.clone(),
                            flight_endpoint: shard.flight_endpoint.clone(),
                        },
                    );
                }
            } else {
                // No Flight endpoints at all — local fallback
                result.insert(table_name, TableClassification::Local);
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(
        node_id: &str,
        node_name: &str,
        kvs: Vec<(&str, &str)>,
    ) -> NodeKeyValueInfo {
        NodeKeyValueInfo {
            node_id: node_id.to_string(),
            node_name: node_name.to_string(),
            gossip_addr: "127.0.0.1:7100".to_string(),
            key_values: kvs
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn catalog_json(rows: u64, schema_hash: u64) -> String {
        format!(r#"{{"rows": {rows}, "schema_hash": {schema_hash}}}"#)
    }

    fn flight_json(host: &str, port: u16, status: &str) -> String {
        format!(r#"{{"host": "{host}", "port": {port}, "status": "{status}"}}"#)
    }

    #[test]
    fn parse_flight_endpoint_running() {
        let json = flight_json("10.0.0.1", 8815, "running");
        assert_eq!(
            parse_flight_endpoint(&json),
            Some("http://10.0.0.1:8815".to_string()),
        );
    }

    #[test]
    fn parse_flight_endpoint_stopped() {
        let json = flight_json("10.0.0.1", 8815, "stopped");
        assert_eq!(parse_flight_endpoint(&json), None);
    }

    #[test]
    fn parse_flight_endpoint_invalid_json() {
        assert_eq!(parse_flight_endpoint("not json"), None);
    }

    #[test]
    fn parse_flight_endpoint_empty() {
        assert_eq!(parse_flight_endpoint(""), None);
    }

    #[test]
    fn parse_catalog_value_valid() {
        let json = catalog_json(42_000, 0xDEAD);
        let val = parse_catalog_value(&json).unwrap();
        assert_eq!(val.rows, 42_000);
        assert_eq!(val.schema_hash, 0xDEAD);
    }

    #[test]
    fn parse_catalog_value_invalid() {
        assert!(parse_catalog_value("nope").is_none());
    }

    #[test]
    fn resolve_table_single_node() {
        let cat = catalog_json(100, 1);
        let flt = flight_json("10.0.0.1", 8815, "running");
        let nodes = vec![make_node(
            "id-a",
            "node-a",
            vec![("catalog:lineitem", &cat), ("service:flight", &flt)],
        )];

        let entries = resolve_table_from_states("lineitem", &nodes);
        assert_eq!(entries.len(), 1);

        let e = &entries[0];
        assert_eq!(e.node_name, "node-a");
        assert_eq!(e.node_id, "id-a");
        assert_eq!(e.table_name, "lineitem");
        assert_eq!(e.approx_rows, 100);
        assert_eq!(e.schema_hash, 1);
        assert_eq!(e.flight_endpoint, Some("http://10.0.0.1:8815".to_string()));
    }

    #[test]
    fn resolve_table_multi_node() {
        let cat = catalog_json(500, 42);
        let flt_a = flight_json("10.0.0.1", 8815, "running");
        let flt_b = flight_json("10.0.0.2", 8815, "running");

        let nodes = vec![
            make_node(
                "id-a",
                "node-a",
                vec![("catalog:orders", &cat), ("service:flight", &flt_a)],
            ),
            make_node(
                "id-b",
                "node-b",
                vec![("catalog:orders", &cat), ("service:flight", &flt_b)],
            ),
            make_node(
                "id-c",
                "node-c",
                vec![("catalog:customers", &catalog_json(10, 99))],
            ),
        ];

        let entries = resolve_table_from_states("orders", &nodes);
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.table_name == "orders"));
        assert!(entries.iter().all(|e| e.approx_rows == 500));
    }

    #[test]
    fn resolve_table_not_found() {
        let nodes = vec![make_node(
            "id-a",
            "node-a",
            vec![("catalog:orders", &catalog_json(10, 1))],
        )];

        let entries = resolve_table_from_states("lineitem", &nodes);
        assert!(entries.is_empty());
    }

    #[test]
    fn resolve_table_no_flight_service() {
        let cat = catalog_json(200, 7);
        let nodes = vec![make_node(
            "id-a",
            "node-a",
            vec![("catalog:orders", &cat)],
        )];

        let entries = resolve_table_from_states("orders", &nodes);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].flight_endpoint, None);
    }

    #[test]
    fn resolve_table_flight_not_running() {
        let cat = catalog_json(200, 7);
        let flt = flight_json("10.0.0.1", 8815, "stopped");
        let nodes = vec![make_node(
            "id-a",
            "node-a",
            vec![("catalog:orders", &cat), ("service:flight", &flt)],
        )];

        let entries = resolve_table_from_states("orders", &nodes);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].flight_endpoint, None);
    }

    #[test]
    fn resolve_table_skips_invalid_catalog_json() {
        let nodes = vec![make_node(
            "id-a",
            "node-a",
            vec![("catalog:broken", "not valid json")],
        )];

        let entries = resolve_table_from_states("broken", &nodes);
        assert!(entries.is_empty());
    }

    #[test]
    fn get_all_tables_multiple_nodes_and_tables() {
        let flt_a = flight_json("10.0.0.1", 8815, "running");
        let flt_b = flight_json("10.0.0.2", 8815, "running");

        let nodes = vec![
            make_node(
                "id-a",
                "node-a",
                vec![
                    ("catalog:orders", &catalog_json(100, 1)),
                    ("catalog:lineitem", &catalog_json(1000, 2)),
                    ("service:flight", &flt_a),
                    ("node_name", "node-a"),
                ],
            ),
            make_node(
                "id-b",
                "node-b",
                vec![
                    ("catalog:orders", &catalog_json(200, 1)),
                    ("service:flight", &flt_b),
                ],
            ),
        ];

        let entries = get_all_tables_from_states(&nodes);
        assert_eq!(entries.len(), 3);

        let orders: Vec<_> = entries
            .iter()
            .filter(|e| e.table_name == "orders")
            .collect();
        assert_eq!(orders.len(), 2);

        let lineitem: Vec<_> = entries
            .iter()
            .filter(|e| e.table_name == "lineitem")
            .collect();
        assert_eq!(lineitem.len(), 1);
        assert_eq!(lineitem[0].approx_rows, 1000);
        assert_eq!(
            lineitem[0].flight_endpoint,
            Some("http://10.0.0.1:8815".to_string()),
        );
    }

    #[test]
    fn get_all_tables_empty_cluster() {
        let entries = get_all_tables_from_states(&[]);
        assert!(entries.is_empty());
    }

    #[test]
    fn get_all_tables_no_catalog_keys() {
        let nodes = vec![make_node(
            "id-a",
            "node-a",
            vec![
                ("node_name", "node-a"),
                ("status", "active"),
                ("service:flight", &flight_json("10.0.0.1", 8815, "running")),
            ],
        )];

        let entries = get_all_tables_from_states(&nodes);
        assert!(entries.is_empty());
    }

    #[test]
    fn list_table_names_deduplicates_and_sorts() {
        let nodes = vec![
            make_node(
                "id-a",
                "node-a",
                vec![
                    ("catalog:orders", &catalog_json(10, 1)),
                    ("catalog:lineitem", &catalog_json(20, 2)),
                ],
            ),
            make_node(
                "id-b",
                "node-b",
                vec![("catalog:orders", &catalog_json(30, 1))],
            ),
        ];

        let names = list_table_names_from_states(&nodes);
        assert_eq!(names, vec!["lineitem", "orders"]);
    }

    #[test]
    fn list_table_names_empty() {
        let names = list_table_names_from_states(&[]);
        assert!(names.is_empty());
    }

    #[test]
    fn catalog_entry_clone_and_debug() {
        let entry = CatalogEntry {
            node_name: "node-a".to_string(),
            node_id: "id-a".to_string(),
            table_name: "orders".to_string(),
            approx_rows: 42,
            schema_hash: 0xFF,
            flight_endpoint: Some("http://localhost:8815".to_string()),
        };

        let cloned = entry.clone();
        assert_eq!(entry, cloned);

        let _ = format!("{:?}", entry);
    }

    #[test]
    fn compute_schema_hash_deterministic() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
        ]));

        let h1 = compute_schema_hash(&schema);
        let h2 = compute_schema_hash(&schema);
        assert_eq!(h1, h2, "Same schema must produce identical hashes");
    }

    #[test]
    fn compute_schema_hash_differs_on_column_name() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema_a = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
        ]));

        assert_ne!(
            compute_schema_hash(&schema_a),
            compute_schema_hash(&schema_b),
            "Different column names should produce different hashes",
        );
    }

    #[test]
    fn compute_schema_hash_differs_on_type() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema_a = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Int64, false),
        ]));
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Utf8, false),
        ]));

        assert_ne!(
            compute_schema_hash(&schema_a),
            compute_schema_hash(&schema_b),
            "Different column types should produce different hashes",
        );
    }

    #[test]
    fn compute_schema_hash_empty_schema() {
        use arrow::datatypes::Schema;
        use std::sync::Arc;

        let schema = Arc::new(Schema::empty());
        let _h = compute_schema_hash(&schema);
    }

    #[test]
    fn compute_schema_hash_column_order_matters() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema_a = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]));
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int64, false),
        ]));

        assert_ne!(
            compute_schema_hash(&schema_a),
            compute_schema_hash(&schema_b),
            "Column order should affect the hash",
        );
    }

    #[test]
    fn catalog_refresh_lock_initializes() {
        let lock = catalog_refresh_lock();
        let guard = lock.lock().unwrap();
        drop(guard);
    }

    #[test]
    fn stop_catalog_refresh_when_not_running_does_not_panic() {
        stop_catalog_refresh();
    }

    #[test]
    fn advertise_local_tables_without_connection() {
        let result = advertise_local_tables();
        assert!(result.is_err());
    }

    #[test]
    fn remove_catalog_keys_without_gossip() {
        let result = remove_catalog_keys();
        assert!(result.is_err());
    }

    #[test]
    fn validate_matching_schemas_succeeds() {
        let hash = 0xABCD;
        let nodes = vec![
            make_node(
                "id-a",
                "node-a",
                vec![("catalog:orders", &catalog_json(100, hash))],
            ),
            make_node(
                "id-b",
                "node-b",
                vec![("catalog:orders", &catalog_json(200, hash))],
            ),
        ];

        let all_entries = get_all_tables_from_states(&nodes);

        let mut by_table: HashMap<&str, Vec<&CatalogEntry>> = HashMap::new();
        for entry in &all_entries {
            by_table.entry(&entry.table_name).or_default().push(entry);
        }

        let table_names = vec!["orders".to_string()];
        let mut ok = true;
        for name in &table_names {
            if let Some(entries) = by_table.get(name.as_str()) {
                let expected = entries[0].schema_hash;
                if entries.iter().any(|e| e.schema_hash != expected) {
                    ok = false;
                }
            }
        }
        assert!(ok, "All nodes have the same schema_hash; validation should pass");
    }

    #[test]
    fn validate_mismatched_schemas_fails() {
        let nodes = vec![
            make_node(
                "id-a",
                "node-a",
                vec![("catalog:orders", &catalog_json(100, 0xAAAA))],
            ),
            make_node(
                "id-b",
                "node-b",
                vec![("catalog:orders", &catalog_json(200, 0xBBBB))],
            ),
        ];

        let all_entries = get_all_tables_from_states(&nodes);

        let mut by_table: HashMap<&str, Vec<&CatalogEntry>> = HashMap::new();
        for entry in &all_entries {
            by_table.entry(&entry.table_name).or_default().push(entry);
        }

        let table_names = vec!["orders".to_string()];
        let mut mismatch_table: Option<String> = None;
        for name in &table_names {
            if let Some(entries) = by_table.get(name.as_str()) {
                let expected = entries[0].schema_hash;
                if entries.iter().any(|e| e.schema_hash != expected) {
                    mismatch_table = Some(name.clone());
                }
            }
        }
        assert!(
            mismatch_table.is_some(),
            "Should detect schema mismatch across nodes",
        );
        assert_eq!(mismatch_table.unwrap(), "orders");
    }

    #[test]
    fn resolve_fallback_returns_error_for_missing_table() {
        let result = resolve_table_with_fallback("nonexistent_table");
        assert!(result.is_err());
    }

    #[test]
    fn escape_identifier_no_quotes() {
        assert_eq!(escape_identifier("orders"), "orders");
    }

    #[test]
    fn escape_identifier_with_quotes() {
        assert_eq!(escape_identifier(r#"my"table"#), r#"my""table"#);
    }

    #[test]
    fn escape_identifier_empty() {
        assert_eq!(escape_identifier(""), "");
    }

    #[test]
    fn is_valid_extension_name_valid() {
        assert!(is_valid_extension_name("flight"));
        assert!(is_valid_extension_name("my_ext"));
        assert!(is_valid_extension_name("ext-name"));
        assert!(is_valid_extension_name("ext123"));
    }

    #[test]
    fn is_valid_extension_name_invalid() {
        assert!(!is_valid_extension_name(""));
        assert!(!is_valid_extension_name("ext.trex"));
        assert!(!is_valid_extension_name("ext;drop"));
        assert!(!is_valid_extension_name("ext name"));
        assert!(!is_valid_extension_name("ext'name"));
    }

    #[test]
    fn compute_schema_hash_stable_across_calls() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let h1 = compute_schema_hash(&schema);
        let h2 = compute_schema_hash(&schema);
        let h3 = compute_schema_hash(&schema);
        assert_eq!(h1, h2);
        assert_eq!(h2, h3);
        assert_ne!(h1, 0);
    }

    #[test]
    fn classify_local_only_table() {
        let cat = catalog_json(100, 1);
        let flt = flight_json("10.0.0.1", 8815, "running");
        let nodes = vec![make_node(
            "id-a",
            "node-a",
            vec![("catalog:orders", &cat), ("service:flight", &flt)],
        )];

        let result = classify_tables_from_states(&nodes, Some("id-a"));
        assert_eq!(result.get("orders"), Some(&TableClassification::Local));
    }

    #[test]
    fn classify_remote_unique_table() {
        let cat = catalog_json(100, 1);
        let flt = flight_json("10.0.0.2", 8815, "running");
        let nodes = vec![make_node(
            "id-b",
            "node-b",
            vec![("catalog:orders", &cat), ("service:flight", &flt)],
        )];

        let result = classify_tables_from_states(&nodes, Some("id-a"));
        assert_eq!(
            result.get("orders"),
            Some(&TableClassification::RemoteUnique {
                node_name: "node-b".to_string(),
                flight_endpoint: "http://10.0.0.2:8815".to_string(),
            })
        );
    }

    #[test]
    fn classify_sharded_table() {
        let cat = catalog_json(500, 42);
        let flt_a = flight_json("10.0.0.1", 8815, "running");
        let flt_b = flight_json("10.0.0.2", 8815, "running");

        let nodes = vec![
            make_node(
                "id-a",
                "node-a",
                vec![("catalog:orders", &cat), ("service:flight", &flt_a)],
            ),
            make_node(
                "id-b",
                "node-b",
                vec![("catalog:orders", &cat), ("service:flight", &flt_b)],
            ),
        ];

        let result = classify_tables_from_states(&nodes, Some("id-a"));
        match result.get("orders") {
            Some(TableClassification::Sharded { shards }) => {
                assert_eq!(shards.len(), 2);
                let endpoints: Vec<&str> = shards.iter().map(|s| s.flight_endpoint.as_str()).collect();
                assert!(endpoints.contains(&"http://10.0.0.1:8815"));
                assert!(endpoints.contains(&"http://10.0.0.2:8815"));
            }
            other => panic!("Expected Sharded, got {:?}", other),
        }
    }

    #[test]
    fn classify_mixed_tables() {
        let cat = catalog_json(100, 1);
        let flt_a = flight_json("10.0.0.1", 8815, "running");
        let flt_b = flight_json("10.0.0.2", 8815, "running");

        let nodes = vec![
            make_node(
                "id-a",
                "node-a",
                vec![
                    ("catalog:local_table", &cat),
                    ("catalog:shared_table", &cat),
                    ("service:flight", &flt_a),
                ],
            ),
            make_node(
                "id-b",
                "node-b",
                vec![
                    ("catalog:remote_table", &cat),
                    ("catalog:shared_table", &cat),
                    ("service:flight", &flt_b),
                ],
            ),
        ];

        let result = classify_tables_from_states(&nodes, Some("id-a"));
        assert_eq!(result.get("local_table"), Some(&TableClassification::Local));
        assert_eq!(
            result.get("remote_table"),
            Some(&TableClassification::RemoteUnique {
                node_name: "node-b".to_string(),
                flight_endpoint: "http://10.0.0.2:8815".to_string(),
            })
        );
        assert!(matches!(result.get("shared_table"), Some(TableClassification::Sharded { .. })));
    }

    #[test]
    fn classify_empty_cluster() {
        let result = classify_tables_from_states(&[], Some("id-a"));
        assert!(result.is_empty());
    }

    #[test]
    fn classify_no_self_node_id_treats_as_local() {
        let cat = catalog_json(100, 1);
        let flt = flight_json("10.0.0.1", 8815, "running");
        let nodes = vec![make_node(
            "id-a",
            "node-a",
            vec![("catalog:orders", &cat), ("service:flight", &flt)],
        )];

        let result = classify_tables_from_states(&nodes, None);
        assert_eq!(result.get("orders"), Some(&TableClassification::Local));
    }
}
