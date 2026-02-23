//! Table partitioning: distribute tables across cluster nodes.

use arrow::array::RecordBatch;
use arrow::compute::take;
use arrow::datatypes::{DataType, SchemaRef};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::catalog;
use crate::flight_client;
use crate::gossip::GossipRegistry;
use crate::logging::SwarmLogger;
use crate::shuffle_descriptor::{ShuffleDescriptor, ShuffleTarget};
use crate::shuffle_partition;
use crate::shuffle_transport;

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStrategy {
    Hash {
        column: String,
        num_partitions: usize,
    },
    Range {
        column: String,
        ranges: Vec<RangeBound>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeBound {
    #[serde(default)]
    pub lower: Option<serde_json::Value>,
    #[serde(default)]
    pub upper: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionAssignment {
    pub partition_id: usize,
    pub node_name: String,
    pub flight_endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub strategy: PartitionStrategy,
    pub assignments: Vec<PartitionAssignment>,
    pub create_sql: String,
}

/// User-facing JSON config parsed from the second argument.
#[derive(Debug, Deserialize)]
pub struct PartitionConfig {
    pub strategy: String,
    pub column: String,
    #[serde(default)]
    pub partitions: Option<usize>,
    #[serde(default)]
    pub ranges: Option<Vec<RangeBound>>,
    #[serde(default)]
    pub nodes: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Gossip helpers
// ---------------------------------------------------------------------------

pub fn publish_partition_metadata(
    table_name: &str,
    metadata: &PartitionMetadata,
) -> Result<(), String> {
    let key = format!("partition:{}", table_name);
    let value = serde_json::to_string(metadata)
        .map_err(|e| format!("Failed to serialize partition metadata: {e}"))?;
    GossipRegistry::instance().set_key(&key, &value)
}

pub fn get_partition_metadata(table_name: &str) -> Result<Option<PartitionMetadata>, String> {
    let nodes = GossipRegistry::instance().get_node_key_values()?;
    let key = format!("partition:{}", table_name);
    for node in &nodes {
        for (k, v) in &node.key_values {
            if k == &key {
                let meta: PartitionMetadata = serde_json::from_str(v)
                    .map_err(|e| format!("Failed to parse partition metadata: {e}"))?;
                return Ok(Some(meta));
            }
        }
    }
    Ok(None)
}

pub fn remove_partition_metadata(table_name: &str) -> Result<(), String> {
    let key = format!("partition:{}", table_name);
    GossipRegistry::instance().delete_key(&key)
}

/// Return all partition metadata entries visible in gossip.
pub fn get_all_partition_metadata() -> Result<Vec<(String, PartitionMetadata)>, String> {
    let nodes = GossipRegistry::instance().get_node_key_values()?;
    let mut result = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for node in &nodes {
        for (k, v) in &node.key_values {
            if let Some(table_name) = k.strip_prefix("partition:") {
                if seen.insert(table_name.to_string()) {
                    if let Ok(meta) = serde_json::from_str::<PartitionMetadata>(v) {
                        result.push((table_name.to_string(), meta));
                    }
                }
            }
        }
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Assignment
// ---------------------------------------------------------------------------

/// Target node info used for partition assignment.
pub struct TargetNode {
    pub node_name: String,
    pub flight_endpoint: String,
}

/// Discover active data nodes with Flight endpoints from gossip.
pub fn discover_target_nodes() -> Result<Vec<TargetNode>, String> {
    let nodes = GossipRegistry::instance().get_node_key_values()?;
    let mut targets = Vec::new();

    for node in &nodes {
        let is_data_node = node
            .key_values
            .iter()
            .any(|(k, v)| k == "data_node" && v == "true");

        if !is_data_node {
            continue;
        }

        let flight_endpoint = node.key_values.iter().find_map(|(k, v)| {
            if k == "service:flight" {
                let svc: serde_json::Value = serde_json::from_str(v).ok()?;
                if svc.get("status")?.as_str()? == "running" {
                    let host = svc.get("host")?.as_str()?;
                    let port = svc.get("port")?.as_u64()?;
                    Some(format!("http://{}:{}", host, port))
                } else {
                    None
                }
            } else {
                None
            }
        });

        if let Some(ep) = flight_endpoint {
            targets.push(TargetNode {
                node_name: node.node_name.clone(),
                flight_endpoint: ep,
            });
        }
    }

    Ok(targets)
}

/// Assign partition IDs to target nodes (round-robin or explicit).
pub fn assign_partitions(
    num_partitions: usize,
    available_nodes: &[TargetNode],
    explicit_nodes: Option<&[String]>,
) -> Result<Vec<PartitionAssignment>, String> {
    if available_nodes.is_empty() {
        return Err("No target nodes available for partitioning".to_string());
    }

    let target_nodes: Vec<&TargetNode> = if let Some(names) = explicit_nodes {
        let mut matched = Vec::new();
        for name in names {
            let node = available_nodes
                .iter()
                .find(|n| n.node_name == *name)
                .ok_or_else(|| format!("Node '{}' not found among available data nodes", name))?;
            matched.push(node);
        }
        matched
    } else {
        available_nodes.iter().collect()
    };

    if target_nodes.is_empty() {
        return Err("No target nodes matched for partitioning".to_string());
    }

    let mut assignments = Vec::with_capacity(num_partitions);
    for partition_id in 0..num_partitions {
        let node = &target_nodes[partition_id % target_nodes.len()];
        assignments.push(PartitionAssignment {
            partition_id,
            node_name: node.node_name.clone(),
            flight_endpoint: node.flight_endpoint.clone(),
        });
    }

    Ok(assignments)
}

// ---------------------------------------------------------------------------
// DDL generation from Arrow schema
// ---------------------------------------------------------------------------

pub fn generate_create_table_sql(table_name: &str, schema: &SchemaRef) -> String {
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| {
            let sql_type = arrow_type_to_sql(field.data_type());
            format!(
                "\"{}\" {}",
                field.name().replace('"', "\"\""),
                sql_type
            )
        })
        .collect();

    format!(
        "CREATE OR REPLACE TABLE \"{}\" ({})",
        table_name.replace('"', "\"\""),
        columns.join(", ")
    )
}

fn arrow_type_to_sql(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float16 => "FLOAT",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
        DataType::Binary | DataType::LargeBinary => "BLOB",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Time32(_) | DataType::Time64(_) => "TIME",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "DECIMAL",
        DataType::Interval(_) => "INTERVAL",
        _ => "VARCHAR",
    }
}

// ---------------------------------------------------------------------------
// Range partitioning
// ---------------------------------------------------------------------------

/// Partition batches by range on a single column.
///
/// Each range in `ranges` defines a bucket. Rows where the column value falls
/// within [lower, upper) are assigned to that bucket. The ranges list must
/// cover all possible values (first range may have no lower, last may have no
/// upper).
pub fn range_partition_batches(
    batches: &[RecordBatch],
    column_name: &str,
    ranges: &[RangeBound],
) -> Result<Vec<Vec<RecordBatch>>, String> {
    if ranges.is_empty() {
        return Err("At least one range is required".to_string());
    }

    let num_partitions = ranges.len();
    let mut result: Vec<Vec<RecordBatch>> = vec![Vec::new(); num_partitions];

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let col_idx = batch
            .schema()
            .index_of(column_name)
            .map_err(|_| format!("Column '{}' not found in schema", column_name))?;

        let col = batch.column(col_idx);
        let num_rows = batch.num_rows();

        let mut partition_indices: Vec<Vec<u32>> = vec![Vec::new(); num_partitions];

        for row in 0..num_rows {
            let value_str =
                arrow::util::display::array_value_to_string(col, row).unwrap_or_default();
            let value_f64: Option<f64> = value_str.parse().ok();

            let mut assigned = false;
            for (part_idx, range) in ranges.iter().enumerate() {
                let above_lower = match &range.lower {
                    None => true,
                    Some(bound) => match (value_f64, bound.as_f64()) {
                        (Some(v), Some(b)) => v >= b,
                        _ => value_str >= bound.to_string(),
                    },
                };

                let below_upper = match &range.upper {
                    None => true,
                    Some(bound) => match (value_f64, bound.as_f64()) {
                        (Some(v), Some(b)) => v < b,
                        _ => value_str < bound.to_string(),
                    },
                };

                if above_lower && below_upper {
                    partition_indices[part_idx].push(row as u32);
                    assigned = true;
                    break;
                }
            }

            if !assigned {
                partition_indices[num_partitions - 1].push(row as u32);
            }
        }

        let schema = batch.schema();
        for (part_idx, indices) in partition_indices.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }

            let indices_array = arrow::array::UInt32Array::from(indices.clone());
            let columns: Vec<_> = batch
                .columns()
                .iter()
                .map(|col_arr| {
                    take(col_arr.as_ref(), &indices_array, None)
                        .map_err(|e| format!("Arrow take error: {e}"))
                })
                .collect::<Result<_, _>>()?;

            let part_batch = RecordBatch::try_new(schema.clone(), columns)
                .map_err(|e| format!("Failed to create partitioned batch: {e}"))?;
            result[part_idx].push(part_batch);
        }
    }

    Ok(result)
}

fn read_local_table(table_name: &str) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    let conn_arc = crate::get_shared_connection()
        .ok_or_else(|| "Shared connection not available".to_string())?;
    let conn = conn_arc
        .lock()
        .map_err(|e| format!("Failed to lock shared connection: {e}"))?;

    let sql = format!(
        "SELECT * FROM \"{}\"",
        table_name.replace('"', "\"\"")
    );

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("Failed to prepare query for table '{}': {e}", table_name))?;

    let result = stmt
        .query_arrow([])
        .map_err(|e| format!("Failed to read table '{}': {e}", table_name))?;

    let schema = result.get_schema();
    let batches: Vec<_> = result.collect();

    Ok((schema, batches))
}

fn drop_local_table(table_name: &str) -> Result<(), String> {
    let conn_arc = crate::get_shared_connection()
        .ok_or_else(|| "Shared connection not available".to_string())?;
    let conn = conn_arc
        .lock()
        .map_err(|e| format!("Failed to lock shared connection: {e}"))?;

    let sql = format!(
        "DROP TABLE IF EXISTS \"{}\"",
        table_name.replace('"', "\"\"")
    );
    conn.execute_batch(&sql)
        .map_err(|e| format!("Failed to drop table '{}': {e}", table_name))?;

    Ok(())
}

fn with_runtime<F, T>(f: F) -> Result<T, String>
where
    F: FnOnce(&tokio::runtime::Runtime) -> Result<T, String>,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create tokio runtime: {e}"))?;
    f(&rt)
}

pub fn swarm_partition_table_impl(
    table_name: &str,
    config_json: &str,
) -> Result<String, String> {
    let config: PartitionConfig = serde_json::from_str(config_json)
        .map_err(|e| format!("Invalid partition config JSON: {e}"))?;

    SwarmLogger::info(
        "partition",
        &format!(
            "Partitioning table '{}' with strategy '{}'",
            table_name, config.strategy
        ),
    );

    let (schema, batches) = read_local_table(table_name)?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    if schema.fields().is_empty() {
        return Err(format!("Table '{}' has no columns", table_name));
    }

    schema
        .index_of(&config.column)
        .map_err(|_| format!("Column '{}' not found in table '{}'", config.column, table_name))?;

    let available_nodes = discover_target_nodes()?;
    if available_nodes.is_empty() {
        return Err("No active data nodes with Flight endpoints found in cluster".to_string());
    }

    let (strategy, partitioned_data) = match config.strategy.as_str() {
        "hash" => {
            let num_partitions = config
                .partitions
                .ok_or("Hash strategy requires 'partitions' field")?;
            if num_partitions == 0 {
                return Err("Number of partitions must be > 0".to_string());
            }

            let key_indices = shuffle_partition::resolve_key_indices(
                &schema,
                &[config.column.clone()],
            )
            .map_err(|e| format!("Failed to resolve partition column: {e}"))?;

            let mut all_partitions: Vec<Vec<RecordBatch>> = vec![Vec::new(); num_partitions];
            for batch in &batches {
                let parts = shuffle_partition::partition_batch(batch, &key_indices, num_partitions)
                    .map_err(|e| format!("Hash partitioning failed: {e}"))?;
                for (i, part) in parts.into_iter().enumerate() {
                    if part.num_rows() > 0 {
                        all_partitions[i].push(part);
                    }
                }
            }

            let strategy = PartitionStrategy::Hash {
                column: config.column.clone(),
                num_partitions,
            };
            (strategy, all_partitions)
        }
        "range" => {
            let ranges = config
                .ranges
                .as_ref()
                .ok_or("Range strategy requires 'ranges' field")?;
            if ranges.is_empty() {
                return Err("At least one range is required".to_string());
            }

            let partitioned = range_partition_batches(&batches, &config.column, ranges)?;

            let strategy = PartitionStrategy::Range {
                column: config.column.clone(),
                ranges: ranges.clone(),
            };
            (strategy, partitioned)
        }
        other => return Err(format!("Unknown partition strategy: '{}'", other)),
    };

    let num_partitions = partitioned_data.len();

    let assignments = assign_partitions(
        num_partitions,
        &available_nodes,
        config.nodes.as_deref(),
    )?;

    let create_sql = generate_create_table_sql(table_name, &schema);

    with_runtime(|rt| {
        rt.block_on(async {
            distribute_partitions(
                table_name,
                &schema,
                &create_sql,
                &assignments,
                partitioned_data,
            )
            .await
        })
    })?;

    let local_ep = get_local_flight_endpoint();
    let coordinator_is_target = local_ep.as_ref().map_or(false, |ep| {
        assignments.iter().any(|a| a.flight_endpoint == *ep)
    });
    if !coordinator_is_target {
        drop_local_table(table_name)?;
    }

    let metadata = PartitionMetadata {
        strategy,
        assignments: assignments.clone(),
        create_sql,
    };
    publish_partition_metadata(table_name, &metadata)?;

    let _ = catalog::advertise_local_tables();

    let partition_summary: Vec<String> = assignments
        .iter()
        .map(|a| format!("  partition {} -> {}", a.partition_id, a.node_name))
        .collect();

    Ok(format!(
        "Partitioned table '{}' ({} rows) into {} partition(s):\n{}",
        table_name,
        total_rows,
        num_partitions,
        partition_summary.join("\n")
    ))
}

pub fn swarm_create_table_impl(
    create_sql: &str,
    config_json: &str,
) -> Result<String, String> {
    SwarmLogger::info(
        "partition",
        &format!("Creating and partitioning table with SQL: {}", create_sql),
    );

    {
        let conn_arc = crate::get_shared_connection()
            .ok_or_else(|| "Shared connection not available".to_string())?;
        let conn = conn_arc
            .lock()
            .map_err(|e| format!("Failed to lock shared connection: {e}"))?;
        conn.execute_batch(create_sql)
            .map_err(|e| format!("Failed to execute CREATE TABLE: {e}"))?;
    }

    let table_name = extract_table_name(create_sql)
        .ok_or_else(|| "Could not extract table name from CREATE SQL".to_string())?;

    match swarm_partition_table_impl(&table_name, config_json) {
        Ok(msg) => Ok(msg),
        Err(e) => {
            let _ = drop_local_table(&table_name);  // rollback
            Err(e)
        }
    }
}

pub fn swarm_repartition_table_impl(
    table_name: &str,
    config_json: &str,
) -> Result<String, String> {
    SwarmLogger::info(
        "partition",
        &format!("Repartitioning table '{}'", table_name),
    );

    let entries = catalog::resolve_table(table_name)?;
    if entries.is_empty() {
        return Err(format!("Table '{}' not found in cluster catalog", table_name));
    }

    let shard_endpoints: Vec<(String, String)> = entries
        .iter()
        .filter_map(|e| {
            e.flight_endpoint
                .as_ref()
                .map(|ep| (ep.clone(), e.node_name.clone()))
        })
        .collect();

    if shard_endpoints.is_empty() {
        return Err(format!(
            "No Flight endpoints found for table '{}' — cannot gather data",
            table_name
        ));
    }

    let (schema, all_batches) = with_runtime(|rt| {
        rt.block_on(async {
            gather_table_from_shards(table_name, &shard_endpoints).await
        })
    })?;

    let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
    SwarmLogger::info(
        "partition",
        &format!(
            "Gathered {} rows from {} shard(s) for table '{}'",
            total_rows,
            shard_endpoints.len(),
            table_name,
        ),
    );

    with_runtime(|rt| {
        rt.block_on(async {
            let mut drop_failures: Vec<String> = Vec::new();
            for (endpoint, node_name) in &shard_endpoints {
                let drop_sql = format!(
                    "DROP TABLE IF EXISTS \"{}\"",
                    table_name.replace('"', "\"\"")
                );
                if let Err(e) = flight_client::execute_remote_sql(endpoint, &drop_sql).await {
                    drop_failures.push(format!("node '{}': {}", node_name, e));
                }
            }
            if !drop_failures.is_empty() {
                return Err(format!(
                    "Aborting repartition of '{}' — failed to drop old shards: {}",
                    table_name,
                    drop_failures.join("; ")
                ));
            }
            Ok(())
        })
    })?;

    let _ = remove_partition_metadata(table_name);

    let config: PartitionConfig = serde_json::from_str(config_json)
        .map_err(|e| format!("Invalid partition config JSON: {e}"))?;

    schema
        .index_of(&config.column)
        .map_err(|_| format!("Column '{}' not found in table '{}'", config.column, table_name))?;

    let available_nodes = discover_target_nodes()?;
    if available_nodes.is_empty() {
        return Err("No active data nodes with Flight endpoints found in cluster".to_string());
    }

    let (strategy, partitioned_data) = match config.strategy.as_str() {
        "hash" => {
            let num_partitions = config
                .partitions
                .ok_or("Hash strategy requires 'partitions' field")?;
            if num_partitions == 0 {
                return Err("Number of partitions must be > 0".to_string());
            }

            let key_indices = shuffle_partition::resolve_key_indices(
                &schema,
                &[config.column.clone()],
            )
            .map_err(|e| format!("Failed to resolve partition column: {e}"))?;

            let mut all_partitions: Vec<Vec<RecordBatch>> = vec![Vec::new(); num_partitions];
            for batch in &all_batches {
                let parts = shuffle_partition::partition_batch(batch, &key_indices, num_partitions)
                    .map_err(|e| format!("Hash partitioning failed: {e}"))?;
                for (i, part) in parts.into_iter().enumerate() {
                    if part.num_rows() > 0 {
                        all_partitions[i].push(part);
                    }
                }
            }

            let strategy = PartitionStrategy::Hash {
                column: config.column.clone(),
                num_partitions,
            };
            (strategy, all_partitions)
        }
        "range" => {
            let ranges = config
                .ranges
                .as_ref()
                .ok_or("Range strategy requires 'ranges' field")?;
            if ranges.is_empty() {
                return Err("At least one range is required".to_string());
            }

            let partitioned = range_partition_batches(&all_batches, &config.column, ranges)?;

            let strategy = PartitionStrategy::Range {
                column: config.column.clone(),
                ranges: ranges.clone(),
            };
            (strategy, partitioned)
        }
        other => return Err(format!("Unknown partition strategy: '{}'", other)),
    };

    let num_partitions = partitioned_data.len();

    let assignments = assign_partitions(
        num_partitions,
        &available_nodes,
        config.nodes.as_deref(),
    )?;

    let create_sql = generate_create_table_sql(table_name, &schema);

    with_runtime(|rt| {
        rt.block_on(async {
            distribute_partitions(
                table_name,
                &schema,
                &create_sql,
                &assignments,
                partitioned_data,
            )
            .await
        })
    })?;

    let metadata = PartitionMetadata {
        strategy,
        assignments: assignments.clone(),
        create_sql,
    };
    publish_partition_metadata(table_name, &metadata)?;

    let _ = catalog::advertise_local_tables();

    let partition_summary: Vec<String> = assignments
        .iter()
        .map(|a| format!("  partition {} -> {}", a.partition_id, a.node_name))
        .collect();

    Ok(format!(
        "Repartitioned table '{}' ({} rows) into {} partition(s):\n{}",
        table_name,
        total_rows,
        num_partitions,
        partition_summary.join("\n")
    ))
}

/// Return the Flight endpoint of the local node, if available.
fn get_local_flight_endpoint() -> Option<String> {
    let self_id = catalog::get_self_node_id()?;
    let entries = catalog::get_all_tables().ok()?;
    entries
        .iter()
        .find(|e| e.node_id == self_id)
        .and_then(|e| e.flight_endpoint.clone())
}

/// Distribute partitioned data to remote nodes via Flight.
async fn distribute_partitions(
    table_name: &str,
    schema: &SchemaRef,
    create_sql: &str,
    assignments: &[PartitionAssignment],
    partitioned_data: Vec<Vec<RecordBatch>>,
) -> Result<(), String> {
    let mut created_on: Vec<String> = Vec::new(); // for rollback

    let mut unique_endpoints: Vec<(String, String)> = Vec::new();
    let mut seen_endpoints = std::collections::HashSet::new();
    for assignment in assignments {
        if seen_endpoints.insert(assignment.flight_endpoint.clone()) {
            unique_endpoints.push((
                assignment.flight_endpoint.clone(),
                assignment.node_name.clone(),
            ));
        }
    }

    for (endpoint, node_name) in &unique_endpoints {
        if let Err(e) = flight_client::execute_remote_sql(endpoint, create_sql).await {
            for rollback_ep in &created_on {  // rollback
                let drop_sql = format!(
                    "DROP TABLE IF EXISTS \"{}\"",
                    table_name.replace('"', "\"\"")
                );
                let _ = flight_client::execute_remote_sql(rollback_ep, &drop_sql).await;
            }
            return Err(format!(
                "Failed to create table '{}' on node '{}': {}",
                table_name, node_name, e
            ));
        }
        created_on.push(endpoint.clone());
    }

    for assignment in assignments {
        let partition_id = assignment.partition_id;
        if partition_id >= partitioned_data.len() {
            continue;
        }

        let partition_batches = &partitioned_data[partition_id];
        if partition_batches.is_empty()
            || partition_batches.iter().all(|b| b.num_rows() == 0)
        {
            continue;
        }

        let descriptor = ShuffleDescriptor {
            shuffle_id: format!("partition-{}-{}", table_name, partition_id),
            join_keys: vec![],
            num_partitions: partitioned_data.len(),
            partition_targets: vec![ShuffleTarget {
                partition_id,
                flight_endpoint: assignment.flight_endpoint.clone(),
                node_name: assignment.node_name.clone(),
            }],
            target_table: Some(table_name.to_string()),
        };

        if let Err(e) = shuffle_transport::send_partition(
            &assignment.flight_endpoint,
            &descriptor,
            partition_id,
            schema.clone(),
            partition_batches.clone(),
        )
        .await
        {
            // rollback
            for rollback_ep in &created_on {
                let drop_sql = format!(
                    "DROP TABLE IF EXISTS \"{}\"",
                    table_name.replace('"', "\"\"")
                );
                let _ = flight_client::execute_remote_sql(rollback_ep, &drop_sql).await;
            }
            return Err(format!(
                "Failed to send partition {} to '{}': {}",
                partition_id, assignment.node_name, e
            ));
        }

        SwarmLogger::debug(
            "partition",
            &format!(
                "Sent partition {} ({} batches) to {}",
                partition_id,
                partition_batches.len(),
                assignment.node_name,
            ),
        );
    }

    // Eagerly refresh catalog (avoids 30s gossip delay)
    for (endpoint, node_name) in &unique_endpoints {
        if let Err(e) = flight_client::refresh_remote_catalog(endpoint).await {
            SwarmLogger::warn(
                "partition",
                &format!(
                    "Failed to trigger catalog refresh on node '{}': {}",
                    node_name, e
                ),
            );
        }
    }

    Ok(())
}

/// Gather all data for a table from multiple shards via DoGet.
async fn gather_table_from_shards(
    table_name: &str,
    shard_endpoints: &[(String, String)],
) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    let sql = format!(
        "SELECT * FROM \"{}\"",
        table_name.replace('"', "\"\"")
    );

    let mut all_batches = Vec::new();
    let mut schema: Option<SchemaRef> = None;

    for (endpoint, node_name) in shard_endpoints {
        let (shard_schema, shard_batches) =
            flight_client::query_node_with_schema(endpoint, &sql).await.map_err(|e| {
                format!(
                    "Failed to gather data from node '{}' ({}): {}",
                    node_name, endpoint, e
                )
            })?;

        if schema.is_none() {
            schema = Some(shard_schema);
        }

        all_batches.extend(shard_batches);
    }

    let schema = schema.unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));

    Ok((schema, all_batches))
}

/// Extract table name from a CREATE TABLE statement.
fn extract_table_name(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let table_pos = upper.find("TABLE")?;
    let after_table = &sql[table_pos + 5..].trim_start();

    let after_clause = if after_table.to_uppercase().starts_with("IF NOT EXISTS") {
        after_table[13..].trim_start()
    } else if after_table.to_uppercase().starts_with("IF EXISTS") {
        after_table[9..].trim_start()
    } else {
        after_table
    };

    if after_clause.starts_with('"') {
        let end = after_clause[1..].find('"')?;
        Some(after_clause[1..1 + end].to_string())
    } else {
        let end = after_clause
            .find(|c: char| c.is_whitespace() || c == '(' || c == ';')
            .unwrap_or(after_clause.len());
        if end == 0 {
            None
        } else {
            Some(after_clause[..end].to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
        ]))
    }

    fn test_batch() -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
                Arc::new(Float64Array::from(vec![
                    10.0, 50.0, 150.0, 300.0, 600.0, 900.0,
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn generate_create_table_sql_basic() {
        let schema = test_schema();
        let sql = generate_create_table_sql("orders", &schema);
        assert!(sql.contains("CREATE OR REPLACE TABLE"));
        assert!(sql.contains("\"orders\""));
        assert!(sql.contains("\"id\" BIGINT"));
        assert!(sql.contains("\"name\" VARCHAR"));
        assert!(sql.contains("\"price\" DOUBLE"));
    }

    #[test]
    fn generate_create_table_sql_quoted_name() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let sql = generate_create_table_sql("my\"table", &schema);
        assert!(sql.contains("\"my\"\"table\""));
    }

    #[test]
    fn range_partition_basic() {
        let batch = test_batch();
        let ranges = vec![
            RangeBound {
                lower: None,
                upper: Some(serde_json::json!(100)),
            },
            RangeBound {
                lower: Some(serde_json::json!(100)),
                upper: Some(serde_json::json!(500)),
            },
            RangeBound {
                lower: Some(serde_json::json!(500)),
                upper: None,
            },
        ];

        let result = range_partition_batches(&[batch], "price", &ranges).unwrap();
        assert_eq!(result.len(), 3);

        let total: usize = result
            .iter()
            .flat_map(|batches| batches.iter())
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(total, 6);

        // price < 100: 10.0, 50.0 -> 2 rows
        let p0_rows: usize = result[0].iter().map(|b| b.num_rows()).sum();
        assert_eq!(p0_rows, 2);

        // 100 <= price < 500: 150.0, 300.0 -> 2 rows
        let p1_rows: usize = result[1].iter().map(|b| b.num_rows()).sum();
        assert_eq!(p1_rows, 2);

        // price >= 500: 600.0, 900.0 -> 2 rows
        let p2_rows: usize = result[2].iter().map(|b| b.num_rows()).sum();
        assert_eq!(p2_rows, 2);
    }

    #[test]
    fn range_partition_empty_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)]));
        let batch = RecordBatch::new_empty(schema);
        let ranges = vec![RangeBound {
            lower: None,
            upper: None,
        }];
        let result = range_partition_batches(&[batch], "x", &ranges).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].is_empty());
    }

    #[test]
    fn range_partition_missing_column() {
        let batch = test_batch();
        let ranges = vec![RangeBound {
            lower: None,
            upper: None,
        }];
        let result = range_partition_batches(&[batch], "missing", &ranges);
        assert!(result.is_err());
    }

    #[test]
    fn range_partition_empty_ranges_errors() {
        let batch = test_batch();
        let result = range_partition_batches(&[batch], "price", &[]);
        assert!(result.is_err());
    }

    #[test]
    fn assign_partitions_round_robin() {
        let nodes = vec![
            TargetNode {
                node_name: "node-a".to_string(),
                flight_endpoint: "http://a:8815".to_string(),
            },
            TargetNode {
                node_name: "node-b".to_string(),
                flight_endpoint: "http://b:8815".to_string(),
            },
        ];

        let assignments = assign_partitions(4, &nodes, None).unwrap();
        assert_eq!(assignments.len(), 4);
        assert_eq!(assignments[0].node_name, "node-a");
        assert_eq!(assignments[1].node_name, "node-b");
        assert_eq!(assignments[2].node_name, "node-a");
        assert_eq!(assignments[3].node_name, "node-b");
    }

    #[test]
    fn assign_partitions_explicit() {
        let nodes = vec![
            TargetNode {
                node_name: "node-a".to_string(),
                flight_endpoint: "http://a:8815".to_string(),
            },
            TargetNode {
                node_name: "node-b".to_string(),
                flight_endpoint: "http://b:8815".to_string(),
            },
            TargetNode {
                node_name: "node-c".to_string(),
                flight_endpoint: "http://c:8815".to_string(),
            },
        ];

        let explicit = vec!["node-b".to_string(), "node-c".to_string()];
        let assignments = assign_partitions(3, &nodes, Some(&explicit)).unwrap();
        assert_eq!(assignments.len(), 3);
        assert_eq!(assignments[0].node_name, "node-b");
        assert_eq!(assignments[1].node_name, "node-c");
        assert_eq!(assignments[2].node_name, "node-b");
    }

    #[test]
    fn assign_partitions_empty_nodes_errors() {
        let result = assign_partitions(2, &[], None);
        assert!(result.is_err());
    }

    #[test]
    fn assign_partitions_unknown_explicit_node_errors() {
        let nodes = vec![TargetNode {
            node_name: "node-a".to_string(),
            flight_endpoint: "http://a:8815".to_string(),
        }];

        let explicit = vec!["node-z".to_string()];
        let result = assign_partitions(1, &nodes, Some(&explicit));
        assert!(result.is_err());
    }

    #[test]
    fn extract_table_name_simple() {
        assert_eq!(
            extract_table_name("CREATE TABLE orders (id INT)"),
            Some("orders".to_string())
        );
    }

    #[test]
    fn extract_table_name_quoted() {
        assert_eq!(
            extract_table_name("CREATE TABLE \"my_orders\" (id INT)"),
            Some("my_orders".to_string())
        );
    }

    #[test]
    fn extract_table_name_if_not_exists() {
        assert_eq!(
            extract_table_name("CREATE TABLE IF NOT EXISTS orders (id INT)"),
            Some("orders".to_string())
        );
    }

    #[test]
    fn extract_table_name_as_select() {
        assert_eq!(
            extract_table_name("CREATE TABLE orders AS SELECT * FROM raw"),
            Some("orders".to_string())
        );
    }

    #[test]
    fn partition_config_deserialize_hash() {
        let json = r#"{"strategy":"hash","column":"id","partitions":3}"#;
        let config: PartitionConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.strategy, "hash");
        assert_eq!(config.column, "id");
        assert_eq!(config.partitions, Some(3));
    }

    #[test]
    fn partition_config_deserialize_range() {
        let json = r#"{"strategy":"range","column":"price","ranges":[{"upper":100},{"lower":100}]}"#;
        let config: PartitionConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.strategy, "range");
        assert_eq!(config.column, "price");
        assert_eq!(config.ranges.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn partition_metadata_roundtrip() {
        let meta = PartitionMetadata {
            strategy: PartitionStrategy::Hash {
                column: "id".to_string(),
                num_partitions: 2,
            },
            assignments: vec![
                PartitionAssignment {
                    partition_id: 0,
                    node_name: "node-a".to_string(),
                    flight_endpoint: "http://a:8815".to_string(),
                },
                PartitionAssignment {
                    partition_id: 1,
                    node_name: "node-b".to_string(),
                    flight_endpoint: "http://b:8815".to_string(),
                },
            ],
            create_sql: "CREATE TABLE orders (id INT)".to_string(),
        };

        let json = serde_json::to_string(&meta).unwrap();
        let restored: PartitionMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.assignments.len(), 2);
        assert_eq!(restored.create_sql, "CREATE TABLE orders (id INT)");
    }

    #[test]
    fn arrow_type_to_sql_coverage() {
        assert_eq!(arrow_type_to_sql(&DataType::Boolean), "BOOLEAN");
        assert_eq!(arrow_type_to_sql(&DataType::Int32), "INTEGER");
        assert_eq!(arrow_type_to_sql(&DataType::Int64), "BIGINT");
        assert_eq!(arrow_type_to_sql(&DataType::Float64), "DOUBLE");
        assert_eq!(arrow_type_to_sql(&DataType::Utf8), "VARCHAR");
        assert_eq!(arrow_type_to_sql(&DataType::Date32), "DATE");
        assert_eq!(
            arrow_type_to_sql(&DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                None
            )),
            "TIMESTAMP"
        );
    }
}
