//! DataFusion `PhysicalOptimizerRule` that detects cross-node joins and inserts
//! shuffle boundaries (ShuffleWriterExec / ShuffleReaderExec) into the plan.
//!
//! Runs AFTER `FederationOptimizerRule` which pushes co-located scans to
//! executors. This rule handles the remaining cross-context joins.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::error::Result as DFResult;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::catalog;
use crate::logging::SwarmLogger;
use crate::shuffle_descriptor::{ShuffleDescriptor, ShuffleTarget};
use crate::shuffle_partition;
use crate::shuffle_registry;
use crate::shuffle_writer::ShuffleWriterExec;

/// Default broadcast threshold: tables with fewer rows than this are broadcast
/// to the other side's node rather than hash-shuffled.
const DEFAULT_BROADCAST_THRESHOLD: u64 = 100_000;

/// Join strategy chosen based on table statistics and node topology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinStrategy {
    /// Both tables on the same node — no shuffle needed.
    CoLocated,
    /// One table is small enough to broadcast to the other node.
    Broadcast { small_side: BroadcastSide },
    /// Both tables are large and on different nodes — hash-shuffle both.
    HashShuffle,
    /// No stats available — fall back to pull-to-coordinator (no change).
    PullToCoordinator,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BroadcastSide {
    Left,
    Right,
}

/// Catalog statistics used for join strategy decisions.
pub struct CatalogStats {
    /// table_name → (approx_rows, node endpoints).
    pub table_stats: HashMap<String, (u64, Vec<String>)>,
    /// Broadcast threshold (rows). Tables below this are broadcast.
    pub broadcast_threshold: u64,
    /// The local node's flight endpoint.
    pub local_endpoint: Option<String>,
    /// Tokio runtime handle for spawning shuffle tasks.
    pub runtime_handle: tokio::runtime::Handle,
}

impl std::fmt::Debug for CatalogStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogStats")
            .field("table_count", &self.table_stats.len())
            .field("broadcast_threshold", &self.broadcast_threshold)
            .finish()
    }
}

impl CatalogStats {
    /// Build from current gossip catalog state.
    pub fn from_catalog(runtime_handle: tokio::runtime::Handle) -> Self {
        let table_stats = Self::fetch_table_stats();
        let local_endpoint = Self::fetch_local_endpoint();
        let broadcast_threshold = std::env::var("SWARM_BROADCAST_THRESHOLD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_BROADCAST_THRESHOLD);

        Self {
            table_stats,
            broadcast_threshold,
            local_endpoint,
            runtime_handle,
        }
    }

    fn fetch_table_stats() -> HashMap<String, (u64, Vec<String>)> {
        let entries = catalog::get_all_tables().unwrap_or_default();
        let mut stats: HashMap<String, (u64, Vec<String>)> = HashMap::new();

        for entry in entries {
            let stat = stats
                .entry(entry.table_name.clone())
                .or_insert((0, Vec::new()));
            stat.0 += entry.approx_rows;
            if let Some(ep) = entry.flight_endpoint {
                if !stat.1.contains(&ep) {
                    stat.1.push(ep);
                }
            }
        }

        stats
    }

    fn fetch_local_endpoint() -> Option<String> {
        let self_id = catalog::get_self_node_id()?;
        let entries = catalog::get_all_tables().ok()?;
        entries
            .iter()
            .find(|e| e.node_id == self_id)
            .and_then(|e| e.flight_endpoint.clone())
    }
}

/// Optimizer rule that inserts shuffle boundaries for cross-node joins.
#[derive(Debug)]
pub struct ShuffleInsertionRule {
    catalog_stats: Arc<CatalogStats>,
}

impl ShuffleInsertionRule {
    pub fn new(catalog_stats: Arc<CatalogStats>) -> Self {
        Self { catalog_stats }
    }

    /// Determine join strategy based on row counts and node topology.
    fn choose_strategy(
        &self,
        left_tables: &[String],
        right_tables: &[String],
    ) -> JoinStrategy {
        let left_rows: Option<u64> = left_tables
            .iter()
            .filter_map(|t| self.catalog_stats.table_stats.get(t).map(|(r, _)| *r))
            .reduce(|a, b| a + b);

        let right_rows: Option<u64> = right_tables
            .iter()
            .filter_map(|t| self.catalog_stats.table_stats.get(t).map(|(r, _)| *r))
            .reduce(|a, b| a + b);

        let left_endpoints: Vec<String> = left_tables
            .iter()
            .filter_map(|t| self.catalog_stats.table_stats.get(t))
            .flat_map(|(_, eps)| eps.clone())
            .collect();

        let right_endpoints: Vec<String> = right_tables
            .iter()
            .filter_map(|t| self.catalog_stats.table_stats.get(t))
            .flat_map(|(_, eps)| eps.clone())
            .collect();

        // Check co-location: if any endpoint is shared, tables are co-located.
        let co_located = left_endpoints
            .iter()
            .any(|ep| right_endpoints.contains(ep));
        if co_located {
            return JoinStrategy::CoLocated;
        }

        match (left_rows, right_rows) {
            (Some(lr), Some(rr)) => {
                if lr <= self.catalog_stats.broadcast_threshold {
                    JoinStrategy::Broadcast {
                        small_side: BroadcastSide::Left,
                    }
                } else if rr <= self.catalog_stats.broadcast_threshold {
                    JoinStrategy::Broadcast {
                        small_side: BroadcastSide::Right,
                    }
                } else {
                    JoinStrategy::HashShuffle
                }
            }
            _ => JoinStrategy::PullToCoordinator,
        }
    }

    /// Recursively walk the plan and insert shuffle boundaries at HashJoinExec nodes.
    fn optimize_node(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // First, recurse into children.
        let children: Vec<Arc<dyn ExecutionPlan>> = plan
            .children()
            .iter()
            .map(|child| self.optimize_node(Arc::clone(child)))
            .collect::<DFResult<_>>()?;

        let plan = if !children.is_empty() {
            plan.with_new_children(children)?
        } else {
            plan
        };

        let is_hash_shuffle = plan
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .map(|hash_join| {
                let left_tables = extract_table_names_from_plan(hash_join.left());
                let right_tables = extract_table_names_from_plan(hash_join.right());
                let strategy = self.choose_strategy(&left_tables, &right_tables);

                SwarmLogger::debug(
                    "shuffle-optimizer",
                    &format!(
                        "Join {:?} x {:?}: strategy={:?}",
                        left_tables, right_tables, strategy,
                    ),
                );

                strategy == JoinStrategy::HashShuffle
            })
            .unwrap_or(false);

        if is_hash_shuffle {
            return self.insert_shuffle(plan);
        }

        Ok(plan)
    }

    /// Insert ShuffleWriter/Reader around a HashJoinExec.
    fn insert_shuffle(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let hash_join = plan
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "insert_shuffle called on non-HashJoinExec".to_string(),
                )
            })?;

        let left = hash_join.left().clone();
        let right = hash_join.right().clone();
        let on = hash_join.on();

        // Extract join key column names from the PhysicalExpr (uses Display).
        let left_key_names: Vec<String> = on
            .iter()
            .map(|(l, _)| format!("{}", l))
            .collect();
        let right_key_names: Vec<String> = on
            .iter()
            .map(|(_, r)| format!("{}", r))
            .collect();

        let left_tables = extract_table_names_from_plan(&left);
        let right_tables = extract_table_names_from_plan(&right);
        let mut all_endpoints: Vec<String> = Vec::new();

        for table in left_tables.iter().chain(right_tables.iter()) {
            if let Some((_, eps)) = self.catalog_stats.table_stats.get(table) {
                for ep in eps {
                    if !all_endpoints.contains(ep) {
                        all_endpoints.push(ep.clone());
                    }
                }
            }
        }

        if all_endpoints.is_empty() {
            SwarmLogger::warn(
                "shuffle-optimizer",
                "No endpoints found for shuffle, falling back to pull-to-coordinator",
            );
            return Ok(plan);
        }

        let num_partitions = all_endpoints.len();
        let shuffle_id = uuid::Uuid::new_v4().to_string();

        let partition_targets: Vec<ShuffleTarget> = all_endpoints
            .iter()
            .enumerate()
            .map(|(i, ep)| ShuffleTarget {
                partition_id: i,
                flight_endpoint: ep.clone(),
                node_name: format!("node-{}", i),
            })
            .collect();

        let local_partition_id = self
            .catalog_stats
            .local_endpoint
            .as_ref()
            .and_then(|local_ep| all_endpoints.iter().position(|ep| ep == local_ep))
            .unwrap_or(0);

        let left_desc = ShuffleDescriptor {
            shuffle_id: format!("{}-left", shuffle_id),
            join_keys: left_key_names.clone(),
            num_partitions,
            partition_targets: partition_targets.clone(),
            target_table: None,
        };
        let left_key_indices =
            shuffle_partition::resolve_key_indices(&left.schema(), &left_key_names)?;

        shuffle_registry::register_shuffle(&left_desc.shuffle_id, num_partitions);

        let left_writer = Arc::new(ShuffleWriterExec::new(
            left,
            left_desc.clone(),
            left_key_indices,
            local_partition_id,
            self.catalog_stats.runtime_handle.clone(),
        ));

        let right_desc = ShuffleDescriptor {
            shuffle_id: format!("{}-right", shuffle_id),
            join_keys: right_key_names.clone(),
            num_partitions,
            partition_targets,
            target_table: None,
        };
        let right_key_indices =
            shuffle_partition::resolve_key_indices(&right.schema(), &right_key_names)?;

        shuffle_registry::register_shuffle(&right_desc.shuffle_id, num_partitions);

        let right_writer = Arc::new(ShuffleWriterExec::new(
            right,
            right_desc.clone(),
            right_key_indices,
            local_partition_id,
            self.catalog_stats.runtime_handle.clone(),
        ));

        // Reconstruct the HashJoinExec with shuffle writers as inputs.
        // The ShuffleWriterExec reads from its child, hash-partitions the data,
        // sends remote partitions via Flight, and yields the local partition.
        let rebuilt = plan.with_new_children(vec![
            left_writer as Arc<dyn ExecutionPlan>,
            right_writer as Arc<dyn ExecutionPlan>,
        ])?;

        SwarmLogger::info(
            "shuffle-optimizer",
            &format!(
                "Inserted hash shuffle for join: shuffle_id={}, partitions={}, local_partition={}",
                shuffle_id, num_partitions, local_partition_id,
            ),
        );

        Ok(rebuilt)
    }
}

impl PhysicalOptimizerRule for ShuffleInsertionRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.optimize_node(plan)
    }

    fn name(&self) -> &str {
        "shuffle_insertion_rule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Extract table names from an execution plan by looking for known plan types
/// (federation scan nodes, distributed exec, etc.).
fn extract_table_names_from_plan(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut names = Vec::new();
    collect_table_names(plan, &mut names);
    names.sort();
    names.dedup();
    names
}

fn collect_table_names(plan: &Arc<dyn ExecutionPlan>, names: &mut Vec<String>) {
    // Check for DistributedExec which carries a table_name.
    if let Some(dist) = plan
        .as_any()
        .downcast_ref::<crate::distributed_table_provider::DistributedExec>()
    {
        let debug = format!("{:?}", dist);
        // Extract table name from debug output (table_name field).
        if let Some(start) = debug.find("table_name: \"") {
            let rest = &debug[start + 13..];
            if let Some(end) = rest.find('"') {
                names.push(rest[..end].to_string());
            }
        }
    }

    for child in plan.children() {
        collect_table_names(child, names);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_stats() -> CatalogStats {
        CatalogStats {
            table_stats: HashMap::new(),
            broadcast_threshold: DEFAULT_BROADCAST_THRESHOLD,
            local_endpoint: None,
            runtime_handle: tokio::runtime::Runtime::new().unwrap().handle().clone(),
        }
    }

    fn stats_with_tables(
        tables: Vec<(&str, u64, Vec<&str>)>,
    ) -> CatalogStats {
        let mut table_stats = HashMap::new();
        for (name, rows, endpoints) in tables {
            table_stats.insert(
                name.to_string(),
                (rows, endpoints.iter().map(|e| e.to_string()).collect()),
            );
        }
        CatalogStats {
            table_stats,
            broadcast_threshold: DEFAULT_BROADCAST_THRESHOLD,
            local_endpoint: Some("http://10.0.0.1:8815".to_string()),
            runtime_handle: tokio::runtime::Runtime::new().unwrap().handle().clone(),
        }
    }

    #[test]
    fn co_located_tables() {
        let stats = stats_with_tables(vec![
            ("orders", 1_000_000, vec!["http://10.0.0.1:8815"]),
            ("customers", 500_000, vec!["http://10.0.0.1:8815"]),
        ]);
        let rule = ShuffleInsertionRule::new(Arc::new(stats));
        let strategy = rule.choose_strategy(
            &["orders".to_string()],
            &["customers".to_string()],
        );
        assert_eq!(strategy, JoinStrategy::CoLocated);
    }

    #[test]
    fn broadcast_small_left() {
        let stats = stats_with_tables(vec![
            ("dim_table", 50_000, vec!["http://10.0.0.1:8815"]),
            ("fact_table", 10_000_000, vec!["http://10.0.0.2:8815"]),
        ]);
        let rule = ShuffleInsertionRule::new(Arc::new(stats));
        let strategy = rule.choose_strategy(
            &["dim_table".to_string()],
            &["fact_table".to_string()],
        );
        assert_eq!(
            strategy,
            JoinStrategy::Broadcast {
                small_side: BroadcastSide::Left,
            }
        );
    }

    #[test]
    fn broadcast_small_right() {
        let stats = stats_with_tables(vec![
            ("fact_table", 10_000_000, vec!["http://10.0.0.1:8815"]),
            ("dim_table", 50_000, vec!["http://10.0.0.2:8815"]),
        ]);
        let rule = ShuffleInsertionRule::new(Arc::new(stats));
        let strategy = rule.choose_strategy(
            &["fact_table".to_string()],
            &["dim_table".to_string()],
        );
        assert_eq!(
            strategy,
            JoinStrategy::Broadcast {
                small_side: BroadcastSide::Right,
            }
        );
    }

    #[test]
    fn hash_shuffle_both_large() {
        let stats = stats_with_tables(vec![
            ("orders", 5_000_000, vec!["http://10.0.0.1:8815"]),
            ("customers", 2_000_000, vec!["http://10.0.0.2:8815"]),
        ]);
        let rule = ShuffleInsertionRule::new(Arc::new(stats));
        let strategy = rule.choose_strategy(
            &["orders".to_string()],
            &["customers".to_string()],
        );
        assert_eq!(strategy, JoinStrategy::HashShuffle);
    }

    #[test]
    fn pull_to_coordinator_no_stats() {
        let stats = empty_stats();
        let rule = ShuffleInsertionRule::new(Arc::new(stats));
        let strategy = rule.choose_strategy(
            &["orders".to_string()],
            &["customers".to_string()],
        );
        assert_eq!(strategy, JoinStrategy::PullToCoordinator);
    }

    #[test]
    fn rule_name() {
        let stats = empty_stats();
        let rule = ShuffleInsertionRule::new(Arc::new(stats));
        assert_eq!(rule.name(), "shuffle_insertion_rule");
    }

    #[test]
    fn rule_schema_check() {
        let stats = empty_stats();
        let rule = ShuffleInsertionRule::new(Arc::new(stats));
        assert!(rule.schema_check());
    }

    #[test]
    fn extract_table_names_from_empty_plan() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let empty = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(schema));
        let names = extract_table_names_from_plan(&(empty as Arc<dyn ExecutionPlan>));
        assert!(names.is_empty());
    }
}
