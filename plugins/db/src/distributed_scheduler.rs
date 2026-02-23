//! Distributed scheduler lifecycle: singleton management, query submission,
//! and co-location detection for distributed query execution.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::prelude::SessionContext;

use crate::catalog;
use crate::logging::SwarmLogger;

pub struct SchedulerConfig {
    pub bind_addr: String,
}

struct SchedulerHandle {
    runtime: tokio::runtime::Runtime,
    bind_addr: String,
    ctx: Arc<tokio::sync::RwLock<SessionContext>>,
    active_queries: Arc<AtomicUsize>,
}

/// RAII guard that decrements active_queries on drop.
struct QueryGuard(Arc<AtomicUsize>);
impl Drop for QueryGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, AtomicOrdering::SeqCst);
    }
}

static SCHEDULER: OnceLock<Mutex<Option<SchedulerHandle>>> = OnceLock::new();

fn scheduler_lock() -> &'static Mutex<Option<SchedulerHandle>> {
    SCHEDULER.get_or_init(|| Mutex::new(None))
}

pub fn start_scheduler(config: SchedulerConfig) -> Result<(), String> {
    let mut guard = scheduler_lock()
        .lock()
        .map_err(|_| "Scheduler lock poisoned".to_string())?;

    if guard.is_some() {
        return Err("Scheduler is already running".to_string());
    }

    // Create the Tokio runtime on a separate thread to avoid "Cannot start a
    // runtime from within a runtime" when called from a DuckDB scalar function
    // that has previously used block_on on the gossip runtime (leaving a
    // thread-local Tokio context).
    let runtime = std::thread::spawn(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
    })
    .join()
    .map_err(|_| "Runtime creation thread panicked".to_string())?
    .map_err(|e| format!("Failed to create scheduler runtime: {e}"))?;

    let rt_handle = runtime.handle().clone();

    // Fetch catalog classifications and CatalogStats.  GossipRegistry methods
    // use exec_on_runtime() internally (spawn + channel), so they are safe to
    // call from any context — no need for a dedicated thread.
    let classifications = catalog::classify_tables().unwrap_or_default();
    let catalog_stats = std::sync::Arc::new(
        crate::shuffle_optimizer::CatalogStats::from_catalog(rt_handle.clone()),
    );

    // Create the DataFusion session on a separate thread so we can use
    // block_on() without interfering with the current thread.
    let ctx = {
        let h = rt_handle.clone();
        std::thread::spawn(move || {
            h.block_on(async {
                if classifications.is_empty() {
                    crate::federation_executor::create_duckdb_session()
                        .await
                        .map_err(|e| format!("Failed to create local session: {e}"))
                } else {
                    crate::federation_executor::create_distributed_session_with_classifications(
                        h.clone(),
                        classifications,
                        catalog_stats,
                    )
                    .await
                    .map_err(|e| format!("Failed to create distributed session: {e}"))
                }
            })
        })
        .join()
        .map_err(|_| "Scheduler initialization thread panicked".to_string())?
    }?;

    SwarmLogger::info(
        "scheduler",
        &format!("Scheduler started on {}", config.bind_addr),
    );

    *guard = Some(SchedulerHandle {
        runtime,
        bind_addr: config.bind_addr,
        ctx: Arc::new(tokio::sync::RwLock::new(ctx)),
        active_queries: Arc::new(AtomicUsize::new(0)),
    });

    Ok(())
}

pub fn stop_scheduler() -> Result<(), String> {
    let active = {
        let guard = scheduler_lock()
            .lock()
            .map_err(|_| "Scheduler lock poisoned".to_string())?;
        let handle = guard
            .as_ref()
            .ok_or_else(|| "Scheduler is not running".to_string())?;
        Arc::clone(&handle.active_queries)
    };

    // Wait up to 5 seconds for active queries to drain.
    for _ in 0..50 {
        if active.load(AtomicOrdering::SeqCst) == 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    let mut guard = scheduler_lock()
        .lock()
        .map_err(|_| "Scheduler lock poisoned".to_string())?;

    let handle = guard
        .take()
        .ok_or_else(|| "Scheduler is not running".to_string())?;

    SwarmLogger::info(
        "scheduler",
        &format!("Scheduler stopped (was on {})", handle.bind_addr),
    );

    Ok(())
}

/// Rebuild session context from catalog to pick up cluster topology changes.
pub fn refresh_session() -> Result<(), String> {
    let (rt_handle, ctx_lock) = {
        let guard = scheduler_lock()
            .lock()
            .map_err(|_| "Scheduler lock poisoned".to_string())?;
        let handle = guard
            .as_ref()
            .ok_or_else(|| "Scheduler is not running".to_string())?;
        (handle.runtime.handle().clone(), Arc::clone(&handle.ctx))
    };

    // Fetch classifications and CatalogStats.  GossipRegistry methods are
    // safe to call from any context (no runtime nesting).
    let classifications = catalog::classify_tables().unwrap_or_default();
    let catalog_stats = std::sync::Arc::new(
        crate::shuffle_optimizer::CatalogStats::from_catalog(rt_handle.clone()),
    );

    // Rebuild session on a separate thread for block_on().
    let new_ctx = {
        let h = rt_handle.clone();
        std::thread::spawn(move || {
            h.block_on(async {
                if classifications.is_empty() {
                    crate::federation_executor::create_duckdb_session()
                        .await
                        .map_err(|e| format!("Failed to rebuild local session: {e}"))
                } else {
                    crate::federation_executor::create_distributed_session_with_classifications(
                        h.clone(),
                        classifications,
                        catalog_stats,
                    )
                    .await
                    .map_err(|e| format!("Failed to rebuild distributed session: {e}"))
                }
            })
        })
        .join()
        .map_err(|_| "Session refresh thread panicked".to_string())?
    }?;

    {
        let h = rt_handle.clone();
        std::thread::spawn(move || {
            h.block_on(async {
                let mut ctx_write = ctx_lock.write().await;
                *ctx_write = new_ctx;
            })
        })
        .join()
        .map_err(|_| "Session refresh write thread panicked".to_string())?;
    }

    crate::logging::SwarmLogger::info(
        "scheduler",
        "Session refreshed with updated catalog",
    );

    Ok(())
}

pub fn is_scheduler_running() -> bool {
    scheduler_lock()
        .lock()
        .map(|guard| guard.is_some())
        .unwrap_or(false)
}

pub fn submit_query(sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    // Release the lock before block_on to avoid holding it across await points.
    let (rt_handle, ctx, active) = {
        let guard = scheduler_lock()
            .lock()
            .map_err(|_| "Scheduler lock poisoned".to_string())?;
        let handle = guard
            .as_ref()
            .ok_or_else(|| "Scheduler is not running".to_string())?;
        (handle.runtime.handle().clone(), Arc::clone(&handle.ctx), Arc::clone(&handle.active_queries))
    };

    active.fetch_add(1, AtomicOrdering::SeqCst);
    let _guard = QueryGuard(active);

    let sql = sql.to_string();
    // Run block_on in a separate thread to avoid nested-runtime panic when
    // called from a DuckDB function that is inside a tokio context.
    let (schema, batches) = std::thread::spawn(move || {
        rt_handle.block_on(async {
            let ctx_read = ctx.read().await;
            let df = ctx_read
                .sql(&sql)
                .await
                .map_err(|e| format!("Distributed SQL planning failed: {e}"))?;
            // Capture schema from the DataFrame before collect() so we have
            // column metadata even when the result set is empty.
            let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
            let batches = df.collect()
                .await
                .map_err(|e| format!("Distributed query execution failed: {e}"))?;
            Ok::<_, String>((schema, batches))
        })
    })
    .join()
    .map_err(|_| "Query execution thread panicked".to_string())??;

    Ok((schema, batches))
}

/// Returns `Some(flight_endpoint)` if all tables are co-located, `None` if distributed.
pub fn check_colocation(table_names: &[String]) -> Result<Option<String>, String> {
    if table_names.is_empty() {
        return Ok(None);
    }

    let all_entries = catalog::get_all_tables()?;

    let mut table_nodes: HashMap<&str, Vec<(&str, Option<&str>)>> = HashMap::new();
    for entry in &all_entries {
        table_nodes
            .entry(&entry.table_name)
            .or_default()
            .push((&entry.node_id, entry.flight_endpoint.as_deref()));
    }

    for name in table_names {
        if !table_nodes.contains_key(name.as_str()) {
            SwarmLogger::debug(
                "scheduler",
                &format!("Co-location check: table '{}' not found in catalog", name),
            );
            return Ok(None);
        }
    }

    let mut candidate_nodes: Option<HashMap<&str, Option<&str>>> = None;

    for name in table_names {
        let nodes_for_table: HashMap<&str, Option<&str>> = table_nodes
            .get(name.as_str())
            .unwrap() // safe: existence checked above
            .iter()
            .map(|&(node_id, endpoint)| (node_id, endpoint))
            .collect();

        candidate_nodes = Some(match candidate_nodes {
            None => nodes_for_table,
            Some(prev) => prev
                .into_iter()
                .filter(|(node_id, _)| nodes_for_table.contains_key(node_id))
                .collect(),
        });
    }

    if let Some(candidates) = candidate_nodes {
        for (_node_id, endpoint) in &candidates {
            if let Some(ep) = endpoint {
                SwarmLogger::debug(
                    "scheduler",
                    &format!(
                        "Co-location check: all {} table(s) co-located at {}",
                        table_names.len(),
                        ep,
                    ),
                );
                return Ok(Some(ep.to_string()));
            }
        }
    }

    SwarmLogger::debug(
        "scheduler",
        &format!(
            "Co-location check: {} table(s) require distributed execution",
            table_names.len(),
        ),
    );

    Ok(None)
}

/// Extract table names from FROM/JOIN clauses. Returns empty vec on parse failure.
pub fn extract_table_names_from_sql(sql: &str) -> Vec<String> {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let statements = match Parser::parse_sql(&dialect, sql) {
        Ok(stmts) => stmts,
        Err(_) => return Vec::new(),
    };

    let mut names = Vec::new();

    for stmt in &statements {
        if let Statement::Query(query) = stmt {
            collect_table_names_from_set_expr(query.body.as_ref(), &mut names);
        }
    }

    names.sort();
    names.dedup();
    names
}

fn collect_table_names_from_set_expr(
    set_expr: &sqlparser::ast::SetExpr,
    names: &mut Vec<String>,
) {
    use sqlparser::ast::SetExpr;

    match set_expr {
        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                collect_table_names_from_table_factor(&table_with_joins.relation, names);
                for join in &table_with_joins.joins {
                    collect_table_names_from_table_factor(&join.relation, names);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_table_names_from_set_expr(left, names);
            collect_table_names_from_set_expr(right, names);
        }
        SetExpr::Query(query) => {
            collect_table_names_from_set_expr(query.body.as_ref(), names);
        }
        _ => {}
    }
}

fn collect_table_names_from_table_factor(
    factor: &sqlparser::ast::TableFactor,
    names: &mut Vec<String>,
) {
    use sqlparser::ast::TableFactor;

    match factor {
        TableFactor::Table { name, .. } => {
            // Use last part of qualified name (e.g. "schema.table" -> "table").
            let table_name = name
                .0
                .last()
                .map(|ident| ident.value.clone())
                .unwrap_or_default();
            if !table_name.is_empty() {
                names.push(table_name);
            }
        }
        TableFactor::Derived { subquery, .. } => {
            collect_table_names_from_set_expr(subquery.body.as_ref(), names);
        }
        TableFactor::NestedJoin { table_with_joins, .. } => {
            collect_table_names_from_table_factor(&table_with_joins.relation, names);
            for join in &table_with_joins.joins {
                collect_table_names_from_table_factor(&join.relation, names);
            }
        }
        _ => {}
    }
}

pub fn check_colocation_for_sql(sql: &str) -> Result<Option<String>, String> {
    let table_names = extract_table_names_from_sql(sql);
    check_colocation(&table_names)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_not_running_initially() {
        assert!(!is_scheduler_running());
    }

    #[test]
    fn submit_query_without_scheduler_returns_error() {
        let result = submit_query("SELECT 1");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not running"));
    }

    #[test]
    fn check_colocation_empty_tables() {
        let result = check_colocation(&[]);
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn stop_scheduler_when_not_running_returns_error() {
        let result = stop_scheduler();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not running"));
    }

    #[test]
    fn extract_tables_simple_select() {
        let tables = extract_table_names_from_sql("SELECT * FROM orders");
        assert_eq!(tables, vec!["orders"]);
    }

    #[test]
    fn extract_tables_join() {
        let tables = extract_table_names_from_sql(
            "SELECT * FROM orders o JOIN customers c ON o.id = c.id",
        );
        assert!(tables.contains(&"orders".to_string()), "Should contain 'orders': {:?}", tables);
        assert!(tables.contains(&"customers".to_string()), "Should contain 'customers': {:?}", tables);
    }

    #[test]
    fn extract_tables_empty_on_error() {
        let tables = extract_table_names_from_sql("NOT VALID SQL !!!@#$");
        assert!(tables.is_empty(), "Invalid SQL should return empty vec: {:?}", tables);
    }
}
