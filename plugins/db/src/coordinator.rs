//! Distributed query coordinator: resolves nodes, fans out via Flight,
//! collects partial results, and merges (with aggregation decomposition).

use std::sync::Arc;
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use uuid::Uuid;

use crate::aggregation::{self, DecomposedQuery};
use crate::catalog;
use crate::flight_client;
use crate::logging::{LogLevel, SwarmLogger};

pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// Execute a SQL query across the cluster. Creates an internal tokio runtime
/// for the async fan-out phase to avoid nested `block_on` calls.
pub fn execute_distributed_query(
    sql: &str,
    partial_results: bool,
) -> Result<QueryResult, String> {
    let query_id = Uuid::new_v4();
    let start = Instant::now();

    SwarmLogger::log_with_context(
        LogLevel::Info,
        "coordinator",
        &[("query_id", &query_id.to_string())],
        &format!("Received query: {sql}"),
    );

    let table_name = match extract_table_name(sql) {
        Ok(name) => name,
        Err(e) if e.contains("No table found") => {
            SwarmLogger::log_with_context(
                LogLevel::Debug,
                "coordinator",
                &[("query_id", &query_id.to_string())],
                "No table in query, executing locally",
            );
            return execute_local_query(sql);
        }
        Err(e) => return Err(e),
    };

    SwarmLogger::log_with_context(
        LogLevel::Debug,
        "coordinator",
        &[("query_id", &query_id.to_string())],
        &format!("Extracted table name: {table_name}"),
    );

    let catalog_entries = catalog::resolve_table(&table_name)?;

    if catalog_entries.is_empty() {
        return Err(format!(
            "No data nodes found for table '{table_name}'"
        ));
    }

    let target_nodes: Vec<String> = catalog_entries
        .iter()
        .filter_map(|e| e.flight_endpoint.clone())
        .collect();

    if target_nodes.is_empty() {
        return Err(format!(
            "No Flight endpoints available for table '{table_name}' (found {} node(s) but none have Flight running)",
            catalog_entries.len(),
        ));
    }

    let node_list = target_nodes.join(", ");
    SwarmLogger::log_with_context(
        LogLevel::Info,
        "coordinator",
        &[("query_id", &query_id.to_string())],
        &format!(
            "Resolved {} target node(s) for table '{}': [{}]",
            target_nodes.len(),
            table_name,
            node_list,
        ),
    );

    let decomposed = aggregation::decompose_query(sql)?;

    SwarmLogger::log_with_context(
        LogLevel::Debug,
        "coordinator",
        &[("query_id", &query_id.to_string())],
        &format!(
            "Decomposed query: has_aggregations={}, node_sql=\"{}\", merge_sql=\"{}\"",
            decomposed.has_aggregations, decomposed.node_sql, decomposed.merge_sql,
        ),
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create fan-out runtime: {e}"))?;

    let fan_out_start = Instant::now();
    let (all_node_batches, errors) = rt.block_on(async {
        let mut handles = Vec::with_capacity(target_nodes.len());

        for endpoint in &target_nodes {
            let ep = endpoint.clone();
            let node_sql = decomposed.node_sql.clone();
            let qid = query_id.to_string();

            handles.push(tokio::spawn(async move {
                let node_start = Instant::now();
                let result = flight_client::query_node(&ep, &node_sql).await;
                let elapsed_ms = node_start.elapsed().as_millis();

                match &result {
                    Ok(batches) => {
                        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                        SwarmLogger::log_with_context(
                            LogLevel::Debug,
                            "coordinator",
                            &[("query_id", &qid), ("node", &ep)],
                            &format!(
                                "Node returned {} batch(es), {} row(s) in {}ms",
                                batches.len(),
                                total_rows,
                                elapsed_ms,
                            ),
                        );
                    }
                    Err(e) => {
                        SwarmLogger::log_with_context(
                            LogLevel::Error,
                            "coordinator",
                            &[("query_id", &qid), ("node", &ep)],
                            &format!("Node query failed after {}ms: {e}", elapsed_ms),
                        );
                    }
                }

                (ep, result)
            }));
        }

        let mut all_node_batches: Vec<Vec<RecordBatch>> = Vec::with_capacity(target_nodes.len());
        let mut errors: Vec<String> = Vec::new();

        for handle in handles {
            match handle.await {
                Ok((endpoint, result)) => match result {
                    Ok(batches) => {
                        all_node_batches.push(batches);
                    }
                    Err(e) => {
                        let msg = format!("Node {endpoint} failed: {e}");
                        if partial_results {
                            SwarmLogger::log_with_context(
                                LogLevel::Warn,
                                "coordinator",
                                &[("query_id", &query_id.to_string())],
                                &format!(
                                    "Partial results mode: ignoring failure from {endpoint}: {e}"
                                ),
                            );
                        } else {
                            SwarmLogger::log_with_context(
                                LogLevel::Error,
                                "coordinator",
                                &[("query_id", &query_id.to_string())],
                                &msg,
                            );
                            errors.push(msg);
                        }
                    }
                },
                Err(e) => {
                    errors.push(format!("Task join error: {e}"));
                }
            }
        }

        (all_node_batches, errors)
    });

    let fan_out_ms = fan_out_start.elapsed().as_millis();

    if !errors.is_empty() {
        return Err(format!(
            "Distributed query failed on {} node(s): {}",
            errors.len(),
            errors.join("; "),
        ));
    }

    if all_node_batches.is_empty()
        || all_node_batches.iter().all(|nb| nb.is_empty())
    {
        SwarmLogger::log_with_context(
            LogLevel::Info,
            "coordinator",
            &[("query_id", &query_id.to_string())],
            &format!(
                "Query returned no results from any node (fan-out took {}ms)",
                fan_out_ms,
            ),
        );

        // Try to preserve schema from any batch (even zero-row ones)
        let schema = all_node_batches
            .iter()
            .flat_map(|nb| nb.iter())
            .find(|b| b.num_columns() > 0)
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
        return Ok(QueryResult {
            schema,
            batches: vec![],
        });
    }

    let merge_start = Instant::now();
    let result = merge_batches(all_node_batches, &decomposed)?;
    let merge_ms = merge_start.elapsed().as_millis();

    let total_rows: usize = result.batches.iter().map(|b| b.num_rows()).sum();
    let total_ms = start.elapsed().as_millis();

    SwarmLogger::log_with_context(
        LogLevel::Info,
        "coordinator",
        &[("query_id", &query_id.to_string())],
        &format!(
            "Query complete: {} row(s), fan-out={}ms, merge={}ms, total={}ms",
            total_rows, fan_out_ms, merge_ms, total_ms,
        ),
    );

    Ok(result)
}

/// Execute locally for queries without a FROM clause.
fn execute_local_query(sql: &str) -> Result<QueryResult, String> {
    let conn_arc = crate::get_shared_connection().ok_or_else(|| {
        "Shared DuckDB connection not available (extension not initialised?)".to_string()
    })?;
    let conn = conn_arc
        .lock()
        .map_err(|e| format!("Failed to lock shared connection: {e}"))?;

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("Failed to prepare local query: {e}"))?;

    let batches: Vec<RecordBatch> = stmt
        .query_arrow([])
        .map_err(|e| format!("Failed to execute local query: {e}"))?
        .collect();

    let schema = if let Some(first) = batches.first() {
        first.schema()
    } else {
        Arc::new(arrow::datatypes::Schema::empty())
    };

    Ok(QueryResult { schema, batches })
}

/// Extract the first table name from the FROM clause of a SQL SELECT.
pub fn extract_table_name(sql: &str) -> Result<String, String> {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| format!("SQL parse error: {e}"))?;

    if statements.is_empty() {
        return Err("Empty SQL statement".to_string());
    }

    let stmt = &statements[0];

    match stmt {
        Statement::Query(query) => extract_table_from_query(query),
        _ => Err("Only SELECT queries are supported for distributed execution".to_string()),
    }
}

fn extract_table_from_query(query: &sqlparser::ast::Query) -> Result<String, String> {
    use sqlparser::ast::SetExpr;

    match query.body.as_ref() {
        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                if let Some(name) = extract_table_from_factor(&table_with_joins.relation) {
                    return Ok(name);
                }
                for join in &table_with_joins.joins {
                    if let Some(name) = extract_table_from_factor(&join.relation) {
                        return Ok(name);
                    }
                }
            }
            Err("No table found in FROM clause".to_string())
        }
        SetExpr::Query(inner) => extract_table_from_query(inner),
        SetExpr::SetOperation { left, .. } => {
            if let SetExpr::Select(select) = left.as_ref() {
                for table_with_joins in &select.from {
                    if let Some(name) =
                        extract_table_from_factor(&table_with_joins.relation)
                    {
                        return Ok(name);
                    }
                }
            }
            Err("No table found in FROM clause".to_string())
        }
        _ => Err("Unsupported query form for distributed execution".to_string()),
    }
}

fn extract_table_from_factor(factor: &sqlparser::ast::TableFactor) -> Option<String> {
    use sqlparser::ast::TableFactor;

    match factor {
        TableFactor::Table { name, .. } => {
            // Last ident, ignoring schema qualifiers.
            name.0.last().map(|ident| ident.value.clone())
        }
        TableFactor::Derived { subquery, .. } => {
            extract_table_from_query(subquery).ok()
        }
        TableFactor::NestedJoin { table_with_joins, .. } => {
            extract_table_from_factor(&table_with_joins.relation)
        }
        _ => None,
    }
}

/// Merge per-node batches: concatenate for non-aggregates, or load into
/// DuckDB and run merge SQL for aggregates.
pub fn merge_batches(
    node_batches: Vec<Vec<RecordBatch>>,
    decomposed: &DecomposedQuery,
) -> Result<QueryResult, String> {
    let all_batches: Vec<RecordBatch> = node_batches
        .into_iter()
        .flat_map(|nb| nb.into_iter())
        .collect();

    if all_batches.is_empty() {
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        return Ok(QueryResult {
            schema,
            batches: vec![],
        });
    }

    let schema = all_batches[0].schema();

    if !decomposed.has_aggregations {
        return Ok(QueryResult {
            schema,
            batches: all_batches,
        });
    }

    merge_with_duckdb(&schema, all_batches, &decomposed.merge_sql)
}

/// Load batches into DuckDB via arrow(?,?) and run merge SQL.
/// Replaces `_merged` with an inline arrow subquery (DuckDB doesn't allow
/// prepared params in DDL).
fn merge_with_duckdb(
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    merge_sql: &str,
) -> Result<QueryResult, String> {
    use duckdb::vtab::arrow::{arrow_recordbatch_to_query_params, ArrowVTab};

    let merged_batch = concat_batches(schema, &batches)
        .map_err(|e| format!("Failed to concatenate record batches: {e}"))?;

    let conn_arc = crate::get_shared_connection().ok_or_else(|| {
        "Shared DuckDB connection not available (extension not initialised?)".to_string()
    })?;

    let conn = conn_arc
        .lock()
        .map_err(|e| format!("Failed to lock shared connection: {e}"))?;

    let _ = conn.register_table_function::<ArrowVTab>("arrow");

    let rewritten_sql = merge_sql.replace(
        "FROM _merged",
        "FROM (SELECT * FROM arrow(?, ?)) AS _merged",
    );

    let params = arrow_recordbatch_to_query_params(merged_batch);

    let mut stmt = conn
        .prepare(&rewritten_sql)
        .map_err(|e| format!("Failed to prepare merge SQL: {e}"))?;

    let result_batches: Vec<RecordBatch> = stmt
        .query_arrow(params)
        .map_err(|e| format!("Failed to execute merge SQL: {e}"))?
        .collect();

    let result_schema = if let Some(first) = result_batches.first() {
        first.schema()
    } else {
        Arc::new(arrow::datatypes::Schema::empty())
    };

    Ok(QueryResult {
        schema: result_schema,
        batches: result_batches,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_simple_table() {
        let result = extract_table_name("SELECT * FROM orders").unwrap();
        assert_eq!(result, "orders");
    }

    #[test]
    fn extract_table_with_schema() {
        let result = extract_table_name("SELECT * FROM public.orders WHERE id > 5").unwrap();
        assert_eq!(result, "orders");
    }

    #[test]
    fn extract_table_with_alias() {
        let result = extract_table_name("SELECT o.id FROM orders o").unwrap();
        assert_eq!(result, "orders");
    }

    #[test]
    fn extract_table_with_join() {
        let result =
            extract_table_name("SELECT * FROM orders o JOIN users u ON o.user_id = u.id")
                .unwrap();
        assert_eq!(result, "orders");
    }

    #[test]
    fn extract_table_case_insensitive_from() {
        let result = extract_table_name("select count(*) from orders").unwrap();
        assert_eq!(result, "orders");
    }

    #[test]
    fn extract_table_no_from_clause() {
        let result = extract_table_name("SELECT 1 + 2");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No table found"));
    }

    #[test]
    fn extract_table_non_select() {
        let result = extract_table_name("INSERT INTO orders VALUES (1)");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Only SELECT queries are supported"));
    }

    #[test]
    fn extract_table_invalid_sql() {
        let result = extract_table_name("NOT VALID SQL !!!");
        assert!(result.is_err());
    }

    #[test]
    fn extract_table_subquery() {
        let result =
            extract_table_name("SELECT * FROM (SELECT id FROM orders) AS sub").unwrap();
        assert_eq!(result, "orders");
    }

    #[test]
    fn extract_table_with_where_and_group_by() {
        let result = extract_table_name(
            "SELECT region, SUM(price) FROM orders WHERE active = true GROUP BY region",
        )
        .unwrap();
        assert_eq!(result, "orders");
    }

    #[test]
    fn merge_empty_batches_no_agg() {
        let decomposed = DecomposedQuery {
            node_sql: "SELECT * FROM t".to_string(),
            merge_sql: "SELECT * FROM _merged".to_string(),
            has_aggregations: false,
        };
        let result = merge_batches(vec![], &decomposed).unwrap();
        assert!(result.batches.is_empty());
    }

    #[test]
    fn merge_concatenates_batches_no_agg() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![4, 5]))],
        )
        .unwrap();

        let decomposed = DecomposedQuery {
            node_sql: "SELECT a FROM t".to_string(),
            merge_sql: "SELECT * FROM _merged".to_string(),
            has_aggregations: false,
        };

        let result = merge_batches(vec![vec![batch1], vec![batch2]], &decomposed).unwrap();
        assert_eq!(result.batches.len(), 2);
        let total_rows: usize = result.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }
}
