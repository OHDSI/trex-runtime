//! [`SQLExecutor`] implementation backed by the local trexsql instance for
//! datafusion-federation SQL pushdown.

use std::sync::Arc;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::sql::unparser::dialect::{Dialect, DuckDBDialect};
use datafusion_federation::sql::SQLExecutor;
use futures::stream;

/// Executes SQL on the local trexsql instance via the shared connection singleton.
pub struct DuckDBSQLExecutor;

#[async_trait]
impl SQLExecutor for DuckDBSQLExecutor {
    fn name(&self) -> &str {
        "duckdb"
    }

    fn compute_context(&self) -> Option<String> {
        None
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(DuckDBDialect::new())
    }

    fn execute(&self, sql: &str, schema: SchemaRef) -> DFResult<SendableRecordBatchStream> {
        let batches = execute_on_duckdb(sql)?;

        let stream = stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> DFResult<Vec<String>> {
        let batches = execute_on_duckdb("SHOW TABLES")?;

        let mut names = Vec::new();
        for batch in &batches {
            if batch.num_columns() == 0 {
                continue;
            }
            let col = batch.column(0);
            if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        names.push(arr.value(i).to_string());
                    }
                }
            } else {
                // Fallback: use display formatting for LargeStringArray, etc.
                for i in 0..col.len() {
                    if !col.is_null(i) {
                        if let Ok(s) = arrow::util::display::array_value_to_string(col, i) {
                            names.push(s);
                        }
                    }
                }
            }
        }
        Ok(names)
    }

    async fn get_table_schema(&self, table_name: &str) -> DFResult<SchemaRef> {
        let sql = format!("SELECT * FROM \"{}\" LIMIT 0", crate::catalog::escape_identifier(table_name));
        let batches = execute_on_duckdb(&sql)?;

        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));

        Ok(schema)
    }
}

fn execute_on_duckdb(sql: &str) -> DFResult<Vec<RecordBatch>> {
    let conn_arc = crate::get_shared_connection().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "Shared DuckDB connection not available".to_string(),
        )
    })?;

    let conn = conn_arc.lock().map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to lock shared connection: {e}"
        ))
    })?;

    let mut stmt = conn.prepare(sql).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!("Failed to prepare SQL: {e}"))
    })?;

    let batches: Vec<RecordBatch> = stmt
        .query_arrow([])
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("Failed to execute SQL: {e}"))
        })?
        .collect();

    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_federation::sql::SQLExecutor;

    #[test]
    fn test_executor_name() {
        let executor = DuckDBSQLExecutor;
        assert_eq!(executor.name(), "duckdb");
    }

    #[test]
    fn test_executor_compute_context() {
        let executor = DuckDBSQLExecutor;
        assert_eq!(executor.compute_context(), None);
    }

    #[test]
    fn test_executor_dialect_is_duckdb() {
        let executor = DuckDBSQLExecutor;
        let dialect = executor.dialect();
        let _dialect_ref: &dyn Dialect = dialect.as_ref();
    }

    #[test]
    fn test_execute_without_connection_returns_error() {
        let executor = DuckDBSQLExecutor;
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        let result = executor.execute("SELECT 1", schema);
        match result {
            Err(e) => {
                let err_msg = format!("{e}");
                assert!(
                    err_msg.contains("not available"),
                    "Error should contain 'not available', got: {err_msg}"
                );
            }
            Ok(_) => panic!("Expected error when no shared connection is available"),
        }
    }

    #[tokio::test]
    async fn test_table_names_without_connection() {
        let executor = DuckDBSQLExecutor;
        let result = executor.table_names().await;
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("not available"),
            "Error should contain 'not available', got: {err_msg}"
        );
    }
}
