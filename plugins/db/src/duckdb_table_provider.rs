//! DataFusion `TableProvider` backed by a trexsql connection. Resolves schema
//! via `PRAGMA table_info` and delegates execution through `datafusion-federation`.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_federation::sql::SQLFederationProvider;

use crate::duckdb_sql_executor::DuckDBSQLExecutor;

/// Maps a trexsql type string to Arrow `DataType`. Unknown types fall back to Utf8.
fn duckdb_type_to_arrow(type_str: &str) -> DataType {
    let upper = type_str.trim().to_uppercase();

    if upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") {
        return parse_decimal(&upper);
    }

    match upper.as_str() {
        "INTEGER" | "INT" | "INT4" | "SIGNED" => DataType::Int32,
        "BIGINT" | "INT8" | "LONG" => DataType::Int64,
        "SMALLINT" | "INT2" | "SHORT" => DataType::Int16,
        "TINYINT" | "INT1" => DataType::Int8,
        "UINTEGER" => DataType::UInt32,
        "UBIGINT" => DataType::UInt64,
        "USMALLINT" => DataType::UInt16,
        "UTINYINT" => DataType::UInt8,
        "HUGEINT" | "UHUGEINT" => DataType::Utf8,

        "FLOAT" | "FLOAT4" | "REAL" => DataType::Float32,
        "DOUBLE" | "FLOAT8" => DataType::Float64,

        "BOOLEAN" | "BOOL" | "LOGICAL" => DataType::Boolean,

        "VARCHAR" | "TEXT" | "STRING" | "CHAR" | "BPCHAR" => DataType::Utf8,
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => DataType::Binary,

        "DATE" => DataType::Date32,
        "TIME" | "TIME WITHOUT TIME ZONE" => DataType::Time64(TimeUnit::Microsecond),
        "TIMESTAMP" | "DATETIME" | "TIMESTAMP WITHOUT TIME ZONE" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }

        "INTERVAL" => DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),

        "UUID" => DataType::Utf8,

        _ => DataType::Utf8,
    }
}

fn parse_decimal(upper: &str) -> DataType {
    let default = DataType::Decimal128(38, 10);
    let start = match upper.find('(') {
        Some(i) => i + 1,
        None => return default,
    };
    let end = match upper.find(')') {
        Some(i) => i,
        None => return default,
    };
    let inner = &upper[start..end];
    let parts: Vec<&str> = inner.split(',').collect();
    if parts.len() != 2 {
        return default;
    }
    let precision: u8 = parts[0].trim().parse().unwrap_or(38);
    let scale: i8 = parts[1].trim().parse().unwrap_or(10);
    DataType::Decimal128(precision, scale)
}

/// trexsql may return notnull as BooleanArray or StringArray depending on mode.
fn notnull_value(col: &dyn Array, row: usize) -> bool {
    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::BooleanArray>() {
        return arr.value(row);
    }
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return matches!(arr.value(row), "true" | "TRUE" | "1");
    }
    let s = arrow::util::display::array_value_to_string(col, row).unwrap_or_default();
    s != "0" && !s.is_empty() && s.to_lowercase() != "false"
}

pub struct DuckDBTableProvider {
    table_name: String,
    schema: SchemaRef,
    federation_provider: Arc<SQLFederationProvider>,
}

impl DuckDBTableProvider {
    pub fn new(table_name: &str, executor: Arc<DuckDBSQLExecutor>) -> DFResult<Self> {
        let schema = Self::resolve_schema(table_name)?;
        let federation_provider = Arc::new(SQLFederationProvider::new(executor));
        Ok(Self {
            table_name: table_name.to_string(),
            schema: Arc::new(schema),
            federation_provider,
        })
    }

    /// Resolve schema via `PRAGMA table_info`: name (col 1), type (col 2), notnull (col 3).
    fn resolve_schema(table_name: &str) -> DFResult<Schema> {
        let conn_arc = crate::get_shared_connection().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "Shared trexsql connection is not available".to_string(),
            )
        })?;

        let conn = conn_arc.lock().map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to lock shared connection: {e}"
            ))
        })?;

        let sql = format!("PRAGMA table_info(\"{}\")", crate::catalog::escape_identifier(table_name));
        let mut stmt = conn.prepare(&sql).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to query table info for '{}': {e}",
                table_name
            ))
        })?;

        let batches: Vec<RecordBatch> = stmt
            .query_arrow([])
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to execute PRAGMA table_info for '{}': {e}",
                    table_name
                ))
            })?
            .collect();

        let mut fields = Vec::new();
        for batch in &batches {
            if batch.num_columns() < 4 {
                continue;
            }

            let name_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "PRAGMA table_info 'name' column is not a string".to_string(),
                    )
                })?;

            let type_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "PRAGMA table_info 'type' column is not a string".to_string(),
                    )
                })?;

            let notnull_col = batch.column(3);

            for row in 0..batch.num_rows() {
                let col_name = name_col.value(row);
                let col_type = type_col.value(row);

                let notnull = notnull_value(notnull_col, row);

                let arrow_type = duckdb_type_to_arrow(col_type);
                fields.push(Field::new(col_name, arrow_type, !notnull));
            }
        }

        if fields.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "Table '{}' not found or has no columns",
                table_name
            )));
        }

        Ok(Schema::new(fields))
    }
}

impl fmt::Debug for DuckDBTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDBTableProvider")
            .field("table_name", &self.table_name)
            .finish()
    }
}

#[async_trait]
impl TableProvider for DuckDBTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let source = Arc::new(DuckDBTableSource::new(
            &self.table_name,
            Arc::clone(&self.schema),
            Arc::clone(&self.federation_provider),
        ));

        let adaptor =
            FederatedTableProviderAdaptor::new_with_provider(source, Arc::new(self.clone_inner()));

        adaptor.scan(state, projection, filters, limit).await
    }
}

impl DuckDBTableProvider {
    fn clone_inner(&self) -> DuckDBTableProviderInner {
        DuckDBTableProviderInner {
            schema: Arc::clone(&self.schema),
        }
    }
}

/// Schema-only fallback provider for the federation adaptor.
struct DuckDBTableProviderInner {
    schema: SchemaRef,
}

impl fmt::Debug for DuckDBTableProviderInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDBTableProviderInner").finish()
    }
}

#[async_trait]
impl TableProvider for DuckDBTableProviderInner {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(datafusion::error::DataFusionError::NotImplemented(
            "Direct scan on DuckDBTableProviderInner is not supported; \
             use the federated path"
                .to_string(),
        ))
    }
}

use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource, FederationProvider};

#[derive(Debug)]
struct DuckDBTableSource {
    table_name: String,
    schema: SchemaRef,
    federation_provider: Arc<SQLFederationProvider>,
}

impl DuckDBTableSource {
    fn new(
        table_name: &str,
        schema: SchemaRef,
        federation_provider: Arc<SQLFederationProvider>,
    ) -> Self {
        Self {
            table_name: table_name.to_string(),
            schema,
            federation_provider,
        }
    }
}

impl datafusion::logical_expr::TableSource for DuckDBTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

impl FederatedTableSource for DuckDBTableSource {
    fn federation_provider(&self) -> Arc<dyn FederationProvider> {
        Arc::clone(&self.federation_provider) as Arc<dyn FederationProvider>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_types() {
        assert_eq!(duckdb_type_to_arrow("INTEGER"), DataType::Int32);
        assert_eq!(duckdb_type_to_arrow("INT"), DataType::Int32);
        assert_eq!(duckdb_type_to_arrow("INT4"), DataType::Int32);
        assert_eq!(duckdb_type_to_arrow("SIGNED"), DataType::Int32);
        assert_eq!(duckdb_type_to_arrow("BIGINT"), DataType::Int64);
        assert_eq!(duckdb_type_to_arrow("INT8"), DataType::Int64);
        assert_eq!(duckdb_type_to_arrow("LONG"), DataType::Int64);
        assert_eq!(duckdb_type_to_arrow("SMALLINT"), DataType::Int16);
        assert_eq!(duckdb_type_to_arrow("INT2"), DataType::Int16);
        assert_eq!(duckdb_type_to_arrow("TINYINT"), DataType::Int8);
        assert_eq!(duckdb_type_to_arrow("INT1"), DataType::Int8);
    }

    #[test]
    fn test_unsigned_types() {
        assert_eq!(duckdb_type_to_arrow("UINTEGER"), DataType::UInt32);
        assert_eq!(duckdb_type_to_arrow("UBIGINT"), DataType::UInt64);
        assert_eq!(duckdb_type_to_arrow("USMALLINT"), DataType::UInt16);
        assert_eq!(duckdb_type_to_arrow("UTINYINT"), DataType::UInt8);
    }

    #[test]
    fn test_hugeint_maps_to_utf8() {
        assert_eq!(duckdb_type_to_arrow("HUGEINT"), DataType::Utf8);
        assert_eq!(duckdb_type_to_arrow("UHUGEINT"), DataType::Utf8);
    }

    #[test]
    fn test_float_types() {
        assert_eq!(duckdb_type_to_arrow("FLOAT"), DataType::Float32);
        assert_eq!(duckdb_type_to_arrow("FLOAT4"), DataType::Float32);
        assert_eq!(duckdb_type_to_arrow("REAL"), DataType::Float32);
        assert_eq!(duckdb_type_to_arrow("DOUBLE"), DataType::Float64);
        assert_eq!(duckdb_type_to_arrow("FLOAT8"), DataType::Float64);
    }

    #[test]
    fn test_boolean() {
        assert_eq!(duckdb_type_to_arrow("BOOLEAN"), DataType::Boolean);
        assert_eq!(duckdb_type_to_arrow("BOOL"), DataType::Boolean);
        assert_eq!(duckdb_type_to_arrow("LOGICAL"), DataType::Boolean);
    }

    #[test]
    fn test_string_types() {
        assert_eq!(duckdb_type_to_arrow("VARCHAR"), DataType::Utf8);
        assert_eq!(duckdb_type_to_arrow("TEXT"), DataType::Utf8);
        assert_eq!(duckdb_type_to_arrow("STRING"), DataType::Utf8);
    }

    #[test]
    fn test_binary_types() {
        assert_eq!(duckdb_type_to_arrow("BLOB"), DataType::Binary);
        assert_eq!(duckdb_type_to_arrow("BYTEA"), DataType::Binary);
        assert_eq!(duckdb_type_to_arrow("BINARY"), DataType::Binary);
        assert_eq!(duckdb_type_to_arrow("VARBINARY"), DataType::Binary);
    }

    #[test]
    fn test_date_time_types() {
        assert_eq!(duckdb_type_to_arrow("DATE"), DataType::Date32);
        assert_eq!(
            duckdb_type_to_arrow("TIME"),
            DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            duckdb_type_to_arrow("TIMESTAMP"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            duckdb_type_to_arrow("DATETIME"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            duckdb_type_to_arrow("TIMESTAMP WITH TIME ZONE"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            duckdb_type_to_arrow("TIMESTAMPTZ"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_interval() {
        assert_eq!(
            duckdb_type_to_arrow("INTERVAL"),
            DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn test_decimal() {
        assert_eq!(
            duckdb_type_to_arrow("DECIMAL(10,2)"),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(
            duckdb_type_to_arrow("NUMERIC(18,4)"),
            DataType::Decimal128(18, 4)
        );
        assert_eq!(
            duckdb_type_to_arrow("DECIMAL"),
            DataType::Decimal128(38, 10)
        );
    }

    #[test]
    fn test_uuid_maps_to_utf8() {
        assert_eq!(duckdb_type_to_arrow("UUID"), DataType::Utf8);
    }

    #[test]
    fn test_unknown_type_fallback() {
        assert_eq!(duckdb_type_to_arrow("GEOMETRY"), DataType::Utf8);
        assert_eq!(duckdb_type_to_arrow("JSON"), DataType::Utf8);
    }

    #[test]
    fn test_case_insensitive() {
        assert_eq!(duckdb_type_to_arrow("integer"), DataType::Int32);
        assert_eq!(duckdb_type_to_arrow("Varchar"), DataType::Utf8);
        assert_eq!(duckdb_type_to_arrow("boolean"), DataType::Boolean);
    }
}
