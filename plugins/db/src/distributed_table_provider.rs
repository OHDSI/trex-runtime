//! Sharded table provider that fans out scans to all shards via Arrow Flight.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Result as DFResult;
use datafusion::datasource::TableProvider;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use datafusion::sql::unparser::expr_to_sql;
use futures::stream;
use futures::StreamExt;

use crate::catalog::ShardInfo;
use crate::flight_client;
use crate::logging::SwarmLogger;

/// TableProvider that fans out scans to all shards via Arrow Flight.
#[derive(Debug)]
pub struct DistributedTableProvider {
    table_name: String,
    schema: SchemaRef,
    shards: Vec<ShardInfo>,
    runtime_handle: tokio::runtime::Handle,
}

impl DistributedTableProvider {
    /// Fetches schema from the first shard to initialize the provider.
    pub async fn new(
        table_name: String,
        shards: Vec<ShardInfo>,
        runtime_handle: tokio::runtime::Handle,
    ) -> Result<Self, String> {
        if shards.is_empty() {
            return Err(format!(
                "DistributedTableProvider for '{}': no shards provided",
                table_name
            ));
        }

        let escaped = crate::catalog::escape_identifier(&table_name);
        // Use LIMIT 1 (not LIMIT 0) because some Flight servers don't send
        // schema metadata when the result set is completely empty.
        let schema_sql = format!("SELECT * FROM \"{}\" LIMIT 1", escaped);
        let endpoint = shards[0].flight_endpoint.clone();

        let (schema, _batches) = flight_client::query_node_with_schema(&endpoint, &schema_sql)
            .await
            .map_err(|e| {
                format!(
                    "Failed to fetch schema for '{}' from {}: {}",
                    table_name, endpoint, e
                )
            })?;

        Ok(Self {
            table_name,
            schema,
            shards,
            runtime_handle,
        })
    }

    /// Build SQL for a shard query with projection, filter, and limit pushdown.
    fn build_shard_sql(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> String {
        let escaped = crate::catalog::escape_identifier(&self.table_name);

        let columns = match projection {
            Some(indices) if !indices.is_empty() => {
                let col_names: Vec<String> = indices
                    .iter()
                    .map(|&i| {
                        let name = self.schema.field(i).name();
                        format!("\"{}\"", crate::catalog::escape_identifier(name))
                    })
                    .collect();
                col_names.join(", ")
            }
            Some(_) => {
                // Empty projection: DataFusion needs rows but no columns (e.g. COUNT(*)).
                "1 AS _row".to_string()
            }
            None => "*".to_string(),
        };

        let mut sql = format!("SELECT {} FROM \"{}\"", columns, escaped);

        if !filters.is_empty() {
            let where_parts: Vec<String> = filters
                .iter()
                .filter_map(|expr| {
                    expr_to_sql(expr)
                        .ok()
                        .map(|ast| ast.to_string())
                })
                .collect();

            if !where_parts.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&where_parts.join(" AND "));
            }
        }

        if let Some(limit) = limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        sql
    }
}

#[async_trait::async_trait]
impl TableProvider for DistributedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let shard_sql = self.build_shard_sql(projection, filters, limit);

        SwarmLogger::debug(
            "distributed-table",
            &format!(
                "Shard scan for '{}' across {} shard(s): {}",
                self.table_name,
                self.shards.len(),
                shard_sql,
            ),
        );

        let output_schema = match projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| self.schema.field(i).clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => self.schema.clone(),
        };

        Ok(Arc::new(DistributedExec::new(
            self.table_name.clone(),
            output_schema,
            self.shards.clone(),
            shard_sql,
            self.runtime_handle.clone(),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Exact: filters are pushed into the SQL sent to each shard.
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}

/// ExecutionPlan that queries all shards in parallel via Arrow Flight.
#[derive(Debug)]
pub struct DistributedExec {
    table_name: String,
    schema: SchemaRef,
    shards: Vec<ShardInfo>,
    shard_sql: String,
    runtime_handle: tokio::runtime::Handle,
    properties: PlanProperties,
}

impl DistributedExec {
    fn new(
        table_name: String,
        schema: SchemaRef,
        shards: Vec<ShardInfo>,
        shard_sql: String,
        runtime_handle: tokio::runtime::Handle,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(shards.len()),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            table_name,
            schema,
            shards,
            shard_sql,
            runtime_handle,
            properties,
        }
    }
}

impl DisplayAs for DistributedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedExec: table={}, shards={}",
            self.table_name,
            self.shards.len(),
        )
    }
}

impl ExecutionPlan for DistributedExec {
    fn name(&self) -> &str {
        "DistributedExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition >= self.shards.len() {
            return Err(DataFusionError::Internal(format!(
                "Partition {} out of range (0..{})",
                partition,
                self.shards.len()
            )));
        }

        let shard = self.shards[partition].clone();
        let sql = self.shard_sql.clone();
        let schema = self.schema.clone();
        let table_name = self.table_name.clone();

        // Spawn the Flight query as a tokio task (non-blocking). Returning a
        // lazy stream avoids blocking tokio worker threads — which would
        // deadlock the runtime when multiple partitions execute concurrently.
        let join_handle = self.runtime_handle.spawn(async move {
            SwarmLogger::debug(
                "distributed-exec",
                &format!(
                    "Querying shard {} ({}) for '{}' [partition {}]",
                    shard.node_name, shard.flight_endpoint, table_name, partition
                ),
            );
            let batches = flight_client::query_node(&shard.flight_endpoint, &sql)
                .await
                .map_err(|e| {
                    format!(
                        "Distributed scan for '{}' failed on shard {} ({}): {}",
                        table_name, shard.node_name, shard.flight_endpoint, e
                    )
                })?;
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            SwarmLogger::debug(
                "distributed-exec",
                &format!(
                    "Shard {} ({}) returned {} row(s) [partition {}]",
                    shard.node_name, shard.flight_endpoint, rows, partition
                ),
            );
            Ok::<Vec<RecordBatch>, String>(batches)
        });

        let empty_projection = schema.fields().is_empty();
        let out_schema = schema.clone();

        // Lazy stream: awaits the tokio task, then yields each batch.
        let result_stream = futures::stream::once(async move {
            let batches = join_handle
                .await
                .map_err(|e| DataFusionError::Internal(format!("Shard query task panicked: {e}")))?
                .map_err(|e| DataFusionError::External(e.into()))?;

            Ok::<Vec<RecordBatch>, DataFusionError>(batches)
        })
        .flat_map(move |result| match result {
            Ok(batches) => {
                // When schema has 0 fields (empty projection, e.g. COUNT(*)),
                // convert shard batches to empty-column batches preserving row counts.
                let projected: Vec<Result<RecordBatch, DataFusionError>> = if empty_projection {
                    batches
                        .into_iter()
                        .map(|b| {
                            RecordBatch::try_new_with_options(
                                out_schema.clone(),
                                vec![],
                                &arrow::array::RecordBatchOptions::new()
                                    .with_row_count(Some(b.num_rows())),
                            )
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                        })
                        .collect()
                } else {
                    batches.into_iter().map(Ok).collect()
                };
                stream::iter(projected).boxed()
            }
            Err(e) => stream::once(async move { Err(e) }).boxed(),
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            result_stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
        ]))
    }

    fn test_shards() -> Vec<ShardInfo> {
        vec![
            ShardInfo {
                node_name: "node-a".to_string(),
                flight_endpoint: "http://10.0.0.1:8815".to_string(),
            },
            ShardInfo {
                node_name: "node-b".to_string(),
                flight_endpoint: "http://10.0.0.2:8815".to_string(),
            },
        ]
    }

    #[test]
    fn distributed_exec_schema() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let schema = test_schema();
        let exec = DistributedExec::new(
            "orders".to_string(),
            schema.clone(),
            test_shards(),
            "SELECT * FROM orders".to_string(),
            rt.handle().clone(),
        );
        assert_eq!(exec.properties().output_partitioning().partition_count(), 2);
        assert_eq!(exec.schema(), schema);
    }

    #[test]
    fn partition_count_matches_shards() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let shards = vec![
            ShardInfo {
                node_name: "a".to_string(),
                flight_endpoint: "http://a:8815".to_string(),
            },
            ShardInfo {
                node_name: "b".to_string(),
                flight_endpoint: "http://b:8815".to_string(),
            },
            ShardInfo {
                node_name: "c".to_string(),
                flight_endpoint: "http://c:8815".to_string(),
            },
        ];
        let exec = DistributedExec::new(
            "t".to_string(),
            test_schema(),
            shards,
            "SELECT * FROM t".to_string(),
            rt.handle().clone(),
        );
        assert_eq!(exec.properties().output_partitioning().partition_count(), 3);
    }

    #[test]
    fn out_of_range_partition_returns_error() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let exec = DistributedExec::new(
            "orders".to_string(),
            test_schema(),
            test_shards(),
            "SELECT * FROM orders".to_string(),
            rt.handle().clone(),
        );
        let ctx = Arc::new(TaskContext::default());
        let result = exec.execute(5, ctx);
        match result {
            Err(e) => {
                let err_msg = e.to_string();
                assert!(
                    err_msg.contains("Partition 5 out of range"),
                    "Expected partition range error, got: {err_msg}",
                );
            }
            Ok(_) => panic!("Expected error for out-of-range partition"),
        }
    }

    #[test]
    fn distributed_exec_no_children() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let exec = DistributedExec::new(
            "orders".to_string(),
            test_schema(),
            test_shards(),
            "SELECT * FROM orders".to_string(),
            rt.handle().clone(),
        );
        assert!(exec.children().is_empty());
    }

    #[test]
    fn distributed_exec_debug() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let exec = DistributedExec::new(
            "orders".to_string(),
            test_schema(),
            test_shards(),
            "SELECT * FROM orders".to_string(),
            rt.handle().clone(),
        );
        let debug = format!("{:?}", exec);
        assert!(debug.contains("orders"));
    }

    #[test]
    fn build_shard_sql_no_pushdown() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let provider = DistributedTableProvider {
            table_name: "orders".to_string(),
            schema: test_schema(),
            shards: test_shards(),
            runtime_handle: rt.handle().clone(),
        };
        let sql = provider.build_shard_sql(None, &[], None);
        assert_eq!(sql, "SELECT * FROM \"orders\"");
    }

    #[test]
    fn build_shard_sql_with_projection() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let provider = DistributedTableProvider {
            table_name: "orders".to_string(),
            schema: test_schema(),
            shards: test_shards(),
            runtime_handle: rt.handle().clone(),
        };
        let sql = provider.build_shard_sql(Some(&vec![0, 2]), &[], None);
        assert!(sql.contains("\"id\""));
        assert!(sql.contains("\"price\""));
        assert!(!sql.contains("\"name\""));
    }

    #[test]
    fn build_shard_sql_with_limit() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let provider = DistributedTableProvider {
            table_name: "orders".to_string(),
            schema: test_schema(),
            shards: test_shards(),
            runtime_handle: rt.handle().clone(),
        };
        let sql = provider.build_shard_sql(None, &[], Some(100));
        assert!(sql.contains("LIMIT 100"));
    }

    #[test]
    fn provider_schema_matches() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let schema = test_schema();
        let provider = DistributedTableProvider {
            table_name: "orders".to_string(),
            schema: schema.clone(),
            shards: test_shards(),
            runtime_handle: rt.handle().clone(),
        };
        assert_eq!(provider.schema(), schema);
        assert_eq!(provider.table_type(), TableType::Base);
    }

    #[test]
    fn supports_filter_pushdown_returns_exact() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let provider = DistributedTableProvider {
            table_name: "orders".to_string(),
            schema: test_schema(),
            shards: test_shards(),
            runtime_handle: rt.handle().clone(),
        };
        let expr = datafusion::prelude::col("id").gt(datafusion::prelude::lit(5));
        let result = provider.supports_filters_pushdown(&[&expr]).unwrap();
        assert_eq!(result, vec![TableProviderFilterPushDown::Exact]);
    }
}
