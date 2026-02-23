use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use duckdb::Connection;
use etl_lib::destination::Destination;
use etl_lib::error::EtlResult;
use etl_lib::types::{Event, TableId, TableRow};
use etl_postgres::types::{ColumnSchema, TableSchema};

use crate::pipeline_registry;
use crate::type_mapping::{cell_to_sql_literal, pg_type_to_duckdb};

/// trexsql destination for the Supabase ETL pipeline.
///
/// Implements the ETL `Destination` trait, writing CDC events and table rows
/// into the local trexsql database via `execute_batch()`.
#[derive(Clone)]
pub struct DuckDbDestination {
    connection: Arc<Mutex<Connection>>,
    pipeline_name: String,
    schemas: Arc<Mutex<HashMap<TableId, TableSchema>>>,
}

impl DuckDbDestination {
    pub fn new(
        connection: Arc<Mutex<Connection>>,
        pipeline_name: String,
        schemas: Arc<Mutex<HashMap<TableId, TableSchema>>>,
    ) -> Self {
        Self {
            connection,
            pipeline_name,
            schemas,
        }
    }

    fn execute_sql(&self, sql: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self
            .connection
            .lock()
            .map_err(|e| format!("connection lock: {}", e))?;
        conn.execute_batch(sql)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
        Ok(())
    }

    fn ensure_schema(&self, schema_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sql = format!(
            "CREATE SCHEMA IF NOT EXISTS \"{}\"",
            schema_name.replace('"', "\"\"")
        );
        self.execute_sql(&sql)
    }

    fn ensure_table(
        &self,
        table_schema: &TableSchema,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let schema_name = &table_schema.name.schema;
        let table_name = &table_schema.name.name;

        self.ensure_schema(schema_name)?;

        let columns: Vec<String> = table_schema
            .column_schemas
            .iter()
            .map(|col| {
                let duckdb_type = pg_type_to_duckdb(&col.typ);
                format!(
                    "\"{}\" {}",
                    col.name.replace('"', "\"\""),
                    duckdb_type
                )
            })
            .collect();

        let sql = format!(
            "CREATE TABLE IF NOT EXISTS \"{}\".\"{}\" ({})",
            schema_name.replace('"', "\"\""),
            table_name.replace('"', "\"\""),
            columns.join(", ")
        );
        self.execute_sql(&sql)
    }

    fn handle_schema_evolution(
        &self,
        table_schema: &TableSchema,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let schema_name = &table_schema.name.schema;
        let table_name = &table_schema.name.name;

        for col in &table_schema.column_schemas {
            let duckdb_type = pg_type_to_duckdb(&col.typ);
            let sql = format!(
                "ALTER TABLE \"{}\".\"{}\" ADD COLUMN IF NOT EXISTS \"{}\" {}",
                schema_name.replace('"', "\"\""),
                table_name.replace('"', "\"\""),
                col.name.replace('"', "\"\""),
                duckdb_type
            );
            let _ = self.execute_sql(&sql);
        }
        Ok(())
    }

    fn build_insert_sql(
        &self,
        schema: &str,
        table: &str,
        columns: &[ColumnSchema],
        row: &TableRow,
    ) -> String {
        let col_names: Vec<String> = columns
            .iter()
            .map(|c| format!("\"{}\"", c.name.replace('"', "\"\"")))
            .collect();

        let values: Vec<String> = row.values.iter().map(|c| cell_to_sql_literal(c)).collect();

        format!(
            "INSERT INTO \"{}\".\"{}\" ({}) VALUES ({})",
            schema.replace('"', "\"\""),
            table.replace('"', "\"\""),
            col_names.join(", "),
            values.join(", ")
        )
    }

    fn build_update_sql(
        &self,
        schema: &str,
        table: &str,
        columns: &[ColumnSchema],
        row: &TableRow,
    ) -> Option<String> {
        let pk_cols: Vec<(usize, &ColumnSchema)> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary)
            .collect();

        if pk_cols.is_empty() {
            return None;
        }

        let set_clauses: Vec<String> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| !c.primary)
            .filter_map(|(i, c)| {
                row.values.get(i).map(|v| {
                    format!(
                        "\"{}\" = {}",
                        c.name.replace('"', "\"\""),
                        cell_to_sql_literal(v)
                    )
                })
            })
            .collect();

        if set_clauses.is_empty() {
            return None;
        }

        let where_clauses: Vec<String> = pk_cols
            .iter()
            .filter_map(|(i, c)| {
                row.values.get(*i).map(|v| {
                    format!(
                        "\"{}\" = {}",
                        c.name.replace('"', "\"\""),
                        cell_to_sql_literal(v)
                    )
                })
            })
            .collect();

        Some(format!(
            "UPDATE \"{}\".\"{}\" SET {} WHERE {}",
            schema.replace('"', "\"\""),
            table.replace('"', "\"\""),
            set_clauses.join(", "),
            where_clauses.join(" AND ")
        ))
    }

    fn build_delete_sql(
        &self,
        schema: &str,
        table: &str,
        columns: &[ColumnSchema],
        row: &TableRow,
    ) -> Option<String> {
        let pk_cols: Vec<(usize, &ColumnSchema)> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary)
            .collect();

        if pk_cols.is_empty() {
            return None;
        }

        let where_clauses: Vec<String> = pk_cols
            .iter()
            .filter_map(|(i, c)| {
                row.values.get(*i).map(|v| {
                    format!(
                        "\"{}\" = {}",
                        c.name.replace('"', "\"\""),
                        cell_to_sql_literal(v)
                    )
                })
            })
            .collect();

        Some(format!(
            "DELETE FROM \"{}\".\"{}\" WHERE {}",
            schema.replace('"', "\"\""),
            table.replace('"', "\"\""),
            where_clauses.join(" AND ")
        ))
    }
}

impl Destination for DuckDbDestination {
    fn name() -> &'static str {
        "duckdb"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        let schema = self
            .schemas
            .lock()
            .map_err(|e| {
                etl_lib::error::EtlError::from((
                    etl_lib::error::ErrorKind::DestinationError,
                    "schema lock failed",
                    e.to_string(),
                ))
            })?
            .get(&table_id)
            .cloned();

        if let Some(ts) = schema {
            let sql = format!(
                "DELETE FROM \"{}\".\"{}\"",
                ts.name.schema.replace('"', "\"\""),
                ts.name.name.replace('"', "\"\"")
            );
            self.execute_sql(&sql).map_err(|e| {
                etl_lib::error::EtlError::from((
                    etl_lib::error::ErrorKind::DestinationQueryFailed,
                    "truncate failed",
                    e.to_string(),
                ))
            })?;
        }

        Ok(())
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        if table_rows.is_empty() {
            return Ok(());
        }

        let schema = self
            .schemas
            .lock()
            .map_err(|e| {
                etl_lib::error::EtlError::from((
                    etl_lib::error::ErrorKind::DestinationError,
                    "schema lock failed",
                    e.to_string(),
                ))
            })?
            .get(&table_id)
            .cloned()
            .ok_or_else(|| {
                etl_lib::error::EtlError::from((
                    etl_lib::error::ErrorKind::DestinationError,
                    "no schema for table during snapshot copy",
                ))
            })?;

        self.ensure_table(&schema).map_err(|e| {
            etl_lib::error::EtlError::from((
                etl_lib::error::ErrorKind::DestinationQueryFailed,
                "ensure_table failed during snapshot copy",
                e.to_string(),
            ))
        })?;

        let mut sql_batch = Vec::new();
        for row in &table_rows {
            sql_batch.push(self.build_insert_sql(
                &schema.name.schema,
                &schema.name.name,
                &schema.column_schemas,
                row,
            ));
        }

        if !sql_batch.is_empty() {
            let combined = sql_batch.join(";\n") + ";";
            self.execute_sql(&combined).map_err(|e| {
                etl_lib::error::EtlError::from((
                    etl_lib::error::ErrorKind::DestinationQueryFailed,
                    "batch SQL execution failed during snapshot copy",
                    e.to_string(),
                ))
            })?;
        }

        let row_count = table_rows.len() as u64;
        pipeline_registry::registry().update_stats(&self.pipeline_name, row_count);

        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        pipeline_registry::registry().transition_to_streaming_once(&self.pipeline_name);

        let mut sql_batch = Vec::new();
        let mut rows_written: u64 = 0;
        let mut current_schemas: std::collections::HashMap<TableId, TableSchema> =
            std::collections::HashMap::new();

        for event in &events {
            match event {
                Event::Relation(rel) => {
                    let ts = &rel.table_schema;
                    if let Err(e) = self.ensure_table(ts) {
                        eprintln!("etl: ensure_table error: {}", e);
                    }
                    if let Err(e) = self.handle_schema_evolution(ts) {
                        eprintln!("etl: schema_evolution error: {}", e);
                    }
                    current_schemas.insert(ts.id, ts.clone());
                    if let Ok(mut cache) = self.schemas.lock() {
                        cache.insert(ts.id, ts.clone());
                    }
                }
                Event::Insert(ins) => {
                    if let Some(ts) = current_schemas.get(&ins.table_id) {
                        let stmt = self.build_insert_sql(
                            &ts.name.schema,
                            &ts.name.name,
                            &ts.column_schemas,
                            &ins.table_row,
                        );
                        sql_batch.push(stmt);
                        rows_written += 1;
                    }
                }
                Event::Update(upd) => {
                    if let Some(ts) = current_schemas.get(&upd.table_id) {
                        if let Some(stmt) = self.build_update_sql(
                            &ts.name.schema,
                            &ts.name.name,
                            &ts.column_schemas,
                            &upd.table_row,
                        ) {
                            sql_batch.push(stmt);
                            rows_written += 1;
                        }
                    }
                }
                Event::Delete(del) => {
                    if let Some(old_row) = &del.old_table_row {
                        if let Some(ts) = current_schemas.get(&del.table_id) {
                            if let Some(stmt) = self.build_delete_sql(
                                &ts.name.schema,
                                &ts.name.name,
                                &ts.column_schemas,
                                &old_row.1,
                            ) {
                                sql_batch.push(stmt);
                                rows_written += 1;
                            }
                        }
                    }
                }
                Event::Truncate(trunc) => {
                    for rel_id in &trunc.rel_ids {
                        let tid = TableId::new(*rel_id);
                        if let Some(ts) = current_schemas.get(&tid) {
                            sql_batch.push(format!(
                                "DELETE FROM \"{}\".\"{}\"",
                                ts.name.schema.replace('"', "\"\""),
                                ts.name.name.replace('"', "\"\"")
                            ));
                        }
                    }
                }
                Event::Begin(_) | Event::Commit(_) | Event::Unsupported => {}
            }
        }

        if !sql_batch.is_empty() {
            let combined = sql_batch.join(";\n") + ";";
            self.execute_sql(&combined).map_err(|e| {
                etl_lib::error::EtlError::from((
                    etl_lib::error::ErrorKind::DestinationQueryFailed,
                    "batch SQL execution failed",
                    e.to_string(),
                ))
            })?;
        }

        if rows_written > 0 {
            pipeline_registry::registry().update_stats(&self.pipeline_name, rows_written);
        }

        Ok(())
    }
}
