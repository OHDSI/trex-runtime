use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use duckdb::Connection;
use etl_lib::error::{EtlResult, ErrorKind};
use etl_lib::state::table::TableReplicationPhase;
use etl_lib::store::cleanup::CleanupStore;
use etl_lib::store::schema::SchemaStore;
use etl_lib::store::state::StateStore;
use etl_postgres::types::{TableId, TableSchema};
use tokio::sync::Mutex as TokioMutex;

/// Schema cache shared between store and destination for snapshot writes.
pub type SchemaCache = Arc<Mutex<HashMap<TableId, TableSchema>>>;

/// State, schema, and cleanup store backed by trexsql metadata tables.
#[derive(Clone)]
pub struct DuckDbStore {
    connection: Arc<Mutex<Connection>>,
    pipeline_name: String,
    inner: Arc<TokioMutex<Inner>>,
    schemas: SchemaCache,
}

struct Inner {
    table_replication_states: BTreeMap<TableId, TableReplicationPhase>,
    table_state_history: HashMap<TableId, Vec<TableReplicationPhase>>,
    table_schemas: HashMap<TableId, Arc<TableSchema>>,
    table_mappings: HashMap<TableId, String>,
    tables_initialized: bool,
}

impl DuckDbStore {
    pub fn new(
        connection: Arc<Mutex<Connection>>,
        pipeline_name: String,
        schemas: SchemaCache,
    ) -> Self {
        Self {
            connection,
            pipeline_name,
            inner: Arc::new(TokioMutex::new(Inner {
                table_replication_states: BTreeMap::new(),
                table_state_history: HashMap::new(),
                table_schemas: HashMap::new(),
                table_mappings: HashMap::new(),
                tables_initialized: false,
            })),
            schemas,
        }
    }

    fn ensure_tables(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self
            .connection
            .lock()
            .map_err(|e| format!("lock: {}", e))?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS _etl_checkpoints (
                pipeline_name VARCHAR PRIMARY KEY,
                lsn VARCHAR,
                snapshot_complete BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS _etl_table_mappings (
                pipeline_name VARCHAR,
                source_table_id INTEGER,
                destination_table_name VARCHAR,
                PRIMARY KEY (pipeline_name, source_table_id)
            );
            CREATE TABLE IF NOT EXISTS _etl_table_schemas (
                pipeline_name VARCHAR,
                source_schema VARCHAR,
                source_table VARCHAR,
                column_name VARCHAR,
                column_type VARCHAR,
                ordinal_position INTEGER,
                PRIMARY KEY (pipeline_name, source_schema, source_table, column_name)
            );",
        )
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

        Ok(())
    }

    fn persist_table_mapping(
        &self,
        source_table_id: TableId,
        destination_table_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self
            .connection
            .lock()
            .map_err(|e| format!("lock: {}", e))?;

        let sql = format!(
            "INSERT OR REPLACE INTO _etl_table_mappings (pipeline_name, source_table_id, destination_table_name) VALUES ('{}', {}, '{}')",
            self.pipeline_name.replace('\'', "''"),
            source_table_id.0,
            destination_table_name.replace('\'', "''")
        );

        conn.execute_batch(&sql)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

        Ok(())
    }
}

fn to_etl_error(e: impl std::fmt::Display) -> etl_lib::error::EtlError {
    etl_lib::error::EtlError::from((ErrorKind::DestinationError, "store operation failed", e.to_string()))
}

impl StateStore for DuckDbStore {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableReplicationPhase>> {
        let inner = self.inner.lock().await;
        Ok(inner.table_replication_states.get(&table_id).cloned())
    }

    async fn get_table_replication_states(
        &self,
    ) -> EtlResult<BTreeMap<TableId, TableReplicationPhase>> {
        let inner = self.inner.lock().await;
        Ok(inner.table_replication_states.clone())
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        {
            let mut inner = self.inner.lock().await;
            if !inner.tables_initialized {
                drop(inner); // Drop async lock before blocking I/O
                self.ensure_tables().map_err(|e| to_etl_error(e))?;
                inner = self.inner.lock().await;
                inner.tables_initialized = true;
            }
        }

        let inner = self.inner.lock().await;
        Ok(inner.table_replication_states.len())
    }

    async fn update_table_replication_states(
        &self,
        updates: Vec<(TableId, TableReplicationPhase)>,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        for (table_id, state) in updates {
            if let Some(current) = inner.table_replication_states.get(&table_id).cloned() {
                inner
                    .table_state_history
                    .entry(table_id)
                    .or_default()
                    .push(current);
            }
            inner.table_replication_states.insert(table_id, state);
        }

        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        let mut inner = self.inner.lock().await;

        let previous = inner
            .table_state_history
            .get_mut(&table_id)
            .and_then(|h| h.pop())
            .ok_or_else(|| {
                etl_lib::error::EtlError::from((
                    ErrorKind::StateRollbackError,
                    "no previous state to rollback to",
                ))
            })?;

        inner
            .table_replication_states
            .insert(table_id, previous.clone());

        Ok(previous)
    }

    async fn get_table_mapping(
        &self,
        source_table_id: &TableId,
    ) -> EtlResult<Option<String>> {
        let inner = self.inner.lock().await;
        Ok(inner.table_mappings.get(source_table_id).cloned())
    }

    async fn get_table_mappings(&self) -> EtlResult<HashMap<TableId, String>> {
        let inner = self.inner.lock().await;
        Ok(inner.table_mappings.clone())
    }

    async fn load_table_mappings(&self) -> EtlResult<usize> {
        let inner = self.inner.lock().await;
        Ok(inner.table_mappings.len())
    }

    async fn store_table_mapping(
        &self,
        source_table_id: TableId,
        destination_table_id: String,
    ) -> EtlResult<()> {
        self.persist_table_mapping(source_table_id, &destination_table_id)
            .map_err(|e| to_etl_error(e))?;

        let mut inner = self.inner.lock().await;
        inner
            .table_mappings
            .insert(source_table_id, destination_table_id);

        Ok(())
    }
}

impl SchemaStore for DuckDbStore {
    async fn get_table_schema(
        &self,
        table_id: &TableId,
    ) -> EtlResult<Option<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;
        Ok(inner.table_schemas.get(table_id).cloned())
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let inner = self.inner.lock().await;
        Ok(inner.table_schemas.values().cloned().collect())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        let inner = self.inner.lock().await;
        Ok(inner.table_schemas.len())
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<()> {
        if let Ok(mut cache) = self.schemas.lock() {
            cache.insert(table_schema.id, table_schema.clone());
        }

        let mut inner = self.inner.lock().await;
        inner
            .table_schemas
            .insert(table_schema.id, Arc::new(table_schema));
        Ok(())
    }
}

impl CleanupStore for DuckDbStore {
    async fn cleanup_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        inner.table_replication_states.remove(&table_id);
        inner.table_state_history.remove(&table_id);
        inner.table_schemas.remove(&table_id);
        inner.table_mappings.remove(&table_id);
        Ok(())
    }
}
