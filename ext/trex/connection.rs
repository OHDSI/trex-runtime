//! DuckDB connection management and query executor initialization.

use crate::query_executor::{QueryExecutor, EXECUTOR_POOL_SIZE};
use duckdb::Connection;
use std::sync::{Arc, Mutex, OnceLock};

static QUERY_EXECUTOR: OnceLock<Arc<QueryExecutor>> = OnceLock::new();
static CONNECTION_PROVIDER: OnceLock<Arc<dyn ConnectionProvider>> =
  OnceLock::new();

pub fn init_query_executor(connection: &Connection) -> Result<(), String> {
  let executor = QueryExecutor::new(connection, EXECUTOR_POOL_SIZE)?;
  QUERY_EXECUTOR
    .set(Arc::new(executor))
    .map_err(|_| "executor already initialized".into())
}

pub fn get_query_executor() -> Option<Arc<QueryExecutor>> {
  QUERY_EXECUTOR.get().cloned()
}

pub trait ConnectionProvider: Send + Sync {
  fn get_connection(&self) -> Arc<Mutex<Connection>>;
}

pub struct OwnedConnectionProvider {
  conn: Arc<Mutex<Connection>>,
}

impl OwnedConnectionProvider {
  pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
    Self { conn }
  }
}

impl ConnectionProvider for OwnedConnectionProvider {
  fn get_connection(&self) -> Arc<Mutex<Connection>> {
    self.conn.clone()
  }
}

pub struct SharedConnectionProvider {
  conn: Arc<Mutex<Connection>>,
}

impl SharedConnectionProvider {
  pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
    Self { conn }
  }
}

impl ConnectionProvider for SharedConnectionProvider {
  fn get_connection(&self) -> Arc<Mutex<Connection>> {
    self.conn.clone()
  }
}

pub fn set_connection_provider(
  provider: Arc<dyn ConnectionProvider>,
) -> Result<(), String> {
  CONNECTION_PROVIDER
    .set(provider)
    .map_err(|_| "provider already set".into())
}

pub fn get_connection_provider() -> Option<Arc<dyn ConnectionProvider>> {
  CONNECTION_PROVIDER.get().cloned()
}

pub fn get_connection() -> Option<Arc<Mutex<Connection>>> {
  get_connection_provider().map(|p| p.get_connection())
}

pub fn init_owned_connection(
  conn: Arc<Mutex<Connection>>,
) -> Result<(), String> {
  set_connection_provider(Arc::new(OwnedConnectionProvider::new(conn)))
}

pub fn init_shared_connection(
  conn: Arc<Mutex<Connection>>,
) -> Result<(), String> {
  set_connection_provider(Arc::new(SharedConnectionProvider::new(conn)))
}
