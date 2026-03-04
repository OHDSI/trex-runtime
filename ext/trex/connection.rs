//! DuckDB connection management and query executor initialization.

use crate::query_executor::{QueryExecutor, EXECUTOR_POOL_SIZE};
use duckdb::Connection;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::warn;

static QUERY_EXECUTOR: OnceLock<Arc<QueryExecutor>> = OnceLock::new();
static CONNECTION_PROVIDER: OnceLock<Arc<dyn ConnectionProvider>> =
  OnceLock::new();
static STREAMING_POOL: OnceLock<StreamingPool> = OnceLock::new();

const STREAMING_POOL_SIZE: usize = 4;

pub struct StreamingPool {
  connections: Mutex<Vec<Connection>>,
}

impl StreamingPool {
  fn new(connection: &Connection, pool_size: usize) -> Result<Self, String> {
    let mut connections = Vec::with_capacity(pool_size);
    for i in 0..pool_size {
      connections.push(
        connection
          .try_clone()
          .map_err(|e| format!("streaming pool clone {i}: {e}"))?,
      );
    }
    Ok(Self {
      connections: Mutex::new(connections),
    })
  }

  pub fn acquire(&self) -> Option<Connection> {
    match self.connections.lock() {
      Ok(mut pool) => pool.pop(),
      Err(poisoned) => {
        warn!("streaming pool lock poisoned on acquire, recovering");
        poisoned.into_inner().pop()
      }
    }
  }

  pub fn release(&self, conn: Connection) {
    match self.connections.lock() {
      Ok(mut pool) => pool.push(conn),
      Err(poisoned) => {
        warn!("streaming pool lock poisoned on release, recovering");
        poisoned.into_inner().push(conn);
      }
    }
  }
}

pub fn init_query_executor(connection: &Connection) -> Result<(), String> {
  let executor = QueryExecutor::new(connection, EXECUTOR_POOL_SIZE)?;
  QUERY_EXECUTOR
    .set(Arc::new(executor))
    .map_err(|_| "executor already initialized".into())
}

pub fn get_query_executor() -> Option<Arc<QueryExecutor>> {
  QUERY_EXECUTOR.get().cloned()
}

pub fn init_streaming_pool(connection: &Connection) -> Result<(), String> {
  let pool = StreamingPool::new(connection, STREAMING_POOL_SIZE)?;
  STREAMING_POOL
    .set(pool)
    .map_err(|_| "streaming pool already initialized".into())
}

pub fn get_streaming_pool() -> Option<&'static StreamingPool> {
  STREAMING_POOL.get()
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

#[cfg(test)]
mod tests {
  use super::*;
  use duckdb::Connection;

  #[test]
  fn test_owned_provider_returns_same_arc() {
    let conn = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
    let provider = OwnedConnectionProvider::new(conn.clone());
    let a = provider.get_connection();
    let b = provider.get_connection();
    assert!(Arc::ptr_eq(&a, &b));
    assert!(Arc::ptr_eq(&a, &conn));
  }

  #[test]
  fn test_shared_provider_returns_same_arc() {
    let conn = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
    let provider = SharedConnectionProvider::new(conn.clone());
    let a = provider.get_connection();
    let b = provider.get_connection();
    assert!(Arc::ptr_eq(&a, &b));
    assert!(Arc::ptr_eq(&a, &conn));
  }

  #[test]
  fn test_connection_is_usable() {
    let conn = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
    let provider = OwnedConnectionProvider::new(conn);
    let c = provider.get_connection();
    let guard = c.lock().unwrap();
    let mut stmt = guard.prepare("SELECT 42 AS answer").unwrap();
    let mut rows = stmt.query([]).unwrap();
    let row = rows.next().unwrap().unwrap();
    let val: i32 = row.get(0).unwrap();
    assert_eq!(val, 42);
  }

  #[test]
  fn test_streaming_pool_acquire_release() {
    let conn = Connection::open_in_memory().unwrap();
    let pool = StreamingPool::new(&conn, 2).unwrap();

    let c1 = pool.acquire();
    let c2 = pool.acquire();
    assert!(c1.is_some());
    assert!(c2.is_some());
    assert!(pool.acquire().is_none());

    pool.release(c1.unwrap());
    assert!(pool.acquire().is_some());
  }

  #[test]
  fn test_set_provider_twice_fails() {
    let lock: OnceLock<Arc<dyn ConnectionProvider>> = OnceLock::new();
    let conn1 = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
    let conn2 = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
    let p1: Arc<dyn ConnectionProvider> =
      Arc::new(OwnedConnectionProvider::new(conn1));
    let p2: Arc<dyn ConnectionProvider> =
      Arc::new(OwnedConnectionProvider::new(conn2));
    assert!(lock.set(p1).is_ok());
    assert!(lock.set(p2).is_err());
  }

  #[test]
  fn test_get_connection_returns_some() {
    let conn = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
    let provider: Arc<dyn ConnectionProvider> =
      Arc::new(OwnedConnectionProvider::new(conn));
    let result = provider.get_connection();
    let guard = result.lock().unwrap();
    let mut stmt = guard.prepare("SELECT 1").unwrap();
    let mut rows = stmt.query([]).unwrap();
    assert!(rows.next().unwrap().is_some());
  }
}
