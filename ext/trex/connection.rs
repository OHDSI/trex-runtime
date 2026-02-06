use duckdb::Connection;
use std::sync::{Arc, Mutex, OnceLock};

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

static CONNECTION_PROVIDER: OnceLock<Arc<dyn ConnectionProvider>> =
  OnceLock::new();

pub fn set_connection_provider(
  provider: Arc<dyn ConnectionProvider>,
) -> Result<(), String> {
  CONNECTION_PROVIDER
    .set(provider)
    .map_err(|_| "Connection provider already set".to_string())
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
  let provider = Arc::new(OwnedConnectionProvider::new(conn));
  set_connection_provider(provider)
}

pub fn init_shared_connection(
  conn: Arc<Mutex<Connection>>,
) -> Result<(), String> {
  let provider = Arc::new(SharedConnectionProvider::new(conn));
  set_connection_provider(provider)
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
