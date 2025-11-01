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
