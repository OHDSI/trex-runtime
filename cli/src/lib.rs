pub mod server;

use std::net::SocketAddr;

use anyhow::{Error, Result};
use base::server::{
  Builder, RequestIdleTimeout, ServerFlags, ServerHealth, Tls,
  WorkerEntrypoints,
};
use base::worker::pool::WorkerPoolPolicy;
use base::InspectorOption;
use std::fs;
use tokio::sync::mpsc;

pub use server::*;

/// Server configuration structure that matches the CLI configuration
#[derive(Clone)]
pub struct ServerConfig {
  // Core configuration
  pub addr: SocketAddr,
  pub main_service_path: String,
  pub event_worker_path: Option<String>,
  pub user_worker_policy: Option<WorkerPoolPolicy>,

  // TLS configuration
  pub tls_cert_path: Option<String>,
  pub tls_key_path: Option<String>,
  pub tls_port: Option<u16>,

  // Static patterns
  pub static_patterns: Vec<String>,

  // Inspector configuration
  pub inspector: Option<InspectorOption>,

  // Server flags
  pub no_module_cache: bool,
  pub allow_main_inspector: bool,
  pub tcp_nodelay: bool,
  pub graceful_exit_deadline_sec: u64,
  pub graceful_exit_keepalive_deadline_ms: Option<u64>,
  pub event_worker_exit_deadline_sec: u64,
  pub request_wait_timeout_ms: Option<u64>,
  pub request_idle_timeout: RequestIdleTimeout,
  pub request_read_timeout_ms: Option<u64>,
  pub request_buffer_size: Option<u64>,
  pub beforeunload_wall_clock_pct: Option<u8>,
  pub beforeunload_cpu_pct: Option<u8>,
  pub beforeunload_memory_pct: Option<u8>,

  // Entrypoints
  pub import_map_path: Option<String>,
  pub jsx_specifier: Option<String>,
  pub jsx_module: Option<String>,

  // Other configurations
  pub worker_pool_max_size: Option<usize>,
  pub worker_memory_limit_mb: Option<usize>,
  pub decorator: bool,

  // Filesystem restrictions
  pub restrict_host_fs: bool,
}

impl std::fmt::Debug for ServerConfig {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ServerConfig")
      .field("addr", &self.addr)
      .field("main_service_path", &self.main_service_path)
      .field("event_worker_path", &self.event_worker_path)
      .field("user_worker_policy", &"<WorkerPoolPolicy>")
      .field("tls_cert_path", &self.tls_cert_path)
      .field("tls_key_path", &self.tls_key_path)
      .field("tls_port", &self.tls_port)
      .field("static_patterns", &self.static_patterns)
      .field("inspector", &self.inspector)
      .field("no_module_cache", &self.no_module_cache)
      .field("allow_main_inspector", &self.allow_main_inspector)
      .field("tcp_nodelay", &self.tcp_nodelay)
      .field(
        "graceful_exit_deadline_sec",
        &self.graceful_exit_deadline_sec,
      )
      .field(
        "graceful_exit_keepalive_deadline_ms",
        &self.graceful_exit_keepalive_deadline_ms,
      )
      .field(
        "event_worker_exit_deadline_sec",
        &self.event_worker_exit_deadline_sec,
      )
      .field("request_wait_timeout_ms", &self.request_wait_timeout_ms)
      .field("request_idle_timeout", &self.request_idle_timeout)
      .field("request_read_timeout_ms", &self.request_read_timeout_ms)
      .field("request_buffer_size", &self.request_buffer_size)
      .field(
        "beforeunload_wall_clock_pct",
        &self.beforeunload_wall_clock_pct,
      )
      .field("beforeunload_cpu_pct", &self.beforeunload_cpu_pct)
      .field("beforeunload_memory_pct", &self.beforeunload_memory_pct)
      .field("import_map_path", &self.import_map_path)
      .field("jsx_specifier", &self.jsx_specifier)
      .field("jsx_module", &self.jsx_module)
      .field("worker_pool_max_size", &self.worker_pool_max_size)
      .field("worker_memory_limit_mb", &self.worker_memory_limit_mb)
      .field("decorator", &self.decorator)
      .field("restrict_host_fs", &self.restrict_host_fs)
      .finish()
  }
}

impl Default for ServerConfig {
  fn default() -> Self {
    Self {
      addr: "127.0.0.1:8080".parse().unwrap(),
      main_service_path: "main.ts".to_string(),
      event_worker_path: None,
      user_worker_policy: None,
      tls_cert_path: None,
      tls_key_path: None,
      tls_port: None,
      static_patterns: vec![],
      inspector: None,
      no_module_cache: false,
      allow_main_inspector: false,
      tcp_nodelay: false,
      graceful_exit_deadline_sec: 30,
      graceful_exit_keepalive_deadline_ms: None,
      event_worker_exit_deadline_sec: 30,
      request_wait_timeout_ms: None,
      request_idle_timeout: RequestIdleTimeout::default(),
      request_read_timeout_ms: None,
      request_buffer_size: None,
      beforeunload_wall_clock_pct: None,
      beforeunload_cpu_pct: None,
      beforeunload_memory_pct: None,
      import_map_path: None,
      jsx_specifier: None,
      jsx_module: None,
      worker_pool_max_size: None,
      worker_memory_limit_mb: None,
      decorator: false,
      restrict_host_fs: false,
    }
  }
}

impl ServerConfig {
  /// Convert to ServerFlags
  pub fn to_server_flags(&self) -> ServerFlags {
    ServerFlags {
      otel: None,
      otel_console: None,
      no_module_cache: self.no_module_cache,
      allow_main_inspector: self.allow_main_inspector,
      tcp_nodelay: self.tcp_nodelay,
      graceful_exit_deadline_sec: self.graceful_exit_deadline_sec,
      graceful_exit_keepalive_deadline_ms: self
        .graceful_exit_keepalive_deadline_ms,
      event_worker_exit_deadline_sec: self.event_worker_exit_deadline_sec,
      request_wait_timeout_ms: self.request_wait_timeout_ms,
      request_idle_timeout: self.request_idle_timeout,
      request_read_timeout_ms: self.request_read_timeout_ms,
      request_buffer_size: self.request_buffer_size,
      beforeunload_wall_clock_pct: self.beforeunload_wall_clock_pct,
      beforeunload_cpu_pct: self.beforeunload_cpu_pct,
      beforeunload_memory_pct: self.beforeunload_memory_pct,
      restrict_host_fs: self.restrict_host_fs,
    }
  }

  /// Convert to WorkerEntrypoints
  pub fn to_worker_entrypoints(&self) -> WorkerEntrypoints {
    WorkerEntrypoints {
      main: Some(self.main_service_path.clone()),
      events: self.event_worker_path.clone(),
    }
  }
}

/// A handle to a running server
pub struct ServerHandle {
  pub id: String,
  pub addr: SocketAddr,
  _health_rx: mpsc::UnboundedReceiver<base::server::ServerEvent>,
}

impl ServerHandle {
  pub fn new(
    id: String,
    addr: SocketAddr,
    _health_rx: mpsc::UnboundedReceiver<base::server::ServerEvent>,
  ) -> Self {
    Self {
      id,
      addr,
      _health_rx,
    }
  }

  /// Get server health information
  pub async fn get_health(&mut self) -> Result<String> {
    // For now, just return a simple status
    Ok("running".to_string())
  }
}

/// Create TLS configuration from file paths
fn create_tls_config(
  cert_path: &str,
  key_path: &str,
  port: u16,
) -> Result<Tls> {
  let cert_data = fs::read(cert_path)
    .map_err(|e| anyhow::anyhow!("Failed to read certificate file: {}", e))?;
  let key_data = fs::read(key_path)
    .map_err(|e| anyhow::anyhow!("Failed to read private key file: {}", e))?;

  Tls::new(port, &key_data, &cert_data)
}

/// Start a Trex server with the given configuration
pub async fn start_server(config: ServerConfig) -> Result<ServerHandle, Error> {
  let mut builder = Builder::new(config.addr, &config.main_service_path);

  // Configure TLS if provided
  if let (Some(cert_path), Some(key_path)) =
    (&config.tls_cert_path, &config.tls_key_path)
  {
    let port = config.tls_port.unwrap_or(443);
    let tls = create_tls_config(cert_path, key_path, port)?;
    builder.tls(tls);
  }

  // Configure event worker path
  if let Some(event_worker_path) = &config.event_worker_path {
    builder.event_worker_path(event_worker_path);
  }

  // Configure user worker policy - borrow to avoid move
  if let Some(ref user_worker_policy) = config.user_worker_policy {
    builder.user_worker_policy(user_worker_policy.clone());
  }

  // Configure static patterns - add them one by one
  for pattern in &config.static_patterns {
    builder.add_static_pattern(pattern);
  }

  // Configure inspector
  if let Some(inspector) = config.inspector {
    builder.inspector(inspector);
  }

  // Configure server flags
  *builder.flags_mut() = config.to_server_flags();

  // Configure entrypoints
  *builder.entrypoints_mut() = config.to_worker_entrypoints();

  // Set up health monitoring
  let (health_tx, _health_rx) = mpsc::unbounded_channel();
  let (callback_tx, mut callback_rx) = mpsc::channel(1);
  builder.event_callback(callback_tx);

  // Build the server
  let mut server = builder.build().await?;

  // Get the server address - we need to use the builder addr since Server doesn't have local_addr
  let server_addr = config.addr;

  // Generate a unique server ID
  let server_id = format!(
    "trex_{}_{}",
    server_addr.port(),
    chrono::Utc::now().timestamp()
  );

  // Register the server with the global manager
  get_global_server_manager()
    .register_server(server_id.clone(), config.clone())?;

  // Spawn the server in a background task
  let server_id_for_cleanup = server_id.clone();
  tokio::spawn(async move {
    let result = server.listen().await;

    // Clean up server registration on exit
    let _ =
      get_global_server_manager().unregister_server(&server_id_for_cleanup);

    if let Err(e) = result {
      eprintln!("Server error: {}", e);
    }
  });

  // Wait for server to be ready
  if let Some(server_health) = callback_rx.recv().await {
    match server_health {
      ServerHealth::Listening(mut event_rx, _metrics) => {
        // Forward events to our health channel
        tokio::spawn(async move {
          while let Some(event) = event_rx.recv().await {
            if health_tx.send(event).is_err() {
              break;
            }
          }
        });
      }
      ServerHealth::Failure => {
        let _ = get_global_server_manager().unregister_server(&server_id);
        return Err(anyhow::anyhow!("Server failed to start"));
      }
    }
  }

  Ok(ServerHandle::new(server_id, server_addr, _health_rx))
}

/// Get the version of the Trex server
pub fn get_version() -> String {
  env!("CARGO_PKG_VERSION").to_string()
}
