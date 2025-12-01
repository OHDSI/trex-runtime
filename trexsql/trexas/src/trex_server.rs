use anyhow::{bail, Result};
use base::server::RequestIdleTimeout;
use base::InspectorOption;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread;

pub use trex_cli::{
  get_global_server_manager, get_version, ServerConfig, ServerManager,
};

static LOG_INIT: AtomicBool = AtomicBool::new(false);

// Define a type alias for the complex type to improve readability
type ServerThreads = Arc<Mutex<HashMap<String, thread::JoinHandle<()>>>>;

static SERVER_THREADS: LazyLock<ServerThreads> =
  LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

// Helper function to normalize a path to a file:// URL
// If the path already starts with file://, return it as-is
// Otherwise, convert the absolute path to a file:// URL
// If the path is a directory, append /index.ts as the default entrypoint
fn normalize_path_to_file_url(path: &str) -> String {
  if path.starts_with("file://") {
    return path.to_string();
  }

  let path_obj = Path::new(path);
  let abs_path = if path_obj.is_absolute() {
    path_obj.to_path_buf()
  } else {
    std::env::current_dir()
      .ok()
      .map(|cwd| cwd.join(path_obj))
      .unwrap_or_else(|| path_obj.to_path_buf())
  };

  // If it's a directory, append index.ts as the default entrypoint
  let final_path = if abs_path.is_dir() {
    abs_path.join("index.ts")
  } else {
    abs_path
  };

  format!("file://{}", final_path.display())
}

// Helper function to parse inspector option from a string
// Expected formats:
//   - "inspect:127.0.0.1:9229"
//   - "inspect-brk:127.0.0.1:9229"
//   - "inspect-wait:127.0.0.1:9229"
fn parse_inspector_option(s: &str) -> Result<InspectorOption> {
  let parts: Vec<&str> = s.split(':').collect();
  if parts.len() < 3 {
    bail!(
      "Invalid inspector format. Expected 'inspect:host:port', 'inspect-brk:host:port', or 'inspect-wait:host:port', got: {}",
      s
    );
  }

  let mode = parts[0];
  let host = parts[1];
  let port_str = parts[2];

  let addr: SocketAddr = format!("{}:{}", host, port_str)
    .parse()
    .map_err(|e| anyhow::anyhow!("Failed to parse socket address: {}", e))?;

  match mode {
    "inspect" => Ok(InspectorOption::Inspect(addr)),
    "inspect-brk" => Ok(InspectorOption::WithBreak(addr)),
    "inspect-wait" => Ok(InspectorOption::WithWait(addr)),
    _ => bail!(
      "Invalid inspector mode '{}'. Expected 'inspect', 'inspect-brk', or 'inspect-wait'",
      mode
    ),
  }
}

fn init_logging() {
  if rustls::crypto::ring::default_provider()
    .install_default()
    .is_err()
  {
    return;
  }
  if LOG_INIT.swap(true, Ordering::Relaxed) {}
}

pub struct TrexServerManagerWrapper {
  manager: &'static ServerManager,
}

impl TrexServerManagerWrapper {
  pub fn new() -> Self {
    Self {
      manager: get_global_server_manager(),
    }
  }

  pub fn get_version(&self) -> String {
    get_version()
  }

  pub fn start_server_sync(&self, config: ServerConfig) -> Result<String> {
    init_logging();
    self.start_server_persistent(config)
  }

  fn start_server_persistent(&self, config: ServerConfig) -> Result<String> {
    use base::server::Builder;
    use std::sync::mpsc;

    let server_id = format!(
      "trex_{}_{}",
      config.addr.port(),
      chrono::Utc::now().timestamp()
    );
    let server_id_clone = server_id.clone();
    let config_clone = config.clone();

    let (result_tx, result_rx) = mpsc::channel();

    let thread_handle = thread::spawn(move || {
      init_logging();

      let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .thread_name("trex-server")
        .build()
      {
        Ok(rt) => rt,
        Err(e) => {
          let _ = result_tx
            .send(Err(anyhow::anyhow!("Failed to create runtime: {}", e)));
          return;
        }
      };

      let local = tokio::task::LocalSet::new();
      let result: Result<()> = local.block_on(&runtime, async {
        let mut builder =
          Builder::new(config_clone.addr, &config_clone.main_service_path);

        if let (Some(cert_path), Some(key_path)) =
          (&config_clone.tls_cert_path, &config_clone.tls_key_path)
        {
          let tls_port = config_clone.tls_port.unwrap_or(443);
          if let Ok(tls) =
            Self::create_tls_config_static(cert_path, key_path, tls_port)
          {
            builder.tls(tls);
          }
        }

        if let Some(event_worker_path) = &config_clone.event_worker_path {
          builder.event_worker_path(event_worker_path);
        }

        if let Some(ref user_worker_policy) = config_clone.user_worker_policy {
          builder.user_worker_policy(user_worker_policy.clone());
        }

        if !config_clone.static_patterns.is_empty() {
          for pattern in &config_clone.static_patterns {
            builder.add_static_pattern(pattern);
          }
        }

        if let Some(inspector) = config_clone.inspector {
          builder.inspector(inspector);
        }

        let mut flags = config_clone.to_server_flags();
        flags.no_module_cache = true;
        *builder.flags_mut() = flags;

        let entrypoints = config_clone.to_worker_entrypoints();
        *builder.entrypoints_mut() = entrypoints;

        get_global_server_manager()
          .register_server(server_id_clone.clone(), config_clone.clone())?;

        match builder.build().await {
          Ok(mut server) => {
            use std::io::Write;
            let _ = std::io::stdout().flush();

            // Signal that server is built and ready
            let _ = result_tx.send(Ok("Server starting".to_string()));

            eprintln!("[TREX-EXT] Server listening on {}", config_clone.addr);

            let _ = server.listen().await;

            eprintln!("[TREX-EXT] Server stopped listening");
          }
          Err(e) => {
            eprintln!("[TREX-EXT] Failed to build server: {}", e);
            eprintln!("[TREX-EXT] Error chain:");
            for (i, cause) in e.chain().enumerate() {
              eprintln!("[TREX-EXT]   {}: {}", i, cause);
            }
            let _ = result_tx
              .send(Err(anyhow::anyhow!("Failed to build server: {}", e)));
          }
        }

        let _ = get_global_server_manager().unregister_server(&server_id_clone);
        Ok(())
      });

      if let Err(e) = result {
        eprintln!("[TREX-EXT] Server thread error: {}", e);
      } else {
        eprintln!("[TREX-EXT] Server thread completed successfully");
      }
    });

    if let Ok(mut threads) = SERVER_THREADS.lock() {
      threads.insert(server_id.clone(), thread_handle);
    }

    match result_rx.recv_timeout(std::time::Duration::from_secs(180)) {
      Ok(Ok(_)) => Ok(format!("Started Trex server: {}", server_id)),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(anyhow::anyhow!("Server start timeout")),
    }
  }

  fn create_tls_config_static(
    cert_path: &str,
    key_path: &str,
    port: u16,
  ) -> Result<base::server::Tls> {
    use std::fs;

    let cert_data = fs::read(cert_path)
      .map_err(|e| anyhow::anyhow!("Failed to read certificate file: {}", e))?;
    let key_data = fs::read(key_path)
      .map_err(|e| anyhow::anyhow!("Failed to read private key file: {}", e))?;

    base::server::Tls::new(port, &key_data, &cert_data)
  }
}

impl TrexServerManagerWrapper {
  pub fn stop_server(&self, server_id: &str) -> Result<String> {
    self.manager.unregister_server(server_id)?;

    if let Ok(mut threads) = SERVER_THREADS.lock() {
      threads.remove(server_id);
    }

    Ok(format!("Stopped Trex server: {}", server_id))
  }

  pub fn stop_all_servers(&self) -> Result<usize> {
    let count = if let Ok(mut threads) = SERVER_THREADS.lock() {
      let count = threads.len();
      threads.clear();
      count
    } else {
      0
    };

    let _ = self.manager.stop_all_servers();
    Ok(count)
  }

  pub fn list_servers(&self) -> Vec<(String, ServerHandle)> {
    match self.manager.list_servers() {
      Ok(servers) => servers
        .into_iter()
        .map(|(id, config, _status)| {
          let handle = ServerHandle {
            config,
            started_at: chrono::Utc::now(),
          };
          (id, handle)
        })
        .collect(),
      Err(_) => Vec::new(),
    }
  }
}

// Create a global manager instance
pub static TREX_MANAGER: LazyLock<TrexServerManagerWrapper> =
  LazyLock::new(|| {
    init_logging();
    TrexServerManagerWrapper::new()
  });

/// Server handle for the extension API
#[derive(Debug, Clone)]
pub struct ServerHandle {
  pub config: ServerConfig,
  pub started_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrexServerConfig {
  #[serde(default = "default_host")]
  pub host: String,
  #[serde(default = "default_port")]
  pub port: u16,
  #[serde(default = "default_main_service_path")]
  pub main_service_path: String,
  #[serde(default)]
  pub event_worker_path: Option<String>,
  #[serde(default)]
  pub tls_cert_path: Option<String>,
  #[serde(default)]
  pub tls_key_path: Option<String>,
  #[serde(default)]
  pub tls_port: Option<u16>,
  #[serde(default)]
  pub static_patterns: Vec<String>,
  #[serde(default)]
  pub user_worker_policy: Option<String>,
  #[serde(default)]
  pub inspector: Option<String>,
  #[serde(default)]
  pub no_module_cache: bool,
  #[serde(default)]
  pub allow_main_inspector: bool,
  #[serde(default = "default_tcp_nodelay")]
  pub tcp_nodelay: bool,
  #[serde(default = "default_graceful_exit_deadline_sec")]
  pub graceful_exit_deadline_sec: u64,
  #[serde(default)]
  pub graceful_exit_keepalive_deadline_ms: Option<u64>,
  #[serde(default = "default_event_worker_exit_deadline_sec")]
  pub event_worker_exit_deadline_sec: u64,
  #[serde(default)]
  pub request_wait_timeout_ms: Option<u64>,
  #[serde(default)]
  pub request_idle_timeout_ms: Option<u64>,
  #[serde(default)]
  pub request_read_timeout_ms: Option<u64>,
  #[serde(default)]
  pub request_buffer_size: Option<usize>,
  #[serde(default)]
  pub beforeunload_wall_clock_pct: Option<f64>,
  #[serde(default)]
  pub beforeunload_cpu_pct: Option<f64>,
  #[serde(default)]
  pub beforeunload_memory_pct: Option<f64>,
  #[serde(default)]
  pub import_map_path: Option<String>,
  #[serde(default)]
  pub jsx_specifier: Option<String>,
  #[serde(default)]
  pub jsx_module: Option<String>,
  #[serde(default)]
  pub worker_pool_max_size: Option<usize>,
  #[serde(default)]
  pub worker_memory_limit_mb: Option<usize>,
  #[serde(default)]
  pub decorator: bool,
}

fn default_host() -> String {
  "0.0.0.0".to_string()
}
fn default_port() -> u16 {
  8080
}
fn default_main_service_path() -> String {
  "main.ts".to_string()
}
fn default_tcp_nodelay() -> bool {
  true
}
fn default_graceful_exit_deadline_sec() -> u64 {
  30
}
fn default_event_worker_exit_deadline_sec() -> u64 {
  30
}

impl TrexServerConfig {
  pub fn into_server_config(self) -> Result<ServerConfig> {
    let addr: SocketAddr = format!("{}:{}", self.host, self.port)
      .parse()
      .map_err(|e| anyhow::anyhow!("Invalid address format: {}", e))?;

    // Normalize paths to file:// URLs for proper Deno module resolution
    let main_service_path_normalized =
      normalize_path_to_file_url(&self.main_service_path);

    let event_worker_path_normalized =
      self.event_worker_path.and_then(|path| {
        if path.is_empty() {
          None
        } else {
          Some(normalize_path_to_file_url(&path))
        }
      });

    // Parse inspector option from string if provided
    let inspector_option = if let Some(ref inspector_str) = self.inspector {
      Some(parse_inspector_option(inspector_str)?)
    } else {
      None
    };

    // Note: user_worker_policy is a complex type that doesn't support
    // JSON deserialization directly. For now, it is not supported via JSON config.
    // Users can use the direct function call if needed.

    Ok(ServerConfig {
      addr,
      main_service_path: main_service_path_normalized,
      event_worker_path: event_worker_path_normalized,
      user_worker_policy: None, // Complex type, not supported in JSON config
      tls_cert_path: self.tls_cert_path,
      tls_key_path: self.tls_key_path,
      tls_port: self.tls_port,
      static_patterns: self.static_patterns,
      inspector: inspector_option,
      no_module_cache: self.no_module_cache,
      allow_main_inspector: self.allow_main_inspector,
      tcp_nodelay: self.tcp_nodelay,
      graceful_exit_deadline_sec: self.graceful_exit_deadline_sec,
      graceful_exit_keepalive_deadline_ms: self
        .graceful_exit_keepalive_deadline_ms,
      event_worker_exit_deadline_sec: self.event_worker_exit_deadline_sec,
      request_wait_timeout_ms: self.request_wait_timeout_ms,
      request_idle_timeout: RequestIdleTimeout::from_millis(
        self.request_idle_timeout_ms,
        self.request_idle_timeout_ms,
      ),
      request_read_timeout_ms: self.request_read_timeout_ms,
      request_buffer_size: self.request_buffer_size.map(|s| s as u64),
      beforeunload_wall_clock_pct: self
        .beforeunload_wall_clock_pct
        .map(|p| p as u8),
      beforeunload_cpu_pct: self.beforeunload_cpu_pct.map(|p| p as u8),
      beforeunload_memory_pct: self.beforeunload_memory_pct.map(|p| p as u8),
      import_map_path: self.import_map_path,
      jsx_specifier: self.jsx_specifier,
      jsx_module: self.jsx_module,
      worker_pool_max_size: self.worker_pool_max_size,
      worker_memory_limit_mb: self.worker_memory_limit_mb,
      decorator: self.decorator,
    })
  }
}
