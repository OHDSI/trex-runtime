use anyhow::{bail, Result};
use base::server::{RequestIdleTimeout, ServerFlags, WorkerEntrypoints};
use base::worker::pool::{SupervisorPolicy, WorkerPoolPolicy};
use base::InspectorOption;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread;

#[derive(Clone)]
pub struct ServerConfig {
  pub addr: SocketAddr,
  pub main_service_path: String,
  pub event_worker_path: Option<String>,
  pub user_worker_policy: Option<WorkerPoolPolicy>,
  pub tls_cert_path: Option<String>,
  pub tls_key_path: Option<String>,
  pub tls_port: Option<u16>,
  pub static_patterns: Vec<String>,
  pub inspector: Option<InspectorOption>,
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
  pub import_map_path: Option<String>,
  pub jsx_specifier: Option<String>,
  pub jsx_module: Option<String>,
  pub worker_pool_max_size: Option<usize>,
  pub worker_memory_limit_mb: Option<usize>,
  pub decorator: bool,
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

  pub fn to_worker_entrypoints(&self) -> WorkerEntrypoints {
    WorkerEntrypoints {
      main: Some(self.main_service_path.clone()),
      events: self.event_worker_path.clone(),
    }
  }
}

pub struct ServerManager {
  servers: Arc<Mutex<HashMap<String, ServerInfo>>>,
}

#[derive(Debug, Clone)]
struct ServerInfo {
  config: ServerConfig,
  status: String,
  _started_at: chrono::DateTime<chrono::Utc>,
}

impl Default for ServerManager {
  fn default() -> Self {
    Self {
      servers: Arc::new(Mutex::new(HashMap::new())),
    }
  }
}

impl ServerManager {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn register_server(
    &self,
    id: String,
    config: ServerConfig,
  ) -> Result<()> {
    let mut servers = self.servers.lock().unwrap();
    servers.insert(
      id,
      ServerInfo {
        config,
        status: "running".to_string(),
        _started_at: chrono::Utc::now(),
      },
    );
    Ok(())
  }

  pub fn unregister_server(&self, id: &str) -> Result<()> {
    let mut servers = self.servers.lock().unwrap();
    servers.remove(id);
    Ok(())
  }

  pub fn list_servers(&self) -> Result<Vec<(String, ServerConfig, String)>> {
    let servers = self.servers.lock().unwrap();
    let result = servers
      .iter()
      .map(|(id, info)| (id.clone(), info.config.clone(), info.status.clone()))
      .collect();
    Ok(result)
  }

  pub fn stop_all_servers(&self) -> Result<usize> {
    let mut servers = self.servers.lock().unwrap();
    let count = servers.len();
    servers.clear();
    Ok(count)
  }
}

static GLOBAL_SERVER_MANAGER: LazyLock<ServerManager> =
  LazyLock::new(ServerManager::new);

pub fn get_global_server_manager() -> &'static ServerManager {
  &GLOBAL_SERVER_MANAGER
}

pub fn get_version() -> String {
  env!("CARGO_PKG_VERSION").to_string()
}

static LOG_INIT: AtomicBool = AtomicBool::new(false);

type ServerThreads = Arc<Mutex<HashMap<String, thread::JoinHandle<()>>>>;

static SERVER_THREADS: LazyLock<ServerThreads> =
  LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

fn init_logging() {
  if LOG_INIT.swap(true, Ordering::Relaxed) {
    return;
  }

  let _ = rustls::crypto::ring::default_provider().install_default();

  let debug_gc = std::env::var("TREX_DEBUG_GC").is_ok();
  let rust_log = std::env::var("RUST_LOG").ok();

  if debug_gc || rust_log.is_some() {
    let level = if debug_gc {
      "debug"
    } else {
      rust_log.as_deref().unwrap_or("info")
    };

    let _ = env_logger::Builder::from_env(
      env_logger::Env::default().default_filter_or(level),
    )
    .format_timestamp_millis()
    .try_init();

    if debug_gc {
      eprintln!("[TREX-EXT] GC debugging enabled via TREX_DEBUG_GC");
    }
  }
}

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

  if path.ends_with(".eszip") {
    return abs_path.display().to_string();
  }

  let final_path = if abs_path.is_dir() {
    abs_path.join("index.ts")
  } else {
    abs_path
  };

  format!("file://{}", final_path.display())
}

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

        if !config_clone.main_service_path.ends_with(".eszip") {
          let entrypoints = config_clone.to_worker_entrypoints();
          *builder.entrypoints_mut() = entrypoints;
        }

        get_global_server_manager()
          .register_server(server_id_clone.clone(), config_clone.clone())?;

        match builder.build().await {
          Ok(mut server) => {
            use std::io::Write;
            let _ = std::io::stdout().flush();

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

pub static TREX_MANAGER: LazyLock<TrexServerManagerWrapper> =
  LazyLock::new(|| {
    init_logging();
    TrexServerManagerWrapper::new()
  });

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
  pub max_parallelism: Option<usize>,
  #[serde(default)]
  pub global_max_parallelism: Option<usize>,
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
  pub worker_memory_limit_mb: Option<usize>,
  #[serde(default)]
  pub decorator: bool,
  #[serde(default)]
  pub restrict_host_fs: bool,
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

    let inspector_option = if let Some(ref inspector_str) = self.inspector {
      Some(parse_inspector_option(inspector_str)?)
    } else {
      None
    };

    let supervisor_policy = self.user_worker_policy.as_ref().map(|s| {
      s.parse::<SupervisorPolicy>()
        .unwrap_or(SupervisorPolicy::PerWorker)
    });

    let server_flags = ServerFlags {
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
      restrict_host_fs: self.restrict_host_fs,
    };

    let user_worker_policy = if supervisor_policy.is_some()
      || self.max_parallelism.is_some()
      || self.global_max_parallelism.is_some()
    {
      let max_parallelism =
        if supervisor_policy.as_ref().is_some_and(|p| p.is_oneshot()) {
          Some(1)
        } else {
          self.max_parallelism
        };
      Some(
        WorkerPoolPolicy::new(supervisor_policy, max_parallelism, server_flags)
          .with_global_limit(self.global_max_parallelism),
      )
    } else {
      None
    };

    Ok(ServerConfig {
      addr,
      main_service_path: main_service_path_normalized,
      event_worker_path: event_worker_path_normalized,
      user_worker_policy,
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
      worker_pool_max_size: self.max_parallelism,
      worker_memory_limit_mb: self.worker_memory_limit_mb,
      decorator: self.decorator,
      restrict_host_fs: self.restrict_host_fs,
    })
  }
}
