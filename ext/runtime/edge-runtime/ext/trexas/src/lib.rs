extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;
extern crate trex_core;

use duckdb::{
  core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
  vscalar::{ScalarFunctionSignature, VScalar},
  vtab::{arrow::WritableVector, BindInfo, InitInfo, TableFunctionInfo, VTab},
  Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use std::{
  error::Error,
  ffi::CString,
  path::Path,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, OnceLock as OnceCell,
  },
};
use tracing::warn;

static SHARED_CONNECTION: OnceCell<Arc<Mutex<Connection>>> = OnceCell::new();

fn store_shared_connection(
  connection: &Connection,
) -> Result<(), Box<dyn Error>> {
  if let Err(e) = trex_core::connection::init_query_executor(connection) {
    warn!(error = %e, "query executor init failed (may already exist)");
  }

  let cloned = connection
    .try_clone()
    .map_err(|e| format!("connection clone: {e}"))?;

  SHARED_CONNECTION
    .set(Arc::new(Mutex::new(cloned)))
    .map_err(|_| "connection already stored")?;

  Ok(())
}

fn get_shared_connection() -> Option<Arc<Mutex<Connection>>> {
  SHARED_CONNECTION.get().cloned()
}

mod bundle;
mod trex_server;

use bundle::{create_bundle_sync, BundleOptions};
use trex_server::{TrexServerConfig, TREX_MANAGER};

fn normalize_path(path: &str) -> String {
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

struct TrexVersionScalar;

impl VScalar for TrexVersionScalar {
  type State = ();

  unsafe fn invoke(
    _state: &Self::State,
    input: &mut DataChunkHandle,
    output: &mut dyn WritableVector,
  ) -> Result<(), Box<dyn std::error::Error>> {
    if !input.is_empty() {
      let version = TREX_MANAGER.get_version();
      let flat_vector = output.flat_vector();
      flat_vector.insert(0, &format!("trex extension v{}", version));
    }
    Ok(())
  }

  fn signatures() -> Vec<ScalarFunctionSignature> {
    vec![ScalarFunctionSignature::exact(
      vec![],
      LogicalTypeId::Varchar.into(),
    )]
  }
}

struct StartTrexServerScalar;

impl VScalar for StartTrexServerScalar {
  type State = ();

  unsafe fn invoke(
    _state: &Self::State,
    input: &mut DataChunkHandle,
    output: &mut dyn WritableVector,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let host_vector = input.flat_vector(0);
    let port_vector = input.flat_vector(1);
    let main_service_vector = input.flat_vector(2);
    let event_worker_vector = input.flat_vector(3);

    let host_slice = host_vector
      .as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
    let port_slice = port_vector.as_slice_with_len::<i32>(input.len());
    let main_service_slice = main_service_vector
      .as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
    let event_worker_slice = event_worker_vector
      .as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

    if input.is_empty() {
      return Err("No input provided".into());
    }

    let host = duckdb::types::DuckString::new(&mut { host_slice[0] })
      .as_str()
      .to_string();
    let port = port_slice[0] as u16;
    let main_service_path =
      duckdb::types::DuckString::new(&mut { main_service_slice[0] })
        .as_str()
        .to_string();
    let event_worker_path =
      duckdb::types::DuckString::new(&mut { event_worker_slice[0] })
        .as_str()
        .to_string();

    let addr: std::net::SocketAddr = format!("{}:{}", host, port)
      .parse()
      .unwrap_or_else(|_| "127.0.0.1:8000".parse().unwrap());

    let main_service_path_normalized = normalize_path(&main_service_path);

    let event_worker_opt = if event_worker_path.is_empty() {
      None
    } else {
      Some(normalize_path(&event_worker_path))
    };

    let config = trex_server::ServerConfig {
      addr,
      main_service_path: main_service_path_normalized,
      event_worker_path: event_worker_opt,
      user_worker_policy: None,
      tls_cert_path: None,
      tls_key_path: None,
      tls_port: None,
      static_patterns: vec![],
      inspector: None,
      no_module_cache: false,
      allow_main_inspector: false,
      tcp_nodelay: true,
      graceful_exit_deadline_sec: 30,
      graceful_exit_keepalive_deadline_ms: None,
      event_worker_exit_deadline_sec: 30,
      request_wait_timeout_ms: None,
      request_idle_timeout: Default::default(),
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
    };

    let response = match TREX_MANAGER.start_server_sync(config) {
      Ok(server_id) => format!("Trex server started: {}", server_id),
      Err(err) => format!("Error: {:#}", err),
    };

    let flat_vector = output.flat_vector();
    flat_vector.insert(0, &response);
    Ok(())
  }

  fn signatures() -> Vec<ScalarFunctionSignature> {
    vec![ScalarFunctionSignature::exact(
      vec![
        LogicalTypeId::Varchar.into(),
        LogicalTypeId::Integer.into(),
        LogicalTypeId::Varchar.into(),
        LogicalTypeId::Varchar.into(),
      ],
      LogicalTypeId::Varchar.into(),
    )]
  }
}

struct StartTrexServerWithConfigScalar;

impl VScalar for StartTrexServerWithConfigScalar {
  type State = ();

  unsafe fn invoke(
    _state: &Self::State,
    input: &mut DataChunkHandle,
    output: &mut dyn WritableVector,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let config_vector = input.flat_vector(0);
    let config_slice = config_vector
      .as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

    if input.is_empty() {
      return Err("No input provided".into());
    }

    let config_json_str =
      duckdb::types::DuckString::new(&mut { config_slice[0] })
        .as_str()
        .to_string();

    let response =
      match serde_json::from_str::<TrexServerConfig>(&config_json_str) {
        Ok(config_struct) => match config_struct.into_server_config() {
          Ok(server_config) => {
            match TREX_MANAGER.start_server_sync(server_config) {
              Ok(server_id) => format!("Trex server started: {}", server_id),
              Err(err) => format!("Error starting server: {}", err),
            }
          }
          Err(err) => format!("Error converting config: {}", err),
        },
        Err(err) => format!("Error parsing JSON config: {}", err),
      };

    let flat_vector = output.flat_vector();
    flat_vector.insert(0, &response);
    Ok(())
  }

  fn signatures() -> Vec<ScalarFunctionSignature> {
    vec![ScalarFunctionSignature::exact(
      vec![LogicalTypeId::Varchar.into()],
      LogicalTypeId::Varchar.into(),
    )]
  }
}

struct StopTrexServerScalar;

impl VScalar for StopTrexServerScalar {
  type State = ();

  unsafe fn invoke(
    _state: &Self::State,
    input: &mut DataChunkHandle,
    output: &mut dyn WritableVector,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let server_id_vector = input.flat_vector(0);
    let server_id_slice = server_id_vector
      .as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

    if input.is_empty() {
      return Err("No input provided".into());
    }

    let server_id = duckdb::types::DuckString::new(&mut { server_id_slice[0] })
      .as_str()
      .to_string();

    let response = match TREX_MANAGER.stop_server(&server_id) {
      Ok(_) => format!("Trex server {} stopped successfully", server_id),
      Err(err) => format!("Error stopping server: {}", err),
    };

    let flat_vector = output.flat_vector();
    flat_vector.insert(0, &response);
    Ok(())
  }

  fn signatures() -> Vec<ScalarFunctionSignature> {
    vec![ScalarFunctionSignature::exact(
      vec![LogicalTypeId::Varchar.into()],
      LogicalTypeId::Varchar.into(),
    )]
  }
}

struct StopAllTrexServersScalar;

impl VScalar for StopAllTrexServersScalar {
  type State = ();

  unsafe fn invoke(
    _state: &Self::State,
    _input: &mut DataChunkHandle,
    output: &mut dyn WritableVector,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let response = match TREX_MANAGER.stop_all_servers() {
      Ok(count) => format!("Stopped {} Trex server(s)", count),
      Err(err) => format!("Error stopping servers: {}", err),
    };

    let flat_vector = output.flat_vector();
    flat_vector.insert(0, &response);
    Ok(())
  }

  fn signatures() -> Vec<ScalarFunctionSignature> {
    vec![ScalarFunctionSignature::exact(
      vec![],
      LogicalTypeId::Varchar.into(),
    )]
  }
}

struct TrexServersTable;

#[repr(C)]
struct TrexServersBindData {}

#[repr(C)]
struct TrexServersInitData {
  done: AtomicBool,
}

impl VTab for TrexServersTable {
  type InitData = TrexServersInitData;
  type BindData = TrexServersBindData;

  fn bind(
    bind: &BindInfo,
  ) -> Result<Self::BindData, Box<dyn std::error::Error>> {
    bind.add_result_column(
      "server_id",
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    );
    bind
      .add_result_column("ip", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column(
      "port",
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    );
    bind.add_result_column(
      "main_service_path",
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    );
    bind.add_result_column(
      "event_worker_path",
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    );
    bind.add_result_column(
      "started_at",
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    );
    bind.add_result_column(
      "policy",
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    );
    bind.add_result_column(
      "status",
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    );
    Ok(TrexServersBindData {})
  }

  fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
    Ok(TrexServersInitData {
      done: AtomicBool::new(false),
    })
  }

  fn func(
    func: &TableFunctionInfo<Self>,
    output: &mut DataChunkHandle,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let init_data = func.get_init_data();

    if init_data.done.swap(true, Ordering::Relaxed) {
      output.set_len(0);
      return Ok(());
    }

    let servers = TREX_MANAGER.list_servers();
    let server_count = servers.len();

    if server_count == 0 {
      output.set_len(0);
      return Ok(());
    }

    let server_id_vector = output.flat_vector(0);
    let ip_vector = output.flat_vector(1);
    let port_vector = output.flat_vector(2);
    let main_service_vector = output.flat_vector(3);
    let event_worker_vector = output.flat_vector(4);
    let started_at_vector = output.flat_vector(5);
    let policy_vector = output.flat_vector(6);
    let status_vector = output.flat_vector(7);

    for (i, (server_id, handle)) in servers.iter().enumerate() {
      let server_id_cstring = CString::new(server_id.as_str())?;
      let ip_cstring = CString::new(handle.config.addr.ip().to_string())?;
      let port_cstring = CString::new(handle.config.addr.port().to_string())?;
      let main_service_cstring =
        CString::new(handle.config.main_service_path.as_str())?;
      let event_worker_cstring = CString::new(
        handle.config.event_worker_path.as_deref().unwrap_or("none"),
      )?;
      let started_at_cstring = CString::new(
        handle
          .started_at
          .format("%Y-%m-%d %H:%M:%S UTC")
          .to_string(),
      )?;
      let policy_cstring = CString::new(
        handle
          .config
          .user_worker_policy
          .as_ref()
          .map(|_| "user-defined")
          .unwrap_or("default"),
      )?;
      let status_cstring = CString::new("running")?;

      server_id_vector.insert(i, server_id_cstring);
      ip_vector.insert(i, ip_cstring);
      port_vector.insert(i, port_cstring);
      main_service_vector.insert(i, main_service_cstring);
      event_worker_vector.insert(i, event_worker_cstring);
      started_at_vector.insert(i, started_at_cstring);
      policy_vector.insert(i, policy_cstring);
      status_vector.insert(i, status_cstring);
    }

    output.set_len(server_count);
    Ok(())
  }

  fn parameters() -> Option<Vec<LogicalTypeHandle>> {
    None
  }
}

struct TrexCreateBundleScalar;

impl VScalar for TrexCreateBundleScalar {
  type State = ();

  unsafe fn invoke(
    _state: &Self::State,
    input: &mut DataChunkHandle,
    output: &mut dyn WritableVector,
  ) -> Result<(), Box<dyn std::error::Error>> {
    if input.is_empty() {
      return Err("No input provided".into());
    }

    let entrypoint_vector = input.flat_vector(0);
    let output_path_vector = input.flat_vector(1);

    let entrypoint_slice = entrypoint_vector
      .as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
    let output_slice = output_path_vector
      .as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

    let entrypoint =
      duckdb::types::DuckString::new(&mut { entrypoint_slice[0] })
        .as_str()
        .to_string();
    let output_path = duckdb::types::DuckString::new(&mut { output_slice[0] })
      .as_str()
      .to_string();

    let options = if input.num_columns() >= 3 {
      let options_vector = input.flat_vector(2);
      let options_slice = options_vector
        .as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
      let options_json =
        duckdb::types::DuckString::new(&mut { options_slice[0] })
          .as_str()
          .to_string();

      if options_json.is_empty() {
        None
      } else {
        match serde_json::from_str::<BundleOptions>(&options_json) {
          Ok(opts) => Some(opts),
          Err(e) => {
            let error_msg = format!("Failed to parse options JSON: {}", e);
            let flat_vector = output.flat_vector();
            flat_vector.insert(0, &error_msg);
            return Ok(());
          }
        }
      }
    } else {
      None
    };

    let response = match create_bundle_sync(&entrypoint, &output_path, options)
    {
      Ok(msg) => msg,
      Err(err) => format!("Error: {:#}", err),
    };

    let flat_vector = output.flat_vector();
    flat_vector.insert(0, &response);
    Ok(())
  }

  fn signatures() -> Vec<ScalarFunctionSignature> {
    vec![
      // 2-argument version: trex_create_bundle(entrypoint, output)
      ScalarFunctionSignature::exact(
        vec![LogicalTypeId::Varchar.into(), LogicalTypeId::Varchar.into()],
        LogicalTypeId::Varchar.into(),
      ),
      // 3-argument version: trex_create_bundle(entrypoint, output, options_json)
      ScalarFunctionSignature::exact(
        vec![
          LogicalTypeId::Varchar.into(),
          LogicalTypeId::Varchar.into(),
          LogicalTypeId::Varchar.into(),
        ],
        LogicalTypeId::Varchar.into(),
      ),
    ]
  }
}

/// # Safety
///
/// Called by DuckDB via `duckdb_entrypoint_c_api`. The connection must be valid.
#[duckdb_entrypoint_c_api(ext_name = "trexas")]
pub unsafe fn extension_entrypoint(
  con: Connection,
) -> Result<(), Box<dyn Error>> {
  store_shared_connection(&con)?;

  if let Some(shared_conn) = get_shared_connection() {
    if let Err(e) = trex_core::connection::init_shared_connection(shared_conn) {
      warn!(error = %e, "trex shared connection init failed");
    }
  }

  con.register_scalar_function::<TrexVersionScalar>("trex_runtime_version")?;
  con.register_scalar_function::<TrexVersionScalar>("trex_version")?;
  con
    .register_scalar_function::<StartTrexServerScalar>("trex_runtime_start")?;
  con.register_scalar_function::<StartTrexServerScalar>("trex_start_server")?;
  con.register_scalar_function::<StartTrexServerWithConfigScalar>(
    "trex_runtime_start_with_config",
  )?;
  con.register_scalar_function::<StartTrexServerWithConfigScalar>(
    "trex_start_server_with_config",
  )?;
  con.register_scalar_function::<StopTrexServerScalar>("trex_runtime_stop")?;
  con.register_scalar_function::<StopTrexServerScalar>("trex_stop_server")?;
  con.register_scalar_function::<StopAllTrexServersScalar>(
    "trex_runtime_stop_all",
  )?;
  con.register_scalar_function::<StopAllTrexServersScalar>(
    "trex_stop_all_servers",
  )?;
  con.register_scalar_function::<TrexCreateBundleScalar>(
    "trex_runtime_create_bundle",
  )?;
  con
    .register_scalar_function::<TrexCreateBundleScalar>("trex_create_bundle")?;
  con.register_table_function::<TrexServersTable>("trex_runtime_list")?;
  con.register_table_function::<TrexServersTable>("trex_list_servers")?;

  Ok(())
}
