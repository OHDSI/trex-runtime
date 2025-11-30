extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

mod npm;

use duckdb::{
  core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
  vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
  Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use std::{
  error::Error,
  ffi::CString,
  sync::atomic::{AtomicBool, Ordering},
};

#[repr(C)]
struct HelloBindData {
  name: String,
}

#[repr(C)]
struct HelloInitData {
  done: AtomicBool,
}

struct HelloVTab;

impl VTab for HelloVTab {
  type InitData = HelloInitData;
  type BindData = HelloBindData;

  fn bind(
    bind: &BindInfo,
  ) -> Result<Self::BindData, Box<dyn std::error::Error>> {
    bind.add_result_column(
      "column0",
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    );
    let name = bind.get_parameter(0).to_string();
    Ok(HelloBindData { name })
  }

  fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
    Ok(HelloInitData {
      done: AtomicBool::new(false),
    })
  }

  fn func(
    func: &TableFunctionInfo<Self>,
    output: &mut DataChunkHandle,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let init_data = func.get_init_data();
    let bind_data = func.get_bind_data();
    if init_data.done.swap(true, Ordering::Relaxed) {
      output.set_len(0);
    } else {
      let vector = output.flat_vector(0);
      let result = CString::new(format!("TPM {} 📦", bind_data.name))?;
      vector.insert(0, result);
      output.set_len(1);
    }
    Ok(())
  }

  fn parameters() -> Option<Vec<LogicalTypeHandle>> {
    Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
  }
}

#[repr(C)]
struct TpmInfoBindData {
  package_name: String,
  registry_url: Option<String>,
}

#[repr(C)]
struct TpmInfoInitData {
  done: AtomicBool,
}

struct TpmInfoVTab;

impl VTab for TpmInfoVTab {
  type InitData = TpmInfoInitData;
  type BindData = TpmInfoBindData;

  fn bind(
    bind: &BindInfo,
  ) -> Result<Self::BindData, Box<dyn std::error::Error>> {
    // Output column: JSON string with package info
    bind.add_result_column(
      "package_info",
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    );
    let package_name = bind.get_parameter(0).to_string();
    // Check for TPM_REGISTRY_URL environment variable
    let registry_url = std::env::var("TPM_REGISTRY_URL").ok();
    Ok(TpmInfoBindData {
      package_name,
      registry_url,
    })
  }

  fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
    Ok(TpmInfoInitData {
      done: AtomicBool::new(false),
    })
  }

  fn func(
    func: &TableFunctionInfo<Self>,
    output: &mut DataChunkHandle,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let init_data = func.get_init_data();
    let bind_data = func.get_bind_data();

    if init_data.done.swap(true, Ordering::Relaxed) {
      output.set_len(0);
      return Ok(());
    }

    let registry =
      npm::NpmRegistry::with_registry_url(bind_data.registry_url.clone())?;
    let package_info = registry.get_package_info(&bind_data.package_name);

    match package_info {
      Ok(info) => {
        let json = serde_json::to_string(&info)?;
        let vector = output.flat_vector(0);
        let result = CString::new(json)?;
        vector.insert(0, result);
        output.set_len(1);
      }
      Err(e) => {
        let error_json = serde_json::json!({
            "error": e.to_string(),
            "package": bind_data.package_name
        });
        let json = serde_json::to_string(&error_json)?;
        let vector = output.flat_vector(0);
        let result = CString::new(json)?;
        vector.insert(0, result);
        output.set_len(1);
      }
    }

    Ok(())
  }

  fn parameters() -> Option<Vec<LogicalTypeHandle>> {
    Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
  }
}

#[repr(C)]
struct TpmResolveBindData {
  package_spec: String,
  registry_url: Option<String>,
}

#[repr(C)]
struct TpmResolveInitData {
  done: AtomicBool,
}

struct TpmResolveVTab;

impl VTab for TpmResolveVTab {
  type InitData = TpmResolveInitData;
  type BindData = TpmResolveBindData;

  fn bind(
    bind: &BindInfo,
  ) -> Result<Self::BindData, Box<dyn std::error::Error>> {
    bind.add_result_column(
      "resolve_info",
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    );
    let package_spec = bind.get_parameter(0).to_string();
    // Check for TPM_REGISTRY_URL environment variable
    let registry_url = std::env::var("TPM_REGISTRY_URL").ok();
    Ok(TpmResolveBindData {
      package_spec,
      registry_url,
    })
  }

  fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
    Ok(TpmResolveInitData {
      done: AtomicBool::new(false),
    })
  }

  fn func(
    func: &TableFunctionInfo<Self>,
    output: &mut DataChunkHandle,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let init_data = func.get_init_data();
    let bind_data = func.get_bind_data();

    if init_data.done.swap(true, Ordering::Relaxed) {
      output.set_len(0);
      return Ok(());
    }

    let registry =
      npm::NpmRegistry::with_registry_url(bind_data.registry_url.clone())?;
    let result = registry.resolve_package(&bind_data.package_spec);

    match result {
      Ok(info) => {
        let json = serde_json::to_string(&info)?;
        let vector = output.flat_vector(0);
        let result = CString::new(json)?;
        vector.insert(0, result);
        output.set_len(1);
      }
      Err(e) => {
        let error_json = serde_json::json!({
            "error": e.to_string(),
            "package_spec": bind_data.package_spec
        });
        let json = serde_json::to_string(&error_json)?;
        let vector = output.flat_vector(0);
        let result = CString::new(json)?;
        vector.insert(0, result);
        output.set_len(1);
      }
    }

    Ok(())
  }

  fn parameters() -> Option<Vec<LogicalTypeHandle>> {
    Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
  }
}

#[repr(C)]
struct TpmInstallBindData {
  package_spec: String,
  install_dir: String,
  registry_url: Option<String>,
}

#[repr(C)]
struct TpmInstallInitData {
  done: AtomicBool,
}

struct TpmInstallVTab;

impl VTab for TpmInstallVTab {
  type InitData = TpmInstallInitData;
  type BindData = TpmInstallBindData;

  fn bind(
    bind: &BindInfo,
  ) -> Result<Self::BindData, Box<dyn std::error::Error>> {
    bind.add_result_column(
      "install_result",
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    );
    let package_spec = bind.get_parameter(0).to_string();
    let install_dir = bind.get_parameter(1).to_string();
    // Check for TPM_REGISTRY_URL environment variable
    let registry_url = std::env::var("TPM_REGISTRY_URL").ok();
    Ok(TpmInstallBindData {
      package_spec,
      install_dir,
      registry_url,
    })
  }

  fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
    Ok(TpmInstallInitData {
      done: AtomicBool::new(false),
    })
  }

  fn func(
    func: &TableFunctionInfo<Self>,
    output: &mut DataChunkHandle,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let init_data = func.get_init_data();
    let bind_data = func.get_bind_data();

    if init_data.done.swap(true, Ordering::Relaxed) {
      output.set_len(0);
      return Ok(());
    }

    let registry =
      npm::NpmRegistry::with_registry_url(bind_data.registry_url.clone())?;
    let result =
      registry.install_package(&bind_data.package_spec, &bind_data.install_dir);

    match result {
      Ok(info) => {
        let json = serde_json::to_string(&info)?;
        let vector = output.flat_vector(0);
        let result = CString::new(json)?;
        vector.insert(0, result);
        output.set_len(1);
      }
      Err(e) => {
        let error_json = serde_json::json!({
            "error": e.to_string(),
            "package_spec": bind_data.package_spec,
            "install_dir": bind_data.install_dir
        });
        let json = serde_json::to_string(&error_json)?;
        let vector = output.flat_vector(0);
        let result = CString::new(json)?;
        vector.insert(0, result);
        output.set_len(1);
      }
    }

    Ok(())
  }

  fn parameters() -> Option<Vec<LogicalTypeHandle>> {
    Some(vec![
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    ])
  }
}

#[repr(C)]
struct TpmInstallDepsBindData {
  package_spec: String,
  install_dir: String,
  registry_url: Option<String>,
}

#[repr(C)]
struct TpmInstallDepsInitData {
  results: Vec<npm::InstallResponse>,
  index: std::sync::atomic::AtomicUsize,
}

struct TpmInstallDepsVTab;

impl VTab for TpmInstallDepsVTab {
  type InitData = TpmInstallDepsInitData;
  type BindData = TpmInstallDepsBindData;

  fn bind(
    bind: &BindInfo,
  ) -> Result<Self::BindData, Box<dyn std::error::Error>> {
    bind.add_result_column(
      "install_results",
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    );
    let package_spec = bind.get_parameter(0).to_string();
    let install_dir = bind.get_parameter(1).to_string();
    // Check for TPM_REGISTRY_URL environment variable
    let registry_url = std::env::var("TPM_REGISTRY_URL").ok();
    Ok(TpmInstallDepsBindData {
      package_spec,
      install_dir,
      registry_url,
    })
  }

  fn init(
    init: &InitInfo,
  ) -> Result<Self::InitData, Box<dyn std::error::Error>> {
    let bind_data = init.get_bind_data::<Self::BindData>();

    let registry = unsafe {
      npm::NpmRegistry::with_registry_url((*bind_data).registry_url.clone())?
    };
    let results = unsafe {
      registry.install_package_with_deps(
        &(*bind_data).package_spec,
        &(*bind_data).install_dir,
      )?
    };

    Ok(TpmInstallDepsInitData {
      results,
      index: std::sync::atomic::AtomicUsize::new(0),
    })
  }

  fn func(
    func: &TableFunctionInfo<Self>,
    output: &mut DataChunkHandle,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let init_data = func.get_init_data();
    let current_index = init_data
      .index
      .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    if current_index >= init_data.results.len() {
      output.set_len(0);
      return Ok(());
    }

    let result = &init_data.results[current_index];
    let json = serde_json::to_string(result)?;
    let vector = output.flat_vector(0);
    let cstring = CString::new(json)?;
    vector.insert(0, cstring);
    output.set_len(1);

    Ok(())
  }

  fn parameters() -> Option<Vec<LogicalTypeHandle>> {
    Some(vec![
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    ])
  }
}

#[repr(C)]
struct TpmTreeBindData {
  package_spec: String,
  registry_url: Option<String>,
}

#[repr(C)]
struct TpmTreeInitData {
  results: Vec<npm::DependencyTreeResponse>,
  index: std::sync::atomic::AtomicUsize,
}

struct TpmTreeVTab;

impl VTab for TpmTreeVTab {
  type InitData = TpmTreeInitData;
  type BindData = TpmTreeBindData;

  fn bind(
    bind: &BindInfo,
  ) -> Result<Self::BindData, Box<dyn std::error::Error>> {
    bind.add_result_column(
      "tree_info",
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    );
    let package_spec = bind.get_parameter(0).to_string();
    let registry_url = std::env::var("TPM_REGISTRY_URL").ok();
    Ok(TpmTreeBindData {
      package_spec,
      registry_url,
    })
  }

  fn init(
    init: &InitInfo,
  ) -> Result<Self::InitData, Box<dyn std::error::Error>> {
    let bind_data = init.get_bind_data::<Self::BindData>();

    let registry = unsafe {
      npm::NpmRegistry::with_registry_url((*bind_data).registry_url.clone())?
    };
    let results =
      unsafe { registry.get_dependency_tree(&(*bind_data).package_spec)? };

    Ok(TpmTreeInitData {
      results,
      index: std::sync::atomic::AtomicUsize::new(0),
    })
  }

  fn func(
    func: &TableFunctionInfo<Self>,
    output: &mut DataChunkHandle,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let init_data = func.get_init_data();
    let current_index = init_data
      .index
      .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    if current_index >= init_data.results.len() {
      output.set_len(0);
      return Ok(());
    }

    let result = &init_data.results[current_index];
    let json = serde_json::to_string(result)?;
    let vector = output.flat_vector(0);
    let cstring = CString::new(json)?;
    vector.insert(0, cstring);
    output.set_len(1);

    Ok(())
  }

  fn parameters() -> Option<Vec<LogicalTypeHandle>> {
    Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
  }
}

#[repr(C)]
struct TpmListBindData {
  install_dir: String,
}

#[repr(C)]
struct TpmListInitData {
  results: Vec<npm::ListResponse>,
  index: std::sync::atomic::AtomicUsize,
}

struct TpmListVTab;

impl VTab for TpmListVTab {
  type InitData = TpmListInitData;
  type BindData = TpmListBindData;

  fn bind(
    bind: &BindInfo,
  ) -> Result<Self::BindData, Box<dyn std::error::Error>> {
    bind.add_result_column(
      "list_info",
      LogicalTypeHandle::from(LogicalTypeId::Varchar),
    );
    let install_dir = bind.get_parameter(0).to_string();
    Ok(TpmListBindData { install_dir })
  }

  fn init(
    init: &InitInfo,
  ) -> Result<Self::InitData, Box<dyn std::error::Error>> {
    let bind_data = init.get_bind_data::<Self::BindData>();

    let results = unsafe {
      npm::NpmRegistry::list_installed_packages(&(*bind_data).install_dir)?
    };

    Ok(TpmListInitData {
      results,
      index: std::sync::atomic::AtomicUsize::new(0),
    })
  }

  fn func(
    func: &TableFunctionInfo<Self>,
    output: &mut DataChunkHandle,
  ) -> Result<(), Box<dyn std::error::Error>> {
    let init_data = func.get_init_data();
    let current_index = init_data
      .index
      .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    if current_index >= init_data.results.len() {
      output.set_len(0);
      return Ok(());
    }

    let result = &init_data.results[current_index];
    let json = serde_json::to_string(result)?;
    let vector = output.flat_vector(0);
    let cstring = CString::new(json)?;
    vector.insert(0, cstring);
    output.set_len(1);

    Ok(())
  }

  fn parameters() -> Option<Vec<LogicalTypeHandle>> {
    Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
  }
}

const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(
  con: Connection,
) -> Result<(), Box<dyn Error>> {
  con
    .register_table_function::<HelloVTab>(EXTENSION_NAME)
    .expect("Failed to register hello table function");

  con
    .register_table_function::<TpmInfoVTab>("tpm_info")
    .expect("Failed to register tpm_info table function");

  con
    .register_table_function::<TpmResolveVTab>("tpm_resolve")
    .expect("Failed to register tpm_resolve table function");

  con
    .register_table_function::<TpmInstallVTab>("tpm_install")
    .expect("Failed to register tpm_install table function");

  con
    .register_table_function::<TpmInstallDepsVTab>("tpm_install_with_deps")
    .expect("Failed to register tpm_install_with_deps table function");

  con
    .register_table_function::<TpmTreeVTab>("tpm_tree")
    .expect("Failed to register tpm_tree table function");

  con
    .register_table_function::<TpmListVTab>("tpm_list")
    .expect("Failed to register tpm_list table function");

  Ok(())
}
