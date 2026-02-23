use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    vscalar::{ScalarFunctionSignature, VScalar},
};
use duckdb::vtab::arrow::WritableVector;
use std::{
    ffi::CString,
    sync::atomic::{AtomicBool, Ordering},
};

use crate::gossip::GossipRegistry;

/// Parsed gossip `service:*` JSON advertisement.
#[derive(Debug, Clone)]
pub struct ServiceInfo {
    pub host: String,
    pub port: String,
    pub status: String,
    pub uptime_seconds: String,
    pub config: String,
}

/// Tolerant parser -- missing fields fall back to defaults.
pub fn parse_service_json(json: &str) -> Option<ServiceInfo> {
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    let obj = v.as_object()?;

    let host = obj
        .get("host")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let port = match obj.get("port") {
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::String(s)) => s.clone(),
        _ => String::new(),
    };

    let status = obj
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    let uptime_seconds = match obj.get("uptime") {
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::String(s)) => s.clone(),
        _ => "0".to_string(),
    };

    let config = obj
        .get("config")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "{}".to_string());

    Some(ServiceInfo {
        host,
        port,
        status,
        uptime_seconds,
        config,
    })
}

/// Map extension name + JSON config to the SQL that starts it.
/// Returns `Ok(None)` for unknown extensions.
pub fn get_start_service_sql(extension: &str, config_json: &str) -> Result<Option<String>, String> {
    let config: serde_json::Value = serde_json::from_str(config_json)
        .map_err(|e| format!("Invalid JSON config: {e}"))?;

    match extension {
        "flight" => {
            let host = config["host"].as_str().unwrap_or("0.0.0.0").replace('\'', "''");
            let port = config["port"].as_u64().unwrap_or(8815);
            if config.get("cert_path").is_some() {
                let cert = config["cert_path"].as_str().unwrap_or("").replace('\'', "''");
                let key = config["key_path"].as_str().unwrap_or("").replace('\'', "''");
                let ca = config["ca_cert_path"].as_str().unwrap_or("").replace('\'', "''");
                Ok(Some(format!(
                    "SELECT start_flight_server_tls('{host}', {port}, '{cert}', '{key}', '{ca}')"
                )))
            } else {
                Ok(Some(format!(
                    "SELECT start_flight_server('{host}', {port})"
                )))
            }
        }
        "pgwire" => {
            let host = config["host"].as_str().unwrap_or("127.0.0.1").replace('\'', "''");
            let port = config["port"].as_u64().unwrap_or(5432);
            let password = config["password"].as_str().unwrap_or("").replace('\'', "''");
            let db_creds = config["db_credentials"].as_str().unwrap_or("").replace('\'', "''");
            Ok(Some(format!(
                "SELECT start_pgwire_server('{host}', {port}, '{password}', '{db_creds}')"
            )))
        }
        "trexas" => {
            let escaped = config_json.replace('\'', "''");
            Ok(Some(format!(
                "SELECT trex_start_server_with_config('{escaped}')"
            )))
        }
        "chdb" => {
            if let Some(path) = config.get("data_path").and_then(|v| v.as_str()).filter(|s| !s.is_empty()) {
                let escaped_path = path.replace('\'', "''");
                Ok(Some(format!("SELECT chdb_start_database('{escaped_path}')")))
            } else {
                Ok(Some("SELECT chdb_start_database()".to_string()))
            }
        }
        "etl" => {
            let pipeline_name = config["pipeline_name"]
                .as_str()
                .ok_or("etl config requires 'pipeline_name'")?;
            let connection_string = config["connection_string"]
                .as_str()
                .ok_or("etl config requires 'connection_string'")?;

            let escaped_name = pipeline_name.replace('\'', "''");
            let escaped_conn = connection_string.replace('\'', "''");

            let batch_size = config["batch_size"].as_u64().unwrap_or(1000);
            let batch_timeout_ms = config["batch_timeout_ms"].as_u64().unwrap_or(5000);
            let retry_delay_ms = config["retry_delay_ms"].as_u64().unwrap_or(10000);
            let retry_max_attempts = config["retry_max_attempts"].as_u64().unwrap_or(5);

            Ok(Some(format!(
                "SELECT etl_start('{}', '{}', {}, {}, {}, {})",
                escaped_name, escaped_conn,
                batch_size, batch_timeout_ms, retry_delay_ms, retry_max_attempts
            )))
        }
        "distributed-scheduler" => {
            let host = config["host"].as_str().unwrap_or("0.0.0.0").replace('\'', "''");
            let port = config["port"].as_u64().unwrap_or(50050);
            Ok(Some(format!(
                "SELECT swarm_start_distributed_scheduler('{host}', {port})"
            )))
        }
        "distributed-executor" => {
            let host = config["host"].as_str().unwrap_or("0.0.0.0").replace('\'', "''");
            let port = config["port"].as_u64().unwrap_or(50051);
            let scheduler = config["scheduler_url"]
                .as_str()
                .unwrap_or("http://localhost:50050")
                .replace('\'', "''");
            Ok(Some(format!(
                "SELECT swarm_start_distributed_executor('{host}', {port}, '{scheduler}')"
            )))
        }
        _ => Ok(None),
    }
}

pub struct SwarmServicesTable;

#[repr(C)]
pub struct SwarmServicesBindData {}

#[repr(C)]
pub struct SwarmServicesInitData {
    done: AtomicBool,
}

impl VTab for SwarmServicesTable {
    type InitData = SwarmServicesInitData;
    type BindData = SwarmServicesBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("node_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "service_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("host", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("port", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "uptime_seconds",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("config", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(SwarmServicesBindData {})
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(SwarmServicesInitData {
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

        let nodes = match GossipRegistry::instance().get_node_key_values() {
            Ok(nodes) => nodes,
            Err(_) => {
                output.set_len(0);
                return Ok(());
            }
        };

        struct ServiceRow {
            node_name: String,
            service_name: String,
            host: String,
            port: String,
            status: String,
            uptime_seconds: String,
            config: String,
        }

        let mut rows: Vec<ServiceRow> = Vec::new();

        for node in &nodes {
            for (key, value) in &node.key_values {
                if let Some(service_name) = key.strip_prefix("service:") {
                    let info = parse_service_json(value);
                    let (host, port, status, uptime, config) = match info {
                        Some(si) => (
                            si.host,
                            si.port,
                            si.status,
                            si.uptime_seconds,
                            si.config,
                        ),
                        None => (
                            String::new(),
                            String::new(),
                            "unknown".to_string(),
                            "0".to_string(),
                            "{}".to_string(),
                        ),
                    };

                    rows.push(ServiceRow {
                        node_name: node.node_name.clone(),
                        service_name: service_name.to_string(),
                        host,
                        port,
                        status,
                        uptime_seconds: uptime,
                        config,
                    });
                }
            }
        }

        if rows.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        let chunk_size = rows.len();
        let node_name_vec = output.flat_vector(0);
        let service_name_vec = output.flat_vector(1);
        let host_vec = output.flat_vector(2);
        let port_vec = output.flat_vector(3);
        let status_vec = output.flat_vector(4);
        let uptime_vec = output.flat_vector(5);
        let config_vec = output.flat_vector(6);

        for (i, row) in rows.iter().enumerate() {
            node_name_vec.insert(i, CString::new(row.node_name.clone())?);
            service_name_vec.insert(i, CString::new(row.service_name.clone())?);
            host_vec.insert(i, CString::new(row.host.clone())?);
            port_vec.insert(i, CString::new(row.port.clone())?);
            status_vec.insert(i, CString::new(row.status.clone())?);
            uptime_vec.insert(i, CString::new(row.uptime_seconds.clone())?);
            config_vec.insert(i, CString::new(row.config.clone())?);
        }

        output.set_len(chunk_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

pub struct SwarmStartServiceScalar;

impl VScalar for SwarmStartServiceScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let ext_vector = input.flat_vector(0);
        let cfg_vector = input.flat_vector(1);

        let ext_slice =
            ext_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let cfg_slice =
            cfg_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

        let extension = duckdb::types::DuckString::new(&mut { ext_slice[0] })
            .as_str()
            .to_string();
        let config_json = duckdb::types::DuckString::new(&mut { cfg_slice[0] })
            .as_str()
            .to_string();

        let sql = match get_start_service_sql(&extension, &config_json) {
            Ok(Some(sql)) => sql,
            Ok(None) => {
                let msg = format!(
                    "Unknown service extension '{}'. Known: flight, pgwire, trexas, chdb, etl",
                    extension
                );
                let flat = output.flat_vector();
                flat.insert(0, &msg);
                return Ok(());
            }
            Err(e) => {
                let flat = output.flat_vector();
                flat.insert(0, &e);
                return Ok(());
            }
        };

        let conn_arc = crate::get_shared_connection().ok_or("No shared connection")?;
        let conn = conn_arc
            .lock()
            .map_err(|e| format!("Lock error: {e}"))?;

        if let Err(e) = conn.execute_batch(&sql) {
            let msg = format!("Failed to start {}: {}", extension, e);
            let flat = output.flat_vector();
            flat.insert(0, &msg);
            return Ok(());
        }

        let config: serde_json::Value = serde_json::from_str(&config_json).unwrap_or_default();
        let host = config["host"].as_str().unwrap_or("");
        let port = config["port"].as_u64().unwrap_or(0);

        let service_json = serde_json::json!({
            "host": host,
            "port": port,
            "status": "running",
            "uptime": 0,
            "config": config
        })
        .to_string();

        let gossip_key = format!("service:{}", extension);
        let gossip_result = GossipRegistry::instance().set_key(&gossip_key, &service_json);

        let response = match gossip_result {
            Ok(()) => format!(
                "Service '{}' started and registered in gossip",
                extension
            ),
            Err(e) => format!(
                "Service '{}' started but gossip registration failed: {}",
                extension, e
            ),
        };

        let flat = output.flat_vector();
        flat.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::Varchar.into(),
            ],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

pub struct SwarmStopServiceScalar;

impl VScalar for SwarmStopServiceScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let ext_vector = input.flat_vector(0);
        let ext_slice =
            ext_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let extension = duckdb::types::DuckString::new(&mut { ext_slice[0] })
            .as_str()
            .to_string();

        // Update to "stopped" rather than removing, so swarm_services() still shows it.
        let gossip_key = format!("service:{}", extension);
        let existing_json = Self::read_own_service_key(&gossip_key);

        let stopped_json = match existing_json {
            Some(info) => serde_json::json!({
                "host": info.host,
                "port": info.port,
                "status": "stopped",
                "uptime": 0,
                "config": {}
            })
            .to_string(),
            None => serde_json::json!({
                "host": "",
                "port": "",
                "status": "stopped",
                "uptime": 0,
                "config": {}
            })
            .to_string(),
        };

        let gossip_result = GossipRegistry::instance().set_key(&gossip_key, &stopped_json);

        let response = match gossip_result {
            Ok(()) => format!(
                "Service '{}' marked as stopped in gossip",
                extension
            ),
            Err(e) => format!(
                "Failed to update gossip for service '{}': {}",
                extension, e
            ),
        };

        let flat = output.flat_vector();
        flat.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

impl SwarmStopServiceScalar {
    fn read_own_service_key(gossip_key: &str) -> Option<ServiceInfo> {
        let nodes = GossipRegistry::instance().get_node_key_values().ok()?;
        let self_config = GossipRegistry::instance().get_self_config().ok()?;
        let self_node_id = self_config
            .iter()
            .find(|(k, _)| k == "node_id")
            .map(|(_, v)| v.clone())?;

        for node in &nodes {
            if node.node_id == self_node_id {
                for (key, value) in &node.key_values {
                    if key == gossip_key {
                        return parse_service_json(value);
                    }
                }
            }
        }
        None
    }
}

pub struct SwarmLoadScalar;

impl VScalar for SwarmLoadScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let ext_vector = input.flat_vector(0);
        let ext_slice =
            ext_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let extension = duckdb::types::DuckString::new(&mut { ext_slice[0] })
            .as_str()
            .to_string();

        if !crate::catalog::is_valid_extension_name(&extension) {
            let flat = output.flat_vector();
            flat.insert(0, &format!("Invalid extension name: '{}'", extension));
            return Ok(());
        }

        let load_sql = format!("LOAD '{}.trex'", extension);

        let conn_arc = crate::get_shared_connection().ok_or("No shared connection")?;
        let conn = conn_arc
            .lock()
            .map_err(|e| format!("Lock error: {e}"))?;

        let response = match conn.execute_batch(&load_sql) {
            Ok(()) => format!("Extension '{}' loaded successfully", extension),
            Err(e) => format!("Failed to load extension '{}': {}", extension, e),
        };

        let flat = output.flat_vector();
        flat.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

pub struct SwarmSetKeyScalar;

impl VScalar for SwarmSetKeyScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let key_vector = input.flat_vector(0);
        let value_vector = input.flat_vector(1);

        let key_slice =
            key_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let value_slice =
            value_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

        let key = duckdb::types::DuckString::new(&mut { key_slice[0] })
            .as_str()
            .to_string();
        let value = duckdb::types::DuckString::new(&mut { value_slice[0] })
            .as_str()
            .to_string();

        let response = match GossipRegistry::instance().set_key(&key, &value) {
            Ok(()) => format!("Set key '{}'", key),
            Err(e) => format!("Error setting key '{}': {}", key, e),
        };

        let flat = output.flat_vector();
        flat.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::Varchar.into(),
            ],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

pub struct SwarmDeleteKeyScalar;

impl VScalar for SwarmDeleteKeyScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let key_vector = input.flat_vector(0);
        let key_slice =
            key_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let key = duckdb::types::DuckString::new(&mut { key_slice[0] })
            .as_str()
            .to_string();

        let response = match GossipRegistry::instance().delete_key(&key) {
            Ok(()) => format!("Deleted key '{}'", key),
            Err(e) => format!("Error deleting key '{}': {}", key, e),
        };

        let flat = output.flat_vector();
        flat.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

pub struct SwarmRegisterServiceScalar;

impl VScalar for SwarmRegisterServiceScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let name_vector = input.flat_vector(0);
        let host_vector = input.flat_vector(1);
        let port_vector = input.flat_vector(2);

        let name_slice =
            name_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let host_slice =
            host_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let port_slice = port_vector.as_slice_with_len::<i32>(input.len());

        let name = duckdb::types::DuckString::new(&mut { name_slice[0] })
            .as_str()
            .to_string();
        let host = duckdb::types::DuckString::new(&mut { host_slice[0] })
            .as_str()
            .to_string();
        let port_raw = port_slice[0];
        if port_raw < 0 || port_raw > 65535 {
            return Err(format!("Port {} out of valid range (0-65535)", port_raw).into());
        }
        let port = port_raw as u16;

        // Publish to gossip only (no server started). Allows ad-hoc registration.
        let service_json = serde_json::json!({
            "host": host,
            "port": port,
            "status": "running",
            "uptime": 0,
            "config": {}
        })
        .to_string();

        let gossip_key = format!("service:{}", name);
        let gossip_result = GossipRegistry::instance().set_key(&gossip_key, &service_json);

        // Flight registration also triggers catalog advertisement and refresh.
        if name == "flight" {
            let _ = crate::catalog::advertise_local_tables();
            let _ = crate::catalog::start_catalog_refresh();
        }

        let response = match gossip_result {
            Ok(()) => format!(
                "Service '{}' registered at {}:{} in gossip",
                name, host, port
            ),
            Err(e) => format!(
                "Failed to register service '{}': {}",
                name, e
            ),
        };

        let flat = output.flat_vector();
        flat.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::Varchar.into(),
                LogicalTypeId::Integer.into(),
            ],
            LogicalTypeId::Varchar.into(),
        )]
    }
}
