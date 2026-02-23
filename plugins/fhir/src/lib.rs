extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

mod error;
mod fhir;
mod fhir_server;
mod handlers;
mod query_executor;
mod router;
mod schema;
mod server_registry;
mod sql_safety;
mod state;
mod cql;
mod export;

pub use query_executor::{QueryExecutor, QueryResult};

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{arrow::WritableVector, BindInfo, InitInfo, TableFunctionInfo, VTab},
    vscalar::{ScalarFunctionSignature, VScalar},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use std::{
    error::Error,
    ffi::CString,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock as OnceCell,
    },
};

static SHARED_CONNECTION: OnceCell<Arc<Mutex<Connection>>> = OnceCell::new();
static QUERY_EXECUTOR: OnceCell<Arc<QueryExecutor>> = OnceCell::new();

const DEFAULT_POOL_SIZE: usize = 8;

fn executor_pool_size() -> usize {
    std::env::var("FHIR_POOL_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_POOL_SIZE)
        .max(1)
}

fn store_shared_connection(connection: &Connection) -> Result<(), Box<dyn Error>> {
    let cloned = connection
        .try_clone()
        .map_err(|e| format!("connection clone: {e}"))?;

    SHARED_CONNECTION
        .set(Arc::new(Mutex::new(cloned)))
        .map_err(|_| "connection already stored")?;

    let pool_size = executor_pool_size();
    let executor = QueryExecutor::new(connection, pool_size)?;
    QUERY_EXECUTOR
        .set(Arc::new(executor))
        .map_err(|_| "executor already created")?;

    Ok(())
}

pub fn get_shared_connection() -> Option<Arc<Mutex<Connection>>> {
    SHARED_CONNECTION.get().cloned()
}

pub fn get_query_executor() -> Option<Arc<QueryExecutor>> {
    QUERY_EXECUTOR.get().cloned()
}

pub async fn init_fhir_meta(executor: &Arc<QueryExecutor>) {
    let _ = executor
        .submit("CREATE SCHEMA IF NOT EXISTS _fhir_meta".to_string())
        .await;

    let _ = executor
        .submit(
            "CREATE TABLE IF NOT EXISTS _fhir_meta._datasets (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT now(),
                updated_at TIMESTAMP NOT NULL DEFAULT now(),
                structure_definitions JSON,
                resource_types VARCHAR[],
                status VARCHAR NOT NULL DEFAULT 'active'
            )"
            .to_string(),
        )
        .await;

    let _ = executor
        .submit(
            "CREATE TABLE IF NOT EXISTS _fhir_meta._export_jobs (
                id VARCHAR PRIMARY KEY,
                dataset_id VARCHAR NOT NULL,
                resource_types VARCHAR[],
                status VARCHAR NOT NULL DEFAULT 'accepted',
                created_at TIMESTAMP NOT NULL DEFAULT now(),
                completed_at TIMESTAMP,
                output_files JSON,
                error_message VARCHAR
            )"
            .to_string(),
        )
        .await;
}

struct FhirStartScalar;

impl VScalar for FhirStartScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let host_vector = input.flat_vector(0);
        let port_vector = input.flat_vector(1);

        let host_slice =
            host_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let port_slice = port_vector.as_slice_with_len::<i32>(input.len());

        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let host = duckdb::types::DuckString::new(&mut { host_slice[0] })
            .as_str()
            .to_string();
        let port = port_slice[0] as u16;

        let response = match fhir_server::start_fhir_server(host, port) {
            Ok(msg) => msg,
            Err(err) => format!("Error: {}", err),
        };

        let flat_vector = output.flat_vector();
        flat_vector.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into(), LogicalTypeId::Integer.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

struct FhirStopScalar;

impl VScalar for FhirStopScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let host_vector = input.flat_vector(0);
        let port_vector = input.flat_vector(1);

        let host_slice =
            host_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let port_slice = port_vector.as_slice_with_len::<i32>(input.len());

        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let host = duckdb::types::DuckString::new(&mut { host_slice[0] })
            .as_str()
            .to_string();
        let port = port_slice[0] as u16;

        let response = match fhir_server::stop_fhir_server(&host, port) {
            Ok(msg) => msg,
            Err(err) => format!("Error: {}", err),
        };

        let flat_vector = output.flat_vector();
        flat_vector.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into(), LogicalTypeId::Integer.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

struct FhirStatusTable;

#[repr(C)]
struct FhirStatusBindData {}

#[repr(C)]
struct FhirStatusInitData {
    done: AtomicBool,
}

impl VTab for FhirStatusTable {
    type InitData = FhirStatusInitData;
    type BindData = FhirStatusBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("hostname", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("port", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "uptime_seconds",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        Ok(FhirStatusBindData {})
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(FhirStatusInitData {
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

        let servers_info = server_registry::ServerRegistry::instance().get_servers_info();

        if servers_info.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        let chunk_size = servers_info.len();
        let hostname_vector = output.flat_vector(0);
        let port_vector = output.flat_vector(1);
        let uptime_vector = output.flat_vector(2);

        for (i, (hostname, port, uptime_secs)) in servers_info.iter().enumerate() {
            let hostname_cstring = CString::new(hostname.clone())?;
            hostname_vector.insert(i, hostname_cstring);

            let port_cstring = CString::new(port.to_string())?;
            port_vector.insert(i, port_cstring);

            let uptime_cstring = CString::new(uptime_secs.to_string())?;
            uptime_vector.insert(i, uptime_cstring);
        }

        output.set_len(chunk_size);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
}

struct FhirVersionScalar;

impl VScalar for FhirVersionScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() > 0 {
            let flat_vector = output.flat_vector();
            flat_vector.insert(0, "fhir extension v0.1.0 (FHIR R4 4.0.1)");
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

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    store_shared_connection(&con)?;

    con.register_scalar_function::<FhirStartScalar>("trex_fhir_start")
        .expect("Failed to register trex_fhir_start function");

    con.register_scalar_function::<FhirStopScalar>("trex_fhir_stop")
        .expect("Failed to register trex_fhir_stop function");

    con.register_scalar_function::<FhirVersionScalar>("trex_fhir_version")
        .expect("Failed to register trex_fhir_version function");

    con.register_table_function::<FhirStatusTable>("trex_fhir_status")
        .expect("Failed to register trex_fhir_status function");

    Ok(())
}
