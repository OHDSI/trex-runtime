use duckdb::{Connection, Result};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use std::error::Error;

mod types;
mod functions;
mod vtab;
mod scalar;
mod safe_query_result;

pub use types::{ChdbError, ChdbPerformanceMetrics, LogLevel, ChdbLogger, ChdbScanBindData, ChdbScanInitData};
pub use functions::{validate_chdb_connection, create_chdb_session};
pub use vtab::ChdbScanVTab;
pub use scalar::{StartChdbDatabaseScalar, StopChdbDatabaseScalar, ExecuteDmlScalar};
pub use safe_query_result::{SafeQueryResult, safe_execute_query};

#[allow(dead_code)]
static EXTENSION_NAME: &str = "chdb";
#[allow(dead_code)]
static EXTENSION_VERSION: &str = "0.1.0";

#[duckdb_entrypoint_c_api(ext_name = "chdb", min_duckdb_version = "v1.3.2")]
pub unsafe fn extension_entrypoint(connection: Connection) -> Result<(), Box<dyn Error>> {
    connection.register_table_function::<vtab::ChdbScanVTab>("trex_chdb_scan")?;
    connection.register_table_function::<vtab::ChdbScanVTab>("trex_chdb_query")?;

    connection.register_scalar_function::<scalar::StartChdbDatabaseScalar>("trex_chdb_start")?;
    connection.register_scalar_function::<scalar::StopChdbDatabaseScalar>("trex_chdb_stop")?;
    connection.register_scalar_function::<scalar::ExecuteDmlScalar>("trex_chdb_execute")?;
    
    Ok(())
}
