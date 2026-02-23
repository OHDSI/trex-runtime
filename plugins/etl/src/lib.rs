extern crate duckdb;
#[cfg(feature = "loadable-extension")]
extern crate duckdb_loadable_macros;
#[cfg(feature = "loadable-extension")]
extern crate libduckdb_sys;

mod credential_mask;
pub mod destination;
#[cfg(feature = "loadable-extension")]
mod etl_start;
#[cfg(feature = "loadable-extension")]
mod etl_status;
#[cfg(feature = "loadable-extension")]
mod etl_stop;
#[cfg(feature = "loadable-extension")]
mod gossip_bridge;
pub mod pipeline_registry;
pub mod store;
pub mod type_mapping;

use duckdb::Connection;
use std::error::Error;
use std::sync::{Arc, Mutex, OnceLock};

static SHARED_CONNECTION: OnceLock<Arc<Mutex<Connection>>> = OnceLock::new();

pub fn store_shared_connection(connection: &Connection) -> Result<(), Box<dyn Error>> {
    let cloned = connection
        .try_clone()
        .map_err(|e| format!("Failed to clone connection: {}", e))?;

    SHARED_CONNECTION
        .set(Arc::new(Mutex::new(cloned)))
        .map_err(|_| "Connection already stored")?;

    Ok(())
}

pub fn get_shared_connection() -> Option<Arc<Mutex<Connection>>> {
    SHARED_CONNECTION.get().cloned()
}

#[cfg(feature = "loadable-extension")]
#[duckdb_loadable_macros::duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    store_shared_connection(&con)?;

    con.register_scalar_function::<etl_start::EtlStartScalar>("trex_etl_start")
        .expect("Failed to register trex_etl_start scalar function");

    con.register_scalar_function::<etl_stop::EtlStopScalar>("trex_etl_stop")
        .expect("Failed to register trex_etl_stop scalar function");

    con.register_table_function::<etl_status::EtlStatusTable>("trex_etl_status")
        .expect("Failed to register trex_etl_status table function");

    Ok(())
}
