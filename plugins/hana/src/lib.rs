use duckdb::Connection;
use libduckdb_sys as ffi;
use std::error::Error;

mod hana_scan;
mod hana_execute;
mod hana_attach;
mod hana_state;
mod hana_replacement;

pub use hana_scan::{
    validate_hana_connection, parse_hana_url, safe_hana_connect, redact_url_password,
    HanaError, LogLevel, HanaLogger,
    HanaScanVTab, HanaScanBindData, HanaScanInitData,
};
pub use hana_execute::HanaExecuteScalar;
pub use hana_attach::{HanaAttachVTab, HanaDetachScalar, HanaTablesVTab};
pub use hdbconnect::Connection as HanaConnection;

unsafe fn extension_entrypoint(connection: Connection) -> Result<(), Box<dyn Error>> {
    connection.register_table_function::<HanaScanVTab>("trex_hana_scan")?;
    // Deprecated alias for external consumers
    connection.register_table_function::<HanaScanVTab>("hana_scan")?;
    connection.register_table_function::<HanaScanVTab>("trex_hana_query")?;
    connection.register_scalar_function::<HanaExecuteScalar>("trex_hana_execute")?;
    connection.register_table_function::<HanaAttachVTab>("trex_hana_attach")?;
    connection.register_scalar_function::<HanaDetachScalar>("trex_hana_detach")?;
    connection.register_table_function::<HanaTablesVTab>("trex_hana_tables")?;
    Ok(())
}

// Manual C-API entrypoint -- inlines the #[duckdb_entrypoint_c_api] macro
// to capture the raw duckdb_database handle for replacement scans and DDL.

unsafe fn hana_scan_init_c_api_internal(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> Result<bool, Box<dyn Error>> {
    let have_api_struct =
        ffi::duckdb_rs_extension_api_init(info, access, "v1.3.2").unwrap();

    if !have_api_struct {
        return Ok(false);
    }

    let db: ffi::duckdb_database = *(*access).get_database.unwrap()(info);

    hana_state::init_ddl_connection(db).map_err(|e| -> Box<dyn Error> { e.into() })?;
    hana_replacement::register(db);

    let connection = Connection::open_from_raw(db.cast())?;
    extension_entrypoint(connection)?;

    Ok(true)
}

#[no_mangle]
pub unsafe extern "C" fn hana_scan_init_c_api(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> bool {
    let init_result = hana_scan_init_c_api_internal(info, access);

    if let Err(x) = init_result {
        let error_c_string = std::ffi::CString::new(x.to_string());
        match error_c_string {
            Ok(e) => {
                (*access).set_error.unwrap()(info, e.as_ptr());
            }
            Err(_e) => {
                let error_alloc_failure =
                    c"An error occured but the extension failed to allocate memory for an error string";
                (*access).set_error.unwrap()(info, error_alloc_failure.as_ptr());
            }
        }
        return false;
    }

    init_result.unwrap()
}
