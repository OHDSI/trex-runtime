extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

mod compile;
mod dag;
mod freshness;
mod parser;
mod plan;
mod project;
mod run;
mod seed;
mod state;
mod test;

use duckdb::Connection;
use libduckdb_sys as ffi;
use std::{
    error::Error,
    ffi::CString,
    sync::{Mutex, OnceLock},
};

// ── Shared Connection ────────────────────────────────────────────────────────

struct SharedConn(ffi::duckdb_connection);

unsafe impl Send for SharedConn {}
unsafe impl Sync for SharedConn {}

impl Drop for SharedConn {
    fn drop(&mut self) {
        unsafe {
            if !self.0.is_null() {
                ffi::duckdb_disconnect(&mut self.0);
            }
        }
    }
}

static SHARED_CONNECTION: OnceLock<Mutex<SharedConn>> = OnceLock::new();

pub fn execute_sql(sql: &str) -> Result<(), Box<dyn Error>> {
    let mutex = SHARED_CONNECTION
        .get()
        .ok_or("Transform extension not initialized")?;
    let guard = mutex.lock().map_err(|_| "Connection mutex poisoned")?;
    let conn = guard.0;

    unsafe {
        let c_sql = CString::new(sql)?;
        let mut result: ffi::duckdb_result = std::mem::zeroed();
        let state = ffi::duckdb_query(conn, c_sql.as_ptr(), &mut result);

        let ok = if state != ffi::duckdb_state_DuckDBSuccess {
            let err_ptr = ffi::duckdb_result_error(&mut result);
            let err_msg = if err_ptr.is_null() {
                format!("SQL execution failed: {}", sql)
            } else {
                let c_str = std::ffi::CStr::from_ptr(err_ptr);
                format!("{}", c_str.to_string_lossy())
            };
            Err(err_msg)
        } else {
            Ok(())
        };

        ffi::duckdb_destroy_result(&mut result);
        ok?;
    }

    Ok(())
}

pub struct QueryRow {
    pub columns: Vec<String>,
}

pub fn query_sql(sql: &str) -> Result<Vec<QueryRow>, Box<dyn Error>> {
    let mutex = SHARED_CONNECTION
        .get()
        .ok_or("Transform extension not initialized")?;
    let guard = mutex.lock().map_err(|_| "Connection mutex poisoned")?;
    let conn = guard.0;

    unsafe {
        let c_sql = CString::new(sql)?;
        let mut result: ffi::duckdb_result = std::mem::zeroed();
        let state = ffi::duckdb_query(conn, c_sql.as_ptr(), &mut result);

        if state != ffi::duckdb_state_DuckDBSuccess {
            let err_ptr = ffi::duckdb_result_error(&mut result);
            let err_msg = if err_ptr.is_null() {
                format!("Query failed: {}", sql)
            } else {
                let c_str = std::ffi::CStr::from_ptr(err_ptr);
                format!("{}", c_str.to_string_lossy())
            };
            ffi::duckdb_destroy_result(&mut result);
            return Err(err_msg.into());
        }

        let row_count = ffi::duckdb_row_count(&mut result);
        let col_count = ffi::duckdb_column_count(&mut result);
        let mut rows = Vec::new();

        for row_idx in 0..row_count {
            let mut columns = Vec::new();
            for col_idx in 0..col_count {
                let val = ffi::duckdb_value_varchar(&mut result, col_idx, row_idx);
                let s = if val.is_null() {
                    String::new()
                } else {
                    let c_str = std::ffi::CStr::from_ptr(val);
                    let s = c_str.to_string_lossy().to_string();
                    ffi::duckdb_free(val as *mut _);
                    s
                };
                columns.push(s);
            }
            rows.push(QueryRow { columns });
        }

        ffi::duckdb_destroy_result(&mut result);
        Ok(rows)
    }
}

fn init_shared_connection(db: ffi::duckdb_database) -> Result<(), Box<dyn Error>> {
    SHARED_CONNECTION.get_or_init(|| unsafe {
        let mut conn: ffi::duckdb_connection = std::ptr::null_mut();
        let state = ffi::duckdb_connect(db, &mut conn);
        if state != ffi::duckdb_state_DuckDBSuccess {
            return Mutex::new(SharedConn(std::ptr::null_mut()));
        }
        Mutex::new(SharedConn(conn))
    });

    let mutex = SHARED_CONNECTION.get().unwrap();
    let guard = mutex.lock().map_err(|_| "Connection mutex poisoned")?;
    if guard.0.is_null() {
        return Err("Failed to create shared connection".into());
    }
    Ok(())
}

pub fn escape_sql_ident(s: &str) -> String {
    s.replace('"', "\"\"")
}

pub fn escape_sql_str(s: &str) -> String {
    s.replace('\'', "''")
}

// ── Extension Entrypoint ─────────────────────────────────────────────────────

unsafe fn extension_entrypoint(connection: Connection) -> Result<(), Box<dyn Error>> {
    connection.register_table_function::<compile::CompileVTab>("trex_transform_compile")?;
    connection.register_table_function::<plan::PlanVTab>("trex_transform_plan")?;
    connection.register_table_function::<run::RunVTab>("trex_transform_run")?;
    connection.register_table_function::<seed::SeedVTab>("trex_transform_seed")?;
    connection.register_table_function::<test::TestVTab>("trex_transform_test")?;
    connection.register_table_function::<freshness::FreshnessVTab>("trex_transform_freshness")?;
    Ok(())
}

unsafe fn transform_init_c_api_internal(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> Result<bool, Box<dyn Error>> {
    let have_api_struct = ffi::duckdb_rs_extension_api_init(info, access, "v1.3.2").unwrap();

    if !have_api_struct {
        return Ok(false);
    }

    let db: ffi::duckdb_database = *(*access).get_database.unwrap()(info);

    init_shared_connection(db)?;

    let connection = Connection::open_from_raw(db.cast())?;
    extension_entrypoint(connection)?;

    Ok(true)
}

#[no_mangle]
pub unsafe extern "C" fn transform_init_c_api(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> bool {
    let init_result = transform_init_c_api_internal(info, access);

    if let Err(x) = init_result {
        let error_c_string = std::ffi::CString::new(x.to_string());
        match error_c_string {
            Ok(e) => {
                (*access).set_error.unwrap()(info, e.as_ptr());
            }
            Err(_) => {
                let error_msg =
                    c"An error occurred but the extension failed to allocate an error string";
                (*access).set_error.unwrap()(info, error_msg.as_ptr());
            }
        }
        return false;
    }

    init_result.unwrap()
}
