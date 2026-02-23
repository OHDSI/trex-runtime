use libduckdb_sys as ffi;
use std::ffi::{CStr, CString};

use crate::hana_state;

pub fn register(db: ffi::duckdb_database) {
    unsafe {
        ffi::duckdb_add_replacement_scan(
            db,
            Some(hana_replacement_scan_callback),
            std::ptr::null_mut(),
            None,
        );
    }
}

/// Resolves `HANA__<dbname>_<schema>_<table>` to `hana_scan(...)`.
/// # Safety
/// Called from C -- must never panic.
unsafe extern "C" fn hana_replacement_scan_callback(
    info: ffi::duckdb_replacement_scan_info,
    table_name: *const std::os::raw::c_char,
    _data: *mut std::os::raw::c_void,
) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        replacement_scan_inner(info, table_name);
    }));
}

unsafe fn replacement_scan_inner(
    info: ffi::duckdb_replacement_scan_info,
    table_name: *const std::os::raw::c_char,
) {
    if table_name.is_null() {
        return;
    }
    let name = match CStr::from_ptr(table_name).to_str() {
        Ok(s) => s,
        Err(_) => return,
    };

    let upper = name.to_uppercase();

    let found = hana_state::read_state(|state| state.table_registry.get(&upper).cloned());

    let entry = match found {
        Some(e) => e,
        None => return,
    };

    let query = format!(
        "SELECT * FROM \"{}\".\"{}\"",
        entry.hana_schema, entry.hana_table
    );

    let fn_name = match CString::new("hana_scan") {
        Ok(s) => s,
        Err(_) => return,
    };
    ffi::duckdb_replacement_scan_set_function_name(info, fn_name.as_ptr());

    let c_query = match CString::new(query) {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut query_val = ffi::duckdb_create_varchar(c_query.as_ptr());
    ffi::duckdb_replacement_scan_add_parameter(info, query_val);
    ffi::duckdb_destroy_value(&mut query_val);

    let c_url = match CString::new(entry.url.as_str()) {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut url_val = ffi::duckdb_create_varchar(c_url.as_ptr());
    ffi::duckdb_replacement_scan_add_parameter(info, url_val);
    ffi::duckdb_destroy_value(&mut url_val);
}
