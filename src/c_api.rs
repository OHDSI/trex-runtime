use std::ffi::{CStr, CString};
use std::os::raw::c_char;

use crate::appender::TrexAppender;
use crate::engine::TrexDatabase;
use crate::error;
use crate::result::TrexResult;

// === Error handling ===

/// Returns the last error message for the current thread, or NULL if no error.
/// The returned pointer is valid until the next C API call on the same thread.
#[no_mangle]
pub extern "C" fn trexsql_last_error() -> *const c_char {
    error::last_error_ptr()
}

/// Free a string allocated by the library (e.g. from trexsql_result_get_string).
#[no_mangle]
pub unsafe extern "C" fn trexsql_free_string(s: *mut c_char) {
    if !s.is_null() {
        drop(CString::from_raw(s));
    }
}

// === Database lifecycle ===

/// Open a trexsql database.
/// `path`: file path or ":memory:" (NULL means ":memory:")
/// `flags`: bit 0 = allow unsigned extensions
/// Returns NULL on error (check trexsql_last_error).
#[no_mangle]
pub unsafe extern "C" fn trexsql_open(
    path: *const c_char,
    flags: u32,
) -> *mut TrexDatabase {
    error::clear_last_error();

    let path_str = if path.is_null() {
        ":memory:"
    } else {
        match CStr::from_ptr(path).to_str() {
            Ok(s) => s,
            Err(e) => {
                error::set_last_error(&format!("Invalid path: {e}"));
                return std::ptr::null_mut();
            }
        }
    };

    match TrexDatabase::open(path_str, flags) {
        Ok(db) => Box::into_raw(Box::new(db)),
        Err(e) => {
            error::set_last_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Close and free a database handle.
#[no_mangle]
pub unsafe extern "C" fn trexsql_close(db: *mut TrexDatabase) {
    if !db.is_null() {
        drop(Box::from_raw(db));
    }
}

// === SQL execution ===

/// Execute a non-query SQL statement (DDL, DML, LOAD, PRAGMA, etc.).
/// Returns 0 on success, -1 on error (check trexsql_last_error).
#[no_mangle]
pub unsafe extern "C" fn trexsql_execute(
    db: *mut TrexDatabase,
    sql: *const c_char,
) -> i32 {
    error::clear_last_error();

    if db.is_null() || sql.is_null() {
        error::set_last_error("NULL argument");
        return -1;
    }

    let db = &*db;
    let sql_str = match CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(e) => {
            error::set_last_error(&format!("Invalid SQL string: {e}"));
            return -1;
        }
    };

    match db.execute(sql_str) {
        Ok(()) => 0,
        Err(e) => {
            error::set_last_error(&e);
            -1
        }
    }
}

/// Execute a query and return a materialized result set.
/// Returns NULL on error (check trexsql_last_error).
/// Caller must free with trexsql_result_close.
#[no_mangle]
pub unsafe extern "C" fn trexsql_query(
    db: *mut TrexDatabase,
    sql: *const c_char,
) -> *mut TrexResult {
    error::clear_last_error();

    if db.is_null() || sql.is_null() {
        error::set_last_error("NULL argument");
        return std::ptr::null_mut();
    }

    let db = &*db;
    let sql_str = match CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(e) => {
            error::set_last_error(&format!("Invalid SQL string: {e}"));
            return std::ptr::null_mut();
        }
    };

    match db.query(sql_str) {
        Ok(result) => Box::into_raw(Box::new(result)),
        Err(e) => {
            error::set_last_error(&e);
            std::ptr::null_mut()
        }
    }
}

// === Result set iteration ===

/// Get the number of columns in the result.
#[no_mangle]
pub unsafe extern "C" fn trexsql_result_column_count(r: *mut TrexResult) -> i32 {
    if r.is_null() {
        return 0;
    }
    (*r).column_count()
}

/// Get the name of a column (0-based). Returns pointer owned by the result.
#[no_mangle]
pub unsafe extern "C" fn trexsql_result_column_name(
    r: *mut TrexResult,
    col: i32,
) -> *const c_char {
    if r.is_null() {
        return std::ptr::null();
    }
    (*r).column_name(col)
}

/// Advance to the next row. Returns 1 if a row is available, 0 if done.
#[no_mangle]
pub unsafe extern "C" fn trexsql_result_next(r: *mut TrexResult) -> i32 {
    if r.is_null() {
        return 0;
    }
    if (*r).next() { 1 } else { 0 }
}

/// Check if the current column value is NULL. Returns 1 if NULL, 0 otherwise.
#[no_mangle]
pub unsafe extern "C" fn trexsql_result_is_null(
    r: *mut TrexResult,
    col: i32,
) -> i32 {
    if r.is_null() {
        return 1;
    }
    if (*r).is_null(col) { 1 } else { 0 }
}

/// Get the current column value as a string. Caller must free with trexsql_free_string.
/// Returns NULL if the value is NULL or on error.
#[no_mangle]
pub unsafe extern "C" fn trexsql_result_get_string(
    r: *mut TrexResult,
    col: i32,
) -> *mut c_char {
    if r.is_null() {
        return std::ptr::null_mut();
    }
    match (*r).get_string(col) {
        Some(s) => s.into_raw(),
        None => std::ptr::null_mut(),
    }
}

/// Get the current column value as an i64. Returns 0 if NULL or not numeric.
#[no_mangle]
pub unsafe extern "C" fn trexsql_result_get_long(
    r: *mut TrexResult,
    col: i32,
) -> i64 {
    if r.is_null() {
        return 0;
    }
    (*r).get_long(col)
}

/// Get the current column value as a f64. Returns 0.0 if NULL or not numeric.
#[no_mangle]
pub unsafe extern "C" fn trexsql_result_get_double(
    r: *mut TrexResult,
    col: i32,
) -> f64 {
    if r.is_null() {
        return 0.0;
    }
    (*r).get_double(col)
}

/// Close and free a result set.
#[no_mangle]
pub unsafe extern "C" fn trexsql_result_close(r: *mut TrexResult) {
    if !r.is_null() {
        drop(Box::from_raw(r));
    }
}

// === Appender ===

/// Create an appender for a table.
/// `schema`: schema name (e.g. "main") or database alias for attached databases.
/// `table`: table name.
/// Returns NULL on error (check trexsql_last_error).
/// Caller must free with trexsql_appender_close.
#[no_mangle]
pub unsafe extern "C" fn trexsql_appender_create(
    db: *mut TrexDatabase,
    schema: *const c_char,
    table: *const c_char,
) -> *mut TrexAppender {
    error::clear_last_error();

    if db.is_null() || table.is_null() {
        error::set_last_error("NULL argument");
        return std::ptr::null_mut();
    }

    let schema_str = if schema.is_null() {
        "main"
    } else {
        match CStr::from_ptr(schema).to_str() {
            Ok(s) => s,
            Err(e) => {
                error::set_last_error(&format!("Invalid schema: {e}"));
                return std::ptr::null_mut();
            }
        }
    };

    let table_str = match CStr::from_ptr(table).to_str() {
        Ok(s) => s,
        Err(e) => {
            error::set_last_error(&format!("Invalid table name: {e}"));
            return std::ptr::null_mut();
        }
    };

    let raw_db = (*db).raw_db();

    match TrexAppender::create(raw_db, schema_str, table_str) {
        Ok(app) => Box::into_raw(Box::new(app)),
        Err(e) => {
            error::set_last_error(&e);
            std::ptr::null_mut()
        }
    }
}

/// Finalize the current row. Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn trexsql_appender_end_row(a: *mut TrexAppender) -> i32 {
    if a.is_null() {
        return -1;
    }
    match (*a).end_row() {
        Ok(()) => 0,
        Err(e) => {
            error::set_last_error(&e);
            -1
        }
    }
}

/// Append a NULL value. Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn trexsql_appender_append_null(a: *mut TrexAppender) -> i32 {
    if a.is_null() {
        return -1;
    }
    match (*a).append_null() {
        Ok(()) => 0,
        Err(e) => {
            error::set_last_error(&e);
            -1
        }
    }
}

/// Append a string value. Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn trexsql_appender_append_string(
    a: *mut TrexAppender,
    val: *const c_char,
) -> i32 {
    if a.is_null() {
        return -1;
    }
    if val.is_null() {
        return match (*a).append_null() {
            Ok(()) => 0,
            Err(e) => {
                error::set_last_error(&e);
                -1
            }
        };
    }
    let s = match CStr::from_ptr(val).to_str() {
        Ok(s) => s,
        Err(e) => {
            error::set_last_error(&format!("Invalid string: {e}"));
            return -1;
        }
    };
    match (*a).append_string(s) {
        Ok(()) => 0,
        Err(e) => {
            error::set_last_error(&e);
            -1
        }
    }
}

/// Append a long value. Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn trexsql_appender_append_long(
    a: *mut TrexAppender,
    val: i64,
) -> i32 {
    if a.is_null() {
        return -1;
    }
    match (*a).append_long(val) {
        Ok(()) => 0,
        Err(e) => {
            error::set_last_error(&e);
            -1
        }
    }
}

/// Append an int value. Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn trexsql_appender_append_int(
    a: *mut TrexAppender,
    val: i32,
) -> i32 {
    if a.is_null() {
        return -1;
    }
    match (*a).append_int(val) {
        Ok(()) => 0,
        Err(e) => {
            error::set_last_error(&e);
            -1
        }
    }
}

/// Append a double value. Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn trexsql_appender_append_double(
    a: *mut TrexAppender,
    val: f64,
) -> i32 {
    if a.is_null() {
        return -1;
    }
    match (*a).append_double(val) {
        Ok(()) => 0,
        Err(e) => {
            error::set_last_error(&e);
            -1
        }
    }
}

/// Append a boolean value (0=false, non-zero=true). Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn trexsql_appender_append_boolean(
    a: *mut TrexAppender,
    val: i32,
) -> i32 {
    if a.is_null() {
        return -1;
    }
    match (*a).append_boolean(val != 0) {
        Ok(()) => 0,
        Err(e) => {
            error::set_last_error(&e);
            -1
        }
    }
}

/// Flush pending data. Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn trexsql_appender_flush(a: *mut TrexAppender) -> i32 {
    if a.is_null() {
        return -1;
    }
    match (*a).flush() {
        Ok(()) => 0,
        Err(e) => {
            error::set_last_error(&e);
            -1
        }
    }
}

/// Close the appender (flushes and frees). Returns 0 on success, -1 on error.
/// After this call, the pointer is invalid.
#[no_mangle]
pub unsafe extern "C" fn trexsql_appender_close(a: *mut TrexAppender) -> i32 {
    if a.is_null() {
        return 0;
    }
    let appender = Box::from_raw(a);
    match appender.close() {
        Ok(()) => 0,
        Err(e) => {
            error::set_last_error(&e);
            -1
        }
    }
}
