use std::ffi::CString;
use std::mem::ManuallyDrop;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Mutex;

use duckdb::ffi;
use duckdb::Connection;

use crate::result::TrexResult;

pub struct TrexDatabase {
    conn: Mutex<ManuallyDrop<Connection>>,
    raw_db: ffi::duckdb_database,
}

unsafe impl Send for TrexDatabase {}
unsafe impl Sync for TrexDatabase {}

impl TrexDatabase {
    /// `flags`: bit 0 = allow unsigned extensions.
    pub fn open(path: &str, flags: u32) -> Result<Self, String> {
        unsafe {
            let mut config: ffi::duckdb_config = ptr::null_mut();
            if ffi::duckdb_create_config(&mut config) != ffi::DuckDBSuccess {
                return Err("Failed to create config".into());
            }

            if flags & 1 != 0 {
                let key = CString::new("allow_unsigned_extensions").unwrap();
                let val = CString::new("true").unwrap();
                ffi::duckdb_set_config(config, key.as_ptr(), val.as_ptr());
            }

            let c_path = CString::new(path).map_err(|e| e.to_string())?;
            let mut raw_db: ffi::duckdb_database = ptr::null_mut();
            let mut c_err: *mut c_char = ptr::null_mut();

            let rc = ffi::duckdb_open_ext(
                c_path.as_ptr(),
                &mut raw_db,
                config,
                &mut c_err,
            );
            ffi::duckdb_destroy_config(&mut config);

            if rc != ffi::DuckDBSuccess {
                let msg = if c_err.is_null() {
                    "Failed to open database".to_string()
                } else {
                    let s = std::ffi::CStr::from_ptr(c_err)
                        .to_string_lossy()
                        .to_string();
                    ffi::duckdb_free(c_err as *mut std::ffi::c_void);
                    s
                };
                return Err(msg);
            }

            // Connection won't close the database on drop -- we manage that in TrexDatabase::drop
            let conn = Connection::open_from_raw(raw_db)
                .map_err(|e| format!("Failed to create connection: {e}"))?;

            let _ = conn.execute_batch("CALL disable_logging()");

            Ok(TrexDatabase {
                conn: Mutex::new(ManuallyDrop::new(conn)),
                raw_db,
            })
        }
    }

    pub fn execute(&self, sql: &str) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        conn.execute_batch(sql)
            .map_err(|e| format!("{e}"))
    }

    pub fn query(&self, sql: &str) -> Result<TrexResult, String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        TrexResult::from_query(&conn, sql)
    }

    pub fn raw_db(&self) -> ffi::duckdb_database {
        self.raw_db
    }
}

impl Drop for TrexDatabase {
    fn drop(&mut self) {
        let conn = self.conn.get_mut().unwrap_or_else(|e| e.into_inner());
        unsafe { ManuallyDrop::drop(conn); }
        if !self.raw_db.is_null() {
            unsafe {
                ffi::duckdb_close(&mut self.raw_db);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_memory() {
        let db = TrexDatabase::open(":memory:", 0).unwrap();
        db.execute("CREATE TABLE t (x INTEGER)").unwrap();
        db.execute("INSERT INTO t VALUES (42)").unwrap();
        let result = db.query("SELECT x FROM t").unwrap();
        assert_eq!(result.column_count(), 1);
    }

    #[test]
    fn test_open_unsigned() {
        let db = TrexDatabase::open(":memory:", 1).unwrap();
        db.execute("SELECT 1").unwrap();
    }

    #[test]
    fn test_execute_error() {
        let db = TrexDatabase::open(":memory:", 0).unwrap();
        let err = db.execute("SELECT * FROM nonexistent_table").unwrap_err();
        assert!(err.contains("nonexistent_table"));
    }

    #[test]
    fn test_error_handling() {
        crate::error::set_last_error("test error");
        let ptr = crate::error::last_error_ptr();
        assert!(!ptr.is_null());
        crate::error::clear_last_error();
        let ptr = crate::error::last_error_ptr();
        assert!(ptr.is_null());
    }
}
