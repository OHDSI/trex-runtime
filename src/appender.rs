use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;

use duckdb::ffi;

/// Owns its own connection to avoid borrowing the main TrexDatabase connection.
pub struct TrexAppender {
    raw_conn: ffi::duckdb_connection,
    raw_app: ffi::duckdb_appender,
}

unsafe impl Send for TrexAppender {}

impl TrexAppender {
    pub fn create(
        raw_db: ffi::duckdb_database,
        schema: &str,
        table: &str,
    ) -> Result<Self, String> {
        unsafe {
            let mut raw_conn: ffi::duckdb_connection = ptr::null_mut();
            let rc = ffi::duckdb_connect(raw_db, &mut raw_conn);
            if rc != ffi::DuckDBSuccess {
                return Err("Failed to create connection for appender".into());
            }

            let c_schema = CString::new(schema).map_err(|e| e.to_string())?;
            let c_table = CString::new(table).map_err(|e| e.to_string())?;
            let mut raw_app: ffi::duckdb_appender = ptr::null_mut();

            let rc = ffi::duckdb_appender_create(
                raw_conn,
                c_schema.as_ptr() as *const c_char,
                c_table.as_ptr() as *const c_char,
                &mut raw_app,
            );
            if rc != ffi::DuckDBSuccess {
                let msg = appender_error_message(raw_app)
                    .unwrap_or_else(|| "Failed to create appender".into());
                if !raw_app.is_null() {
                    ffi::duckdb_appender_destroy(&mut raw_app);
                }
                ffi::duckdb_disconnect(&mut raw_conn);
                return Err(msg);
            }

            Ok(TrexAppender { raw_conn, raw_app })
        }
    }

    pub fn end_row(&mut self) -> Result<(), String> {
        unsafe {
            let rc = ffi::duckdb_appender_end_row(self.raw_app);
            if rc != ffi::DuckDBSuccess {
                return Err(
                    appender_error_message(self.raw_app)
                        .unwrap_or_else(|| "end_row failed".into()),
                );
            }
            Ok(())
        }
    }

    pub fn append_null(&mut self) -> Result<(), String> {
        unsafe {
            let rc = ffi::duckdb_append_null(self.raw_app);
            if rc != ffi::DuckDBSuccess {
                return Err(
                    appender_error_message(self.raw_app)
                        .unwrap_or_else(|| "append_null failed".into()),
                );
            }
            Ok(())
        }
    }

    pub fn append_string(&mut self, val: &str) -> Result<(), String> {
        unsafe {
            let c_val = CString::new(val).map_err(|e| e.to_string())?;
            let rc = ffi::duckdb_append_varchar(self.raw_app, c_val.as_ptr());
            if rc != ffi::DuckDBSuccess {
                return Err(
                    appender_error_message(self.raw_app)
                        .unwrap_or_else(|| "append_string failed".into()),
                );
            }
            Ok(())
        }
    }

    pub fn append_long(&mut self, val: i64) -> Result<(), String> {
        unsafe {
            let rc = ffi::duckdb_append_int64(self.raw_app, val);
            if rc != ffi::DuckDBSuccess {
                return Err(
                    appender_error_message(self.raw_app)
                        .unwrap_or_else(|| "append_long failed".into()),
                );
            }
            Ok(())
        }
    }

    pub fn append_int(&mut self, val: i32) -> Result<(), String> {
        unsafe {
            let rc = ffi::duckdb_append_int32(self.raw_app, val);
            if rc != ffi::DuckDBSuccess {
                return Err(
                    appender_error_message(self.raw_app)
                        .unwrap_or_else(|| "append_int failed".into()),
                );
            }
            Ok(())
        }
    }

    pub fn append_double(&mut self, val: f64) -> Result<(), String> {
        unsafe {
            let rc = ffi::duckdb_append_double(self.raw_app, val);
            if rc != ffi::DuckDBSuccess {
                return Err(
                    appender_error_message(self.raw_app)
                        .unwrap_or_else(|| "append_double failed".into()),
                );
            }
            Ok(())
        }
    }

    pub fn append_boolean(&mut self, val: bool) -> Result<(), String> {
        unsafe {
            let rc = ffi::duckdb_append_bool(self.raw_app, val);
            if rc != ffi::DuckDBSuccess {
                return Err(
                    appender_error_message(self.raw_app)
                        .unwrap_or_else(|| "append_boolean failed".into()),
                );
            }
            Ok(())
        }
    }

    pub fn flush(&mut self) -> Result<(), String> {
        unsafe {
            let rc = ffi::duckdb_appender_flush(self.raw_app);
            if rc != ffi::DuckDBSuccess {
                return Err(
                    appender_error_message(self.raw_app)
                        .unwrap_or_else(|| "flush failed".into()),
                );
            }
            Ok(())
        }
    }

    pub fn close(mut self) -> Result<(), String> {
        self.close_inner()
    }

    fn close_inner(&mut self) -> Result<(), String> {
        if self.raw_app.is_null() {
            return Ok(());
        }
        unsafe {
            let rc = ffi::duckdb_appender_close(self.raw_app);
            let err = if rc != ffi::DuckDBSuccess {
                appender_error_message(self.raw_app)
            } else {
                None
            };
            ffi::duckdb_appender_destroy(&mut self.raw_app);
            self.raw_app = ptr::null_mut();
            ffi::duckdb_disconnect(&mut self.raw_conn);
            self.raw_conn = ptr::null_mut();
            match err {
                Some(msg) => Err(msg),
                None => Ok(()),
            }
        }
    }
}

impl Drop for TrexAppender {
    fn drop(&mut self) {
        let _ = self.close_inner();
    }
}

unsafe fn appender_error_message(app: ffi::duckdb_appender) -> Option<String> {
    if app.is_null() {
        return None;
    }
    let msg = ffi::duckdb_appender_error(app);
    if msg.is_null() {
        return None;
    }
    Some(
        std::ffi::CStr::from_ptr(msg)
            .to_string_lossy()
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_appender() {
        unsafe {
            let mut db: ffi::duckdb_database = ptr::null_mut();
            let mut err: *mut std::os::raw::c_char = ptr::null_mut();
            let c_path = CString::new(":memory:").unwrap();
            ffi::duckdb_open_ext(c_path.as_ptr(), &mut db, ptr::null_mut(), &mut err);

            let mut conn: ffi::duckdb_connection = ptr::null_mut();
            ffi::duckdb_connect(db, &mut conn);
            let sql = CString::new("CREATE TABLE t (id INTEGER, name VARCHAR, val DOUBLE)").unwrap();
            let mut result = std::mem::zeroed();
            ffi::duckdb_query(conn, sql.as_ptr(), &mut result);
            ffi::duckdb_destroy_result(&mut result);

            let mut app = TrexAppender::create(db, "main", "t").unwrap();
            app.append_int(1).unwrap();
            app.append_string("hello").unwrap();
            app.append_double(3.14).unwrap();
            app.end_row().unwrap();

            app.append_null().unwrap();
            app.append_string("world").unwrap();
            app.append_double(2.72).unwrap();
            app.end_row().unwrap();

            app.flush().unwrap();
            app.close().unwrap();

            let sql = CString::new("SELECT COUNT(*) FROM t").unwrap();
            let mut result = std::mem::zeroed();
            ffi::duckdb_query(conn, sql.as_ptr(), &mut result);
            let count = ffi::duckdb_value_int64(&mut result, 0, 0);
            assert_eq!(count, 2);
            ffi::duckdb_destroy_result(&mut result);

            ffi::duckdb_disconnect(&mut conn);
            ffi::duckdb_close(&mut db);
        }
    }
}
