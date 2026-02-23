use libduckdb_sys as ffi;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, RwLock};

/// Wraps a raw connection for static storage. Protected by Mutex.
struct DdlConn(ffi::duckdb_connection);

// SAFETY: exclusive access via Mutex.
unsafe impl Send for DdlConn {}
unsafe impl Sync for DdlConn {}

impl Drop for DdlConn {
    fn drop(&mut self) {
        unsafe {
            if !self.0.is_null() {
                ffi::duckdb_disconnect(&mut self.0);
            }
        }
    }
}

static DDL_CONNECTION: OnceLock<Mutex<DdlConn>> = OnceLock::new();

/// Create and store a DDL connection. Must be called during extension init.
pub fn init_ddl_connection(db: ffi::duckdb_database) -> Result<(), String> {
    if DDL_CONNECTION.get().is_some() {
        return Ok(());
    }
    unsafe {
        let mut conn: ffi::duckdb_connection = std::ptr::null_mut();
        let state = ffi::duckdb_connect(db, &mut conn);
        if state != ffi::duckdb_state_DuckDBSuccess {
            return Err("Failed to create DDL connection".to_string());
        }
        if DDL_CONNECTION.set(Mutex::new(DdlConn(conn))).is_err() {
            ffi::duckdb_disconnect(&mut conn);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TableAttachmentInfo {
    pub url: String,
    pub hana_schema: String,
    pub hana_table: String,
}

#[derive(Debug, Clone)]
pub struct AttachedDatabase {
    pub url: String,
    pub dbname: String,
    pub schema: String,
    pub table_names: Vec<String>,
}

#[derive(Debug, Default)]
pub struct HanaGlobalState {
    /// Key: uppercase `HANA__<dbname>_<schema>_<TABLE>`
    pub table_registry: HashMap<String, TableAttachmentInfo>,
    /// Key: `<dbname>|<schema>`
    pub attachments: HashMap<String, AttachedDatabase>,
}

static GLOBAL_STATE: OnceLock<RwLock<HanaGlobalState>> = OnceLock::new();

fn global_state() -> &'static RwLock<HanaGlobalState> {
    GLOBAL_STATE.get_or_init(|| RwLock::new(HanaGlobalState::default()))
}

pub fn read_state<F, R>(f: F) -> R
where
    F: FnOnce(&HanaGlobalState) -> R,
{
    let guard = global_state().read().expect("HANA global state read lock poisoned");
    f(&guard)
}

pub fn write_state<F, R>(f: F) -> R
where
    F: FnOnce(&mut HanaGlobalState) -> R,
{
    let mut guard = global_state().write().expect("HANA global state write lock poisoned");
    f(&mut guard)
}

pub fn prefixed_name(dbname: &str, schema: &str, table: &str) -> String {
    format!("HANA__{}_{}_{}", dbname, schema, table)
}

pub fn duckdb_schema_name(dbname: &str, schema: &str) -> String {
    format!("{}_{}", dbname, schema)
}

pub fn attachment_key(dbname: &str, schema: &str) -> String {
    format!("{}|{}", dbname, schema)
}

/// Escape `"` inside SQL identifiers: `"` -> `""`.
pub fn escape_identifier(s: &str) -> String {
    s.replace('"', "\"\"")
}

pub fn execute_ddl(sql: &str) -> Result<(), String> {
    execute_ddl_batch(&[sql])
}

/// Execute SQL statements sequentially. Stops at the first error.
pub fn execute_ddl_batch(statements: &[&str]) -> Result<(), String> {
    let ddl_mutex = DDL_CONNECTION
        .get()
        .ok_or("DDL connection not initialized")?;
    let ddl_guard = ddl_mutex
        .lock()
        .map_err(|_| "DDL connection mutex poisoned".to_string())?;
    let conn = ddl_guard.0;

    unsafe {
        for sql in statements {
            let c_sql = std::ffi::CString::new(*sql)
                .map_err(|e| format!("CString error: {}", e))?;
            let mut result: ffi::duckdb_result = std::mem::zeroed();
            let q_state = ffi::duckdb_query(conn, c_sql.as_ptr(), &mut result);

            let ok = if q_state != ffi::duckdb_state_DuckDBSuccess {
                let err_ptr = ffi::duckdb_result_error(&mut result);
                let err_msg = if err_ptr.is_null() {
                    format!("DDL execution failed: {}", sql)
                } else {
                    let c_str = std::ffi::CStr::from_ptr(err_ptr);
                    format!("DDL execution failed: {}", c_str.to_string_lossy())
                };
                Err(err_msg)
            } else {
                Ok(())
            };

            ffi::duckdb_destroy_result(&mut result);
            ok?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefixed_name() {
        assert_eq!(prefixed_name("dev", "SYS", "DUMMY"), "HANA__dev_SYS_DUMMY");
    }

    #[test]
    fn test_duckdb_schema_name() {
        assert_eq!(duckdb_schema_name("dev", "SYS"), "dev_SYS");
    }

    #[test]
    fn test_attachment_key() {
        assert_eq!(attachment_key("dev", "SYS"), "dev|SYS");
    }

    #[test]
    fn test_escape_identifier() {
        assert_eq!(escape_identifier("DUMMY"), "DUMMY");
        assert_eq!(escape_identifier(r#"ta"ble"#), r#"ta""ble"#);
        assert_eq!(escape_identifier(r#""""#), r#""""""#);
        assert_eq!(escape_identifier(""), "");
    }

    #[test]
    fn test_write_and_read_state() {
        write_state(|state| {
            state.table_registry.insert(
                "HANA__DEV_SYS_DUMMY".to_string(),
                TableAttachmentInfo {
                    url: "hdbsql://u:p@h:30015/HDB".to_string(),
                    hana_schema: "SYS".to_string(),
                    hana_table: "DUMMY".to_string(),
                },
            );
        });
        let found = read_state(|state| state.table_registry.contains_key("HANA__DEV_SYS_DUMMY"));
        assert!(found);
    }
}
