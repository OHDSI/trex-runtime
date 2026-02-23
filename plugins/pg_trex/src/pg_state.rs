use duckdb::Connection;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, RwLock};

/// Wraps Connection for static storage. Protected by Mutex.
struct DdlConn(Connection);

// SAFETY: exclusive access via Mutex.
unsafe impl Send for DdlConn {}
unsafe impl Sync for DdlConn {}

static DDL_CONNECTION: OnceLock<Mutex<DdlConn>> = OnceLock::new();

/// Store a cloned DDL connection. Must be called during engine init.
pub fn init_ddl_connection(conn: &Connection) -> Result<(), String> {
    if DDL_CONNECTION.get().is_some() {
        return Ok(());
    }
    let cloned = conn
        .try_clone()
        .map_err(|e| format!("DDL connection clone: {e}"))?;
    // Race with another call is harmless — cloned connection is just dropped
    let _ = DDL_CONNECTION.set(Mutex::new(DdlConn(cloned)));
    Ok(())
}

#[derive(Debug, Clone)]
pub struct AttachedSchema {
    pub pg_schema: String,
    pub table_names: Vec<String>,
}

#[derive(Debug, Default)]
pub struct PgGlobalState {
    /// Key: PostgreSQL schema name
    pub attachments: HashMap<String, AttachedSchema>,
}

static GLOBAL_STATE: OnceLock<RwLock<PgGlobalState>> = OnceLock::new();

fn global_state() -> &'static RwLock<PgGlobalState> {
    GLOBAL_STATE.get_or_init(|| RwLock::new(PgGlobalState::default()))
}

pub fn read_state<F, R>(f: F) -> R
where
    F: FnOnce(&PgGlobalState) -> R,
{
    let guard = global_state()
        .read()
        .expect("pg global state read lock poisoned");
    f(&guard)
}

pub fn write_state<F, R>(f: F) -> R
where
    F: FnOnce(&mut PgGlobalState) -> R,
{
    let mut guard = global_state()
        .write()
        .expect("pg global state write lock poisoned");
    f(&mut guard)
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

    for sql in statements {
        ddl_guard
            .0
            .execute_batch(sql)
            .map_err(|e| format!("DDL execution failed: {e}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_identifier() {
        assert_eq!(escape_identifier("users"), "users");
        assert_eq!(escape_identifier(r#"ta"ble"#), r#"ta""ble"#);
        assert_eq!(escape_identifier(r#""""#), r#""""""#);
        assert_eq!(escape_identifier(""), "");
    }

    #[test]
    fn test_write_and_read_state() {
        write_state(|state| {
            state.attachments.insert(
                "public".to_string(),
                AttachedSchema {
                    pg_schema: "public".to_string(),
                    table_names: vec!["users".to_string()],
                },
            );
        });
        let found = read_state(|state| state.attachments.contains_key("public"));
        assert!(found);
    }
}
