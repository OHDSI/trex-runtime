use std::ffi::CString;
use std::os::raw::c_char;

use duckdb::types::Value;
use duckdb::Connection;

pub struct TrexResult {
    column_names: Vec<CString>,
    rows: Vec<Vec<Value>>,
    current_row: isize, // -1 initially; trexsql_result_next increments
}

impl TrexResult {
    pub fn from_query(conn: &Connection, sql: &str) -> Result<Self, String> {
        let mut stmt = conn.prepare(sql).map_err(|e| format!("{e}"))?;
        let mut rows_iter = stmt.query([]).map_err(|e| format!("{e}"))?;

        // Column info is only available after query() has been called
        let (column_count, column_names) = {
            let s = rows_iter.as_ref().expect("statement should be available");
            let names: Vec<CString> = s
                .column_names()
                .into_iter()
                .map(|n| CString::new(n).unwrap_or_else(|_| CString::new("?").unwrap()))
                .collect();
            (s.column_count(), names)
        };

        let mut rows = Vec::new();
        while let Some(row) = rows_iter.next().map_err(|e| format!("{e}"))? {
            let mut values = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let val = row
                    .get_ref(i)
                    .map(|v| v.to_owned())
                    .unwrap_or(Value::Null);
                values.push(val);
            }
            rows.push(values);
        }

        Ok(TrexResult {
            column_names,
            rows,
            current_row: -1,
        })
    }

    pub fn column_count(&self) -> i32 {
        self.column_names.len() as i32
    }

    /// Returned pointer is owned by the result; valid until result is freed.
    pub fn column_name(&self, col: i32) -> *const c_char {
        match self.column_names.get(col as usize) {
            Some(name) => name.as_ptr(),
            None => std::ptr::null(),
        }
    }

    pub fn next(&mut self) -> bool {
        self.current_row += 1;
        (self.current_row as usize) < self.rows.len()
    }

    fn current_value(&self, col: i32) -> Option<&Value> {
        if self.current_row < 0 {
            return None;
        }
        self.rows
            .get(self.current_row as usize)
            .and_then(|row| row.get(col as usize))
    }

    pub fn is_null(&self, col: i32) -> bool {
        match self.current_value(col) {
            Some(Value::Null) | None => true,
            _ => false,
        }
    }

    /// Caller must free with trexsql_free_string.
    pub fn get_string(&self, col: i32) -> Option<CString> {
        let val = self.current_value(col)?;
        let s = value_to_string(val);
        CString::new(s).ok()
    }

    pub fn get_long(&self, col: i32) -> i64 {
        match self.current_value(col) {
            Some(v) => value_to_long(v),
            None => 0,
        }
    }

    pub fn get_double(&self, col: i32) -> f64 {
        match self.current_value(col) {
            Some(v) => value_to_double(v),
            None => 0.0,
        }
    }
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Boolean(b) => b.to_string(),
        Value::TinyInt(i) => i.to_string(),
        Value::SmallInt(i) => i.to_string(),
        Value::Int(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::HugeInt(i) => i.to_string(),
        Value::UTinyInt(i) => i.to_string(),
        Value::USmallInt(i) => i.to_string(),
        Value::UInt(i) => i.to_string(),
        Value::UBigInt(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Double(f) => f.to_string(),
        Value::Decimal(d) => format!("{d}"),
        Value::Text(s) => s.clone(),
        Value::Blob(b) => hex::encode(b),
        Value::Timestamp(_, us) => format!("{us}"),
        Value::Date32(d) => d.to_string(),
        Value::Time64(_, t) => t.to_string(),
        _ => format!("{v:?}"),
    }
}

fn value_to_long(v: &Value) -> i64 {
    match v {
        Value::Boolean(b) => *b as i64,
        Value::TinyInt(i) => *i as i64,
        Value::SmallInt(i) => *i as i64,
        Value::Int(i) => *i as i64,
        Value::BigInt(i) => *i,
        Value::HugeInt(i) => *i as i64,
        Value::UTinyInt(i) => *i as i64,
        Value::USmallInt(i) => *i as i64,
        Value::UInt(i) => *i as i64,
        Value::UBigInt(i) => *i as i64,
        Value::Float(f) => *f as i64,
        Value::Double(f) => *f as i64,
        _ => 0,
    }
}

fn value_to_double(v: &Value) -> f64 {
    match v {
        Value::Boolean(b) => if *b { 1.0 } else { 0.0 },
        Value::TinyInt(i) => *i as f64,
        Value::SmallInt(i) => *i as f64,
        Value::Int(i) => *i as f64,
        Value::BigInt(i) => *i as f64,
        Value::HugeInt(i) => *i as f64,
        Value::UTinyInt(i) => *i as f64,
        Value::USmallInt(i) => *i as f64,
        Value::UInt(i) => *i as f64,
        Value::UBigInt(i) => *i as f64,
        Value::Float(f) => *f as f64,
        Value::Double(f) => *f,
        _ => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    #[test]
    fn test_query_result() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER, name VARCHAR)").unwrap();
        conn.execute_batch("INSERT INTO t VALUES (1, 'hello'), (2, 'world')").unwrap();

        let mut result = TrexResult::from_query(&conn, "SELECT id, name FROM t ORDER BY id").unwrap();
        assert_eq!(result.column_count(), 2);

        let name0 = unsafe { CStr::from_ptr(result.column_name(0)) };
        assert_eq!(name0.to_str().unwrap(), "id");
        let name1 = unsafe { CStr::from_ptr(result.column_name(1)) };
        assert_eq!(name1.to_str().unwrap(), "name");

        assert!(result.next());
        assert_eq!(result.get_long(0), 1);
        let s = result.get_string(1).unwrap();
        assert_eq!(s.to_str().unwrap(), "hello");

        assert!(result.next());
        assert_eq!(result.get_long(0), 2);
        let s = result.get_string(1).unwrap();
        assert_eq!(s.to_str().unwrap(), "world");

        assert!(!result.next());
    }

    #[test]
    fn test_null_handling() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (x INTEGER)").unwrap();
        conn.execute_batch("INSERT INTO t VALUES (NULL)").unwrap();

        let mut result = TrexResult::from_query(&conn, "SELECT x FROM t").unwrap();
        assert!(result.next());
        assert!(result.is_null(0));
        assert_eq!(result.get_long(0), 0);
    }
}
