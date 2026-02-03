pub mod connection;

use base64::{engine::general_purpose, Engine as _};
use deno_core::op2;
use deno_error::JsError;
use thiserror::Error;

/// Error type for trex operations that implements JsErrorClass
#[derive(Debug, Error, JsError)]
pub enum TrexError {
  #[class(generic)]
  #[error("{0}")]
  Generic(String),
  #[class(generic)]
  #[error("Resource error: {0}")]
  Resource(#[from] deno_core::error::ResourceError),
}
use duckdb::arrow::array::{
  Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
  Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
  LargeBinaryArray, LargeStringArray, StringArray, Time32SecondArray,
  Time64MicrosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
  TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array,
  UInt64Array, UInt8Array,
};
use duckdb::arrow::datatypes::{DataType, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{
  params_from_iter, types::ToSqlOutput, types::Value, Config, Connection, ToSql,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::cell::RefCell;
use std::env;
use std::error::Error as StdError;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, LazyLock, Mutex};
use tracing::warn;
use uuid::Uuid;

use deno_core::{OpState, Resource, ResourceId};
use std::collections::HashMap;
use std::rc::Rc;
use tokio::sync::{mpsc, oneshot};

type PendingRequestsMap =
  Arc<Mutex<HashMap<String, oneshot::Sender<JsonValue>>>>;
type RequestChannelType = Arc<Mutex<Option<mpsc::Sender<JsonValue>>>>;

static TREX_DB: LazyLock<Arc<Mutex<Connection>>> = LazyLock::new(|| {
  let cfg = match Config::default().allow_unsigned_extensions() {
    Ok(c) => c,
    Err(e) => {
      eprintln!("Failed to allow unsigned extensions: {e}");
      Config::default()
    }
  };
  let conn = Connection::open_in_memory_with_flags(cfg)
    .expect("Failed to open DuckDB in-memory with config");
  if let Ok(path) = std::env::var("DUCKDB_CIRCE_EXTENSION") {
    let escaped = path.replace('\'', "''");
    if let Err(e) = conn.execute(&format!("LOAD '{}'", escaped), []) {
      eprintln!("Failed to LOAD extension from {}: {e}", path);
    }
  } else {
    let _ = conn.execute("LOAD circe", []);
  }
  let conn_arc = Arc::new(Mutex::new(conn));
  let _ = connection::init_owned_connection(conn_arc.clone());
  conn_arc
});
static DB_CREDENTIALS: LazyLock<Arc<Mutex<String>>> = LazyLock::new(|| {
  Arc::new(Mutex::new(String::from(
    "{\"credentials\":[], \"publications\":{}}",
  )))
});

static REQUEST_CHANNEL: LazyLock<RequestChannelType> =
  LazyLock::new(|| Arc::new(Mutex::new(None)));

static PENDING_REQUESTS: LazyLock<PendingRequestsMap> =
  LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

fn get_active_connection() -> Arc<Mutex<Connection>> {
  connection::get_connection().unwrap_or_else(|| TREX_DB.clone())
}

#[op2]
#[string]
fn op_get_dbc() -> String {
  return (*(*DB_CREDENTIALS)).lock().unwrap().clone();
}

#[op2]
#[string]
fn op_get_dbc2() -> String {
  let mut base_creds: serde_json::Value =
    serde_json::from_str(&(*(*DB_CREDENTIALS)).lock().unwrap().clone())
      .unwrap_or_else(
        |_| serde_json::json!({"credentials": [], "publications": {}}),
      );

  if let (Ok(host), Ok(port), Ok(user), Ok(password), Ok(dbname)) = (
    std::env::var("TREX__SQL__HOST"),
    std::env::var("TREX__SQL__PORT"),
    std::env::var("TREX__SQL__USER"),
    std::env::var("TREX__SQL__PASSWORD"),
    std::env::var("TREX__SQL__DBNAME"),
  ) {
    let result_db = serde_json::json!({
      "id": "RESULT",
      "code": "RESULT",
      "dialect": "postgres",
      "authentication_mode": "Password",
      "host": host,
      "port": port.parse::<u16>().unwrap_or(5432),
      "name": dbname,
      "credentials": [
        {
          "username": user,
          "password": password,
          "userScope": "Admin",
          "serviceScope": "Internal"
        }
      ],
      "publications": [],
      "vocab_schemas": []
    });

    if let Some(credentials) = base_creds
      .get_mut("credentials")
      .and_then(|c| c.as_array_mut())
    {
      if !credentials
        .iter()
        .any(|c| c.get("id").and_then(|id| id.as_str()) == Some("RESULT"))
      {
        credentials.push(result_db);
      }
    }
  }

  if let (Ok(host), Ok(dbname), Ok(user), Ok(password)) = (
    std::env::var("PG__HOST"),
    std::env::var("PG__FHIR_DB_NAME"),
    std::env::var("PG_USER"),
    std::env::var("PG_PASSWORD"),
  ) {
    let port = std::env::var("PG__PORT")
      .ok()
      .and_then(|p| p.parse::<u16>().ok())
      .unwrap_or(5432);

    let fhir_db = serde_json::json!({
      "id": "FHIR",
      "code": "FHIR",
      "dialect": "postgres",
      "authentication_mode": "Password",
      "host": host,
      "port": port,
      "name": dbname,
      "credentials": [
        {
          "username": user,
          "password": password,
          "userScope": "Admin",
          "serviceScope": "Internal"
        }
      ],
      "publications": [],
      "vocab_schemas": []
    });

    if let Some(credentials) = base_creds
      .get_mut("credentials")
      .and_then(|c| c.as_array_mut())
    {
      if !credentials
        .iter()
        .any(|c| c.get("id").and_then(|id| id.as_str()) == Some("FHIR"))
      {
        credentials.push(fhir_db);
      }
    }
  }

  serde_json::to_string(&base_creds).unwrap_or_else(|_| {
    String::from("{\"credentials\":[], \"publications\":{}}")
  })
}

#[op2(fast)]
fn op_set_dbc(#[string] dbc: String) {
  *(*(*DB_CREDENTIALS)).lock().unwrap() = dbc;
}

#[op2(fast)]
fn op_install_plugin(#[string] name: String, #[string] dir: String) {
  // Check if we should use node_modules structure (for backward compatibility with bun)
  // Environment variable: TPM_USE_NODE_MODULES=false to disable (default: true)
  let use_node_modules = env::var("TPM_USE_NODE_MODULES")
    .unwrap_or_else(|_| "true".to_string())
    .to_lowercase()
    != "false";

  // Determine install directory based on structure preference
  let install_dir = if use_node_modules {
    format!("{}/node_modules", dir)
  } else {
    dir.clone()
  };

  // Try to load TPM extension (ignore if already loaded)
  let _ = execute_query("memory".to_string(), "LOAD 'tpm'".to_string(), vec![]);

  // Escape SQL special characters
  let escaped_name = name.replace("'", "''");
  let escaped_dir = install_dir.replace("'", "''");

  let sql = format!(
    "SELECT install_results FROM tpm_install('{}', '{}')",
    escaped_name, escaped_dir
  );

  let result = execute_query("memory".to_string(), sql, vec![]);

  match result {
    Ok(json_str) => {
      match serde_json::from_str::<Vec<serde_json::Value>>(&json_str) {
        Ok(rows) => {
          if rows.is_empty() {
            eprintln!("Warning: No packages installed for: {}", name);
            return;
          }

          let mut success_count = 0;
          let mut error_count = 0;

          for row in rows {
            if let Some(install_result) = row.get("install_results") {
              if let Ok(result_str) =
                serde_json::from_value::<String>(install_result.clone())
              {
                if let Ok(result_obj) =
                  serde_json::from_str::<serde_json::Value>(&result_str)
                {
                  let package = result_obj
                    .get("package")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                  let version = result_obj
                    .get("version")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                  let success = result_obj
                    .get("success")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);

                  if success {
                    println!("Successfully installed: {}@{}", package, version);
                    success_count += 1;
                  } else {
                    let error = result_obj
                      .get("error")
                      .and_then(|v| v.as_str())
                      .unwrap_or("unknown error");
                    eprintln!("Failed to install {}: {}", package, error);
                    error_count += 1;
                  }
                }
              }
            }
          }

          println!(
            "Plugin installation complete: {} succeeded, {} failed",
            success_count, error_count
          );
        }
        Err(e) => {
          eprintln!("Warning: Failed to parse installation results: {}. Raw response: {}", e, json_str);
        }
      }
    }
    Err(e) => {
      eprintln!("Warning: Failed to install plugin '{}': {}. Make sure TPM extension is installed.", name, e);
    }
  }
}

#[derive(Serialize, Deserialize)]
enum TrexType {
  Integer(i64),
  String(String),
  Number(f64),
  DateTime(i64),
}

impl ToSql for TrexType {
  fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
    match self {
      TrexType::Integer(v) => {
        let value: Value = (*v).into();
        Ok(ToSqlOutput::Owned(value))
      }
      TrexType::String(v) => {
        let value: Value = v.clone().into();
        Ok(ToSqlOutput::Owned(value))
      }
      TrexType::DateTime(v) => {
        let value: Value =
          Value::Timestamp(duckdb::types::TimeUnit::Millisecond, *v);
        Ok(ToSqlOutput::Owned(value))
      }
      TrexType::Number(v) => {
        let value: Value = (*v).into();
        Ok(ToSqlOutput::Owned(value))
      }
    }
  }
}

fn field_value_to_json(
  array: &dyn Array,
  row: usize,
  dt: &DataType,
) -> JsonValue {
  if array.is_null(row) {
    return JsonValue::Null;
  }
  match dt {
    DataType::Utf8 => {
      let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
      JsonValue::String(arr.value(row).to_string())
    }
    DataType::LargeUtf8 => {
      let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
      JsonValue::String(arr.value(row).to_string())
    }
    DataType::Binary => {
      let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
      let bytes = arr.value(row);
      JsonValue::String(general_purpose::STANDARD.encode(bytes))
    }
    DataType::LargeBinary => {
      let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
      let bytes = arr.value(row);
      JsonValue::String(general_purpose::STANDARD.encode(bytes))
    }
    DataType::Int8 => {
      let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
      JsonValue::from(arr.value(row) as i64)
    }
    DataType::Int16 => {
      let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
      JsonValue::from(arr.value(row) as i64)
    }
    DataType::Int32 => {
      let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
      JsonValue::from(arr.value(row) as i64)
    }
    DataType::Int64 => {
      let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
      JsonValue::from(arr.value(row))
    }
    DataType::UInt8 => {
      let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
      JsonValue::from(arr.value(row) as u64)
    }
    DataType::UInt16 => {
      let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
      JsonValue::from(arr.value(row) as u64)
    }
    DataType::UInt32 => {
      let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
      JsonValue::from(arr.value(row) as u64)
    }
    DataType::UInt64 => {
      let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
      JsonValue::from(arr.value(row))
    }
    DataType::Float32 => {
      let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
      JsonValue::from(arr.value(row) as f64)
    }
    DataType::Float64 => {
      let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
      JsonValue::from(arr.value(row))
    }
    DataType::Boolean => {
      let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
      JsonValue::from(arr.value(row))
    }
    DataType::Date32 => {
      let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
      let days = arr.value(row);
      let timestamp = days as i64 * 86400;
      let datetime = chrono::DateTime::from_timestamp(timestamp, 0)
        .unwrap_or(chrono::DateTime::UNIX_EPOCH);
      JsonValue::String(datetime.format("%Y-%m-%d").to_string())
    }
    DataType::Date64 => {
      let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
      let millis = arr.value(row);
      let datetime = chrono::DateTime::from_timestamp_millis(millis)
        .unwrap_or(chrono::DateTime::UNIX_EPOCH);
      JsonValue::String(datetime.format("%Y-%m-%d").to_string())
    }
    DataType::Time32(_) => {
      let arr = array.as_any().downcast_ref::<Time32SecondArray>().unwrap();
      JsonValue::from(arr.value(row))
    }
    DataType::Time64(_) => {
      let arr = array
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap();
      JsonValue::from(arr.value(row))
    }
    DataType::Timestamp(TimeUnit::Second, _) => {
      let arr = array
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .unwrap();
      let seconds = arr.value(row);
      let datetime = chrono::DateTime::from_timestamp(seconds, 0)
        .unwrap_or(chrono::DateTime::UNIX_EPOCH);
      JsonValue::String(datetime.to_rfc3339())
    }
    DataType::Timestamp(TimeUnit::Millisecond, _) => {
      let arr = array
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
      let millis = arr.value(row);
      let datetime = chrono::DateTime::from_timestamp_millis(millis)
        .unwrap_or(chrono::DateTime::UNIX_EPOCH);
      JsonValue::String(datetime.to_rfc3339())
    }
    DataType::Timestamp(TimeUnit::Microsecond, _) => {
      let arr = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
      let micros = arr.value(row);
      let datetime = chrono::DateTime::from_timestamp_micros(micros)
        .unwrap_or(chrono::DateTime::UNIX_EPOCH);
      JsonValue::String(datetime.to_rfc3339())
    }
    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
      let arr = array
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
      let nanos = arr.value(row);
      let datetime = chrono::DateTime::from_timestamp_nanos(nanos);
      JsonValue::String(datetime.to_rfc3339())
    }
    DataType::Decimal128(_, scale) => {
      let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
      let value = arr.value(row);
      let decimal_value = value as f64 / 10_f64.powi(*scale as i32);
      JsonValue::from(decimal_value)
    }
    _ => JsonValue::Null,
  }
}

fn record_batches_to_json(batches: &[RecordBatch]) -> String {
  let mut rows: Vec<JsonValue> = Vec::new();
  for batch in batches {
    let schema = batch.schema();
    let n_rows = batch.num_rows();
    for r in 0..n_rows {
      let mut obj = JsonMap::with_capacity(batch.num_columns());
      for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        let v = field_value_to_json(col.as_ref(), r, field.data_type());
        obj.insert(field.name().clone(), v);
      }
      rows.push(JsonValue::Object(obj));
    }
  }
  serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_string())
}

/// Extract a human-readable message from a panic payload
fn extract_panic_message(panic_err: Box<dyn std::any::Any + Send>) -> String {
  if let Some(s) = panic_err.downcast_ref::<&str>() {
    s.to_string()
  } else if let Some(s) = panic_err.downcast_ref::<String>() {
    s.clone()
  } else {
    "Unknown panic".to_string()
  }
}

fn execute_query(
  database: String,
  sql: String,
  params: Vec<TrexType>,
) -> Result<String, TrexError> {
  // Wrap the entire DuckDB operation in catch_unwind to prevent panics
  // from external extensions (like hana_scan) from crashing the V8 runtime
  let result = panic::catch_unwind(AssertUnwindSafe(|| {
    execute_query_inner(database, sql, params)
  }));

  match result {
    Ok(inner_result) => inner_result,
    Err(panic_err) => {
      let panic_msg = extract_panic_message(panic_err);
      Err(TrexError::Generic(format!(
        "Query execution panicked: {panic_msg}"
      )))
    }
  }
}

fn execute_query_inner(
  database: String,
  sql: String,
  params: Vec<TrexType>,
) -> Result<String, TrexError> {
  let conn_arc = get_active_connection();
  let conn = match conn_arc.lock() {
    Ok(guard) => guard,
    Err(poisoned) => {
      warn!("Lock was poisoned, recovering");
      poisoned.into_inner()
    }
  };
  let _ = conn
    .execute(&format!("USE {database}"), [])
    .inspect_err(|e| warn!("{e}"));
  if sql.trim().is_empty() {
    return Ok("[]".to_string());
  }
  let tmpstmt = conn
    .prepare(&sql)
    .inspect_err(|e| warn!("prepare error: {e:?}"));
  match tmpstmt {
    Ok(mut stmt) => match stmt.query_arrow(params_from_iter(params.iter())) {
      Ok(iter) => {
        let batches: Vec<RecordBatch> = iter.collect();
        Ok(record_batches_to_json(&batches))
      }
      Err(e) => Err(TrexError::Generic(format!("Query execution failed: {e}"))),
    },
    Err(e) => {
      // Build full error chain for better debugging
      let err: &dyn StdError = &e;
      let mut msg = format!("{err}");
      let mut source = err.source();
      while let Some(s) = source {
        msg = format!("{msg}: {s}");
        source = s.source();
      }
      Err(TrexError::Generic(format!("Query failed: {msg}")))
    }
  }
}

#[op2]
#[string]
fn op_execute_query(
  #[string] database: String,
  #[string] sql: String,
  #[serde] params: Vec<TrexType>,
) -> Result<String, TrexError> {
  execute_query(database, sql, params)
}

pub struct QueryStreamResource {
  receiver: Arc<Mutex<mpsc::Receiver<String>>>,
}

impl Resource for QueryStreamResource {
  fn name(&self) -> std::borrow::Cow<str> {
    "QueryStreamResource".into()
  }
}

pub struct RequestResource {
  receiver: RefCell<Option<mpsc::Receiver<JsonValue>>>,
}

impl Resource for RequestResource {
  fn name(&self) -> std::borrow::Cow<str> {
    "RequestResource".into()
  }
}

#[op2(async)]
#[serde]
async fn op_req(#[serde] message: JsonValue) -> Result<JsonValue, TrexError> {
  let request_id = Uuid::new_v4().to_string();

  let (response_sender, response_receiver) = oneshot::channel::<JsonValue>();

  {
    let mut pending = PENDING_REQUESTS.lock().unwrap();
    pending.insert(request_id.clone(), response_sender);
  }

  let request_with_id = serde_json::json!({
    "id": request_id,
    "message": message
  });

  let send_result = {
    let channel_guard = REQUEST_CHANNEL.lock().unwrap();
    if let Some(sender) = channel_guard.as_ref() {
      sender.try_send(request_with_id)
    } else {
      return {
        let mut pending = PENDING_REQUESTS.lock().unwrap();
        pending.remove(&request_id);
        Err(TrexError::Generic("No active listeners".to_string()))
      };
    }
  };

  match send_result {
    Ok(()) => {
      match tokio::time::timeout(
        std::time::Duration::from_secs(30),
        response_receiver,
      )
      .await
      {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(_)) => {
          let mut pending = PENDING_REQUESTS.lock().unwrap();
          pending.remove(&request_id);
          Err(TrexError::Generic("Request cancelled".to_string()))
        }
        Err(_) => {
          let mut pending = PENDING_REQUESTS.lock().unwrap();
          pending.remove(&request_id);
          Err(TrexError::Generic("Request timeout".to_string()))
        }
      }
    }
    Err(_) => {
      let mut pending = PENDING_REQUESTS.lock().unwrap();
      pending.remove(&request_id);
      Err(TrexError::Generic("Failed to send request".to_string()))
    }
  }
}

#[op2]
#[serde]
fn op_req_listen(state: &mut OpState) -> Result<ResourceId, TrexError> {
  let (sender, receiver) = mpsc::channel::<JsonValue>(1000);

  {
    let mut channel_guard = REQUEST_CHANNEL.lock().unwrap();
    *channel_guard = Some(sender);
  }

  let resource = RequestResource {
    receiver: RefCell::new(Some(receiver)),
  };
  Ok(state.resource_table.add(resource))
}

#[op2(async)]
#[serde]
async fn op_req_next(
  state: Rc<RefCell<OpState>>,
  #[smi] rid: ResourceId,
) -> Result<Option<JsonValue>, TrexError> {
  let resource = state.borrow().resource_table.get::<RequestResource>(rid)?;

  let receiver = resource.receiver.borrow_mut().take();

  if let Some(mut rx) = receiver {
    let next_message = rx.recv().await;

    if next_message.is_none() {
      {
        let mut channel_guard = REQUEST_CHANNEL.lock().unwrap();
        *channel_guard = None;
      }

      state
        .borrow_mut()
        .resource_table
        .take::<RequestResource>(rid)?;
    } else {
      resource.receiver.borrow_mut().replace(rx);
    }

    Ok(next_message)
  } else {
    Ok(None)
  }
}

#[op2]
#[serde]
fn op_req_respond(
  #[string] request_id: String,
  #[serde] response: JsonValue,
) -> Result<serde_json::Value, TrexError> {
  let mut pending = PENDING_REQUESTS.lock().unwrap();

  if let Some(sender) = pending.remove(&request_id) {
    match sender.send(response) {
      Ok(()) => Ok(serde_json::Value::Bool(true)),
      Err(_) => Ok(serde_json::Value::Bool(false)),
    }
  } else {
    Ok(serde_json::Value::Bool(false))
  }
}

#[op2]
#[serde]
fn op_execute_query_stream(
  state: &mut OpState,
  #[string] database: String,
  #[string] sql: String,
  #[serde] params: Vec<TrexType>,
) -> Result<ResourceId, TrexError> {
  let (sender, receiver) = mpsc::channel::<String>(1000);
  let conn_arc = get_active_connection();
  tokio::spawn(async move {
    tokio::task::spawn_blocking(move || {
      // Wrap DuckDB operations in catch_unwind to prevent panics from crashing V8
      let result = panic::catch_unwind(AssertUnwindSafe(|| {
        let conn = match conn_arc.lock() {
          Ok(guard) => guard,
          Err(poisoned) => {
            warn!("Lock was poisoned in streaming query, recovering");
            poisoned.into_inner()
          }
        };
        if conn.execute(&format!("USE {database}"), []).is_err() {
          return;
        }
        if let Ok(mut stmt) = conn.prepare(&sql) {
          if let Ok(iter) = stmt.query_arrow(params_from_iter(params.iter())) {
            for batch in iter {
              let json = record_batches_to_json(std::slice::from_ref(&batch));
              if sender.blocking_send(json).is_err() {
                break;
              }
            }
          }
        }
      }));
      if let Err(e) = result {
        let msg = extract_panic_message(e);
        warn!("Streaming query panicked: {msg}");
        let error_json = serde_json::json!({
          "error": format!("Query execution panicked: {}", msg)
        });
        let _ = sender.blocking_send(error_json.to_string());
      }
    });
  });
  let resource = QueryStreamResource {
    receiver: Arc::new(Mutex::new(receiver)),
  };
  Ok(state.resource_table.add(resource))
}

#[allow(clippy::await_holding_lock)]
#[op2(async)]
#[string]
async fn op_execute_query_stream_next(
  state: Rc<RefCell<OpState>>,
  #[smi] rid: ResourceId,
) -> Result<Option<String>, TrexError> {
  let resource = state
    .borrow()
    .resource_table
    .get::<QueryStreamResource>(rid)?;

  let mut rx = match resource.receiver.lock() {
    Ok(guard) => guard,
    Err(poisoned) => {
      warn!("Lock was poisoned in stream_next, recovering");
      poisoned.into_inner()
    }
  };
  let next_chunk = rx.recv().await;

  if next_chunk.is_none() {
    state
      .borrow_mut()
      .resource_table
      .take::<QueryStreamResource>(rid)?;
  }
  Ok(next_chunk)
}

deno_core::extension!(
    trex,
    ops = [
        op_install_plugin,
        op_execute_query,
        op_get_dbc,
        op_get_dbc2,
        op_set_dbc,
        op_execute_query_stream,
        op_execute_query_stream_next,
        op_req,
        op_req_listen,
        op_req_next,
        op_req_respond
    ],
    esm_entry_point = "ext:trex/trex_lib.js",
    esm = [
        dir "js",
        "trex_lib.js",
        "dbconnection.js"
    ]
);
