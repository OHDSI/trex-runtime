pub mod clients;
pub mod connection;
pub mod conversions;
pub mod pipeline;
pub mod sql;
use std::process;

use base64::{engine::general_purpose, Engine as _};
use conversions::table::TableName;
use deno_core::error::AnyError;
use deno_core::op2;
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
use pgwire::tokio::process_socket;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
pub use sql::{
  auth::AuthType,
  duckdb::{TrexDuckDB, TrexDuckDBFactory},
};
use std::cell::RefCell;
use std::env;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::SystemTime;
use std::{error::Error, time::Duration};
use tokio::net::TcpListener;
use tracing::warn;
use uuid::Uuid;

use deno_core::{OpState, Resource, ResourceId};
use std::collections::HashMap;
use std::rc::Rc;
use tokio::sync::{mpsc, oneshot};

use crate::pipeline::{
  batching::{data_pipeline::BatchDataPipeline, BatchConfig},
  sinks::duckdb::DuckDbSink,
  sources::postgres::{PostgresSource, TableNamesFrom},
  PipelineAction,
};

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

pub async fn start_sql_server(ip: &str, port: u16, auth_type: AuthType) {
  let conn = get_active_connection();
  let factory = Arc::new(TrexDuckDBFactory {
    handler: Arc::new(TrexDuckDB::new(&conn)),
    auth_type,
  });
  let _server_addr = format!("{ip}:{port}");
  let server_addr = _server_addr.as_str();
  let listener = TcpListener::bind(server_addr).await.unwrap();
  warn!("TREX SQL Server Listening to {}", server_addr);
  loop {
    let incoming_socket = listener.accept().await.unwrap();
    let factory_ref = factory.clone();

    tokio::spawn(async move {
      process_socket(incoming_socket.0, None, factory_ref).await
    });
  }
}

#[derive(Clone)]
pub enum ReplicateCommand {
  CopyTable {
    tables: Vec<TableName>,
  },
  Cdc {
    publication: String,
    slot_name: String,
  },
}

#[allow(clippy::too_many_arguments)]
async fn create_pipeline(
  duckdb: &Arc<Mutex<Connection>>,
  command: ReplicateCommand,
  duckdb_file: &str,
  db_host: &str,
  db_port: u16,
  db_name: &str,
  db_username: &str,
  db_password: Option<String>,
) -> Result<BatchDataPipeline<PostgresSource, DuckDbSink>, Box<dyn Error>> {
  let (postgres_source, action) = match command {
    ReplicateCommand::CopyTable { tables } => {
      let table_names: Vec<TableName> = tables;

      let postgres_source = PostgresSource::new(
        db_host,
        db_port,
        db_name,
        db_username,
        db_password,
        None,
        TableNamesFrom::Vec(table_names),
      )
      .await?;
      (postgres_source, PipelineAction::TableCopiesOnly)
    }
    ReplicateCommand::Cdc {
      publication,
      slot_name,
    } => {
      let postgres_source: PostgresSource = PostgresSource::new(
        db_host,
        db_port,
        db_name,
        db_username,
        db_password,
        Some(slot_name),
        TableNamesFrom::Publication(publication),
      )
      .await?;

      (postgres_source, PipelineAction::Both)
    }
  };

  let duckdb_sink: DuckDbSink = DuckDbSink::trexdb(duckdb, duckdb_file).await?; //DuckDbSink::file(duckdb_file).await?;//

  let batch_config = BatchConfig::new(100000, Duration::from_secs(10));
  Ok(BatchDataPipeline::new(
    postgres_source,
    duckdb_sink,
    action,
    batch_config,
  ))
}

#[allow(clippy::too_many_arguments)]
pub async fn trex_replicate(
  duckdb: &Arc<Mutex<Connection>>,
  command: ReplicateCommand,
  duckdb_file: &str,
  db_host: &str,
  db_port: u16,
  db_name: &str,
  db_username: &str,
  db_password: Option<String>,
) -> Result<(), Box<dyn Error>> {
  let mut retries = 0;
  let mut start = SystemTime::now();
  if matches!(command, ReplicateCommand::CopyTable { tables: _ }) {
    retries = 4;
  }
  while retries < 5 {
    let mut pipeline = create_pipeline(
      duckdb,
      command.clone(),
      duckdb_file,
      db_host,
      db_port,
      db_name,
      db_username,
      db_password.clone(),
    )
    .await?;
    pipeline.start().await?;
    let duration = SystemTime::now().duration_since(start)?;
    if matches!(command, ReplicateCommand::CopyTable { tables: _ }) {
      retries += 1;
    } else {
      if duration.as_secs() < 300 {
        retries += 1;
      } else {
        retries = 0;
        start = SystemTime::now();
      }
      println!("restarting pipeline ... (try {retries})");
    }
  }
  Ok(())
}

#[op2]
fn op_copy_tables(
  #[serde] tables: Vec<TableName>,
  #[string] duckdb_file: String,
  #[string] db_host: String,
  db_port: u16,
  #[string] db_name: String,
  #[string] db_username: String,
  #[string] db_password: String,
) {
  warn!("TREX START TABLE COPY: {duckdb_file}");
  let command = ReplicateCommand::CopyTable { tables };
  let conn = get_active_connection();
  tokio::spawn(async move {
    trex_replicate(
      &conn,
      command,
      duckdb_file.as_str(),
      db_host.as_str(),
      db_port,
      db_name.as_str(),
      db_username.as_str(),
      Some(db_password),
    )
    .await
    .map_err(|error| println!("ERROR: {error}"))
  });
}

#[allow(clippy::too_many_arguments)]
#[op2(fast)]
fn op_add_replication(
  #[string] publication: String,
  #[string] slot_name: String,
  #[string] duckdb_file: String,
  #[string] db_host: String,
  db_port: u16,
  #[string] db_name: String,
  #[string] db_username: String,
  #[string] db_password: String,
) {
  warn!("TREX START REPLICATION: {duckdb_file}");
  let command: ReplicateCommand = ReplicateCommand::Cdc {
    publication,
    slot_name,
  };
  let conn = get_active_connection();
  tokio::spawn(async move {
    trex_replicate(
      &conn,
      command,
      duckdb_file.as_str(),
      db_host.as_str(),
      db_port,
      db_name.as_str(),
      db_username.as_str(),
      Some(db_password),
    )
    .await
    .map_err(|error| println!("ERROR: {error}"))
  });
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

  // Add hardcoded RESULT database from TREX__SQL__* env variables
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
      // Check if RESULT already exists
      if !credentials
        .iter()
        .any(|c| c.get("id").and_then(|id| id.as_str()) == Some("RESULT"))
      {
        credentials.push(result_db);
      }
    }
  }

  // Add hardcoded FHIR database from PG__* env variables
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
      // Check if FHIR already exists
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

pub struct LlamaStreamResource {
  receiver: Arc<Mutex<mpsc::Receiver<String>>>,
}

impl Resource for LlamaStreamResource {
  fn name(&self) -> std::borrow::Cow<str> {
    "LlamaStreamResource".into()
  }
}

#[op2]
#[serde]
fn op_prompt(
  state: &mut OpState,
  #[string] prompt: String,
  #[smi] max_tokens: u32,
  #[serde] model: Model,
) -> Result<ResourceId, AnyError> {
  let (sender, receiver) = mpsc::channel::<String>((max_tokens) as usize);

  tokio::spawn(async move {
    tokio::task::spawn_blocking(move || {
      if let Err(e) = run_llama_model(prompt, max_tokens, model, sender) {
        eprintln!("Error running llama model: {}", e);
      }
    });
  });

  let resource = LlamaStreamResource {
    receiver: Arc::new(Mutex::new(receiver)),
  };
  Ok(state.resource_table.add(resource))
}

#[allow(clippy::await_holding_lock)]
#[op2(async)]
#[string]
async fn op_prompt_next(
  state: Rc<RefCell<OpState>>,
  #[smi] rid: ResourceId,
) -> Result<Option<String>, AnyError> {
  let resource = state
    .borrow()
    .resource_table
    .get::<LlamaStreamResource>(rid)?;

  let mut rx = resource.receiver.lock().unwrap();
  let next_chunk = rx.recv().await;

  if next_chunk.is_none() {
    state
      .borrow_mut()
      .resource_table
      .take::<LlamaStreamResource>(rid)?;
  }
  Ok(next_chunk)
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum Model {
  Local { path: String },
  HuggingFace { repo: String, model: String },
  None,
}

fn run_llama_model(
  _prompt: String,
  _max_tokens: u32,
  _model: Model,
  _sender: mpsc::Sender<String>,
) -> Result<(), AnyError> {
  Ok(())
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

  // Execute tpm_install_with_deps
  let sql = format!(
    "SELECT install_results FROM tpm_install_with_deps('{}', '{}')",
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

#[op2(fast)]
fn op_exit(code: i32) {
  process::exit(code);
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
      // Convert to ISO 8601 date string (YYYY-MM-DD)
      let timestamp = days as i64 * 86400; // seconds in a day
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

fn execute_query(
  database: String,
  sql: String,
  params: Vec<TrexType>,
) -> Result<String, AnyError> {
  let conn_arc = get_active_connection();
  let conn = &*conn_arc.lock().unwrap();
  let _ = conn
    .execute(&format!("USE {database}"), [])
    .inspect_err(|e| warn!("{e}"));
  if sql.trim().is_empty() {
    return Ok("[]".to_string());
  }
  let tmpstmt = conn.prepare(&sql).inspect_err(|e| warn!("{e}"));
  match tmpstmt {
    Ok(mut stmt) => match stmt.query_arrow(params_from_iter(params.iter())) {
      Ok(iter) => {
        let batches: Vec<RecordBatch> = iter.collect();
        Ok(record_batches_to_json(&batches))
      }
      Err(_) => Ok("[]".to_string()),
    },
    Err(_) => Ok("[]".to_string()),
  }
}

#[op2]
#[string]
fn op_execute_query(
  #[string] database: String,
  #[string] sql: String,
  #[serde] params: Vec<TrexType>,
) -> Result<String, AnyError> {
  execute_query(database, sql, params)
}

#[op2]
#[string]
fn op_atlas(
  #[string] _database: String,
  #[string] _query: String,
) -> Result<String, AnyError> {
  Ok("".to_string())
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
async fn op_req(#[serde] message: JsonValue) -> Result<JsonValue, AnyError> {
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
        Err(deno_core::error::generic_error("No active listeners"))
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
          Err(deno_core::error::generic_error("Request cancelled"))
        }
        Err(_) => {
          let mut pending = PENDING_REQUESTS.lock().unwrap();
          pending.remove(&request_id);
          Err(deno_core::error::generic_error("Request timeout"))
        }
      }
    }
    Err(_) => {
      let mut pending = PENDING_REQUESTS.lock().unwrap();
      pending.remove(&request_id);
      Err(deno_core::error::generic_error("Failed to send request"))
    }
  }
}

#[op2]
#[serde]
fn op_req_listen(state: &mut OpState) -> Result<ResourceId, AnyError> {
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
) -> Result<Option<JsonValue>, AnyError> {
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
) -> Result<serde_json::Value, AnyError> {
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
) -> Result<ResourceId, AnyError> {
  let (sender, receiver) = mpsc::channel::<String>(1000);
  let conn_arc = get_active_connection();
  tokio::spawn(async move {
    tokio::task::spawn_blocking(move || {
      let conn = &*conn_arc.lock().unwrap();
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
) -> Result<Option<String>, AnyError> {
  let resource = state
    .borrow()
    .resource_table
    .get::<QueryStreamResource>(rid)?;

  let mut rx = resource.receiver.lock().unwrap();
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
        op_prompt,
        op_prompt_next,
        op_add_replication,
        op_install_plugin,
        op_atlas,
        op_execute_query,
        op_exit,
        op_get_dbc,
        op_set_dbc,
        op_copy_tables,
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
