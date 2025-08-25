pub mod clients;
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
use std::env;
use std::process::Command;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::SystemTime;
use std::{error::Error, time::Duration};
use tokio::net::TcpListener;
use tracing::warn;

/*
use std::io::Write;

use anyhow::{bail, Context};
use hf_hub::api::sync::ApiBuilder;

use llama_cpp_2::context::params::LlamaContextParams;
use llama_cpp_2::ggml_time_us;
use llama_cpp_2::llama_backend::LlamaBackend;
use llama_cpp_2::llama_batch::LlamaBatch;
use llama_cpp_2::model::params::LlamaModelParams;
use llama_cpp_2::model::LlamaModel;
use llama_cpp_2::model::{AddBos, Special};
use llama_cpp_2::sampling::LlamaSampler;


use std::fs;
use std::num::NonZeroU32;
use std::pin::pin;
*/

use deno_core::{OpState, Resource, ResourceId};
use std::cell::RefCell;
use std::rc::Rc;
use tokio::sync::mpsc;

/*
use circe::{
  build_expression_query, init_jvm, render_and_translate_sql,
  validate_cohort_expression, BuildExpressionQueryOptions,
};
*/

use crate::pipeline::{
  batching::{data_pipeline::BatchDataPipeline, BatchConfig},
  sinks::duckdb::DuckDbSink,
  sources::postgres::{PostgresSource, TableNamesFrom},
  PipelineAction,
};

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
  Arc::new(Mutex::new(conn))
});
static DB_CREDENTIALS: LazyLock<Arc<Mutex<String>>> = LazyLock::new(|| {
  Arc::new(Mutex::new(String::from(
    "{\"credentials\":[], \"publications\":{}}",
  )))
});

pub async fn start_sql_server(ip: &str, port: u16, auth_type: AuthType) {
  let factory = Arc::new(TrexDuckDBFactory {
    handler: Arc::new(TrexDuckDB::new(&TREX_DB)),
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
  tokio::spawn(async move {
    trex_replicate(
      &TREX_DB,
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
  tokio::spawn(async move {
    trex_replicate(
      &TREX_DB,
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
) -> Result<ResourceId, anyhow::Error> {
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
) -> Result<(), anyhow::Error> {
  /*
  let backend = LlamaBackend::init()?;
  let model_params = {
    // #[cfg(any(feature = "cuda", feature = "vulkan"))]
    // if !disable_gpu {
    //     LlamaModelParams::default().with_n_gpu_layers(1000)
    // } else {
    //     LlamaModelParams::default()
    // }
    // #[cfg(not(any(feature = "cuda", feature = "vulkan")))]
    LlamaModelParams::default()
  };
  let ctx_size: Option<NonZeroU32> = Some(NonZeroU32::new(max_tokens).unwrap());
  let n_len = max_tokens as i32;
  let seed = 1234;

  let model_params = pin!(model_params);

  // for (k, v) in &key_value_overrides {
  //     let k = CString::new(k.as_bytes()).with_context(|| format!("invalid key: {k}"))?;
  //     model_params.as_mut().append_kv_override(k.as_c_str(), *v);
  // }

  let model_path: String = match model {
    Model::Local { path } => path,
    Model::HuggingFace { model, repo } => ApiBuilder::new()
      .with_progress(true)
      .build()
      .with_context(|| "unable to create huggingface api")?
      .model(repo)
      .get(&model)
      .with_context(|| "unable to download model")?
      .into_os_string()
      .into_string()
      .expect("Not UTF-8 String"),
    Model::None => match env::var("TREX_MODEL") {
      Ok(val) => val,
      Err(_e) => {
        "./data/plugins/node_modules/@data2evidence/chat/llm.gguf".to_string()
      }
    },
  };

  if fs::metadata(&model_path).is_err() {
    eprintln!("Model file does not exist at path: {}", model_path);
    return Err(anyhow::anyhow!(
      "Model file does not exist at path: {}",
      model_path
    ));
  }

  let model =
    match LlamaModel::load_from_file(&backend, model_path, &model_params) {
      Ok(model) => model,
      Err(e) => {
        eprintln!("Error loading model: {}", e);
        return Err(anyhow::anyhow!("Unable to load model: {}", e));
      }
    };

  let ctx_params = LlamaContextParams::default()
    .with_n_ctx(ctx_size.or(Some(NonZeroU32::new(2048).unwrap())));

  let mut ctx = model
    .new_context(&backend, ctx_params)
    .with_context(|| "unable to create the llama_context")?;

  let tokens_list = model
    .str_to_token(&prompt, AddBos::Always)
    .with_context(|| format!("failed to tokenize {prompt}"))?;

  let n_cxt = ctx.n_ctx() as i32;
  let n_kv_req = tokens_list.len() as i32 + (n_len - tokens_list.len() as i32);

  eprintln!("n_len = {n_len}, n_ctx = {n_cxt}, k_kv_req = {n_kv_req}");

  if n_kv_req > n_cxt {
    bail!(
            "n_kv_req > n_ctx, the required kv cache size is not big enough either reduce n_len or increase n_ctx"
        )
  }

  if tokens_list.len() >= usize::try_from(n_len)? {
    bail!("the prompt is too long, it has more tokens than n_len")
  }

  eprintln!();

  for token in &tokens_list {
    eprint!("{}", model.token_to_str(*token, Special::Tokenize)?);
  }

  std::io::stderr().flush()?;

  // create a llama_batch with size 512
  let mut batch = LlamaBatch::new(512, 1);

  let last_index: i32 = (tokens_list.len() - 1) as i32;
  for (i, token) in (0_i32..).zip(tokens_list.into_iter()) {
    let is_last = i == last_index;
    batch.add(token, i, &[0], is_last)?;
  }

  ctx
    .decode(&mut batch)
    .with_context(|| "llama_decode() failed")?;

  let mut n_cur = batch.n_tokens();
  let mut n_decode = 0;

  eprintln!("DONE INIT");

  let t_main_start = ggml_time_us();

  let mut decoder = encoding_rs::UTF_8.new_decoder();

  let mut sampler = LlamaSampler::chain_simple([
    LlamaSampler::dist(seed),
    LlamaSampler::greedy(),
  ]);

  while n_cur <= n_len {
    {
      let token = sampler.sample(&ctx, batch.n_tokens() - 1);

      sampler.accept(token);

      if model.is_eog_token(token) {
        eprintln!();
        break;
      }

      let output_bytes = model.token_to_bytes(token, Special::Tokenize)?;
      let mut output_string = String::with_capacity(32);
      let _decode_result =
        decoder.decode_to_string(&output_bytes, &mut output_string, false);
      for chunk in output_string.chars().collect::<Vec<_>>().chunks(1024) {
        let s: String = chunk.iter().collect();
        if sender.blocking_send(s).is_err() {
          warn!("TREX Error: send llm result to deno");
          break;
        }
      }
      //print!("{output_string}");
      //std::io::stdout().flush()?;
      std::thread::yield_now();
      batch.clear();
      batch.add(token, n_cur, &[0], true)?;
    }

    n_cur += 1;

    ctx.decode(&mut batch).with_context(|| "failed to eval")?;

    n_decode += 1;
  }

  eprintln!("\n");

  let t_main_end = ggml_time_us();

  let duration = Duration::from_micros((t_main_end - t_main_start) as u64);

  eprintln!(
    "decoded {} tokens in {:.2} s, speed {:.2} t/s\n",
    n_decode,
    duration.as_secs_f32(),
    n_decode as f32 / duration.as_secs_f32()
  );

  println!("{}", ctx.timings());
  */
  Ok(())
}

#[op2(fast)]
fn op_install_plugin(#[string] name: String, #[string] dir: String) {
  Command::new("npx")
    .args(["bun", "install", "-f", "--no-cache", "--no-save", &name])
    .current_dir(dir)
    .status()
    .expect("failed to execute process");
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
      JsonValue::from(arr.value(row))
    }
    DataType::Date64 => {
      let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
      JsonValue::from(arr.value(row))
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
      JsonValue::from(arr.value(row))
    }
    DataType::Timestamp(TimeUnit::Millisecond, _) => {
      let arr = array
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
      JsonValue::from(arr.value(row))
    }
    DataType::Timestamp(TimeUnit::Microsecond, _) => {
      let arr = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
      JsonValue::from(arr.value(row))
    }
    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
      let arr = array
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
      JsonValue::from(arr.value(row))
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
  let conn = &*TREX_DB.lock().unwrap();
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
  /*init_jvm()?;
  warn!("CIRCE input query: {}", query);

  match validate_cohort_expression(&query) {
    Ok(validation_result) => {
      warn!("CIRCE validation result: {}", validation_result);
      if validation_result.contains("Error")
        || validation_result.contains("error")
      {
        return Ok(format!(
          "{{\"error\": \"Cohort validation failed: {}\"}}",
          validation_result.replace("\"", "\\\"")
        ));
      }
    }
    Err(e) => {
      warn!("CIRCE validation error: {}", e);
      return Ok(format!(
        "{{\"error\": \"Cohort validation error: {}\"}}",
        e.to_string().replace("\"", "\\\"")
      ));
    }
  }

  let options = BuildExpressionQueryOptions {
    cohort_id: Some(1),
    cdm_schema: Some("demo_cdm".to_string()),
    result_schema: Some("demo_cdm".to_string()),
    vocabulary_schema: Some("demo_cdm".to_string()),
    generate_stats: true,
    ..Default::default()
  };
  warn!(
        "CIRCE options: cdm_schema={:?}, result_schema={:?}, vocabulary_schema={:?}",
        options.cdm_schema, options.result_schema, options.vocabulary_schema
    );

  let sql_query = build_expression_query(&query, Some(&options))?;
  warn!("Generated SQL from CIRCE: {}", sql_query);

  // Check if CIRCE returned an error message instead of SQL
  if sql_query.contains("Error building cohort SQL")
    || sql_query.trim().is_empty()
  {
    return Ok(format!(
      "{{\"error\": \"CIRCE failed to generate SQL: {}\"}}",
      sql_query.replace("\"", "\\\"")
    ));
  }

  let translated_sql = render_and_translate_sql(&sql_query, "postgresql")?;
  warn!("Translated SQL: {}", translated_sql);

  // Check if translation also failed
  if translated_sql.contains("Error building cohort SQL") {
    return Ok(format!(
      "{{\"error\": \"SQL translation failed: {}\"}}",
      translated_sql.replace("\"", "\\\"")
    ));
  }
  Ok(translated_sql)
  //execute_query(database, translated_sql, vec![])
  */
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

#[op2]
#[serde]
fn op_execute_query_stream(
  state: &mut OpState,
  #[string] database: String,
  #[string] sql: String,
  #[serde] params: Vec<TrexType>,
) -> Result<ResourceId, anyhow::Error> {
  let (sender, receiver) = mpsc::channel::<String>(1000);
  tokio::spawn(async move {
    tokio::task::spawn_blocking(move || {
      let conn = &*TREX_DB.lock().unwrap();
      if conn.execute(&format!("USE {database}"), []).is_err() {
        return;
      }
      if let Ok(mut stmt) = conn.prepare(&sql) {
        if let Ok(iter) = stmt.query_arrow(params_from_iter(params.iter())) {
          for batch in iter {
            // each item is a RecordBatch
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
        op_execute_query_stream_next
    ],
    esm_entry_point = "ext:trex/trex_lib.js",
    esm = [
        dir "js",
        "trex_lib.js",
        "dbconnection.js"
    ]
);
