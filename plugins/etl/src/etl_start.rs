use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeId};
use duckdb::vtab::arrow::WritableVector;
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use secrecy::SecretString;
use tokio::sync::oneshot;

use crate::credential_mask;
use crate::destination::DuckDbDestination;
use crate::pipeline_registry::{self, PipelineMode, PipelineState};
use crate::store::DuckDbStore;

struct PipelineParams {
    batch_size: usize,
    batch_timeout_ms: u64,
    retry_delay_ms: u64,
    retry_max_attempts: u32,
}

impl Default for PipelineParams {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            batch_timeout_ms: 5000,
            retry_delay_ms: 10000,
            retry_max_attempts: 5,
        }
    }
}

pub struct EtlStartScalar;

impl VScalar for EtlStartScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input.len() == 0 {
            return Err("No input provided".into());
        }

        let name_vector = input.flat_vector(0);
        let conn_vector = input.flat_vector(1);

        let name_slice =
            name_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
        let conn_slice =
            conn_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());

        let pipeline_name = duckdb::types::DuckString::new(&mut { name_slice[0] })
            .as_str()
            .to_string();
        let connection_string = duckdb::types::DuckString::new(&mut { conn_slice[0] })
            .as_str()
            .to_string();

        let num_cols = input.num_columns();

        // Determine mode and params based on signature:
        //   2 cols: (name, conn) -> default mode, default params
        //   3 cols: (name, conn, mode) -> parse mode, default params
        //   6 cols: (name, conn, batch_size, batch_timeout, retry_delay, retry_max) -> default mode, parse params from col 2-5
        //   7 cols: (name, conn, mode, batch_size, batch_timeout, retry_delay, retry_max) -> parse mode, parse params from col 3-6
        let has_mode_col = num_cols == 3 || num_cols == 7;

        let mode_str = if has_mode_col {
            let mode_vector = input.flat_vector(2);
            let mode_slice =
                mode_vector.as_slice_with_len::<libduckdb_sys::duckdb_string_t>(input.len());
            duckdb::types::DuckString::new(&mut { mode_slice[0] })
                .as_str()
                .to_string()
        } else {
            "copy_and_cdc".to_string()
        };

        let mode = PipelineMode::from_str(&mode_str)
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

        let params = if num_cols == 7 {
            // 7-param: mode at col 2, batch params at cols 3-6
            let batch_size_vector = input.flat_vector(3);
            let batch_timeout_vector = input.flat_vector(4);
            let retry_delay_vector = input.flat_vector(5);
            let retry_max_vector = input.flat_vector(6);

            let batch_size_slice = batch_size_vector.as_slice_with_len::<i32>(input.len());
            let batch_timeout_slice = batch_timeout_vector.as_slice_with_len::<i32>(input.len());
            let retry_delay_slice = retry_delay_vector.as_slice_with_len::<i32>(input.len());
            let retry_max_slice = retry_max_vector.as_slice_with_len::<i32>(input.len());

            let batch_size = batch_size_slice[0];
            let batch_timeout_ms = batch_timeout_slice[0];
            let retry_delay_ms = retry_delay_slice[0];
            let retry_max_attempts = retry_max_slice[0];

            if batch_size <= 0 {
                return Err("batch_size must be greater than 0".into());
            }
            if batch_timeout_ms <= 0 {
                return Err("batch_timeout_ms must be greater than 0".into());
            }
            if retry_delay_ms < 0 {
                return Err("retry_delay_ms must be >= 0".into());
            }
            if retry_max_attempts < 0 {
                return Err("retry_max_attempts must be >= 0".into());
            }

            PipelineParams {
                batch_size: batch_size as usize,
                batch_timeout_ms: batch_timeout_ms as u64,
                retry_delay_ms: retry_delay_ms as u64,
                retry_max_attempts: retry_max_attempts as u32,
            }
        } else if num_cols == 6 {
            // 6-param legacy: batch params at cols 2-5 (no mode column)
            let batch_size_vector = input.flat_vector(2);
            let batch_timeout_vector = input.flat_vector(3);
            let retry_delay_vector = input.flat_vector(4);
            let retry_max_vector = input.flat_vector(5);

            let batch_size_slice = batch_size_vector.as_slice_with_len::<i32>(input.len());
            let batch_timeout_slice = batch_timeout_vector.as_slice_with_len::<i32>(input.len());
            let retry_delay_slice = retry_delay_vector.as_slice_with_len::<i32>(input.len());
            let retry_max_slice = retry_max_vector.as_slice_with_len::<i32>(input.len());

            let batch_size = batch_size_slice[0];
            let batch_timeout_ms = batch_timeout_slice[0];
            let retry_delay_ms = retry_delay_slice[0];
            let retry_max_attempts = retry_max_slice[0];

            if batch_size <= 0 {
                return Err("batch_size must be greater than 0".into());
            }
            if batch_timeout_ms <= 0 {
                return Err("batch_timeout_ms must be greater than 0".into());
            }
            if retry_delay_ms < 0 {
                return Err("retry_delay_ms must be >= 0".into());
            }
            if retry_max_attempts < 0 {
                return Err("retry_max_attempts must be >= 0".into());
            }

            PipelineParams {
                batch_size: batch_size as usize,
                batch_timeout_ms: batch_timeout_ms as u64,
                retry_delay_ms: retry_delay_ms as u64,
                retry_max_attempts: retry_max_attempts as u32,
            }
        } else {
            PipelineParams::default()
        };

        let response = start_pipeline(&pipeline_name, &connection_string, mode, params)?;

        let flat_vector = output.flat_vector();
        flat_vector.insert(0, &response);
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            // 2-param: (name, connection_string)
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeId::Varchar.into(),
                    LogicalTypeId::Varchar.into(),
                ],
                LogicalTypeId::Varchar.into(),
            ),
            // 3-param: (name, connection_string, mode)
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeId::Varchar.into(),
                    LogicalTypeId::Varchar.into(),
                    LogicalTypeId::Varchar.into(),
                ],
                LogicalTypeId::Varchar.into(),
            ),
            // 6-param legacy: (name, connection_string, batch_size, batch_timeout, retry_delay, retry_max)
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeId::Varchar.into(),
                    LogicalTypeId::Varchar.into(),
                    LogicalTypeId::Integer.into(),
                    LogicalTypeId::Integer.into(),
                    LogicalTypeId::Integer.into(),
                    LogicalTypeId::Integer.into(),
                ],
                LogicalTypeId::Varchar.into(),
            ),
            // 7-param: (name, connection_string, mode, batch_size, batch_timeout, retry_delay, retry_max)
            ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeId::Varchar.into(),
                    LogicalTypeId::Varchar.into(),
                    LogicalTypeId::Varchar.into(),
                    LogicalTypeId::Integer.into(),
                    LogicalTypeId::Integer.into(),
                    LogicalTypeId::Integer.into(),
                    LogicalTypeId::Integer.into(),
                ],
                LogicalTypeId::Varchar.into(),
            ),
        ]
    }
}

fn start_pipeline(
    pipeline_name: &str,
    connection_string: &str,
    mode: PipelineMode,
    params: PipelineParams,
) -> Result<String, Box<dyn std::error::Error>> {
    let host = credential_mask::extract_param(connection_string, "host")
        .unwrap_or("localhost")
        .to_string();
    let port: u16 = credential_mask::extract_param(connection_string, "port")
        .unwrap_or("5432")
        .parse()?;
    let dbname = credential_mask::extract_param(connection_string, "dbname")
        .unwrap_or("postgres")
        .to_string();
    let username = credential_mask::extract_param(connection_string, "user")
        .unwrap_or("postgres")
        .to_string();
    let password = credential_mask::extract_param(connection_string, "password")
        .map(|p| SecretString::from(p.to_string()));

    // Publication is required for CDC modes, not for copy_only
    let publication = match mode {
        PipelineMode::CopyOnly => {
            credential_mask::extract_param(connection_string, "publication")
                .unwrap_or("")
                .to_string()
        }
        _ => {
            credential_mask::extract_param(connection_string, "publication")
                .ok_or("connection string must include 'publication=<name>'")?
                .to_string()
        }
    };

    let schema_name = credential_mask::extract_param(connection_string, "schema")
        .unwrap_or("public")
        .to_string();

    let masked_conn = credential_mask::mask_password(connection_string);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    pipeline_registry::registry()
        .reserve(
            pipeline_name,
            &masked_conn,
            &publication,
            mode.clone(),
            shutdown_tx,
        )
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    let shared_conn = crate::get_shared_connection()
        .ok_or("Extension connection not initialized")?;

    let name = pipeline_name.to_string();
    let name_for_thread = pipeline_name.to_string();

    if mode == PipelineMode::CopyOnly {
        return start_copy_only_pipeline(
            &name,
            &name_for_thread,
            connection_string,
            &schema_name,
            shared_conn,
            shutdown_rx,
        );
    }

    let table_sync_copy = match mode {
        PipelineMode::CdcOnly => etl_lib::config::TableSyncCopyConfig::SkipAllTables,
        _ => etl_lib::config::TableSyncCopyConfig::IncludeAllTables,
    };

    let thread_result = thread::Builder::new()
        .name(format!("etl-pipeline-{}", pipeline_name))
        .spawn(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            rt.block_on(async move {
                pipeline_registry::registry().update_state(&name, PipelineState::Starting);

                let schemas = Arc::new(Mutex::new(HashMap::new()));
                let destination =
                    DuckDbDestination::new(shared_conn.clone(), name.clone(), schemas.clone());
                let store = DuckDbStore::new(shared_conn.clone(), name.clone(), schemas);

                let pg_config = etl_lib::config::PgConnectionConfig {
                    host,
                    port,
                    name: dbname,
                    username,
                    password,
                    tls: etl_lib::config::TlsConfig {
                        trusted_root_certs: String::new(),
                        enabled: true,
                    },
                    keepalive: None,
                };

                let pipeline_config = etl_lib::config::PipelineConfig {
                    id: 1,
                    publication_name: publication,
                    pg_connection: pg_config,
                    batch: etl_lib::config::BatchConfig {
                        max_size: params.batch_size,
                        max_fill_ms: params.batch_timeout_ms,
                    },
                    table_error_retry_delay_ms: params.retry_delay_ms,
                    table_error_retry_max_attempts: params.retry_max_attempts,
                    max_table_sync_workers: 4,
                    table_sync_copy,
                    invalidated_slot_behavior: etl_lib::config::InvalidatedSlotBehavior::Error,
                };

                let mut pipeline = etl_lib::pipeline::Pipeline::new(
                    pipeline_config,
                    store,
                    destination,
                );

                pipeline_registry::registry()
                    .update_state(&name, PipelineState::Snapshotting);

                tokio::select! {
                    result = pipeline.start() => {
                        match result {
                            Ok(()) => {
                                pipeline_registry::registry()
                                    .update_state(&name, PipelineState::Stopped);
                            }
                            Err(e) => {
                                pipeline_registry::registry()
                                    .set_error(&name, &format!("{}", e));
                                return Err(format!("{}", e).into());
                            }
                        }
                    }
                    _ = shutdown_rx => {
                        pipeline_registry::registry()
                            .update_state(&name, PipelineState::Stopping);
                    }
                }

                Ok(())
            })
        });

    match thread_result {
        Ok(handle) => {
            pipeline_registry::registry().set_thread_handle(&name_for_thread, handle);
        }
        Err(e) => {
            pipeline_registry::registry().deregister(&name_for_thread);
            return Err(format!("Failed to spawn pipeline thread: {}", e).into());
        }
    }

    Ok(format!("Pipeline '{}' started", pipeline_name))
}

fn start_copy_only_pipeline(
    name: &str,
    name_for_thread: &str,
    connection_string: &str,
    schema_name: &str,
    shared_conn: Arc<Mutex<duckdb::Connection>>,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<String, Box<dyn std::error::Error>> {
    let pipeline_name = name.to_string();
    let name_for_thread = name_for_thread.to_string();
    let conn_str = connection_string.to_string();
    let schema = schema_name.to_string();
    let attach_name = format!("__etl_{}", pipeline_name.replace('-', "_"));

    let thread_result = thread::Builder::new()
        .name(format!("etl-copy-{}", pipeline_name))
        .spawn(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let mut shutdown = shutdown_rx;

            pipeline_registry::registry()
                .update_state(&pipeline_name, PipelineState::Starting);

            {
                let conn = shared_conn
                    .lock()
                    .map_err(|e| format!("connection lock: {}", e))?;
                conn.execute_batch("LOAD postgres")
                    .map_err(|e| format!("Failed to load postgres scanner: {}", e))?;

                let escaped_conn = conn_str.replace('\'', "''");
                let attach_sql = format!(
                    "ATTACH IF NOT EXISTS '{}' AS \"{}\" (TYPE postgres, READ_ONLY)",
                    escaped_conn, attach_name
                );
                conn.execute_batch(&attach_sql)
                    .map_err(|e| format!("Failed to attach source: {}", e))?;
            }

            pipeline_registry::registry()
                .update_state(&pipeline_name, PipelineState::Snapshotting);

            let tables: Vec<String> = {
                let conn = shared_conn
                    .lock()
                    .map_err(|e| format!("connection lock: {}", e))?;
                let escaped_schema = schema.replace('\'', "''");
                let sql = format!(
                    "SELECT table_name FROM \"{}\".information_schema.tables WHERE table_schema = '{}'",
                    attach_name, escaped_schema
                );
                let mut stmt = conn.prepare(&sql)
                    .map_err(|e| format!("Failed to query tables: {}", e))?;
                let rows = stmt
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|e| format!("Failed to query tables: {}", e))?;
                rows.filter_map(|r| r.ok()).collect()
            };

            let mut had_error = false;
            for table in &tables {
                match shutdown.try_recv() {
                    Ok(_) | Err(oneshot::error::TryRecvError::Closed) => {
                        pipeline_registry::registry()
                            .update_state(&pipeline_name, PipelineState::Stopping);
                        if let Ok(conn) = shared_conn.lock() {
                            let _ = conn.execute_batch(&format!("DETACH \"{}\"", attach_name));
                        }
                        return Ok(());
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {}
                }

                let escaped_table = table.replace('"', "\"\"");
                let escaped_schema = schema.replace('"', "\"\"");

                let copy_sql = format!(
                    "CREATE TABLE IF NOT EXISTS \"{}\".\"{}\" AS SELECT * FROM \"{}\".\"{}\".\"{}\";",
                    escaped_schema,
                    escaped_table,
                    attach_name,
                    escaped_schema,
                    escaped_table
                );

                let ensure_schema_sql = format!(
                    "CREATE SCHEMA IF NOT EXISTS \"{}\"",
                    escaped_schema
                );

                match shared_conn.lock() {
                    Ok(conn) => {
                        if let Err(e) = conn.execute_batch(&ensure_schema_sql) {
                            eprintln!("etl: create schema error for '{}': {}", schema, e);
                        }
                        match conn.execute_batch(&copy_sql) {
                            Ok(_) => {
                                let count_sql = format!(
                                    "SELECT COUNT(*) FROM \"{}\".\"{}\"",
                                    escaped_schema, escaped_table
                                );
                                let row_count = conn
                                    .prepare(&count_sql)
                                    .and_then(|mut stmt| {
                                        stmt.query_row([], |row| row.get::<_, i64>(0))
                                    })
                                    .unwrap_or(0) as u64;
                                pipeline_registry::registry()
                                    .update_stats(&pipeline_name, row_count);
                            }
                            Err(e) => {
                                eprintln!(
                                    "etl: copy error for table '{}.{}': {}",
                                    schema, table, e
                                );
                                had_error = true;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("etl: connection lock error: {}", e);
                        had_error = true;
                    }
                }
            }

            if let Ok(conn) = shared_conn.lock() {
                let _ = conn.execute_batch(&format!("DETACH \"{}\"", attach_name));
            }

            if had_error {
                pipeline_registry::registry()
                    .set_error(&pipeline_name, "Some tables failed to copy");
            } else {
                pipeline_registry::registry()
                    .update_state(&pipeline_name, PipelineState::Stopped);
            }

            Ok(())
        });

    match thread_result {
        Ok(handle) => {
            pipeline_registry::registry().set_thread_handle(&name_for_thread, handle);
        }
        Err(e) => {
            pipeline_registry::registry().deregister(&name_for_thread);
            return Err(format!("Failed to spawn pipeline thread: {}", e).into());
        }
    }

    Ok(format!("Pipeline '{}' started (copy_only)", name))
}
