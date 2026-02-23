// pg_scan: DuckDB table function that queries PostgreSQL via SPI.
//
// Routes queries through the SPI bridge so DuckDB worker threads can read
// PostgreSQL tables without libpq (which conflicts with backend symbols).

use std::ffi::CString;
use std::sync::atomic::{AtomicUsize, Ordering};

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use pgrx::pg_sys;

use crate::spi_bridge;

// ── PG OID → DuckDB type mapping ───────────────────────────────────────

fn pg_oid_to_logical_type(oid: pg_sys::Oid) -> LogicalTypeId {
    match oid {
        pg_sys::BOOLOID => LogicalTypeId::Boolean,
        pg_sys::INT2OID => LogicalTypeId::Smallint,
        pg_sys::INT4OID => LogicalTypeId::Integer,
        pg_sys::INT8OID => LogicalTypeId::Bigint,
        pg_sys::FLOAT4OID => LogicalTypeId::Float,
        pg_sys::FLOAT8OID => LogicalTypeId::Double,
        // Text, date, timestamp, numeric, json — all as VARCHAR for V1
        _ => LogicalTypeId::Varchar,
    }
}

// ── VTab data structures ────────────────────────────────────────────────

const BATCH_SIZE: usize = 2048;

#[repr(C)]
pub struct PgScanBindData {
    columns: Vec<(String, LogicalTypeId)>,
    rows: Vec<Vec<Option<String>>>,
}

#[repr(C)]
pub struct PgScanInitData {
    current_offset: AtomicUsize,
}

// ── Helpers ──────────────────────────────────────────────────────────────

use duckdb::core::FlatVector;

/// Two-pass fill: write values first via `as_mut_slice`, then set nulls.
/// Avoids double mutable borrow on FlatVector.
fn fill_typed<T: Default + Copy + 'static>(
    vector: &mut FlatVector,
    rows: &[Vec<Option<String>>],
    offset: usize,
    chunk_size: usize,
    col_idx: usize,
    parse: impl Fn(&str) -> Option<T>,
) {
    let mut null_indices = Vec::new();
    {
        let slice = vector.as_mut_slice::<T>();
        for row_idx in 0..chunk_size {
            match &rows[offset + row_idx][col_idx] {
                Some(v) => match parse(v) {
                    Some(val) => slice[row_idx] = val,
                    None => null_indices.push(row_idx),
                },
                None => null_indices.push(row_idx),
            }
        }
    }
    for idx in null_indices {
        vector.set_null(idx);
    }
}

// ── VTab implementation ─────────────────────────────────────────────────

pub struct PgScanVTab;

impl VTab for PgScanVTab {
    type InitData = PgScanInitData;
    type BindData = PgScanBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let sql = bind.get_parameter(0).to_string();

        let response = spi_bridge::request(&sql)
            .map_err(|e| format!("pg_scan bind: {e}"))?;

        if let Some(err) = response.error {
            return Err(format!("pg_scan: {err}").into());
        }

        let mut columns = Vec::with_capacity(response.columns.len());
        for (name, oid) in &response.columns {
            let logical_type = pg_oid_to_logical_type(*oid);
            bind.add_result_column(name, LogicalTypeHandle::from(pg_oid_to_logical_type(*oid)));
            columns.push((name.clone(), logical_type));
        }

        Ok(PgScanBindData {
            columns,
            rows: response.rows,
        })
    }

    fn init(_init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(PgScanInitData {
            current_offset: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        let offset = init_data.current_offset.load(Ordering::Relaxed);
        let total_rows = bind_data.rows.len();

        if offset >= total_rows {
            output.set_len(0);
            return Ok(());
        }

        let chunk_size = std::cmp::min(BATCH_SIZE, total_rows - offset);

        for (col_idx, (_, logical_type)) in bind_data.columns.iter().enumerate() {
            let mut vector = output.flat_vector(col_idx);

            match logical_type {
                LogicalTypeId::Boolean => {
                    fill_typed::<bool>(&mut vector, &bind_data.rows, offset, chunk_size, col_idx,
                        |v| if v == "t" || v == "true" { Some(true) } else { Some(false) });
                }
                LogicalTypeId::Smallint => {
                    fill_typed::<i16>(&mut vector, &bind_data.rows, offset, chunk_size, col_idx,
                        |v| v.parse().ok());
                }
                LogicalTypeId::Integer => {
                    fill_typed::<i32>(&mut vector, &bind_data.rows, offset, chunk_size, col_idx,
                        |v| v.parse().ok());
                }
                LogicalTypeId::Bigint => {
                    fill_typed::<i64>(&mut vector, &bind_data.rows, offset, chunk_size, col_idx,
                        |v| v.parse().ok());
                }
                LogicalTypeId::Float => {
                    fill_typed::<f32>(&mut vector, &bind_data.rows, offset, chunk_size, col_idx,
                        |v| v.parse().ok());
                }
                LogicalTypeId::Double => {
                    fill_typed::<f64>(&mut vector, &bind_data.rows, offset, chunk_size, col_idx,
                        |v| v.parse().ok());
                }
                _ => {
                    for row_idx in 0..chunk_size {
                        let cell = &bind_data.rows[offset + row_idx][col_idx];
                        match cell {
                            Some(v) => {
                                let cstr = CString::new(v.as_str())
                                    .unwrap_or_else(|_| CString::new("").unwrap());
                                vector.insert(row_idx, cstr);
                            }
                            None => vector.set_null(row_idx),
                        }
                    }
                }
            }
        }

        output.set_len(chunk_size);
        init_data
            .current_offset
            .store(offset + chunk_size, Ordering::Relaxed);

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}
