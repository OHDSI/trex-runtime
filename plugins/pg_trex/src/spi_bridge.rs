// SPI channel bridge: routes SQL queries from DuckDB worker threads to the
// main background worker thread where SPI is available.
//
// Worker threads cannot call SPI directly because SPI requires a PostgreSQL
// transaction context that only exists on the main BGW thread (the one that
// called BackgroundWorkerInitializeConnection).

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use pgrx::pg_sys;
use std::sync::OnceLock;

// ── Types ───────────────────────────────────────────────────────────────

pub struct SpiRequest {
    pub sql: String,
    pub response_tx: Sender<SpiResponse>,
}

pub struct SpiResponse {
    pub columns: Vec<(String, pg_sys::Oid)>,
    pub rows: Vec<Vec<Option<String>>>,
    pub error: Option<String>,
}

// ── Global channels ─────────────────────────────────────────────────────

static SPI_REQUEST_TX: OnceLock<Sender<SpiRequest>> = OnceLock::new();
static SPI_REQUEST_RX: OnceLock<Receiver<SpiRequest>> = OnceLock::new();

/// Create the SPI request channel. Must be called once from the main BGW thread.
pub fn init() {
    let (tx, rx) = unbounded::<SpiRequest>();
    SPI_REQUEST_TX.set(tx).expect("spi_bridge::init called twice");
    SPI_REQUEST_RX.set(rx).expect("spi_bridge::init called twice");
}

/// Send an SPI request from a worker thread and block until the main thread
/// processes it. Returns the SPI result or an error string.
pub fn request(sql: &str) -> Result<SpiResponse, String> {
    let tx = SPI_REQUEST_TX
        .get()
        .ok_or_else(|| "spi_bridge not initialized".to_string())?;

    let (response_tx, response_rx) = bounded::<SpiResponse>(1);

    tx.send(SpiRequest {
        sql: sql.to_string(),
        response_tx,
    })
    .map_err(|e| format!("spi_bridge send failed: {e}"))?;

    response_rx
        .recv()
        .map_err(|e| format!("spi_bridge recv failed: {e}"))
}

/// Process all pending SPI requests. Called from the main BGW thread's poll loop.
///
/// Each request is executed inside its own transaction via SPI. Column metadata
/// and row data are extracted and sent back to the waiting worker thread.
pub fn process_pending() {
    let rx = match SPI_REQUEST_RX.get() {
        Some(rx) => rx,
        None => return,
    };

    while let Ok(req) = rx.try_recv() {
        let response = execute_spi(&req.sql);
        let _ = req.response_tx.send(response);
    }
}

/// Execute a single SQL statement via SPI inside a transaction.
fn execute_spi(sql: &str) -> SpiResponse {
    unsafe {
        pg_sys::SetCurrentStatementStartTimestamp();
        pg_sys::StartTransactionCommand();
        pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Use raw SPI API to extract values as text via PostgreSQL's type output
        // functions. pgrx's value::<String>() only works for varlena types.
        unsafe {
            let spi_ret = pg_sys::SPI_connect();
            if spi_ret != pg_sys::SPI_OK_CONNECT as i32 {
                return Err(format!("SPI_connect returned {spi_ret}"));
            }

            let ret = pg_sys::SPI_execute(
                std::ffi::CString::new(sql).unwrap().as_ptr(),
                true,
                0,
            );
            if ret != pg_sys::SPI_OK_SELECT as i32 {
                pg_sys::SPI_finish();
                return Err(format!("SPI_execute returned {ret}"));
            }

            let tuptable = pg_sys::SPI_tuptable;
            if tuptable.is_null() {
                pg_sys::SPI_finish();
                return Ok(SpiResponse {
                    columns: vec![],
                    rows: vec![],
                    error: None,
                });
            }

            let tupdesc = (*tuptable).tupdesc;
            let natts = (*tupdesc).natts as usize;
            let nrows = pg_sys::SPI_processed as usize;

            let mut columns: Vec<(String, pg_sys::Oid)> = Vec::with_capacity(natts);
            for i in 0..natts {
                let attr = *(*tupdesc).attrs.as_ptr().add(i);
                let name = std::ffi::CStr::from_ptr(attr.attname.data.as_ptr())
                    .to_string_lossy()
                    .into_owned();
                let oid = attr.atttypid;
                columns.push((name, oid));
            }

            let vals = std::slice::from_raw_parts((*tuptable).vals, nrows);
            let mut rows = Vec::with_capacity(nrows);
            for row_idx in 0..nrows {
                let tuple = vals[row_idx];
                let mut row_data = Vec::with_capacity(natts);
                for col_idx in 0..natts {
                    let fnumber = (col_idx + 1) as i32;
                    let cstr = pg_sys::SPI_getvalue(tuple, tupdesc, fnumber);
                    let val = if cstr.is_null() {
                        None
                    } else {
                        let s = std::ffi::CStr::from_ptr(cstr)
                            .to_string_lossy()
                            .into_owned();
                        pg_sys::pfree(cstr as *mut _);
                        Some(s)
                    };
                    row_data.push(val);
                }
                rows.push(row_data);
            }

            pg_sys::SPI_finish();

            Ok(SpiResponse {
                columns,
                rows,
                error: None,
            })
        }
    }));

    unsafe {
        pg_sys::PopActiveSnapshot();
        pg_sys::CommitTransactionCommand();
    }

    match result {
        Ok(Ok(resp)) => resp,
        Ok(Err(e)) => SpiResponse {
            columns: vec![],
            rows: vec![],
            error: Some(format!("SPI error: {e:?}")),
        },
        Err(_) => SpiResponse {
            columns: vec![],
            rows: vec![],
            error: Some("SPI panicked".to_string()),
        },
    }
}
