use pgrx::pg_guard;
use pgrx::pg_sys;
use std::ffi::CStr;
use std::os::raw::c_char;

use crate::ipc;
use crate::types;

/// Private state stored in CustomScanState for the duration of a scan.
struct PgTrexScanState {
    sql: String,
    slot_idx: Option<usize>,
    dsm_handle: u32,
    response_data: Option<Vec<u8>>,
    rows: Vec<Vec<Option<String>>>,
    current_row: usize,
    column_count: usize,
    finished: bool,
}

/// Wrapper to assert Sync/Send for pg_sys method structs that contain raw
/// pointers and function pointers. These statics are initialized once at
/// compile time and never mutated, so sharing across threads is safe.
struct SyncMethods<T>(T);
unsafe impl<T> Sync for SyncMethods<T> {}
unsafe impl<T> Send for SyncMethods<T> {}

/// Static CustomScanMethods structure.
static CUSTOM_SCAN_METHODS: SyncMethods<pg_sys::CustomScanMethods> =
    SyncMethods(pg_sys::CustomScanMethods {
        CustomName: b"pg_trex_scan\0".as_ptr() as *const c_char,
        CreateCustomScanState: Some(create_custom_scan_state),
    });

/// Static CustomExecMethods structure.
static CUSTOM_EXEC_METHODS: SyncMethods<pg_sys::CustomExecMethods> =
    SyncMethods(pg_sys::CustomExecMethods {
        CustomName: b"pg_trex_scan\0".as_ptr() as *const c_char,
        BeginCustomScan: Some(begin_custom_scan),
        ExecCustomScan: Some(exec_custom_scan),
        EndCustomScan: Some(end_custom_scan),
        ReScanCustomScan: Some(rescan_custom_scan),
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        EstimateDSMCustomScan: None,
        InitializeDSMCustomScan: None,
        ReInitializeDSMCustomScan: None,
        InitializeWorkerCustomScan: None,
        ShutdownCustomScan: None,
        ExplainCustomScan: Some(explain_custom_scan),
    });

/// Return a pointer to the static CustomScanMethods.
pub fn get_custom_scan_methods() -> *const pg_sys::CustomScanMethods {
    &CUSTOM_SCAN_METHODS.0
}

/// CreateCustomScanState callback: allocate CustomScanState node.
#[pg_guard]
unsafe extern "C-unwind" fn create_custom_scan_state(
    _cscan: *mut pg_sys::CustomScan,
) -> *mut pg_sys::Node {
    let css = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScanState>()) as *mut pg_sys::CustomScanState;
    (*css).ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
    (*css).methods = &CUSTOM_EXEC_METHODS.0;
    (*css).custom_ps = std::ptr::null_mut();
    css as *mut pg_sys::Node
}

/// BeginCustomScan callback: extract SQL and initialize scan state.
#[pg_guard]
unsafe extern "C-unwind" fn begin_custom_scan(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: std::os::raw::c_int,
) {
    let cscan = (*node).ss.ps.plan as *mut pg_sys::CustomScan;
    let private_list = (*cscan).custom_private;

    let sql = if !private_list.is_null() && (*private_list).length > 0 {
        let first_cell = (*private_list).elements;
        let str_node = (*first_cell).ptr_value as *mut pg_sys::String;
        if !str_node.is_null() {
            let cstr = (*str_node).sval;
            if !cstr.is_null() {
                CStr::from_ptr(cstr).to_string_lossy().to_string()
            } else {
                String::new()
            }
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    if sql.is_empty() {
        pgrx::error!("pg_trex: empty query in CustomScan");
    }

    pgrx::debug1!("pg_trex: BeginCustomScan sql={}", &sql);

    let state = Box::new(PgTrexScanState {
        sql,
        slot_idx: None,
        dsm_handle: 0,
        response_data: None,
        rows: Vec::new(),
        current_row: 0,
        column_count: 0,
        finished: false,
    });

    // Store scan state in custom_ps as a raw pointer. We repurpose the
    // `custom_ps` field (typed as *mut List) to hold a Box<PgTrexScanState>.
    // This is safe because PostgreSQL's executor does not independently
    // traverse custom_ps — it is reserved for the custom scan provider.
    // The pointer is reclaimed via Box::from_raw in end_custom_scan.
    (*node).custom_ps = Box::into_raw(state) as *mut pg_sys::List;
}

/// ExecCustomScan callback: execute query via worker on first call, then return rows.
#[pg_guard]
unsafe extern "C-unwind" fn exec_custom_scan(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    let state = &mut *((*node).custom_ps as *mut PgTrexScanState);
    let slot = (*node).ss.ss_ScanTupleSlot;

    if state.rows.is_empty() && !state.finished {
        let shmem = crate::get_shmem();
        let shmem_ref = shmem.share();

        match ipc::execute_query(&*shmem_ref, &state.sql, types::QUERY_FLAG_LOCAL) {
            Ok(rows) => {
                state.rows = rows;
                state.current_row = 0;
            }
            Err(e) => {
                drop(shmem_ref);
                pgrx::error!("pg_trex: query execution failed: {}", e);
            }
        }
        drop(shmem_ref);
    }

    pg_sys::ExecClearTuple(slot);

    if state.current_row >= state.rows.len() {
        state.finished = true;
        return slot;
    }

    let row = &state.rows[state.current_row];
    state.current_row += 1;

    let natts = (*(*slot).tts_tupleDescriptor).natts as usize;
    let tts_values = std::slice::from_raw_parts_mut((*slot).tts_values, natts);
    let tts_isnull = std::slice::from_raw_parts_mut((*slot).tts_isnull, natts);

    // TODO: native type conversion (DATE, INT, etc.) is planned; currently all
    // columns are returned as TEXT datums.
    for i in 0..natts {
        if i < row.len() {
            match &row[i] {
                None => {
                    tts_isnull[i] = true;
                    tts_values[i] = pg_sys::Datum::from(0);
                }
                Some(val) => {
                    let cstr = std::ffi::CString::new(val.as_str()).unwrap_or_default();
                    let text = pg_sys::cstring_to_text(cstr.as_ptr());
                    tts_values[i] = pg_sys::Datum::from(text);
                    tts_isnull[i] = false;
                }
            }
        } else {
            tts_isnull[i] = true;
            tts_values[i] = pg_sys::Datum::from(0);
        }
    }

    pg_sys::ExecStoreVirtualTuple(slot);
    slot
}

/// EndCustomScan callback: release resources.
/// Reclaims the PgTrexScanState stored in custom_ps via Box::from_raw.
#[pg_guard]
unsafe extern "C-unwind" fn end_custom_scan(node: *mut pg_sys::CustomScanState) {
    let state_ptr = (*node).custom_ps as *mut PgTrexScanState;
    if !state_ptr.is_null() {
        let state = &*state_ptr;
        if let Some(slot_idx) = state.slot_idx {
            let shmem = crate::get_shmem();
            let shmem_ref = shmem.share();
            ipc::release_slot(&*shmem_ref, slot_idx);
            drop(shmem_ref);
        }
        let _ = Box::from_raw(state_ptr);
        (*node).custom_ps = std::ptr::null_mut();
    }
}

/// ReScanCustomScan callback: discard previous results so the next
/// ExecCustomScan re-executes the query with fresh parameters.
#[pg_guard]
unsafe extern "C-unwind" fn rescan_custom_scan(node: *mut pg_sys::CustomScanState) {
    let state_ptr = (*node).custom_ps as *mut PgTrexScanState;
    if !state_ptr.is_null() {
        let state = &mut *state_ptr;
        state.rows.clear();
        state.current_row = 0;
        state.finished = false;
    }
}

/// ExplainCustomScan callback: add info to EXPLAIN output.
#[pg_guard]
unsafe extern "C-unwind" fn explain_custom_scan(
    node: *mut pg_sys::CustomScanState,
    _ancestors: *mut pg_sys::List,
    es: *mut pg_sys::ExplainState,
) {
    let state_ptr = (*node).custom_ps as *mut PgTrexScanState;
    if !state_ptr.is_null() {
        let state = &*state_ptr;
        let label = std::ffi::CString::new("pg_trex Distributed Query").unwrap();
        let value = std::ffi::CString::new(state.sql.as_str()).unwrap();
        pg_sys::ExplainPropertyText(label.as_ptr(), value.as_ptr(), es);
    }
}
