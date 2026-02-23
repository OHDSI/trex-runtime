use pgrx::pg_guard;
use pgrx::pg_sys;
use std::ffi::{CStr, CString};
use std::os::raw::c_int;
use std::sync::atomic::Ordering;

use crate::catalog;
use crate::custom_scan;
use crate::types::*;

/// The planner hook entry point. Installed in `_PG_init`.
///
/// If the query references only distributed tables (and no local PG tables),
/// we create a CustomScan plan that routes execution to the background worker.
/// Otherwise, we fall through to the previous/standard planner.
#[pg_guard]
pub unsafe extern "C-unwind" fn pg_trex_planner(
    parse: *mut pg_sys::Query,
    query_string: *const std::os::raw::c_char,
    cursor_options: c_int,
    bound_params: *mut pg_sys::ParamListInfoData,
) -> *mut pg_sys::PlannedStmt {
    if (*parse).commandType != pg_sys::CmdType::CMD_SELECT {
        return call_prev_planner(parse, query_string, cursor_options, bound_params);
    }

    if should_route(parse) {
        if let Some(plan) = create_custom_scan_plan(parse, query_string) {
            pgrx::debug1!("pg_trex: routing query to analytical engine");
            return plan;
        }
    }

    call_prev_planner(parse, query_string, cursor_options, bound_params)
}

/// Call the previous planner hook, or standard_planner if none.
unsafe fn call_prev_planner(
    parse: *mut pg_sys::Query,
    query_string: *const std::os::raw::c_char,
    cursor_options: c_int,
    bound_params: *mut pg_sys::ParamListInfoData,
) -> *mut pg_sys::PlannedStmt {
    match crate::get_prev_planner_hook() {
        Some(prev_hook) => prev_hook(parse, query_string, cursor_options, bound_params),
        None => pg_sys::standard_planner(parse, query_string, cursor_options, bound_params),
    }
}

/// Determine whether a query should be routed to the trexsql engine.
///
/// Returns true only when ALL referenced tables exist in the distributed
/// catalog AND NONE are local PostgreSQL tables.
unsafe fn should_route(parse: *mut pg_sys::Query) -> bool {
    let shmem = crate::get_shmem();
    let shmem_ref = shmem.share();

    if shmem_ref.worker_state.load(Ordering::Acquire) != WORKER_STATE_RUNNING {
        return false;
    }

    let rtable = (*parse).rtable;
    if rtable.is_null() {
        return false;
    }

    let mut has_distributed = false;
    let mut has_local = false;

    let length = (*rtable).length;
    if length == 0 {
        return false;
    }

    let mut cell = (*rtable).elements;
    for _ in 0..length {
        if cell.is_null() {
            break;
        }
        let rte = (*cell).ptr_value as *mut pg_sys::RangeTblEntry;
        cell = cell.add(1);

        if rte.is_null() {
            continue;
        }

        if (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION {
            continue;
        }

        let relid = (*rte).relid;

        let rel_name = pg_sys::get_rel_name(relid);
        if rel_name.is_null() {
            continue;
        }

        let table_name = CStr::from_ptr(rel_name).to_string_lossy();

        // Check if table exists in pg syscache (local PG table)
        let tuple = pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::RELOID as i32,
            pg_sys::ObjectIdGetDatum(relid),
        );
        if !tuple.is_null() {
            has_local = true;
            pg_sys::ReleaseSysCache(tuple);
        } else if catalog::catalog_contains_table(&shmem_ref, &table_name) {
            has_distributed = true;
        }

        pg_sys::pfree(rel_name as *mut std::ffi::c_void);
    }

    drop(shmem_ref);

    has_distributed && !has_local
}

/// Create a CustomScan plan node that delegates to the background worker.
unsafe fn create_custom_scan_plan(
    parse: *mut pg_sys::Query,
    query_string: *const std::os::raw::c_char,
) -> Option<*mut pg_sys::PlannedStmt> {
    if query_string.is_null() {
        return None;
    }

    let sql = CStr::from_ptr(query_string).to_string_lossy().to_string();

    let planned_stmt =
        pg_sys::palloc0(std::mem::size_of::<pg_sys::PlannedStmt>()) as *mut pg_sys::PlannedStmt;
    (*planned_stmt).type_ = pg_sys::NodeTag::T_PlannedStmt;
    (*planned_stmt).commandType = pg_sys::CmdType::CMD_SELECT;
    (*planned_stmt).queryId = (*parse).queryId;
    (*planned_stmt).hasReturning = false;
    (*planned_stmt).hasModifyingCTE = false;
    (*planned_stmt).canSetTag = (*parse).canSetTag;
    (*planned_stmt).transientPlan = false;
    (*planned_stmt).dependsOnRole = false;
    (*planned_stmt).parallelModeNeeded = false;

    let custom_scan =
        pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScan>()) as *mut pg_sys::CustomScan;
    (*custom_scan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;

    let sql_cstr = CString::new(sql.as_str()).ok()?;
    let pg_str = pg_sys::pstrdup(sql_cstr.as_ptr());
    let sql_value = pg_sys::makeString(pg_str);
    (*custom_scan).custom_private =
        pg_sys::lappend(std::ptr::null_mut(), sql_value as *mut std::ffi::c_void);

    (*custom_scan).methods = custom_scan::get_custom_scan_methods();
    (*custom_scan).scan.plan.targetlist = (*parse).targetList;
    (*planned_stmt).planTree = &mut (*custom_scan).scan.plan;
    (*planned_stmt).rtable = (*parse).rtable;

    Some(planned_stmt)
}
