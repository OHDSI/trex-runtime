use pgrx::prelude::*;
use pgrx::bgworkers::*;
use pgrx::pg_shmem_init;
use std::time::Duration;

mod arrow_to_pg;
mod catalog;
mod custom_scan;
mod guc;
mod ipc;
mod pg_attach;
mod pg_scan;
mod pg_state;
mod planner;
mod spi_bridge;
mod types;
mod worker;

use types::PgTrexShmem;

::pgrx::pg_module_magic!();

static PG_TREX_SHMEM: pgrx::PgLwLock<PgTrexShmem> = unsafe { pgrx::PgLwLock::new(c"pg_trex_shmem") };

static mut PREV_PLANNER_HOOK: pg_sys::planner_hook_type = None;

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    guc::register_gucs();
    pg_shmem_init!(PG_TREX_SHMEM);

    unsafe {
        PREV_PLANNER_HOOK = pg_sys::planner_hook;
        pg_sys::planner_hook = Some(planner::pg_trex_planner);
    }

    BackgroundWorkerBuilder::new("pg_trex analytical engine")
        .set_function("pg_trex_worker_main")
        .set_library("pg_trex")
        .enable_shmem_access(None)
        .enable_spi_access()
        .set_restart_time(Some(Duration::from_secs(10)))
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .load();
}

/// Background worker entry point -- called by PostgreSQL postmaster.
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_trex_worker_main(_arg: pg_sys::Datum) {
    worker::worker_main(&PG_TREX_SHMEM);
}

/// Send a query to the embedded trexsql engine and return results as a PostgreSQL result set.
#[pg_extern]
fn pg_trex_query(
    sql: &str,
) -> pgrx::iter::TableIterator<'static, (pgrx::name!(result, String),)> {
    let shmem = PG_TREX_SHMEM.share();
    let result = ipc::execute_query(&*shmem, sql, types::QUERY_FLAG_LOCAL);
    drop(shmem);

    match result {
        Ok(rows) => pgrx::iter::TableIterator::new(rows.into_iter().map(|row| {
            let text = row
                .into_iter()
                .map(|col| col.unwrap_or_else(|| "NULL".to_string()))
                .collect::<Vec<_>>()
                .join("\t");
            (text,)
        })),
        Err(e) => pgrx::error!("pg_trex: {}", e),
    }
}

/// Send a query for distributed execution via trex_db_query() across the cluster.
#[pg_extern]
fn pg_trex_distributed_query(
    sql: &str,
) -> pgrx::iter::TableIterator<'static, (pgrx::name!(result, String),)> {
    let shmem = PG_TREX_SHMEM.share();
    let result = ipc::execute_query(&*shmem, sql, types::QUERY_FLAG_DISTRIBUTED);
    drop(shmem);

    match result {
        Ok(rows) => pgrx::iter::TableIterator::new(rows.into_iter().map(|row| {
            let text = row
                .into_iter()
                .map(|col| col.unwrap_or_else(|| "NULL".to_string()))
                .collect::<Vec<_>>()
                .join("\t");
            (text,)
        })),
        Err(e) => pgrx::error!("pg_trex: {}", e),
    }
}

/// Return current status of the pg_trex background worker.
#[pg_extern]
fn pg_trex_status() -> pgrx::iter::TableIterator<
    'static,
    (
        pgrx::name!(state, String),
        pgrx::name!(active_queries, i32),
        pgrx::name!(catalog_entries, i32),
        pgrx::name!(gossip_addr, String),
        pgrx::name!(flight_addr, String),
        pgrx::name!(data_node, bool),
    ),
> {
    use std::sync::atomic::Ordering;

    let shmem = PG_TREX_SHMEM.share();

    let state = match shmem.worker_state.load(Ordering::Acquire) {
        types::WORKER_STATE_STOPPED => "stopped",
        types::WORKER_STATE_STARTING => "starting",
        types::WORKER_STATE_RUNNING => "running",
        _ => "unknown",
    };

    let active = shmem
        .request_slots
        .iter()
        .filter(|s| {
            let st = s.state.load(Ordering::Relaxed);
            st == types::SLOT_PENDING || st == types::SLOT_IN_PROGRESS
        })
        .count() as i32;

    let catalog_count = shmem.catalog.count.load(Ordering::Relaxed) as i32;

    let gossip = guc::get_str(&guc::GOSSIP_ADDR, "");
    let flight = guc::get_str(&guc::FLIGHT_ADDR, "");
    let data = guc::DATA_NODE.get();

    drop(shmem);

    pgrx::iter::TableIterator::once((
        state.to_string(),
        active,
        catalog_count,
        gossip,
        flight,
        data,
    ))
}

pub fn get_shmem() -> &'static pgrx::PgLwLock<PgTrexShmem> {
    &PG_TREX_SHMEM
}

pub fn get_prev_planner_hook() -> pg_sys::planner_hook_type {
    unsafe { PREV_PLANNER_HOOK }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_pg_trex_status() {
        let result = Spi::get_one::<String>("SELECT state FROM pg_trex_status()");
        assert!(result.is_ok());
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries = 'pg_trex'"]
    }
}
