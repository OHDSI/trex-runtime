// Background worker: trexsql engine initialization, thread pool, main loop.
//
// Thread-safety: All PostgreSQL pg_sys calls (DSM, shm_mq, palloc-based APIs)
// are performed exclusively in the main background worker thread. Worker pool
// threads only execute trexsql queries and communicate results via crossbeam
// channels. This is necessary because palloc is NOT thread-safe.

use crate::catalog;
use crate::guc;
use crate::ipc;
use crate::pg_attach;
use crate::pg_scan;
use crate::pg_state;
use crate::spi_bridge;
use crate::types::*;

use arrow::datatypes::Schema;
use arrow_ipc::writer::StreamWriter;
use crossbeam_channel::{unbounded, Receiver, Sender};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{params, Config, Connection};
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// How often (in milliseconds) the main loop polls for pending request slots.
const POLL_INTERVAL_MS: i64 = 100;

// ---------------------------------------------------------------------------
// Thread pool types
// ---------------------------------------------------------------------------

/// A request dispatched from the main loop to a worker thread.
/// The SQL string has already been read from shm_mq by the main loop.
struct WorkerRequest {
    slot_index: usize,
    sql: String,
    flags: u32,
}

/// A response from a worker thread back to the main loop.
/// Contains either Arrow IPC bytes (success) or an error message.
struct WorkerResponse {
    slot_index: usize,
    result: Result<Vec<u8>, String>,
}

/// A single worker thread handle.
struct PoolWorker {
    handle: Option<JoinHandle<()>>,
}

/// Tracks a query that has been dispatched to a thread and is awaiting response.
/// The DSM segment and response shm_mq handle are held here so the main loop
/// can write the response back to the backend when the thread finishes.
///
/// These raw pointers are only used in the main background worker thread.
struct PendingQuery {
    dsm_seg: *mut pg_sys::dsm_segment,
    response_handle: *mut pg_sys::shm_mq_handle,
}

/// Thread pool that distributes queries across pre-cloned trexsql connections.
///
/// Each thread owns a cloned Connection and receives work via a crossbeam channel.
/// Responses are sent back via a shared MPSC response channel so the main loop
/// (which runs in the PostgreSQL process context) can handle all pg_sys IPC calls.
struct QueryPool {
    senders: Vec<Sender<WorkerRequest>>,
    workers: Vec<PoolWorker>,
    next_worker: AtomicUsize,
    response_rx: Receiver<WorkerResponse>,
}

impl QueryPool {
    /// Create a new pool with `pool_size` threads, each cloning the given connection.
    fn new(connection: &Connection, pool_size: usize) -> Result<Self, String> {
        if pool_size == 0 {
            return Err("pool_size must be > 0".into());
        }

        let mut connections = Vec::with_capacity(pool_size);
        for i in 0..pool_size {
            connections.push(
                connection
                    .try_clone()
                    .map_err(|e| format!("connection clone {i}: {e}"))?,
            );
        }

        let (response_tx, response_rx) = unbounded::<WorkerResponse>();

        let mut senders = Vec::with_capacity(pool_size);
        let mut workers = Vec::with_capacity(pool_size);

        for (i, conn) in connections.into_iter().enumerate() {
            let (tx, rx): (Sender<WorkerRequest>, Receiver<WorkerRequest>) = unbounded();
            senders.push(tx);
            let resp_tx = response_tx.clone();
            let handle = thread::Builder::new()
                .name(format!("pg_trex-executor-{i}"))
                .spawn(move || worker_thread_fn(conn, rx, resp_tx))
                .map_err(|e| format!("spawn worker {i}: {e}"))?;
            workers.push(PoolWorker {
                handle: Some(handle),
            });
        }

        Ok(Self {
            senders,
            workers,
            next_worker: AtomicUsize::new(0),
            response_rx,
        })
    }

    /// Submit a request to the next worker thread via round-robin.
    fn submit(&self, request: WorkerRequest) -> Result<(), String> {
        let idx = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        self.senders[idx]
            .send(request)
            .map_err(|e| format!("submit to worker {idx}: {e}"))
    }

    /// Number of threads in the pool.
    fn pool_size(&self) -> usize {
        self.workers.len()
    }
}

impl Drop for QueryPool {
    fn drop(&mut self) {
        // Drop senders to signal workers to exit
        self.senders.clear();
        // Wait for workers to finish
        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                let _ = handle.join();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Engine initialization
// ---------------------------------------------------------------------------

/// Validate that an extension file path does not contain characters that could
/// allow SQL injection in a LOAD statement. LOAD does not support parameterized
/// queries, so we must validate the path before interpolation.
fn validate_extension_path(path: &str) -> Result<(), String> {
    if path.contains('\'') || path.contains(';') || path.contains('\0') {
        return Err(format!(
            "invalid extension path (contains forbidden character): {}",
            path
        ));
    }
    Ok(())
}

/// Load all .trex and .duckdb_extension files from a directory.
fn load_extensions_from_dir(conn: &Connection, dir: &str) -> (u32, u32) {
    let mut loaded = 0u32;
    let mut failed = 0u32;

    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            pgrx::warning!("pg_trex: could not read extension_dir {}: {}", dir, e);
            return (0, 0);
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let ext = path.extension().and_then(|e| e.to_str());
        if ext != Some("trex") && ext != Some("duckdb_extension") {
            continue;
        }

        let path_str = path.display().to_string();
        if let Err(e) = validate_extension_path(&path_str) {
            pgrx::warning!("pg_trex: skipping {}: {}", path_str, e);
            failed += 1;
            continue;
        }

        pgrx::log!("pg_trex: loading extension {}", path_str);
        match conn.execute_batch(&format!("LOAD '{}'", path_str)) {
            Ok(_) => {
                pgrx::log!("pg_trex: loaded {}", path_str);
                loaded += 1;
            }
            Err(e) => {
                pgrx::warning!("pg_trex: failed to load {}: {}", path_str, e);
                failed += 1;
            }
        }
    }

    (loaded, failed)
}

/// Open an in-memory trexsql instance and load extensions from extension_dir.
fn init_trexsql_engine() -> Result<Connection, String> {
    pgrx::log!("pg_trex: initializing trexsql engine");

    let config = Config::default()
        .allow_unsigned_extensions()
        .map_err(|e| format!("config allow_unsigned_extensions: {e}"))?;
    let conn = Connection::open_in_memory_with_flags(config)
        .map_err(|e| format!("open_in_memory: {e}"))?;

    conn.execute_batch("SET autoinstall_known_extensions=true; SET autoload_known_extensions=true;")
        .map_err(|e| format!("set autoinstall/autoload: {e}"))?;

    let extension_dir = guc::get_str(&guc::EXTENSION_DIR, "");
    if !extension_dir.is_empty() {
        pgrx::log!("pg_trex: loading extensions from {}", extension_dir);
        let (loaded, failed) = load_extensions_from_dir(&conn, &extension_dir);
        pgrx::log!(
            "pg_trex: loaded {} extensions ({} failed) from {}",
            loaded, failed, extension_dir
        );
    } else {
        pgrx::log!("pg_trex: extension_dir not configured, no extensions loaded");
    }

    pgrx::log!("pg_trex: trexsql engine initialized");
    Ok(conn)
}

// ---------------------------------------------------------------------------
// Cluster startup
// ---------------------------------------------------------------------------

/// Start the cluster via trex_db_start() with GUC-configured addresses.
fn start_cluster(conn: &Connection) -> Result<(), String> {
    let extension_dir = guc::get_str(&guc::EXTENSION_DIR, "");
    if extension_dir.is_empty() {
        pgrx::log!("pg_trex: extension_dir not set, skipping cluster start");
        return Ok(());
    }

    let gossip_addr = guc::get_str(&guc::GOSSIP_ADDR, "127.0.0.1:7946");
    let flight_addr = guc::get_str(&guc::FLIGHT_ADDR, "127.0.0.1:50051");
    let seeds = guc::get_str(&guc::SEEDS, "");
    let cluster_id = guc::get_str(&guc::CLUSTER_ID, "pg_trex");
    let node_name = guc::get_str(&guc::NODE_NAME, "");
    let data_node = guc::DATA_NODE.get();

    pgrx::log!(
        "pg_trex: starting cluster (gossip={}, flight={}, seeds={}, cluster={}, node={}, data_node={})",
        gossip_addr, flight_addr, seeds, cluster_id, node_name, data_node
    );

    let mut stmt = conn
        .prepare("SELECT trex_db_start(gossip_addr := $1, flight_addr := $2, seeds := $3, cluster_id := $4, node_name := $5, data_node := $6)")
        .map_err(|e| format!("trex_db_start prepare: {e}"))?;

    stmt.execute(params![&gossip_addr, &flight_addr, &seeds, &cluster_id, &node_name, data_node])
        .map_err(|e| format!("trex_db_start: {e}"))?;

    pgrx::log!("pg_trex: cluster started successfully");
    Ok(())
}

/// Stop the cluster (called before re-starting with new addresses on SIGHUP).
fn stop_cluster(conn: &Connection) {
    let extension_dir = guc::get_str(&guc::EXTENSION_DIR, "");
    if extension_dir.is_empty() {
        return;
    }

    pgrx::log!("pg_trex: stopping cluster for reconfiguration");
    if let Err(e) = conn.execute_batch("SELECT trex_db_stop()") {
        pgrx::warning!("pg_trex: trex_db_stop failed: {}", e);
    }
}

// ---------------------------------------------------------------------------
// SIGHUP handling
// ---------------------------------------------------------------------------

/// Cached GUC values for detecting changes on SIGHUP.
struct CachedGucs {
    gossip_addr: String,
    flight_addr: String,
    seeds: String,
    data_node: bool,
    catalog_refresh_secs: i32,
}

impl CachedGucs {
    fn from_current() -> Self {
        Self {
            gossip_addr: guc::get_str(&guc::GOSSIP_ADDR, ""),
            flight_addr: guc::get_str(&guc::FLIGHT_ADDR, ""),
            seeds: guc::get_str(&guc::SEEDS, ""),
            data_node: guc::DATA_NODE.get(),
            catalog_refresh_secs: guc::CATALOG_REFRESH_SECS.get(),
        }
    }

    /// Check if cluster-related addresses changed (requires cluster restart).
    fn cluster_changed(&self, new: &CachedGucs) -> bool {
        self.gossip_addr != new.gossip_addr
            || self.flight_addr != new.flight_addr
            || self.seeds != new.seeds
            || self.data_node != new.data_node
    }
}

/// Handle SIGHUP: re-read GUC values and restart cluster if config changed.
fn handle_sighup(conn: &Connection, cached: &mut CachedGucs) {
    pgrx::log!("pg_trex: received SIGHUP, reloading configuration");

    let new = CachedGucs::from_current();

    if cached.catalog_refresh_secs != new.catalog_refresh_secs {
        pgrx::log!(
            "pg_trex: catalog_refresh_secs changed: {} -> {}",
            cached.catalog_refresh_secs,
            new.catalog_refresh_secs
        );
    }

    if cached.cluster_changed(&new) {
        pgrx::log!("pg_trex: cluster configuration changed, restarting cluster");
        stop_cluster(conn);
        if let Err(e) = start_cluster(conn) {
            pgrx::warning!("pg_trex: failed to restart cluster after SIGHUP: {}", e);
        }
    }

    *cached = new;
}

// ---------------------------------------------------------------------------
// Worker thread function
// ---------------------------------------------------------------------------

/// Thread function for each worker in the query pool.
///
/// Receives SQL strings from the main loop, executes them on the trexsql
/// connection, and sends back Arrow IPC bytes or error messages.
///
/// This function does NOT call any pg_sys functions -- all PostgreSQL IPC
/// (DSM, shm_mq) is handled by the main background worker thread.
fn worker_thread_fn(
    conn: Connection,
    receiver: Receiver<WorkerRequest>,
    response_tx: Sender<WorkerResponse>,
) {
    let thread_name = thread::current()
        .name()
        .unwrap_or("pg_trex-executor")
        .to_string();

    while let Ok(req) = receiver.recv() {
        let slot_index = req.slot_index;

        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            execute_and_serialize(&conn, &req.sql, req.flags)
        }));

        let result = match result {
            Ok(Ok(ipc_bytes)) => Ok(ipc_bytes),
            Ok(Err(e)) => {
                // NOTE: pgrx::warning!() must NOT be used here — this runs in
                // a spawned Rust thread, not a PostgreSQL backend process, so
                // ereport() would access invalid per-process state and hang.
                eprintln!(
                    "pg_trex [{}]: query error on slot {}: {}",
                    thread_name, slot_index, e
                );
                Err(e)
            }
            Err(panic_err) => {
                let msg = extract_panic_message(panic_err);
                eprintln!(
                    "pg_trex [{}]: query panicked on slot {}: {}",
                    thread_name, slot_index, msg
                );
                Err(format!("query panicked: {}", msg))
            }
        };

        if response_tx
            .send(WorkerResponse { slot_index, result })
            .is_err()
        {
            // Main loop has shut down, exit thread
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Query execution and Arrow IPC serialization
// ---------------------------------------------------------------------------

/// Execute a SQL query on the trexsql connection and serialize the results
/// as Arrow IPC bytes.
fn execute_and_serialize(conn: &Connection, sql: &str, flags: u32) -> Result<Vec<u8>, String> {
    let trimmed = sql.trim();

    if flags == QUERY_FLAG_DISTRIBUTED {
        execute_distributed_to_ipc(conn, trimmed)
    } else if is_result_returning_query(trimmed) {
        execute_select_to_ipc(conn, trimmed)
    } else {
        execute_non_select_to_ipc(conn, trimmed)
    }
}

/// Wrap SQL in a trex_db_query() call for distributed execution via the db extension.
fn execute_distributed_to_ipc(conn: &Connection, sql: &str) -> Result<Vec<u8>, String> {
    let escaped = sql.replace('\'', "''");
    let wrapped = format!("SELECT * FROM trex_db_query('{}')", escaped);
    execute_select_to_ipc(conn, &wrapped)
}

/// Check whether a query is expected to return a result set.
fn is_result_returning_query(sql: &str) -> bool {
    let upper = sql.to_uppercase();
    upper.starts_with("SELECT")
        || upper.starts_with("WITH")
        || upper.starts_with("SHOW")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("EXPLAIN")
        || upper.starts_with("TABLE")
        || upper.starts_with("VALUES")
        || upper.starts_with("FROM")
        || upper.starts_with("PRAGMA")
}

/// Execute a SELECT-like query and return Arrow IPC bytes.
///
/// Note: results are fully materialized into memory before being sent back via
/// IPC. Streaming results incrementally through shm_mq is a future improvement.
fn execute_select_to_ipc(conn: &Connection, sql: &str) -> Result<Vec<u8>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| format!("prepare: {e}"))?;
    let arrow_result = stmt
        .query_arrow(params![])
        .map_err(|e| format!("query_arrow: {e}"))?;

    let schema = arrow_result.get_schema();
    let batches: Vec<RecordBatch> = arrow_result.collect();

    serialize_batches_to_ipc(&schema, &batches)
}

/// Execute a non-SELECT statement and return an empty Arrow IPC stream.
fn execute_non_select_to_ipc(conn: &Connection, sql: &str) -> Result<Vec<u8>, String> {
    conn.execute_batch(sql)
        .map_err(|e| format!("execute: {e}"))?;

    let schema = Schema::empty();
    serialize_batches_to_ipc(&Arc::new(schema), &[])
}

/// Serialize Arrow RecordBatches to IPC stream format bytes.
fn serialize_batches_to_ipc(schema: &Arc<Schema>, batches: &[RecordBatch]) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema)
        .map_err(|e| format!("Arrow IPC StreamWriter init: {e}"))?;
    for batch in batches {
        writer.write(batch)
            .map_err(|e| format!("Arrow IPC write: {e}"))?;
    }
    writer.finish()
        .map_err(|e| format!("Arrow IPC finish: {e}"))?;
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

fn extract_panic_message(err: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Get the current Unix epoch timestamp in seconds.
fn now_epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ---------------------------------------------------------------------------
// Main loop: request polling and response collection
// ---------------------------------------------------------------------------

/// Scan request slots for pending queries, read SQL from shm_mq, and dispatch
/// to the thread pool.
///
/// All pg_sys calls (DSM attach, shm_mq receive) happen in this function,
/// which runs in the main background worker thread (PostgreSQL process context).
fn poll_and_read_requests(
    shmem: &pgrx::PgLwLock<PgTrexShmem>,
    pool: &QueryPool,
    pending: &mut HashMap<usize, PendingQuery>,
) {
    // First pass: atomically claim pending slots
    let mut claimed: Vec<(usize, u32)> = Vec::new();
    {
        let shared = shmem.share();
        for i in 0..MAX_CONCURRENT {
            let slot = &shared.request_slots[i];
            let state = slot.state.load(Ordering::Acquire);

            if state == SLOT_PENDING {
                match slot.state.compare_exchange(
                    SLOT_PENDING,
                    SLOT_IN_PROGRESS,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let dsm_handle = slot.dsm_handle.load(Ordering::Acquire);
                        claimed.push((i, dsm_handle));
                    }
                    Err(_) => {
                        // Another CAS won, skip
                    }
                }
            }
        }
    } // drop share guard before doing DSM operations

    // Second pass: attach to DSM, read SQL, dispatch to threads
    for (slot_idx, dsm_handle) in claimed {
        match ipc::attach_to_dsm(dsm_handle) {
            Ok(channel) => {
                match ipc::read_request_from_mq(channel.request_handle) {
                    Ok((flags, sql)) => {
                        pgrx::debug1!("pg_trex: dispatching slot {} flags={} sql={}", slot_idx, flags, &sql);

                        pending.insert(
                            slot_idx,
                            PendingQuery {
                                dsm_seg: channel.dsm_seg,
                                response_handle: channel.response_handle,
                            },
                        );

                        if let Err(e) = pool.submit(WorkerRequest {
                            slot_index: slot_idx,
                            sql,
                            flags,
                        }) {
                            pgrx::warning!(
                                "pg_trex: failed to submit slot {} to pool: {}",
                                slot_idx,
                                e
                            );
                            // Recover: write error response and complete slot
                            if let Some(pq) = pending.remove(&slot_idx) {
                                let _ = ipc::write_response_to_mq(
                                    pq.response_handle,
                                    e.as_bytes(),
                                    true,
                                );
                                {
                                    let shared = shmem.share();
                                    ipc::complete_slot(&shared, slot_idx, false);
                                }
                                unsafe { pg_sys::dsm_detach(pq.dsm_seg) };
                            }
                        }
                    }
                    Err(e) => {
                        pgrx::warning!(
                            "pg_trex: failed to read request from slot {}: {}",
                            slot_idx,
                            e
                        );
                        let _ = ipc::write_response_to_mq(
                            channel.response_handle,
                            e.as_bytes(),
                            true,
                        );
                        {
                            let shared = shmem.share();
                            ipc::complete_slot(&shared, slot_idx, false);
                        }
                        unsafe { pg_sys::dsm_detach(channel.dsm_seg) };
                    }
                }
            }
            Err(e) => {
                pgrx::warning!(
                    "pg_trex: failed to attach to DSM for slot {}: {}",
                    slot_idx,
                    e
                );
                // Mark slot as error directly
                let shared = shmem.share();
                shared.request_slots[slot_idx]
                    .state
                    .store(SLOT_ERROR, Ordering::Release);
                let backend_pid = shared.request_slots[slot_idx]
                    .backend_pid
                    .load(Ordering::Acquire);
                ipc::signal_backend_latch(backend_pid);
            }
        }
    }
}

/// Collect completed responses from worker threads and write them back to
/// the shm_mq response queues.
///
/// All pg_sys calls (shm_mq send, DSM detach) happen in this function,
/// which runs in the main background worker thread.
fn collect_responses(
    shmem: &pgrx::PgLwLock<PgTrexShmem>,
    pool: &QueryPool,
    pending: &mut HashMap<usize, PendingQuery>,
) {
    while let Ok(resp) = pool.response_rx.try_recv() {
        if let Some(pq) = pending.remove(&resp.slot_index) {
            // If the backend cancelled this query, the DSM segment has already
            // been detached by the backend's SlotGuard -- writing to
            // pq.response_handle would access freed memory.
            let slot_state = {
                let shared = shmem.share();
                shared.request_slots[resp.slot_index]
                    .state
                    .load(Ordering::Acquire)
            };

            if slot_state == SLOT_CANCELLED {
                pgrx::debug1!(
                    "pg_trex: slot {} cancelled by backend, discarding result",
                    resp.slot_index
                );
                // Free the slot without writing to DSM (already detached)
                let shared = shmem.share();
                ipc::release_slot(&shared, resp.slot_index);
                // Do NOT dsm_detach -- backend already detached
                continue;
            }

            match resp.result {
                Ok(ref ipc_bytes) => {
                    let _ = ipc::write_response_to_mq(pq.response_handle, ipc_bytes, false);
                    let shared = shmem.share();
                    ipc::complete_slot(&shared, resp.slot_index, true);
                }
                Err(ref err_msg) => {
                    let _ =
                        ipc::write_response_to_mq(pq.response_handle, err_msg.as_bytes(), true);
                    let shared = shmem.share();
                    ipc::complete_slot(&shared, resp.slot_index, false);
                }
            }
            unsafe { pg_sys::dsm_detach(pq.dsm_seg) };
        } else {
            pgrx::warning!(
                "pg_trex: received response for unknown slot {}",
                resp.slot_index
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Schema migration + pg_attach
// ---------------------------------------------------------------------------

/// Run core schema migrations via the migration extension and attach PostgreSQL
/// tables via pg_attach. Migrations use postgres_scanner (ATTACH ... TYPE postgres)
/// so they do not go through SPI. pg_attach uses spi_bridge and must run on a
/// spawned thread while the main thread pumps process_pending().
fn run_migrations_and_attach(conn: &Connection) {
    let database_url = std::env::var("DATABASE_URL").unwrap_or_default();
    let schema_dir = std::env::var("SCHEMA_DIR").unwrap_or_default();

    if !database_url.is_empty() && !schema_dir.is_empty() {
        pgrx::log!("pg_trex: running core schema migrations");
        let escaped_url = database_url.replace('\'', "''");
        let escaped_dir = schema_dir.replace('\'', "''");

        let migration_sql = format!(
            "INSTALL postgres;\
             ATTACH '{}' AS _config (TYPE postgres);\
             SELECT * FROM trex_migration_run_schema('{}', 'trex', '_config');\
             DETACH _config;",
            escaped_url, escaped_dir,
        );

        match conn.execute_batch(&migration_sql) {
            Ok(_) => pgrx::log!("pg_trex: core schema migrations completed"),
            Err(e) => pgrx::warning!("pg_trex: core schema migrations failed (non-fatal): {}", e),
        }
    }

    // pg_attach uses spi_bridge::request() which deadlocks on the main thread,
    // so we run it on a spawned thread and pump process_pending() until done.
    let (done_tx, done_rx) = crossbeam_channel::bounded::<Result<(), String>>(1);
    let attach_conn = match conn.try_clone() {
        Ok(c) => c,
        Err(e) => {
            pgrx::warning!("pg_trex: could not clone connection for pg_attach: {}", e);
            return;
        }
    };

    thread::Builder::new()
        .name("pg_trex-attach".into())
        .spawn(move || {
            let result = attach_conn
                .execute_batch("SELECT * FROM pg_attach('trex')")
                .map_err(|e| format!("{e}"));
            let _ = done_tx.send(result);
        })
        .unwrap_or_else(|e| {
            panic!("pg_trex: failed to spawn pg_attach thread: {}", e);
        });

    // Pump SPI bridge while waiting for pg_attach to complete
    loop {
        spi_bridge::process_pending();
        match done_rx.try_recv() {
            Ok(Ok(())) => {
                pgrx::log!("pg_trex: pg_attach('trex') completed");
                break;
            }
            Ok(Err(e)) => {
                pgrx::warning!("pg_trex: pg_attach('trex') failed (non-fatal): {}", e);
                break;
            }
            Err(crossbeam_channel::TryRecvError::Empty) => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                pgrx::warning!("pg_trex: pg_attach thread disconnected");
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Worker main entry point
// ---------------------------------------------------------------------------

/// Background worker main function.
///
/// Called by PostgreSQL's postmaster when the background worker process starts.
/// This function:
/// 1. Unblocks signals (SIGHUP, SIGTERM)
/// 2. Initializes the trexsql engine (in-memory instance with extensions)
/// 3. Runs schema migrations and pg_attach
/// 4. Starts the cluster (gossip + flight)
/// 5. Creates the query thread pool
/// 6. Enters the main loop: poll slots, dispatch queries, handle signals
pub fn worker_main(shmem: &pgrx::PgLwLock<PgTrexShmem>) {
    // Mark worker as starting
    {
        let exclusive = shmem.exclusive();
        exclusive
            .worker_state
            .store(WORKER_STATE_STARTING, Ordering::Release);
        exclusive
            .worker_start_time
            .store(now_epoch_secs(), Ordering::Release);
    }

    pgrx::log!("pg_trex: background worker starting");

    // Unblock signals so we can receive SIGHUP and SIGTERM
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Connect to PostgreSQL so SPI is available on this thread
    let database = guc::get_str(&guc::DATABASE, "");
    let db_name = if database.is_empty() {
        std::env::var("POSTGRES_DB").unwrap_or_else(|_| "postgres".to_string())
    } else {
        database
    };
    pgrx::log!("pg_trex: connecting to database '{}' for SPI", db_name);
    let db_cstr = std::ffi::CString::new(db_name)
        .expect("pg_trex: invalid database name");
    unsafe {
        pg_sys::BackgroundWorkerInitializeConnection(
            db_cstr.as_ptr(),
            std::ptr::null(),
            0,
        );
    }

    // Store the worker latch pointer in shared memory so backends can wake us
    {
        let exclusive = shmem.exclusive();
        let latch_ptr = unsafe { pg_sys::MyLatch as u64 };
        exclusive.worker_latch.store(latch_ptr, Ordering::Release);
    }

    // Initialize the trexsql engine
    let conn = match init_trexsql_engine() {
        Ok(c) => c,
        Err(e) => {
            pgrx::warning!("pg_trex: engine initialization failed: {}", e);
            let exclusive = shmem.exclusive();
            exclusive
                .worker_state
                .store(WORKER_STATE_STOPPED, Ordering::Release);
            return;
        }
    };

    // Initialize SPI bridge channels before creating the thread pool so worker
    // threads can submit pg_scan requests from the start.
    spi_bridge::init();

    // Register table functions on the connection before cloning into the pool
    conn.register_table_function::<pg_scan::PgScanVTab>("pg_scan")
        .map_err(|e| format!("register pg_scan: {e}"))
        .unwrap_or_else(|e| {
            pgrx::warning!("pg_trex: {}", e);
        });

    pg_state::init_ddl_connection(&conn)
        .unwrap_or_else(|e| pgrx::warning!("pg_trex: DDL connection init: {}", e));

    conn.register_table_function::<pg_attach::PgAttachVTab>("pg_attach")
        .map_err(|e| format!("register pg_attach: {e}"))
        .unwrap_or_else(|e| {
            pgrx::warning!("pg_trex: {}", e);
        });
    conn.register_table_function::<pg_attach::PgDetachVTab>("pg_detach")
        .map_err(|e| format!("register pg_detach: {e}"))
        .unwrap_or_else(|e| {
            pgrx::warning!("pg_trex: {}", e);
        });
    conn.register_table_function::<pg_attach::PgTablesVTab>("pg_tables")
        .map_err(|e| format!("register pg_tables: {e}"))
        .unwrap_or_else(|e| {
            pgrx::warning!("pg_trex: {}", e);
        });

    // Run schema migrations (via postgres_scanner) and pg_attach before cluster start
    run_migrations_and_attach(&conn);

    // When SWARM_CONFIG is set, the db extension's init already started gossip
    // and orchestrated services (including trexas). Skip the GUC-based startup.
    if std::env::var("SWARM_CONFIG").is_ok() {
        pgrx::log!("pg_trex: SWARM_CONFIG detected, skipping GUC-based cluster start");
    } else if let Err(e) = start_cluster(&conn) {
        pgrx::warning!("pg_trex: trex_db_start failed (non-fatal): {}", e);
    }

    // Create the query thread pool (size from GUC, default 4)
    let pool_size = guc::POOL_SIZE.get().max(1) as usize;
    let pool = match QueryPool::new(&conn, pool_size) {
        Ok(p) => p,
        Err(e) => {
            pgrx::warning!("pg_trex: thread pool creation failed: {}", e);
            let exclusive = shmem.exclusive();
            exclusive
                .worker_state
                .store(WORKER_STATE_STOPPED, Ordering::Release);
            return;
        }
    };

    pgrx::log!(
        "pg_trex: thread pool created with {} workers",
        pool.pool_size()
    );

    // Clean up any abandoned slots from a previous worker crash
    {
        let shared = shmem.share();
        ipc::cleanup_abandoned_slots(&shared);
    }

    // Mark worker as running
    {
        let exclusive = shmem.exclusive();
        exclusive
            .worker_state
            .store(WORKER_STATE_RUNNING, Ordering::Release);
    }

    pgrx::log!("pg_trex: background worker running");

    // Cache GUC values for SIGHUP change detection
    let mut cached_gucs = CachedGucs::from_current();

    // Initialize to epoch so the first main-loop iteration triggers a catalog
    // refresh.  We deliberately do NOT block here before entering the main loop:
    // trex_db_tables() can take an unbounded amount of time if the gossip / flight
    // services are still initializing, and blocking here would prevent the worker
    // from processing any queries (the worker state is already RUNNING so
    // backends will start submitting requests).
    let mut last_catalog_refresh = Instant::now() - Duration::from_secs(
        cached_gucs.catalog_refresh_secs.max(1) as u64 + 1,
    );

    // Track in-flight queries dispatched to worker threads
    let mut pending_queries: HashMap<usize, PendingQuery> = HashMap::new();

    // -----------------------------------------------------------------------
    // Main loop
    // -----------------------------------------------------------------------
    loop {
        // Check for SIGTERM -- graceful shutdown
        if BackgroundWorker::sigterm_received() {
            pgrx::log!("pg_trex: SIGTERM received, shutting down");
            break;
        }

        // Check for SIGHUP -- reload GUCs
        if BackgroundWorker::sighup_received() {
            handle_sighup(&conn, &mut cached_gucs);
        }

        // Poll request slots: read SQL from shm_mq, dispatch to threads
        poll_and_read_requests(shmem, &pool, &mut pending_queries);

        // Collect completed responses: write results to shm_mq
        collect_responses(shmem, &pool, &mut pending_queries);

        // Process any pending SPI requests from pg_scan calls
        spi_bridge::process_pending();

        // Periodic catalog refresh
        let refresh_interval =
            Duration::from_secs(cached_gucs.catalog_refresh_secs.max(1) as u64);
        if last_catalog_refresh.elapsed() >= refresh_interval {
            let shared = shmem.share();
            let _ = catalog::refresh_catalog(&shared, &conn);
            shared
                .catalog_last_refresh
                .store(now_epoch_secs(), Ordering::Release);
            last_catalog_refresh = Instant::now();
        }

        // Wait on the latch with a timeout. The latch is signaled by backends
        // when they submit a new request, or by PostgreSQL on SIGHUP/SIGTERM.
        BackgroundWorker::wait_latch(Some(Duration::from_millis(POLL_INTERVAL_MS as u64)));
        unsafe { pg_sys::ResetLatch(pg_sys::MyLatch) };
    }

    // -----------------------------------------------------------------------
    // Shutdown
    // -----------------------------------------------------------------------

    // Clean up any in-flight queries
    for (slot_idx, pq) in pending_queries.drain() {
        let shared = shmem.share();
        let current = shared.request_slots[slot_idx]
            .state
            .load(Ordering::Acquire);
        if current == SLOT_CANCELLED {
            // Backend already detached DSM and is gone; just free the slot
            ipc::release_slot(&shared, slot_idx);
        } else {
            shared.request_slots[slot_idx]
                .state
                .store(SLOT_ERROR, Ordering::Release);
            let backend_pid = shared.request_slots[slot_idx]
                .backend_pid
                .load(Ordering::Acquire);
            ipc::signal_backend_latch(backend_pid);
            unsafe { pg_sys::dsm_detach(pq.dsm_seg) };
        }
    }

    pgrx::log!("pg_trex: shutting down thread pool");
    drop(pool);

    // Stop cluster — either via SWARM_CONFIG path or GUC-based
    if std::env::var("SWARM_CONFIG").is_ok() {
        pgrx::log!("pg_trex: stopping SWARM_CONFIG-managed services");
        if let Err(e) = conn.execute_batch("SELECT trex_db_stop()") {
            pgrx::warning!("pg_trex: trex_db_stop failed: {}", e);
        }
    } else {
        stop_cluster(&conn);
    }

    // Mark worker as stopped
    {
        let exclusive = shmem.exclusive();
        exclusive
            .worker_state
            .store(WORKER_STATE_STOPPED, Ordering::Release);
        exclusive.worker_latch.store(0, Ordering::Release);
    }

    pgrx::log!("pg_trex: background worker stopped");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_result_returning_query() {
        assert!(is_result_returning_query("SELECT 1"));
        assert!(is_result_returning_query("select 1"));
        assert!(is_result_returning_query("WITH cte AS (SELECT 1) SELECT * FROM cte"));
        assert!(is_result_returning_query("SHOW tables"));
        assert!(is_result_returning_query("DESCRIBE my_table"));
        assert!(is_result_returning_query("EXPLAIN SELECT 1"));
        assert!(is_result_returning_query("FROM my_table SELECT *"));
        assert!(is_result_returning_query("PRAGMA version"));
        assert!(is_result_returning_query("VALUES (1, 2, 3)"));
        assert!(!is_result_returning_query("CREATE TABLE t (id INT)"));
        assert!(!is_result_returning_query("INSERT INTO t VALUES (1)"));
        assert!(!is_result_returning_query("DROP TABLE t"));
    }

    #[test]
    fn test_serialize_batches_to_ipc_empty() {
        let schema = Arc::new(Schema::empty());
        let bytes = serialize_batches_to_ipc(&schema, &[]).unwrap();
        // Should produce valid Arrow IPC stream bytes (header + EOS marker)
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_validate_extension_path() {
        assert!(validate_extension_path("/usr/lib/db.trex").is_ok());
        assert!(validate_extension_path("./build/debug/ext.trex").is_ok());
        assert!(validate_extension_path("path with spaces/ext.trex").is_ok());
        assert!(validate_extension_path("'; DROP TABLE x; --").is_err());
        assert!(validate_extension_path("path\0evil").is_err());
        assert!(validate_extension_path("path;evil").is_err());
    }

    #[test]
    fn test_extract_panic_message_str() {
        let msg = extract_panic_message(Box::new("test panic"));
        assert_eq!(msg, "test panic");
    }

    #[test]
    fn test_extract_panic_message_string() {
        let msg = extract_panic_message(Box::new("owned panic".to_string()));
        assert_eq!(msg, "owned panic");
    }

    #[test]
    fn test_extract_panic_message_unknown() {
        let msg = extract_panic_message(Box::new(42i32));
        assert_eq!(msg, "unknown panic");
    }
}
