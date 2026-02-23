use std::sync::atomic::{AtomicU32, AtomicU64, AtomicI64};

/// Fixed compile-time limit on concurrent IPC request slots in shared memory.
/// This determines the size of the `request_slots` array in `PgTrexShmem` and
/// cannot be changed at runtime — a PostgreSQL restart with a recompiled binary
/// is required to alter it.
pub const MAX_CONCURRENT: usize = 32;
pub const MAX_CATALOG_ENTRIES: usize = 1024;

/// Unix epoch (1970-01-01) to PostgreSQL epoch (2000-01-01) offset in days.
pub const UNIX_TO_PG_EPOCH_DAYS: i32 = 10957;

/// Same offset in microseconds for timestamp conversion.
pub const UNIX_TO_PG_EPOCH_USEC: i64 = UNIX_TO_PG_EPOCH_DAYS as i64 * 86_400 * 1_000_000;

// Worker states
pub const WORKER_STATE_STOPPED: u32 = 0;
pub const WORKER_STATE_STARTING: u32 = 1;
pub const WORKER_STATE_RUNNING: u32 = 2;

// Query execution mode flags (stored in IPC request flags field)
pub const QUERY_FLAG_LOCAL: u32 = 0;
pub const QUERY_FLAG_DISTRIBUTED: u32 = 1;

// Slot states
pub const SLOT_FREE: u32 = 0;
pub const SLOT_PENDING: u32 = 1;
pub const SLOT_IN_PROGRESS: u32 = 2;
pub const SLOT_DONE: u32 = 3;
pub const SLOT_ERROR: u32 = 4;
pub const SLOT_CANCELLED: u32 = 5;

// IPC layout constants
pub const REQUEST_QUEUE_SIZE: usize = 8 * 1024; // 8 KB
pub const RESPONSE_QUEUE_SIZE: usize = 1024 * 1024; // 1 MB

// Response status bytes
pub const RESPONSE_OK: u32 = 0;
pub const RESPONSE_ERROR: u32 = 1;

/// Per-slot IPC coordination state in shared memory.
#[repr(C)]
pub struct RequestSlot {
    pub state: AtomicU32,
    pub dsm_handle: AtomicU32,
    pub backend_pid: AtomicU32,
}

impl Default for RequestSlot {
    fn default() -> Self {
        Self {
            state: AtomicU32::new(SLOT_FREE),
            dsm_handle: AtomicU32::new(0),
            backend_pid: AtomicU32::new(0),
        }
    }
}

/// Single entry in the distributed catalog.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CatalogEntry {
    pub schema_name: [u8; 64],
    pub table_name: [u8; 64],
    pub node_name: [u8; 64],
    pub approx_rows: u64,
}

impl Default for CatalogEntry {
    fn default() -> Self {
        Self {
            schema_name: [0u8; 64],
            table_name: [0u8; 64],
            node_name: [0u8; 64],
            approx_rows: 0,
        }
    }
}

impl CatalogEntry {
    /// Read the table_name as a &str (up to the first null byte).
    pub fn table_name_str(&self) -> &str {
        let len = self.table_name.iter().position(|&b| b == 0).unwrap_or(64);
        std::str::from_utf8(&self.table_name[..len]).unwrap_or("")
    }

    /// Read the schema_name as a &str (up to the first null byte).
    pub fn schema_name_str(&self) -> &str {
        let len = self.schema_name.iter().position(|&b| b == 0).unwrap_or(64);
        std::str::from_utf8(&self.schema_name[..len]).unwrap_or("")
    }

    /// Read the node_name as a &str (up to the first null byte).
    pub fn node_name_str(&self) -> &str {
        let len = self.node_name.iter().position(|&b| b == 0).unwrap_or(64);
        std::str::from_utf8(&self.node_name[..len]).unwrap_or("")
    }
}

/// Shared-memory distributed catalog with seqlock-style concurrency.
#[repr(C)]
pub struct DistributedCatalog {
    pub generation: AtomicU64,
    pub count: AtomicU32,
    pub entries: [CatalogEntry; MAX_CATALOG_ENTRIES],
}

impl Default for DistributedCatalog {
    fn default() -> Self {
        Self {
            generation: AtomicU64::new(0),
            count: AtomicU32::new(0),
            entries: [CatalogEntry::default(); MAX_CATALOG_ENTRIES],
        }
    }
}

/// Top-level shared memory structure for pg_trex.
#[repr(C)]
pub struct PgTrexShmem {
    pub request_slots: [RequestSlot; MAX_CONCURRENT],
    pub worker_latch: AtomicU64, // stores *mut pg_sys::Latch as u64 for atomicity
    pub worker_state: AtomicU32,
    pub worker_start_time: AtomicI64,
    pub catalog: DistributedCatalog,
    pub catalog_last_refresh: AtomicI64,
}

impl Default for PgTrexShmem {
    fn default() -> Self {
        Self {
            request_slots: std::array::from_fn(|_| RequestSlot::default()),
            worker_latch: AtomicU64::new(0),
            worker_state: AtomicU32::new(WORKER_STATE_STOPPED),
            worker_start_time: AtomicI64::new(0),
            catalog: DistributedCatalog::default(),
            catalog_last_refresh: AtomicI64::new(0),
        }
    }
}

unsafe impl PGRXSharedMemory for PgTrexShmem {}

use pgrx::PGRXSharedMemory;
