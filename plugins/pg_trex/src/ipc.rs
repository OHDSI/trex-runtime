// IPC protocol: DSM + shm_mq management, request/response serialization, slot pool
//
// Implements the IPC bridge between PostgreSQL backends and the pg_trex background
// worker. Each query creates a DSM segment containing two shm_mq ring buffers
// (request + response), acquires a shared-memory slot for coordination, and uses
// PostgreSQL latch signaling for wake-up notifications.

use pgrx::pg_sys;
use crate::types::*;
use std::sync::atomic::Ordering;

/// Worker-side DSM access handle: holds references to a DSM segment and the
/// two shm_mq handles (request + response) for a single query.
pub struct WorkerIpcChannel {
    pub dsm_seg: *mut pg_sys::dsm_segment,
    pub request_handle: *mut pg_sys::shm_mq_handle,
    pub response_handle: *mut pg_sys::shm_mq_handle,
}

/// RAII guard that ensures a request slot and DSM segment are cleaned up
/// when the query completes, errors out, or is cancelled. On Drop during
/// panic unwind (from check_for_interrupts), the slot state is set to
/// SLOT_CANCELLED so the worker discards the result instead of writing
/// to a detached DSM.
struct SlotGuard<'a> {
    shmem: &'a PgTrexShmem,
    slot_idx: usize,
    dsm_seg: *mut pg_sys::dsm_segment,
    released: bool,
}

impl<'a> SlotGuard<'a> {
    fn new(shmem: &'a PgTrexShmem, slot_idx: usize, dsm_seg: *mut pg_sys::dsm_segment) -> Self {
        Self {
            shmem,
            slot_idx,
            dsm_seg,
            released: false,
        }
    }

    /// Normal cleanup: release slot and detach DSM.
    fn release(mut self) {
        release_slot(self.shmem, self.slot_idx);
        unsafe { pg_sys::dsm_detach(self.dsm_seg) };
        self.released = true;
    }
}

impl<'a> Drop for SlotGuard<'a> {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        // We are being dropped without explicit release -- this is a cancel
        // or panic unwind. Mark the slot as cancelled so the worker skips
        // writing to the (now-detached) DSM segment.
        let slot = &self.shmem.request_slots[self.slot_idx];
        let prev = slot.state.load(Ordering::Acquire);
        if prev == SLOT_PENDING || prev == SLOT_IN_PROGRESS {
            slot.state.store(SLOT_CANCELLED, Ordering::Release);
            pgrx::debug1!(
                "pg_trex: slot {} cancelled (was state {})",
                self.slot_idx,
                prev
            );
        } else {
            // Slot already reached Done/Error -- just release normally
            release_slot(self.shmem, self.slot_idx);
        }
        unsafe { pg_sys::dsm_detach(self.dsm_seg) };
    }
}

/// Execute a SQL query through the background worker IPC bridge.
///
/// 1. Checks worker readiness
/// 2. Creates a DSM segment with request + response shm_mq
/// 3. Acquires a free request slot
/// 4. Writes the SQL to the request queue
/// 5. Signals the worker latch
/// 6. Waits for the worker to complete the request
/// 7. Reads the response and returns rows as strings
/// 8. Cleans up slot and DSM via SlotGuard RAII
pub fn execute_query(shmem: &PgTrexShmem, sql: &str, flags: u32) -> Result<Vec<Vec<Option<String>>>, String> {
    let state = shmem.worker_state.load(Ordering::Acquire);
    if state == WORKER_STATE_STOPPED {
        return Err("pg_trex: analytical engine is unavailable".to_string());
    }
    if state == WORKER_STATE_STARTING {
        return Err("pg_trex: analytical engine is starting, please retry".to_string());
    }

    let (dsm_handle, dsm_seg, req_handle, resp_handle) = create_ipc_channel()?;

    let slot_idx = match acquire_slot(shmem, dsm_handle) {
        Ok(idx) => idx,
        Err(e) => {
            unsafe { pg_sys::dsm_detach(dsm_seg) };
            return Err(e);
        }
    };

    let guard = SlotGuard::new(shmem, slot_idx, dsm_seg);

    write_request(req_handle, sql, flags)?;
    signal_worker_latch(shmem);
    wait_for_completion(shmem, slot_idx)?;

    let final_state = shmem.request_slots[slot_idx].state.load(Ordering::Acquire);
    if final_state == SLOT_CANCELLED {
        guard.release();
        return Err("pg_trex: query was cancelled".to_string());
    }
    let response_bytes = read_response(resp_handle)?;
    guard.release();

    // Response wire format: [4 bytes status][payload]
    if response_bytes.len() < 4 {
        return Err("pg_trex: invalid response from worker (too short)".to_string());
    }

    let status = u32::from_le_bytes([
        response_bytes[0],
        response_bytes[1],
        response_bytes[2],
        response_bytes[3],
    ]);
    let payload = &response_bytes[4..];

    if status == RESPONSE_ERROR || final_state == SLOT_ERROR {
        let msg = String::from_utf8_lossy(payload).to_string();
        return Err(format!("pg_trex: {}", msg));
    }

    if payload.is_empty() {
        return Ok(vec![]);
    }
    let (_schema, batches) = crate::arrow_to_pg::deserialize_arrow_ipc(payload)
        .map_err(|e| format!("pg_trex: {}", e))?;
    Ok(crate::arrow_to_pg::batches_to_structured_rows(&batches))
}

/// Call shm_mq_send with the correct signature for the PostgreSQL version.
/// PG17+ added a `force_flush` parameter. We always set force_flush=true to
/// ensure the write pointer is updated immediately in shared memory, making
/// the data visible to the receiver without delay.
unsafe fn shm_mq_send_compat(
    handle: *mut pg_sys::shm_mq_handle,
    nbytes: usize,
    data: *const std::ffi::c_void,
    nowait: bool,
) -> pg_sys::shm_mq_result::Type {
    #[cfg(any(feature = "pg17", feature = "pg18"))]
    {
        pg_sys::shm_mq_send(handle, nbytes, data, nowait, true)
    }
    #[cfg(not(any(feature = "pg17", feature = "pg18")))]
    {
        pg_sys::shm_mq_send(handle, nbytes, data, nowait)
    }
}

/// Create a DSM segment containing two shm_mq ring buffers: one for the request
/// (backend -> worker) and one for the response (worker -> backend).
fn create_ipc_channel() -> Result<(u32, *mut pg_sys::dsm_segment, *mut pg_sys::shm_mq_handle, *mut pg_sys::shm_mq_handle), String> {
    unsafe {
        let total_size = REQUEST_QUEUE_SIZE + RESPONSE_QUEUE_SIZE;
        let dsm_seg = pg_sys::dsm_create(total_size, 0);
        if dsm_seg.is_null() {
            return Err("pg_trex: failed to create DSM segment".to_string());
        }

        pg_sys::dsm_pin_mapping(dsm_seg);

        let base = pg_sys::dsm_segment_address(dsm_seg) as *mut u8;
        if base.is_null() {
            pg_sys::dsm_detach(dsm_seg);
            return Err("pg_trex: DSM segment has null address".to_string());
        }

        // Layout: [0..REQUEST_QUEUE_SIZE) = request, [REQUEST_QUEUE_SIZE..total) = response
        let req_mq_ptr = base as *mut pg_sys::shm_mq;
        let resp_mq_ptr = base.add(REQUEST_QUEUE_SIZE) as *mut pg_sys::shm_mq;

        let req_mq = pg_sys::shm_mq_create(req_mq_ptr as *mut std::ffi::c_void, REQUEST_QUEUE_SIZE);
        let resp_mq = pg_sys::shm_mq_create(resp_mq_ptr as *mut std::ffi::c_void, RESPONSE_QUEUE_SIZE);

        let my_proc = pg_sys::MyProc;

        pg_sys::shm_mq_set_sender(req_mq, my_proc);
        pg_sys::shm_mq_set_receiver(resp_mq, my_proc);

        let req_handle = pg_sys::shm_mq_attach(req_mq, dsm_seg, std::ptr::null_mut());
        let resp_handle = pg_sys::shm_mq_attach(resp_mq, dsm_seg, std::ptr::null_mut());

        if req_handle.is_null() || resp_handle.is_null() {
            pg_sys::dsm_detach(dsm_seg);
            return Err("pg_trex: failed to attach to shm_mq".to_string());
        }

        let handle = pg_sys::dsm_segment_handle(dsm_seg);

        Ok((handle, dsm_seg, req_handle, resp_handle))
    }
}

/// Scan request slots and atomically claim the first free one via CAS
/// (Free -> Pending), storing the DSM handle and backend PID.
fn acquire_slot(shmem: &PgTrexShmem, dsm_handle: u32) -> Result<usize, String> {
    // Re-check worker state (may have changed since execute_query checked)
    let state = shmem.worker_state.load(Ordering::Acquire);
    if state == WORKER_STATE_STOPPED {
        return Err("pg_trex: analytical engine is unavailable".to_string());
    }
    if state == WORKER_STATE_STARTING {
        return Err("pg_trex: analytical engine is starting, please retry".to_string());
    }

    let my_pid = unsafe { pg_sys::MyProcPid as u32 };

    for idx in 0..MAX_CONCURRENT {
        let slot = &shmem.request_slots[idx];

        match slot.state.compare_exchange(
            SLOT_FREE,
            SLOT_PENDING,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => {
                slot.dsm_handle.store(dsm_handle, Ordering::Release);
                slot.backend_pid.store(my_pid, Ordering::Release);
                return Ok(idx);
            }
            Err(_) => continue,
        }
    }

    Err("pg_trex: too many concurrent analytical queries, please retry".to_string())
}

/// Release a request slot back to Free state.
pub fn release_slot(shmem: &PgTrexShmem, idx: usize) {
    if idx >= MAX_CONCURRENT {
        return;
    }
    let slot = &shmem.request_slots[idx];
    slot.dsm_handle.store(0, Ordering::Release);
    slot.backend_pid.store(0, Ordering::Release);
    slot.state.store(SLOT_FREE, Ordering::Release);
}

/// Write a SQL query into the request shm_mq.
///
/// Wire format: [4 bytes: flags][4 bytes: sql_length][N bytes: UTF-8 SQL]
fn write_request(handle: *mut pg_sys::shm_mq_handle, sql: &str, flags: u32) -> Result<(), String> {
    let sql_bytes = sql.as_bytes();
    let sql_len = sql_bytes.len() as u32;

    let mut msg = Vec::with_capacity(8 + sql_bytes.len());
    msg.extend_from_slice(&flags.to_le_bytes());
    msg.extend_from_slice(&sql_len.to_le_bytes());
    msg.extend_from_slice(sql_bytes);

    unsafe {
        let result = shm_mq_send_compat(
            handle,
            msg.len(),
            msg.as_ptr() as *const std::ffi::c_void,
            false, // nowait = false (blocking send)
        );

        if result != pg_sys::shm_mq_result::SHM_MQ_SUCCESS {
            return Err("pg_trex: failed to write request to shm_mq".to_string());
        }
    }

    Ok(())
}

/// Read the raw response bytes (including status prefix) from the response shm_mq.
fn read_response(handle: *mut pg_sys::shm_mq_handle) -> Result<Vec<u8>, String> {
    unsafe {
        let mut nbytes: usize = 0;
        let mut data: *mut std::ffi::c_void = std::ptr::null_mut();

        let result = pg_sys::shm_mq_receive(
            handle,
            &mut nbytes,
            &mut data,
            false, // nowait = false (blocking receive)
        );

        if result != pg_sys::shm_mq_result::SHM_MQ_SUCCESS {
            return Err("pg_trex: failed to read response from shm_mq".to_string());
        }

        if data.is_null() || nbytes == 0 {
            return Err("pg_trex: empty response from worker".to_string());
        }

        let slice = std::slice::from_raw_parts(data as *const u8, nbytes);
        Ok(slice.to_vec())
    }
}

/// Wait for a slot to reach a terminal state, checking for worker crashes.
///
/// Uses PostgreSQL WaitLatch to sleep between polls. Aborts with an error
/// if the worker transitions to Stopped during execution.
fn wait_for_completion(shmem: &PgTrexShmem, slot_idx: usize) -> Result<(), String> {
    let slot = &shmem.request_slots[slot_idx];

    loop {
        let state = slot.state.load(Ordering::Acquire);
        if state == SLOT_DONE || state == SLOT_ERROR || state == SLOT_CANCELLED {
            return Ok(());
        }

        let worker_state = shmem.worker_state.load(Ordering::Acquire);
        if worker_state == WORKER_STATE_STOPPED {
            return Err(
                "pg_trex: analytical engine crashed during query execution".to_string(),
            );
        }

        unsafe {
            let wl_flags = pg_sys::WL_LATCH_SET
                | pg_sys::WL_TIMEOUT
                | pg_sys::WL_POSTMASTER_DEATH;

            pg_sys::WaitLatch(
                pg_sys::MyLatch,
                wl_flags as std::ffi::c_int,
                100_i64 as std::ffi::c_long, // 100ms poll interval
                pg_sys::PG_WAIT_EXTENSION,
            );

            pg_sys::ResetLatch(pg_sys::MyLatch);
        }

        pgrx::check_for_interrupts!();
    }
}

/// Signal the worker latch to wake it up.
fn signal_worker_latch(shmem: &PgTrexShmem) {
    let latch_ptr = shmem.worker_latch.load(Ordering::Acquire);
    if latch_ptr != 0 {
        unsafe {
            pg_sys::SetLatch(latch_ptr as *mut pg_sys::Latch);
        }
    }
}

/// Signal a backend's process latch by PID to wake it up for response reading.
pub fn signal_backend_latch(backend_pid: u32) {
    if backend_pid == 0 {
        return;
    }
    unsafe {
        let proc_ptr = pg_sys::BackendPidGetProc(backend_pid as i32);
        if !proc_ptr.is_null() {
            let latch = std::ptr::addr_of_mut!((*proc_ptr).procLatch) as *mut pg_sys::Latch;
            pg_sys::SetLatch(latch);
        }
    }
}

/// Worker claims a Pending slot by CAS (Pending -> InProgress).
pub fn claim_slot(shmem: &PgTrexShmem, idx: usize) -> bool {
    if idx >= MAX_CONCURRENT {
        return false;
    }
    let slot = &shmem.request_slots[idx];
    slot.state
        .compare_exchange(
            SLOT_PENDING,
            SLOT_IN_PROGRESS,
            Ordering::AcqRel,
            Ordering::Relaxed,
        )
        .is_ok()
}

/// Worker marks a slot as Done or Error and signals the backend latch.
pub fn complete_slot(shmem: &PgTrexShmem, idx: usize, success: bool) {
    if idx >= MAX_CONCURRENT {
        return;
    }
    let slot = &shmem.request_slots[idx];
    let new_state = if success { SLOT_DONE } else { SLOT_ERROR };
    slot.state.store(new_state, Ordering::Release);

    let backend_pid = slot.backend_pid.load(Ordering::Acquire);
    signal_backend_latch(backend_pid);
}

/// Worker attaches to the DSM segment created by the backend.
/// The worker takes the opposite queue roles: receiver on request, sender on response.
pub fn attach_to_dsm(handle: u32) -> Result<WorkerIpcChannel, String> {
    unsafe {
        let dsm_seg = pg_sys::dsm_attach(handle);
        if dsm_seg.is_null() {
            return Err("pg_trex worker: failed to attach to DSM segment".to_string());
        }

        let base = pg_sys::dsm_segment_address(dsm_seg) as *mut u8;
        if base.is_null() {
            pg_sys::dsm_detach(dsm_seg);
            return Err("pg_trex worker: DSM segment has null address".to_string());
        }

        let req_mq = base as *mut pg_sys::shm_mq;
        let resp_mq = base.add(REQUEST_QUEUE_SIZE) as *mut pg_sys::shm_mq;

        let my_proc = pg_sys::MyProc;

        pg_sys::shm_mq_set_receiver(req_mq, my_proc);
        pg_sys::shm_mq_set_sender(resp_mq, my_proc);

        let req_handle = pg_sys::shm_mq_attach(req_mq, dsm_seg, std::ptr::null_mut());
        let resp_handle = pg_sys::shm_mq_attach(resp_mq, dsm_seg, std::ptr::null_mut());

        if req_handle.is_null() || resp_handle.is_null() {
            pg_sys::dsm_detach(dsm_seg);
            return Err("pg_trex worker: failed to attach to shm_mq".to_string());
        }

        Ok(WorkerIpcChannel {
            dsm_seg,
            request_handle: req_handle,
            response_handle: resp_handle,
        })
    }
}

/// Worker reads the SQL query from the request shm_mq.
///
/// Wire format: [4 bytes: flags][4 bytes: sql_length][N bytes: UTF-8 SQL]
pub fn read_request_from_mq(handle: *mut pg_sys::shm_mq_handle) -> Result<(u32, String), String> {
    unsafe {
        let mut nbytes: usize = 0;
        let mut data: *mut std::ffi::c_void = std::ptr::null_mut();

        let result = pg_sys::shm_mq_receive(
            handle,
            &mut nbytes,
            &mut data,
            false, // nowait = false (blocking)
        );

        if result != pg_sys::shm_mq_result::SHM_MQ_SUCCESS {
            return Err("pg_trex worker: failed to read request from shm_mq".to_string());
        }

        if data.is_null() || nbytes < 8 {
            return Err("pg_trex worker: request message too short".to_string());
        }

        let msg = std::slice::from_raw_parts(data as *const u8, nbytes);

        let flags = u32::from_le_bytes([msg[0], msg[1], msg[2], msg[3]]);
        let sql_len = u32::from_le_bytes([msg[4], msg[5], msg[6], msg[7]]) as usize;

        if msg.len() < 8 + sql_len {
            return Err("pg_trex worker: request message truncated".to_string());
        }

        let sql_bytes = &msg[8..8 + sql_len];
        let sql = String::from_utf8(sql_bytes.to_vec())
            .map_err(|e| format!("pg_trex worker: invalid UTF-8 in SQL: {}", e))?;
        Ok((flags, sql))
    }
}

/// Worker writes a response to the response shm_mq.
///
/// Wire format: [4 bytes: status (0=ok, 1=error)][remaining bytes: Arrow IPC or error message]
pub fn write_response_to_mq(
    handle: *mut pg_sys::shm_mq_handle,
    data: &[u8],
    is_error: bool,
) -> Result<(), String> {
    let status: u32 = if is_error { RESPONSE_ERROR } else { RESPONSE_OK };

    let mut msg = Vec::with_capacity(4 + data.len());
    msg.extend_from_slice(&status.to_le_bytes());
    msg.extend_from_slice(data);

    unsafe {
        let result = shm_mq_send_compat(
            handle,
            msg.len(),
            msg.as_ptr() as *const std::ffi::c_void,
            false, // nowait = false (blocking)
        );

        if result != pg_sys::shm_mq_result::SHM_MQ_SUCCESS {
            return Err("pg_trex worker: failed to write response to shm_mq".to_string());
        }
    }

    Ok(())
}

/// Reset slots left in non-Free states from a previous worker crash.
///
/// Called once at worker startup. Pending/InProgress slots are marked as Error
/// and the backend is signaled. Done/Error/Cancelled slots are freed if the
/// backend has disconnected.
pub fn cleanup_abandoned_slots(shmem: &PgTrexShmem) {
    for idx in 0..MAX_CONCURRENT {
        let slot = &shmem.request_slots[idx];
        let state = slot.state.load(Ordering::Acquire);

        match state {
            SLOT_PENDING | SLOT_IN_PROGRESS => {
                pgrx::warning!(
                    "pg_trex: cleaning up abandoned slot {} (state={})",
                    idx,
                    state
                );

                slot.state.store(SLOT_ERROR, Ordering::Release);

                let backend_pid = slot.backend_pid.load(Ordering::Acquire);
                signal_backend_latch(backend_pid);

                // Free the slot if the backend has already disconnected
                if backend_pid != 0 {
                    unsafe {
                        let proc_ptr = pg_sys::BackendPidGetProc(backend_pid as i32);
                        if proc_ptr.is_null() {
                            slot.dsm_handle.store(0, Ordering::Release);
                            slot.backend_pid.store(0, Ordering::Release);
                            slot.state.store(SLOT_FREE, Ordering::Release);
                        }
                    }
                } else {
                    slot.dsm_handle.store(0, Ordering::Release);
                    slot.state.store(SLOT_FREE, Ordering::Release);
                }
            }
            SLOT_DONE => {
                let backend_pid = slot.backend_pid.load(Ordering::Acquire);
                if backend_pid != 0 {
                    unsafe {
                        let proc_ptr = pg_sys::BackendPidGetProc(backend_pid as i32);
                        if proc_ptr.is_null() {
                            slot.dsm_handle.store(0, Ordering::Release);
                            slot.backend_pid.store(0, Ordering::Release);
                            slot.state.store(SLOT_FREE, Ordering::Release);
                            pgrx::log!("pg_trex: freed orphaned Done slot {}", idx);
                        }
                    }
                } else {
                    slot.dsm_handle.store(0, Ordering::Release);
                    slot.state.store(SLOT_FREE, Ordering::Release);
                }
            }
            SLOT_ERROR => {
                let backend_pid = slot.backend_pid.load(Ordering::Acquire);
                let should_free = if backend_pid != 0 {
                    unsafe {
                        let proc_ptr = pg_sys::BackendPidGetProc(backend_pid as i32);
                        proc_ptr.is_null()
                    }
                } else {
                    true
                };

                if should_free {
                    slot.dsm_handle.store(0, Ordering::Release);
                    slot.backend_pid.store(0, Ordering::Release);
                    slot.state.store(SLOT_FREE, Ordering::Release);
                    pgrx::log!("pg_trex: freed stale Error slot {}", idx);
                }
            }
            SLOT_CANCELLED => {
                slot.dsm_handle.store(0, Ordering::Release);
                slot.backend_pid.store(0, Ordering::Release);
                slot.state.store(SLOT_FREE, Ordering::Release);
                pgrx::log!("pg_trex: freed cancelled slot {}", idx);
            }
            SLOT_FREE => {}

            _ => {
                // Unknown state, reset to Free as a safety measure
                pgrx::warning!(
                    "pg_trex: slot {} in unknown state {}, resetting to Free",
                    idx,
                    state
                );
                slot.dsm_handle.store(0, Ordering::Release);
                slot.backend_pid.store(0, Ordering::Release);
                slot.state.store(SLOT_FREE, Ordering::Release);
            }
        }
    }
}
