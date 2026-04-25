//! V8 interrupt callback handlers.

use std::sync::Arc;

use base_rt::RuntimeState;
use deno_core::v8;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::instrument;

use crate::runtime::WillTerminateReason;

use super::IsolateMemoryStats;

pub type RawInterruptCallback = unsafe extern "C" fn(
  isolate: v8::UnsafeRawIsolatePtr,
  data: *mut std::ffi::c_void,
);

#[inline]
pub fn as_interrupt_callback(f: RawInterruptCallback) -> RawInterruptCallback {
  f
}

#[repr(C)]
pub struct V8HandleTerminationData {
  pub should_terminate: bool,
  pub isolate_memory_usage_tx: Option<oneshot::Sender<IsolateMemoryStats>>,
}

/// # Safety
/// `data` must be a `Box::into_raw` pointer to `V8HandleTerminationData`.
/// `isolate_ptr` is provided by V8 and must be valid for the duration of the
/// callback (or null when V8 is tearing the isolate down).
pub unsafe extern "C" fn v8_handle_termination_raw(
  isolate_ptr: v8::UnsafeRawIsolatePtr,
  data: *mut std::ffi::c_void,
) {
  let mut data = unsafe { Box::from_raw(data as *mut V8HandleTerminationData) };

  if isolate_ptr.is_null() {
    drop(data.isolate_memory_usage_tx.take());
    return;
  }

  // SAFETY: V8 guarantees the pointer is valid for the duration of the
  // interrupt callback. We reconstruct an Isolate handle from the raw ptr
  // without taking ownership — dropping it would dispose the isolate.
  let isolate = std::mem::ManuallyDrop::new(unsafe {
    v8::Isolate::from_raw_isolate_ptr(isolate_ptr)
  });

  if data.should_terminate {
    isolate.terminate_execution();
  }

  drop(data.isolate_memory_usage_tx.take());
}

#[repr(C)]
pub struct V8HandleBeforeunloadData {
  pub reason: WillTerminateReason,
  pub runtime_drop_token: CancellationToken,
  pub runtime_state: Arc<RuntimeState>,
}

/// # Safety
/// `data` must be a `Box::into_raw` pointer to `V8HandleBeforeunloadData`.
pub unsafe extern "C" fn v8_handle_beforeunload_raw(
  _isolate_ptr: v8::UnsafeRawIsolatePtr,
  data: *mut std::ffi::c_void,
) {
  let data = unsafe { Box::from_raw(data as *mut V8HandleBeforeunloadData) };

  if data.runtime_drop_token.is_cancelled() {
    return;
  }
  data.runtime_state.wall_clock_beforeunload_triggered.raise();
}

#[repr(C)]
pub struct V8HandleEarlyDropData {
  pub token: CancellationToken,
}

/// # Safety
/// `data` must be a `Box::into_raw` pointer to `V8HandleEarlyDropData`.
pub unsafe extern "C" fn v8_handle_early_drop_beforeunload_raw(
  _isolate_ptr: v8::UnsafeRawIsolatePtr,
  data: *mut std::ffi::c_void,
) {
  let data = unsafe { Box::from_raw(data as *mut V8HandleEarlyDropData) };
  data.token.cancel();
}

/// # Safety
/// Invoked by V8 as an interrupt callback; `_data` is unused.
#[instrument(level = "debug", skip_all)]
pub unsafe extern "C" fn v8_handle_early_retire_raw(
  _isolate_ptr: v8::UnsafeRawIsolatePtr,
  _data: *mut std::ffi::c_void,
) {
  debug!("early retire signal received");
}

#[repr(C)]
pub struct V8HandleDrainData {
  pub runtime_drop_token: CancellationToken,
  pub runtime_state: Arc<RuntimeState>,
}

/// # Safety
/// `data` must be a `Box::into_raw` pointer to `V8HandleDrainData`.
#[instrument(level = "debug", skip_all)]
pub unsafe extern "C" fn v8_handle_drain_raw(
  _isolate_ptr: v8::UnsafeRawIsolatePtr,
  data: *mut std::ffi::c_void,
) {
  let data = unsafe { Box::from_raw(data as *mut V8HandleDrainData) };

  if data.runtime_drop_token.is_cancelled() {
    return;
  }
  data.runtime_state.drain_triggered.raise();
}
