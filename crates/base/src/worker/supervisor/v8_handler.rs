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

pub type RawInterruptCallback =
  extern "C" fn(isolate: *mut v8::Isolate, data: *mut std::ffi::c_void);

pub type RefInterruptCallback =
  extern "C" fn(isolate: &mut v8::Isolate, data: *mut std::ffi::c_void);

#[inline]
pub fn as_interrupt_callback(f: RawInterruptCallback) -> RefInterruptCallback {
  // SAFETY: *mut T and &mut T have identical ABI representation.
  unsafe { std::mem::transmute(f) }
}

#[repr(C)]
pub struct V8HandleTerminationData {
  pub should_terminate: bool,
  pub isolate_memory_usage_tx: Option<oneshot::Sender<IsolateMemoryStats>>,
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn v8_handle_termination_raw(
  isolate_ptr: *mut v8::Isolate,
  data: *mut std::ffi::c_void,
) {
  let mut data = unsafe { Box::from_raw(data as *mut V8HandleTerminationData) };

  // Use black_box to prevent LLVM from optimizing away the null check
  // by back-propagating the nonnull attribute from the &mut reference below.
  let isolate_ptr = std::hint::black_box(isolate_ptr);
  if isolate_ptr.is_null() {
    drop(data.isolate_memory_usage_tx.take());
    return;
  }

  let isolate = unsafe { &mut *isolate_ptr };

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

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn v8_handle_beforeunload_raw(
  _isolate_ptr: *mut v8::Isolate,
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

pub extern "C" fn v8_handle_early_drop_beforeunload_raw(
  _isolate_ptr: *mut v8::Isolate,
  data: *mut std::ffi::c_void,
) {
  let data = unsafe { Box::from_raw(data as *mut V8HandleEarlyDropData) };
  data.token.cancel();
}

#[instrument(level = "debug", skip_all)]
pub extern "C" fn v8_handle_early_retire_raw(
  _isolate_ptr: *mut v8::Isolate,
  _data: *mut std::ffi::c_void,
) {
  debug!("early retire signal received");
}

#[repr(C)]
pub struct V8HandleDrainData {
  pub runtime_drop_token: CancellationToken,
  pub runtime_state: Arc<RuntimeState>,
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[instrument(level = "debug", skip_all)]
pub extern "C" fn v8_handle_drain_raw(
  _isolate_ptr: *mut v8::Isolate,
  data: *mut std::ffi::c_void,
) {
  let data = unsafe { Box::from_raw(data as *mut V8HandleDrainData) };

  if data.runtime_drop_token.is_cancelled() {
    return;
  }
  data.runtime_state.drain_triggered.raise();
}
