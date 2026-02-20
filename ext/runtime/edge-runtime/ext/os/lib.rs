use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;

use deno_core::AsyncRefCell;
use deno_core::OpState;
use deno_core::RcRef;
use deno_core::Resource;
use deno_core::ResourceId;
use deno_core::error::ResourceError;
use deno_core::op2;
use serde::Serialize;

// ── SignalError ─────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error, deno_error::JsError)]
pub enum SignalError {
  #[class(type)]
  #[error(transparent)]
  InvalidSignalStr(#[from] deno_signals::InvalidSignalStrError),
  #[class(type)]
  #[error(transparent)]
  InvalidSignalInt(#[from] deno_signals::InvalidSignalIntError),
  #[class(type)]
  #[error("Binding to signal '{0}' is not allowed")]
  SignalNotAllowed(String),
  #[class(inherit)]
  #[error("{0}")]
  Io(#[from] std::io::Error),
}

// ── ExitCode ────────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
pub struct ExitCode(Arc<AtomicI32>);

impl ExitCode {
  pub fn get(&self) -> i32 {
    self.0.load(Ordering::Relaxed)
  }

  pub fn set(&mut self, code: i32) {
    self.0.store(code, Ordering::Relaxed);
  }
}

// ── Exit ops ────────────────────────────────────────────────────────────────

pub fn exit(code: i32) -> ! {
  deno_signals::run_exit();
  #[allow(clippy::disallowed_methods)]
  std::process::exit(code);
}

#[op2(fast)]
fn op_exit(state: &mut OpState) {
  if let Some(exit_code) = state.try_borrow::<ExitCode>() {
    exit(exit_code.get())
  }
}

#[op2(fast)]
fn op_set_exit_code(state: &mut OpState, #[smi] code: i32) {
  if let Some(exit_code) = state.try_borrow_mut::<ExitCode>() {
    exit_code.set(code);
  }
}

#[op2(fast)]
#[smi]
fn op_get_exit_code(state: &mut OpState) -> i32 {
  state
    .try_borrow::<ExitCode>()
    .map(|e| e.get())
    .unwrap_or_default()
}

// ── Signal ops ──────────────────────────────────────────────────────────────

struct SignalStreamResource {
  signo: i32,
  id: u32,
  rx: AsyncRefCell<tokio::sync::watch::Receiver<()>>,
}

impl Resource for SignalStreamResource {
  fn name(&self) -> std::borrow::Cow<'_, str> {
    "signal".into()
  }

  fn close(self: Rc<Self>) {
    deno_signals::unregister(self.signo, self.id);
  }
}

#[op2(fast)]
#[smi]
fn op_signal_bind(
  state: &mut OpState,
  #[string] sig: &str,
) -> Result<ResourceId, SignalError> {
  let signo = deno_signals::signal_str_to_int(sig)?;
  if deno_signals::is_forbidden(signo) {
    return Err(SignalError::SignalNotAllowed(sig.to_string()));
  }

  let (tx, rx) = tokio::sync::watch::channel(());
  let id = deno_signals::register(
    signo,
    true,
    Box::new(move || {
      let _ = tx.send(());
    }),
  )?;

  let rid = state.resource_table.add(SignalStreamResource {
    signo,
    id,
    rx: AsyncRefCell::new(rx),
  });

  Ok(rid)
}

#[op2(async)]
async fn op_signal_poll(
  state: Rc<RefCell<OpState>>,
  #[smi] rid: ResourceId,
) -> Result<bool, ResourceError> {
  let resource = state
    .borrow_mut()
    .resource_table
    .get::<SignalStreamResource>(rid)?;

  let mut rx = RcRef::map(&resource, |r| &r.rx).borrow_mut().await;

  Ok(rx.changed().await.is_err())
}

#[op2(fast)]
fn op_signal_unbind(
  state: &mut OpState,
  #[smi] rid: ResourceId,
) -> Result<(), ResourceError> {
  let resource = state.resource_table.take::<SignalStreamResource>(rid)?;
  resource.close();
  Ok(())
}

// ── Env stub ops (required by deno_node polyfills) ─────────────────────────

#[op2]
#[string]
fn op_get_env_no_permission_check(#[string] _key: &str) -> Option<String> {
  None
}

// ── Memory info op ──────────────────────────────────────────────────────────

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MemInfo {
  pub total: u64,
  pub free: u64,
  pub available: u64,
  pub buffers: u64,
  pub cached: u64,
  pub swap_total: u64,
  pub swap_free: u64,
}

#[op2]
#[serde]
fn op_system_memory_info() -> Option<MemInfo> {
  #[cfg(any(target_os = "android", target_os = "linux"))]
  {
    let mut mem_info = MemInfo::default();
    let mut info = std::mem::MaybeUninit::uninit();
    // SAFETY: `info` is a valid pointer to a `libc::sysinfo` struct.
    let res = unsafe { libc::sysinfo(info.as_mut_ptr()) };
    if res == 0 {
      // SAFETY: `sysinfo` initializes the struct.
      let info = unsafe { info.assume_init() };
      let mem_unit = info.mem_unit as u64;
      mem_info.swap_total = info.totalswap * mem_unit;
      mem_info.swap_free = info.freeswap * mem_unit;
      mem_info.total = info.totalram * mem_unit;
      mem_info.free = info.freeram * mem_unit;
      mem_info.available = mem_info.free;
      mem_info.buffers = info.bufferram * mem_unit;

      // Prefer /proc/meminfo MemAvailable which accounts for reclaimable
      // page cache/buffers, falling back to freeram above.
      if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
        for line in contents.lines() {
          if let Some(rest) = line.strip_prefix("MemAvailable:") {
            if let Some(kb_str) = rest.trim().strip_suffix("kB")
              && let Ok(kb) = kb_str.trim().parse::<u64>()
            {
              mem_info.available = kb.saturating_mul(1024);
            }
            break;
          }
        }
      }
    }

    Some(mem_info)
  }

  #[cfg(not(any(target_os = "android", target_os = "linux")))]
  {
    Some(MemInfo::default())
  }
}

// ── Extension registration ──────────────────────────────────────────────────

deno_core::extension!(
  os,
  ops = [
    op_system_memory_info,
    op_exit,
    op_set_exit_code,
    op_get_exit_code,
    op_signal_bind,
    op_signal_poll,
    op_signal_unbind,
  ],
  esm_entry_point = "ext:os/os.js",
  esm = ["os.js", "exit.js"],
  options = { exit_code: Option<ExitCode> },
  state = |state, options| {
    state.put::<ExitCode>(options.exit_code.unwrap_or_default());
  }
);

// Facade extension that satisfies `ext:deno_os/30_os.js` imports from upstream
// deno_node polyfills (os.ts, process.ts) with stubbed/mocked implementations.
deno_core::extension!(
  deno_os,
  deps = [os],
  ops = [op_get_env_no_permission_check],
  esm = ["30_os.js"],
);
