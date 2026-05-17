use std::future::pending;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use base_mem_check::MemCheckState;
use base_rt::RuntimeState;
use cpu_timer::CPUTimer;
use deno_core::serde_json;
use deno_core::v8;
use deno_core::InspectorSessionChannels;
use deno_core::InspectorSessionKind;
use deno_core::InspectorSessionProxy;
use enum_as_inner::EnumAsInner;
use ext_event_worker::events::ShutdownEvent;
use ext_event_worker::events::WorkerEvents;
use ext_event_worker::events::WorkerMemoryUsed;
use ext_runtime::PromiseMetrics;
use ext_workers::context::Timing;
use ext_workers::context::UserWorkerMsgs;
use ext_workers::context::UserWorkerRuntimeOpts;
use futures_util::task::AtomicWaker;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::{self};
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::pool::SupervisorPolicy;
use super::termination_token::TerminationToken;

use crate::runtime::DenoRuntime;
use crate::server::ServerFlags;
use crate::utils::units::percentage_value;

pub mod strategy_per_request;
pub mod strategy_per_worker;

pub mod v8_handler;

pub use v8_handler::*;

static NEXT_MSG_ID: AtomicI32 = AtomicI32::new(0);

fn next_msg_id() -> i32 {
  NEXT_MSG_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[repr(C)]
pub struct IsolateMemoryStats {
  pub used_heap_size: usize,
  pub external_memory: usize,
}

pub struct Tokens {
  pub termination: Option<TerminationToken>,
  pub supervise: CancellationToken,
  /// Token that is cancelled when the runtime is being dropped.
  /// Supervisors should check this before calling thread_safe_handle methods.
  pub runtime_drop: CancellationToken,
  /// Lifecycle guard for safe V8 isolate access.
  /// Use `try_enter()` to get a guard before calling thread_safe_handle methods.
  pub isolate_lifecycle: Arc<base_rt::IsolateLifecycle>,
}

pub struct Arguments {
  pub key: Uuid,
  pub runtime_opts: UserWorkerRuntimeOpts,
  pub cpu_usage_metrics_rx: Option<mpsc::UnboundedReceiver<CPUUsageMetrics>>,
  pub supervisor_policy: SupervisorPolicy,
  pub runtime_state: Arc<RuntimeState>,
  pub promise_metrics: PromiseMetrics,
  pub timing: Option<Timing>,
  pub memory_limit_rx: mpsc::UnboundedReceiver<()>,
  pub pool_msg_tx: Option<mpsc::UnboundedSender<UserWorkerMsgs>>,
  pub isolate_memory_usage_tx: oneshot::Sender<IsolateMemoryStats>,
  pub thread_safe_handle: v8::IsolateHandle,
  pub waker: Arc<AtomicWaker>,
  pub tokens: Tokens,
  pub flags: Arc<ServerFlags>,
}

pub struct CPUUsage {
  pub accumulated: i64,
  pub diff: i64,
}

#[derive(EnumAsInner)]
pub enum CPUUsageMetrics {
  Enter(std::thread::ThreadId, CPUTimer),
  Leave(CPUUsage),
}

#[inline]
#[allow(unused)]
fn cpu_budget(conf: &UserWorkerRuntimeOpts) -> u64 {
  conf
    .cpu_time_max_budget_per_task_ms
    .unwrap_or(if cfg!(debug_assertions) {
      conf.cpu_time_soft_limit_ms
    } else {
      1
    })
}

async fn wait_cpu_alarm(
  maybe_alarm: Option<&mut UnboundedReceiver<()>>,
) -> Option<()> {
  match maybe_alarm {
    Some(alarm) => Some(alarm.recv().await?),
    None => None,
  }
}

async fn create_wall_clock_beforeunload_alert(
  wall_clock_limit_ms: u64,
  pct: Option<u8>,
) {
  let dur = pct
    .and_then(|it| percentage_value(wall_clock_limit_ms, it))
    .map(Duration::from_millis);

  if let Some(dur) = dur {
    tokio::time::sleep(dur).await;
  } else {
    pending::<()>().await;
    unreachable!()
  }
}

#[allow(clippy::too_many_arguments)]
pub fn create_supervisor(
  key: Uuid,
  runtime: &mut DenoRuntime,
  policy: SupervisorPolicy,
  termination_event_tx: oneshot::Sender<WorkerEvents>,
  pool_msg_tx: Option<mpsc::UnboundedSender<UserWorkerMsgs>>,
  cpu_usage_metrics_rx: Option<mpsc::UnboundedReceiver<CPUUsageMetrics>>,
  cancel: Option<CancellationToken>,
  timing: Option<Timing>,
  termination_token: Option<TerminationToken>,
  flags: Arc<ServerFlags>,
) -> Result<CancellationToken, anyhow::Error> {
  let (memory_limit_tx, memory_limit_rx) = mpsc::unbounded_channel();
  let (waker, thread_safe_handle) = (
    runtime.waker.clone(),
    runtime.js_runtime.v8_isolate().thread_safe_handle(),
  );

  // we assert supervisor is only run for user workers
  let conf = runtime.conf.as_user_worker().unwrap().clone();
  let mem_check_state = runtime.mem_check_state();
  let termination_request_token = runtime.termination_request_token.clone();
  let runtime_drop_token = runtime.drop_token.clone();

  let giveup_process_requests_token = cancel.clone();
  let supervise_cancel_token = CancellationToken::new();
  let tokens = Tokens {
    termination: termination_token.clone(),
    supervise: supervise_cancel_token.clone(),
    runtime_drop: runtime_drop_token.clone(),
    isolate_lifecycle: runtime.mem_check_lifecycle(),
  };

  let maybe_inspector_params = runtime.inspector().map(|insp| {
    (
      runtime.js_runtime.inspector().get_session_sender(),
      runtime.runtime_state.clone(),
      insp.server.clone(),
      runtime.main_module_url().to_string(),
      insp.generation(),
    )
  });

  let send_memory_limit_fn = move |kind: &'static str| {
    log::debug!("memory limit triggered: isolate: {:?}, kind: {}", key, kind);

    if memory_limit_tx.send(()).is_err() {
      log::error!(
        "failed to send memory limit reached notification(isolate may already be terminating): isolate: {:?}, kind: {}",
        key, kind
      );
    }
  };

  runtime.add_memory_limit_callback({
    let send_fn = send_memory_limit_fn.clone();
    move |_| {
      send_fn("mem_check");
    }
  });

  runtime.js_runtime.add_near_heap_limit_callback({
    let send_fn = send_memory_limit_fn;
    let low_memory_multiplier = conf.low_memory_multiplier;
    move |current, _| {
      send_fn("v8");

      // give an allowance on current limit (until the isolate is
      // terminated) we do this so that oom won't end up killing the
      // edge-runtime process
      current * (low_memory_multiplier as usize)
    }
  });

  drop({
    let _rt_guard = base_rt::SUPERVISOR_RT.enter();
    let supervise_cancel_token_inner = supervise_cancel_token.clone();
    let runtime_state = runtime.runtime_state.clone();
    let promise_metrics = runtime.promise_metrics();

    tokio::spawn(async move {
      let (isolate_memory_usage_tx, isolate_memory_usage_rx) =
        oneshot::channel::<IsolateMemoryStats>();

      let args = Arguments {
        key,
        runtime_opts: (*conf).clone(),
        cpu_usage_metrics_rx,
        supervisor_policy: policy,
        runtime_state,
        promise_metrics,
        timing,
        memory_limit_rx,
        pool_msg_tx,
        isolate_memory_usage_tx,
        thread_safe_handle,
        waker: waker.clone(),
        tokens,
        flags,
      };

      let (reason, cpu_usage_ms) = {
        match policy {
          SupervisorPolicy::PerWorker => {
            strategy_per_worker::supervise(args).await
          }
          SupervisorPolicy::PerRequest { oneshot, .. } => {
            strategy_per_request::supervise(args, oneshot).await
          }
        }
      };

      // NOTE: Sending a signal to the pooler that it is the user worker going
      // disposed down and will not accept awaiting subsequent requests, so
      // they must be re-polled again.
      if let Some(cancel) = giveup_process_requests_token.as_ref() {
        cancel.cancel();
      }

      if let Some((
        session_tx,
        state,
        inspector_server,
        module_url,
        inspector_generation,
      )) = maybe_inspector_params
      {
        use deno_core::futures::channel::mpsc;

        // First, tell the inspector server to tear down any attached
        // DevTools WebSocket for this worker. The worker thread is often
        // stuck inside V8 after a supervisor kill (terminate_execution
        // doesn't always yield), so the normal runtime-drop deregister path
        // won't fire in time. Driving the close from here keeps DevTools
        // from hanging on a half-open socket.
        //
        // The generation pairs this disconnect with our specific registration:
        // if a new worker for the same module URL re-registers before the
        // server processes this signal, the server will ignore it (same v5
        // UUID, but a higher generation).
        inspector_server
          .force_disconnect_url(&module_url, inspector_generation);

        let termination_request_token = termination_request_token.clone();

        // Inline the inspector unblock directly on this supervisor task.
        // The previous shape — `SUPERVISOR_RT.spawn_blocking(move ||
        // SUPERVISOR_RT.block_on(...))` — re-entered the runtime from a
        // blocking thread and could deadlock if blocking threads were
        // saturated. None of the work below is `!Send`: the captured types
        // are `UnboundedSender<InspectorSessionProxy>`, `RuntimeState`
        // (`Arc`-based), `CancellationToken`, `String`, `u64`, and
        // `tokio::sync::Mutex` guards — all `Send`. An inline await on the
        // current task is sufficient. If you add a new captured value, make
        // sure it stays `Send` or this whole task will silently regress to a
        // single-threaded executor (or fail to compile under `tokio::spawn`).
        let cleanup = async move {
          if state.is_terminated() || termination_request_token.is_cancelled() {
            return;
          }

          termination_request_token.cancel();

          if state.is_found_inspector_session() {
            return;
          }

          let (outbound_tx, _outbound_rx) = mpsc::unbounded();
          let (inbound_tx, inbound_rx) = mpsc::unbounded();

          if session_tx
            .unbounded_send(InspectorSessionProxy {
              channels: InspectorSessionChannels::Regular {
                tx: outbound_tx,
                rx: inbound_rx,
              },
              kind: InspectorSessionKind::Blocking,
            })
            .is_err()
          {
            return;
          }

          // In the new V8/deno_core API, LocalInspectorSession is created by
          // JsRuntimeInspector::create_local_session and requires a
          // SessionContainer. Since we're outside the runtime and just need
          // to send CDP messages, we send directly through the inbound
          // channel instead.
          let inbound_tx = Arc::new(Mutex::new(inbound_tx));

          let send_msg_fn = |msg: &str| {
            let state = state.clone();
            let inbound_tx = inbound_tx.clone();
            let msg_id = next_msg_id();
            let msg = msg.to_string();
            async move {
              let inbound_tx = inbound_tx.lock().await;
              let message = serde_json::json!({
                "id": msg_id,
                "method": msg,
                "params": serde_json::Value::Null,
              });
              let _ = inbound_tx
                .unbounded_send(serde_json::to_string(&message).unwrap());

              // Give V8 a moment to process the message, but bound the wait
              // so a stuck isolate can't pin the supervisor here forever.
              let mut int = tokio::time::interval(Duration::from_millis(61));
              let deadline = tokio::time::sleep(Duration::from_millis(500));
              tokio::pin!(deadline);
              loop {
                tokio::select! {
                  _ = int.tick() => if state.is_terminated() { break; },
                  _ = &mut deadline => break,
                }
              }
            }
          };

          send_msg_fn("Debugger.enable").await;
          send_msg_fn("Runtime.runIfWaitingForDebugger").await;
        };

        // Hard cap so misbehaving v8 state can't hang the supervisor.
        let _ =
          tokio::time::timeout(Duration::from_millis(1500), cleanup).await;
      }

      // NOTE: If we issue a hard CPU time limit, It's OK because it is
      // still possible the worker's context is in the v8 event loop. The
      // interrupt callback would be invoked from the V8 engine
      // gracefully. But some case doesn't.
      //
      // Such as the worker going to a retired state due to the soft CPU
      // time limit but not hitting the hard CPU time limit. In this case,
      // we must wake up the worker's event loop manually. Otherwise, the
      // supervisor has to wait until the wall clock future that we placed
      // out on the runtime side is times out.
      waker.wake();

      let memory_report = tokio::select! {
        report = isolate_memory_usage_rx => report.map_err(anyhow::Error::from),
        _ = runtime_drop_token.cancelled() => Err(
          anyhow!("termination requested"
        ))
      };

      let memory_used = match memory_report {
        Ok(v) => WorkerMemoryUsed {
          total: v.used_heap_size + v.external_memory,
          heap: v.used_heap_size,
          external: v.external_memory,
          mem_check_captured: tokio::task::spawn_blocking(move || {
            *mem_check_state.read().unwrap()
          })
          .await
          .unwrap(),
        },

        Err(_) => {
          if !supervise_cancel_token_inner.is_cancelled() {
            log::warn!("isolate memory usage sender dropped");
          }

          WorkerMemoryUsed {
            total: 0,
            heap: 0,
            external: 0,
            mem_check_captured: MemCheckState::default(),
          }
        }
      };

      if !termination_request_token.is_cancelled() {
        termination_request_token.cancel();
        waker.wake();
      }

      // send termination reason
      let termination_event = WorkerEvents::Shutdown(ShutdownEvent {
        reason,
        memory_used,
        cpu_time_used: cpu_usage_ms as usize,
      });

      let _ = termination_event_tx.send(termination_event);
    })
  });

  Ok(supervise_cancel_token)
}
