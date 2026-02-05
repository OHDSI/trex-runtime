use std::future::ready;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::Error;
use base_rt::error::CloneableError;
use deno_core::unsync::MaskFutureAsSend;
use ext_event_worker::events::EventMetadata;
use ext_event_worker::events::ShutdownEvent;
use ext_event_worker::events::ShutdownReason;
use ext_event_worker::events::UncaughtExceptionEvent;
use ext_event_worker::events::WorkerEventWithMetadata;
use ext_event_worker::events::WorkerEvents;
use ext_event_worker::events::WorkerMemoryUsed;
use ext_runtime::MetricSource;
use ext_runtime::RuntimeMetricSource;
use ext_runtime::WorkerMetricSource;
use ext_workers::context::UserWorkerMsgs;
use ext_workers::context::WorkerContextInitOpts;
use ext_workers::context::WorkerExit;
use ext_workers::context::WorkerExitStatus;
use ext_workers::context::WorkerKind;
use ext_workers::context::WorkerRequestMsg;
use futures_util::FutureExt;
use log::debug;
use log::error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::debug_span;
use tracing::Instrument;
use uuid::Uuid;

use crate::inspector_server::Inspector;
use crate::runtime::DenoRuntime;
use crate::server::ServerFlags;
use crate::worker::utils::apply_source_maps;
use crate::worker::utils::get_event_metadata;
use crate::worker::utils::send_event_if_event_worker_available;
use crate::worker::utils::translate_vfs_paths;

use super::driver::WorkerDriver;
use super::driver::WorkerDriverImpl;
use super::pool::SupervisorPolicy;
use super::termination_token::TerminationToken;

pub use base_rt::DuplexStreamEntry;

pub struct WorkerCx {
  pub flags: Arc<ServerFlags>,
  pub worker_boot_start_time: Instant,
  pub events_msg_tx: Option<mpsc::UnboundedSender<WorkerEventWithMetadata>>,
  pub pool_msg_tx: Option<mpsc::UnboundedSender<UserWorkerMsgs>>,
  pub cancel: Option<CancellationToken>,
  pub event_metadata: EventMetadata,
  pub worker_key: Option<Uuid>,
  pub inspector: Option<Inspector>,
  pub supervisor_policy: SupervisorPolicy,
  pub worker_name: String,
  pub worker_kind: WorkerKind,
  pub termination_token: Option<TerminationToken>,
}

pub struct WorkerBuilder {
  init_opts: WorkerContextInitOpts,
  flags: Arc<ServerFlags>,
  inspector: Option<Inspector>,
  supervisor_policy: Option<SupervisorPolicy>,
  termination_token: Option<TerminationToken>,
  worker_naming_fn: Option<Box<dyn Fn(Option<Uuid>) -> String + Send>>,
}

impl WorkerBuilder {
  pub fn new(
    init_opts: WorkerContextInitOpts,
    flags: Arc<ServerFlags>,
  ) -> Self {
    Self {
      init_opts,
      flags,
      inspector: None,
      supervisor_policy: None,
      termination_token: None,
      worker_naming_fn: None,
    }
  }

  pub fn inspector(mut self, value: Inspector) -> Self {
    self.inspector = Some(value);
    self
  }

  pub fn supervisor_policy(mut self, value: SupervisorPolicy) -> Self {
    self.supervisor_policy = Some(value);
    self
  }

  pub fn termination_token(mut self, value: TerminationToken) -> Self {
    self.termination_token = Some(value);
    self
  }

  pub fn worker_naming_fn<F>(mut self, value: F) -> Self
  where
    F: Fn(Option<Uuid>) -> String + Send + 'static,
  {
    self.worker_naming_fn = Some(Box::new(value) as _);
    self
  }

  pub fn set_inspector(&mut self, value: Option<Inspector>) -> &mut Self {
    self.inspector = value;
    self
  }

  pub fn set_supervisor_policy(
    &mut self,
    value: Option<SupervisorPolicy>,
  ) -> &mut Self {
    self.supervisor_policy = value;
    self
  }

  pub fn set_termination_token(
    &mut self,
    value: Option<TerminationToken>,
  ) -> &mut Self {
    self.termination_token = value;
    self
  }

  pub fn set_worker_naming_fn<F>(&mut self, value: Option<F>) -> &mut Self
  where
    F: Fn(Option<Uuid>) -> String + Send + 'static,
  {
    self.worker_naming_fn = value.map(|it| Box::new(it) as _);
    self
  }

  pub(crate) fn build(self) -> Result<Worker, Error> {
    let Self {
      mut init_opts,
      flags,
      inspector,
      supervisor_policy,
      termination_token,
      worker_naming_fn,
    } = self;

    let conf = &init_opts.conf;
    let worker_kind = conf.to_worker_kind();
    let worker_naming_fn =
      worker_naming_fn.unwrap_or(Box::new(|uuid| match worker_kind {
        WorkerKind::MainWorker => "main-worker".to_string(),
        WorkerKind::EventsWorker => "events-workers".to_string(),
        WorkerKind::UserWorker => uuid
          .map(|it| format!("sb-iso-{:?}", it))
          .unwrap_or("isolate-worker-unknown".to_string()),
      }));

    let worker_key = conf.as_user_worker().and_then(|it| it.key);
    let worker_name = worker_naming_fn(worker_key);
    let worker_cancel_token =
      conf.as_user_worker().and_then(|it| it.cancel.clone());
    let worker_pool_msg_tx =
      conf.as_user_worker().and_then(|it| it.pool_msg_tx.clone());
    let worker_events_msg_tx = conf
      .as_user_worker()
      .and_then(|it| it.events_msg_tx.clone());

    let cx = Arc::new(WorkerCx {
      flags,
      worker_boot_start_time: Instant::now(),
      events_msg_tx: worker_events_msg_tx,
      pool_msg_tx: worker_pool_msg_tx,
      cancel: worker_cancel_token,
      event_metadata: get_event_metadata(conf),
      worker_key,
      supervisor_policy: supervisor_policy.unwrap_or_default(),
      inspector,
      worker_name,
      worker_kind,
      termination_token,
    });

    let imp = WorkerDriverImpl::new(&mut init_opts, cx.clone());

    Ok(Worker {
      imp,
      cx,
      init_opts: Some(init_opts),
    })
  }
}

pub(crate) struct Worker {
  pub(crate) imp: WorkerDriverImpl,
  pub(crate) cx: Arc<WorkerCx>,
  pub(crate) init_opts: Option<WorkerContextInitOpts>,
}

impl std::ops::Deref for Worker {
  type Target = WorkerCx;

  fn deref(&self) -> &Self::Target {
    &self.cx
  }
}

impl Worker {
  /// Start the worker on a dedicated thread (user workers) or thread pool (main/event workers).
  /// Returns WorkerThreadHandles for user workers, None for main/event workers.
  pub async fn start(
    self,
    eager_module_init: bool,
    booter_signal: oneshot::Sender<
      Result<(MetricSource, CancellationToken), Error>,
    >,
    exit: WorkerExit,
  ) -> Option<WorkerThreadHandles> {
    let worker_name = self.worker_name.clone();
    let worker_key = self.worker_key;
    let event_metadata = self.event_metadata.clone();

    let events_msg_tx = self.events_msg_tx.clone();
    let pool_msg_tx = self.pool_msg_tx.clone();

    let imp = self.imp.clone();
    let cx = self.cx.clone();
    let worker_kind = cx.worker_kind;

    // For user workers, use dedicated thread spawning
    // For main/event workers, use the existing thread pool approach
    if worker_kind.is_user_worker() {
      return self
        .start_on_dedicated_thread(
          eager_module_init,
          booter_signal,
          exit,
          worker_name,
          worker_key,
          Some(event_metadata.clone()),
          events_msg_tx.clone(),
          pool_msg_tx.clone(),
        )
        .await;
    }

    let rt = imp.runtime_handle();
    let boot_service_path = event_metadata.service_path.clone();
    let worker_fut = async move {
      Some(
        rt.spawn_pinned(move || async move {
          // Create the Deno runtime (without module init yet)
          let mut new_runtime = match DenoRuntime::new(self).await {
            Ok(v) => v,
            Err(err) => {
              let err_msg = apply_source_maps(&format!("{err:#}"));
              let err_msg = translate_vfs_paths(&err_msg, boot_service_path.as_deref());
              let err = CloneableError::from(anyhow::anyhow!("{}", err_msg).context("worker boot error"));
              let _ = booter_signal.send(Err(err.clone().into()));

              return imp.on_boot_error(err.into()).await;
            }
          };

          let metric_src = {
            let metric_src =
              WorkerMetricSource::from_js_runtime(&mut new_runtime.js_runtime);

            if let Some(opts) = new_runtime.conf.as_main_worker().cloned() {
              let state = new_runtime.js_runtime.op_state();
              let mut state_mut = state.borrow_mut();
              let metric_src = RuntimeMetricSource::new(
                metric_src.clone(),
                opts
                  .event_worker_metric_src
                  .and_then(|it| it.into_worker().ok()),
                opts.shared_metric_src,
              );

              state_mut.put(metric_src.clone());
              MetricSource::Runtime(metric_src)
            } else {
              MetricSource::Worker(metric_src)
            }
          };

          let _ =
            booter_signal.send(Ok((metric_src, new_runtime.drop_token.clone())));

          let span = debug_span!(
            "poll",
            thread = ?std::thread::current().id(),
          );

          // IMPORTANT: Set up supervisor BEFORE init_main_module so CPU time tracking
          // is active during module evaluation. This is critical for enforcing CPU
          // limits on slow code that runs at module load time.
          let supervise_fut = match imp.clone().supervise(&mut new_runtime) {
            Some(v) => v.boxed(),
            None if worker_kind.is_user_worker() => return Ok(WorkerEvents::Shutdown(ShutdownEvent {
                reason: ShutdownReason::EarlyDrop,
                cpu_time_used: 0,
                memory_used: WorkerMemoryUsed {
                    total: 0,
                    heap: 0,
                    external: 0,
                    mem_check_captured: Default::default(),
                }
            })),
            None => ready(Ok(())).boxed(),
          };

          // Now initialize the main module with CPU tracking active
          if eager_module_init {
            if let Err(err) = new_runtime.init_main_module().await {
              let err_msg = apply_source_maps(&format!("{err:#}"));
              let err_msg = translate_vfs_paths(&err_msg, boot_service_path.as_deref());
              let err = CloneableError::from(anyhow::anyhow!("{}", err_msg).context("worker boot error"));
              // Cancel the supervisor since we're exiting
              new_runtime.drop_token.cancel();
              let _ = supervise_fut.await;
              return imp.on_boot_error(err.into()).await;
            }
          }

          let _guard = scopeguard::guard((), |_| {
            if let Some((key, tx)) = worker_key.zip(pool_msg_tx) {
              if let Err(err) = tx.send(UserWorkerMsgs::Shutdown(key)) {
                error!(
                  "failed to send the shutdown signal to user worker pool: {:?}",
                  err
                );
              }
            }
          });

          let service_path = new_runtime.conf.as_user_worker()
            .and_then(|u| u.service_path.clone());

          async move {
            let result = imp.on_created(&mut new_runtime).await;
            let maybe_uncaught_exception_event = match result.as_ref() {
              Ok(WorkerEvents::UncaughtException(ev)) => Some(ev.clone()),
              Err(err) => {
                let exception = apply_source_maps(&err.to_string());
                let exception = translate_vfs_paths(&exception, service_path.as_deref());
                Some(UncaughtExceptionEvent {
                  cpu_time_used: 0,
                  exception,
                })
              },

              _ => None,
            };

            if let Some(ev) = maybe_uncaught_exception_event {
              exit.set(WorkerExitStatus::WithUncaughtException(ev)).await;
            }

            // Cancel drop_token to signal supervisors to stop, then wait for them
            // BEFORE dropping the runtime to avoid V8 access during disposal.
            new_runtime.drop_token.cancel();
            let _ = supervise_fut.await;
            drop(new_runtime);

            result
          }
          .instrument(span)
          .await
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(|it| it),
      )
    };
    let worker_result_fut = {
      let event_metadata = event_metadata.clone();
      async move {
        let Some(result) = worker_fut.await else {
          return;
        };

        match result {
          Ok(event) => {
            match event {
              WorkerEvents::Shutdown(ShutdownEvent {
                cpu_time_used, ..
              })
              | WorkerEvents::UncaughtException(UncaughtExceptionEvent {
                cpu_time_used,
                ..
              }) => {
                debug!("CPU time used: {:?}ms", cpu_time_used);
              }

              _ => {}
            };

            send_event_if_event_worker_available(
              events_msg_tx.as_ref(),
              event,
              event_metadata,
            );
          }

          Err(err) => error!("unexpected worker error {}", err),
        };
      }
    }
    .instrument(debug_span!(
      "worker",
      id = worker_name.as_str(),
      kind = %worker_kind,
      metadata = ?event_metadata
    ));

    // Main/event workers use the existing thread pool approach
    drop(tokio::spawn(unsafe {
      MaskFutureAsSend::new(worker_result_fut)
    }));
    None
  }

  /// Start a user worker on a dedicated OS thread.
  /// This eliminates the need for v8::Locker by ensuring the isolate never migrates threads.
  #[allow(clippy::too_many_arguments)]
  async fn start_on_dedicated_thread(
    self,
    eager_module_init: bool,
    booter_signal: oneshot::Sender<
      Result<(MetricSource, CancellationToken), Error>,
    >,
    exit: WorkerExit,
    worker_name: String,
    worker_key: Option<Uuid>,
    event_metadata: Option<EventMetadata>,
    events_msg_tx: Option<mpsc::UnboundedSender<WorkerEventWithMetadata>>,
    pool_msg_tx: Option<mpsc::UnboundedSender<UserWorkerMsgs>>,
  ) -> Option<WorkerThreadHandles> {
    use crate::runtime::thread_utils;

    let (isolate_handle_tx, isolate_handle_rx) = oneshot::channel();

    // Clone imp before moving self into the thread
    let imp_for_supervise = self.imp.clone();

    // V8 requires substantial stack space for complex operations like npm module loading.
    // The default stack size (512KB on macOS) is insufficient and causes SIGBUS/SIGSEGV
    // crashes in v8threads.cc during thread initialization.
    // 8MB is the same as the default main thread stack size on macOS and matches
    // what Deno uses for its worker threads.
    const WORKER_STACK_SIZE: usize = 8 * 1024 * 1024; // 8MB

    let boot_service_path =
      event_metadata.as_ref().and_then(|m| m.service_path.clone());
    let thread_result = std::thread::Builder::new()
      .name(format!("user-worker-{}", worker_name))
      .stack_size(WORKER_STACK_SIZE)
      .spawn(move || {
        // Create current-thread runtime on THIS dedicated thread
        let rt = thread_utils::create_current_thread_runtime(&worker_name)?;

        // Execute worker logic on the dedicated thread using LocalSet
        thread_utils::block_on_local(&rt, async move {
          // Create the Deno runtime (without module init yet)
          let mut new_runtime = match DenoRuntime::new(self).await {
            Ok(v) => v,
            Err(err) => {
              let err_msg = apply_source_maps(&format!("{err:#}"));
              let err_msg = translate_vfs_paths(&err_msg, boot_service_path.as_deref());
              let err = CloneableError::from(anyhow::anyhow!("{}", err_msg).context("worker boot error"));
              let _ = booter_signal.send(Err(err.clone().into()));
              return Err(err.into());
            }
          };

          // Extract the isolate handle before starting the event loop
          let isolate_handle = new_runtime.js_runtime.v8_isolate().thread_safe_handle();

          // Send the isolate handle back to the parent thread
          if isolate_handle_tx.send(isolate_handle).is_err() {
            return Err(anyhow::anyhow!("failed to send isolate handle to parent thread"));
          }

          // Continue with normal worker initialization
          let metric_src = {
            let metric_src =
              WorkerMetricSource::from_js_runtime(&mut new_runtime.js_runtime);
            MetricSource::Worker(metric_src)
          };

          let _ = booter_signal.send(Ok((metric_src, new_runtime.drop_token.clone())));

          let span = debug_span!(
            "poll",
            thread = ?std::thread::current().id(),
          );

          // IMPORTANT: Set up supervisor BEFORE init_main_module so CPU time tracking
          // is active during module evaluation. This is critical for enforcing CPU
          // limits on slow code that runs at module load time.
          let supervise_fut = match imp_for_supervise.clone().supervise(&mut new_runtime) {
            Some(v) => v.boxed(),
            None => return Ok(()),
          };

          // Now initialize the main module with CPU tracking active
          if eager_module_init {
            if let Err(err) = new_runtime.init_main_module().await {
              let err_msg = apply_source_maps(&format!("{err:#}"));
              let err_msg = translate_vfs_paths(&err_msg, boot_service_path.as_deref());
              let err = CloneableError::from(anyhow::anyhow!("{}", err_msg).context("worker boot error"));
              // Cancel the supervisor since we're exiting
              new_runtime.drop_token.cancel();
              let _ = supervise_fut.await;
              return Err(err.into());
            }
          }

          let _guard = scopeguard::guard((), |_| {
            if let Some((key, tx)) = worker_key.zip(pool_msg_tx.clone()) {
              if let Err(err) = tx.send(UserWorkerMsgs::Shutdown(key)) {
                error!(
                  "failed to send the shutdown signal to user worker pool: {:?}",
                  err
                );
              }
            }
          });

          let service_path = new_runtime.conf.as_user_worker()
            .and_then(|u| u.service_path.clone());

          let worker_poll_fut = async move {
            let result = imp_for_supervise.on_created(&mut new_runtime).await;
            let maybe_uncaught_exception_event = match result.as_ref() {
              Ok(WorkerEvents::UncaughtException(ev)) => Some(ev.clone()),
              Err(err) => {
                let exception = apply_source_maps(&err.to_string());
                let exception = translate_vfs_paths(&exception, service_path.as_deref());
                Some(UncaughtExceptionEvent {
                  cpu_time_used: 0,
                  exception,
                })
              },
              _ => None,
            };

            if let Some(ev) = maybe_uncaught_exception_event {
              exit.set(WorkerExitStatus::WithUncaughtException(ev)).await;
            }

            // Cancel drop_token to signal supervisors to stop, then wait for them
            // BEFORE dropping the runtime to avoid V8 access during disposal.
            new_runtime.drop_token.cancel();
            let _ = supervise_fut.await;
            drop(new_runtime);

            result
          }
          .instrument(span);

          // Run the worker on the local thread
          let result = tokio::task::spawn_local(worker_poll_fut)
            .await
            .map_err(anyhow::Error::from)?;

          // Handle the result
          match result {
            Ok(event) => {
              match event {
                WorkerEvents::Shutdown(ShutdownEvent { cpu_time_used, .. })
                | WorkerEvents::UncaughtException(UncaughtExceptionEvent {
                  cpu_time_used,
                  ..
                }) => {
                  debug!("CPU time used: {:?}ms", cpu_time_used);
                }
                _ => {}
              };

              if let Some(metadata) = event_metadata {
                send_event_if_event_worker_available(
                  events_msg_tx.as_ref(),
                  event,
                  metadata,
                );
              }
              Ok(())
            }
            Err(err) => {
              error!("unexpected worker error {}", err);
              Err(err)
            }
          }
        })
      });

    match thread_result {
      Ok(join_handle) => {
        // Wait for the isolate handle to be sent back (async)
        match isolate_handle_rx.await {
          Ok(isolate_handle) => Some(WorkerThreadHandles {
            thread_handle: join_handle,
            isolate_handle,
          }),
          Err(_) => {
            // The worker failed to boot and didn't send an isolate handle.
            // We must join the thread to properly clean up resources and avoid
            // orphaned threads that could cause resource exhaustion or crashes.
            error!("failed to receive isolate handle from worker thread");

            // Spawn a blocking task to join the thread so we don't block the async runtime.
            // The thread should exit quickly since it already failed during boot.
            tokio::task::spawn_blocking(move || {
              match join_handle.join() {
                Ok(Ok(())) => {
                  debug!("worker thread exited cleanly after boot failure");
                }
                Ok(Err(err)) => {
                  // This is expected - the worker boot error was already sent via booter_signal
                  debug!(
                    "worker thread exited with error after boot failure: {}",
                    err
                  );
                }
                Err(_panic) => {
                  error!("worker thread panicked during boot failure cleanup");
                }
              }
            });

            None
          }
        }
      }
      Err(err) => {
        error!("failed to spawn worker thread: {}", err);
        None
      }
    }
  }
}

#[derive(Debug, Clone)]
pub struct WorkerSurface {
  pub metric: MetricSource,
  pub msg_tx: mpsc::UnboundedSender<WorkerRequestMsg>,
  pub exit: WorkerExit,
  pub cancel: CancellationToken,
  /// Thread handles for user workers running on dedicated threads.
  /// Wrapped in Arc<Mutex<Option<>>> to allow Clone while storing non-Clone handles.
  /// The Option will be taken when extracting handles into UserWorkerProfile.
  pub thread_handles: Arc<Mutex<Option<WorkerThreadHandles>>>,
}

/// Handles for a worker running on a dedicated thread.
/// Only populated for user workers; None for main/event workers.
#[derive(Debug)]
pub struct WorkerThreadHandles {
  pub thread_handle: std::thread::JoinHandle<Result<(), Error>>,
  pub isolate_handle: deno_core::v8::IsolateHandle,
}
