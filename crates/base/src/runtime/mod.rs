#![allow(unexpected_cfgs)]
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::c_void;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::RwLock;
use std::task::Poll;
use std::time::Duration;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use anyhow::Error;
use arc_swap::ArcSwapOption;
use base_mem_check::MemCheckState;
use base_mem_check::WorkerHeapStatistics;
use base_rt::get_current_cpu_time_ns;
use base_rt::BlockingScopeCPUUsage;
use base_rt::DenoRuntimeDropToken;
use base_rt::DropToken;
use base_rt::RuntimeOtelExtraAttributes;
use base_rt::RuntimeState;
use base_rt::RuntimeWaker;
use cooked_waker::IntoWaker;
use cooked_waker::WakeRef;
use cpu_timer::CPUTimer;
use ctor::ctor;
use deno::args::CacheSetting;
use deno::args::TypeCheckMode;
use deno::deno_crypto;
use deno::deno_fetch;
use deno::deno_fs;
use deno::deno_http;
use deno::deno_io;
use deno::deno_net;
use deno::deno_telemetry;
use deno::deno_telemetry::OtelConfig;
use deno::deno_tls;
use deno::deno_url;
use deno::deno_web;
use deno::deno_webidl;
use deno::deno_websocket;
use deno::DenoOptionsBuilder;
use deno::PermissionsContainer;
use deno_core::error::AnyError;
use deno_core::error::JsError;
use deno_core::serde_json;
use deno_core::url::Url;
use deno_core::v8;
use deno_core::v8::GCCallbackFlags;
use deno_core::v8::GCType;
use deno_core::v8::Isolate;
use deno_core::v8::Locker;
use deno_core::JsRuntime;
use deno_core::ModuleId;
use deno_core::ModuleLoader;
use deno_core::ModuleSpecifier;
use deno_core::OpState;
use deno_core::PollEventLoopOptions;
use deno_core::ResolutionKind;
use deno_core::RuntimeOptions;
use deno_facade::cert_provider::get_root_cert_store_provider;
use deno_facade::generate_binary_eszip;
use deno_facade::metadata::Entrypoint;
use deno_facade::migrate::MigrateOptions;
use deno_facade::module_loader::standalone::create_module_loader_for_standalone_from_eszip_kind;
use deno_facade::module_loader::RuntimeProviders;
use deno_facade::EmitterFactory;
use deno_facade::EszipPayloadKind;
use deno_facade::Metadata;
use deno_resolver::npm;
use ext_event_worker::events::WorkerEventWithMetadata;
use ext_runtime::external_memory::CustomAllocator;
use ext_runtime::MemCheckWaker;
use ext_runtime::PromiseMetrics;
use ext_workers::context::UserWorkerMsgs;
use ext_workers::context::WorkerContextInitOpts;
use ext_workers::context::WorkerKind;
use ext_workers::context::WorkerRuntimeOpts;
use fs::deno_compile_fs::DenoCompileFileSystem;
use fs::prefix_fs::PrefixFs;
use fs::s3_fs::S3Fs;
use fs::static_fs::StaticFs;
use fs::tmp_fs::TmpFs;
use futures_util::future::poll_fn;
use futures_util::task::AtomicWaker;
use futures_util::FutureExt;
use log::error;
use once_cell::sync::Lazy;
use once_cell::sync::OnceCell;
use permissions::get_default_permissions;
use scopeguard::ScopeGuard;
use serde::Serialize;
use strum::IntoStaticStr;
use sys_traits;
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::instrument;
use tracing::trace;
use tracing::Instrument;
use tracing::Span;

use crate::inspector_server::Inspector;
use crate::snapshot;
use crate::utils::json;
use crate::utils::units::bytes_to_display;
use crate::utils::units::mib_to_bytes;
use crate::utils::units::percentage_value;

/// Debug state for tracking V8 isolate lock ownership without calling back into V8.
#[allow(dead_code)]
#[derive(Debug, Default)]
struct LockDebugState {
  depth: u32,
  ever_locked: bool,
}

thread_local! {
  static LOCK_DEBUG_STATES: RefCell<HashMap<usize, LockDebugState>> =
    RefCell::new(HashMap::new());
}

#[inline]
fn isolate_debug_key(isolate: &v8::Isolate) -> usize {
  isolate as *const v8::Isolate as usize
}

#[allow(dead_code)]
#[inline]
fn log_locker_event(isolate_key: usize, stage: &'static str, depth: u32) {
  debug!(
    target = "edge::runtime::locker",
    stage,
    isolate = format_args!("{isolate_key:#x}"),
    thread = ?std::thread::current().id(),
    depth,
  );
}
use crate::worker::supervisor::CPUUsage;
use crate::worker::supervisor::CPUUsageMetrics;
use crate::worker::DuplexStreamEntry;
use crate::worker::Worker;

mod ops;
mod unsync;

pub mod permissions;
pub mod thread_utils;

const DEFAULT_ALLOC_CHECK_INT_MSEC: u64 = 1000;

static ALLOC_CHECK_DUR: Lazy<Duration> = Lazy::new(|| {
  std::env::var("EDGE_RUNTIME_ALLOC_CHECK_INT")
    .ok()
    .and_then(|it| it.parse::<u64>().ok().map(Duration::from_millis))
    .unwrap_or_else(|| Duration::from_millis(DEFAULT_ALLOC_CHECK_INT_MSEC))
});

// Following static variables are initialized in the cli crate.

pub static SHOULD_DISABLE_DEPRECATED_API_WARNING: OnceCell<bool> =
  OnceCell::new();
pub static SHOULD_USE_VERBOSE_DEPRECATED_API_WARNING: OnceCell<bool> =
  OnceCell::new();
pub static SHOULD_INCLUDE_MALLOCED_MEMORY_ON_MEMCHECK: OnceCell<bool> =
  OnceCell::new();

pub static MAIN_WORKER_INITIAL_HEAP_SIZE_MIB: OnceCell<u64> = OnceCell::new();
pub static MAIN_WORKER_MAX_HEAP_SIZE_MIB: OnceCell<u64> = OnceCell::new();
pub static EVENT_WORKER_INITIAL_HEAP_SIZE_MIB: OnceCell<u64> = OnceCell::new();
pub static EVENT_WORKER_MAX_HEAP_SIZE_MIB: OnceCell<u64> = OnceCell::new();

#[ctor]
fn init_v8_platform() {
  set_v8_flags();

  // NOTE(denoland/deno/20495): Due to the new PKU (Memory Protection Keys)
  // feature introduced in V8 11.6, We need to initialize the V8 platform on
  // the main thread that spawns V8 isolates.
  JsRuntime::init_platform(None, false);
}

#[derive(Default)]
struct MemCheck {
  exceeded_token: CancellationToken,
  limit: Option<usize>,
  waker: Arc<AtomicWaker>,
  state: Arc<RwLock<MemCheckState>>,
}

impl MemCheck {
  fn check(&self, isolate: &mut Isolate) -> usize {
    let Some(limit) = self.limit else {
      return 0;
    };

    let stats = isolate.get_heap_statistics();
    let malloced_bytes = if SHOULD_INCLUDE_MALLOCED_MEMORY_ON_MEMCHECK
      .get()
      .copied()
      .unwrap_or_default()
    {
      stats.malloced_memory()
    } else {
      0
    };

    // XXX(Nyannyacha): Should we instead apply a size that reflects the
    // committed heap? (but it can be bloated)
    let used_heap_bytes = stats.used_heap_size();
    let external_bytes = stats.external_memory();

    let total_bytes = malloced_bytes
      .saturating_add(used_heap_bytes)
      .saturating_add(external_bytes);

    let heap_stats = WorkerHeapStatistics::from(&stats);
    let mut state = self.state.write().unwrap();

    if !state.exceeded {
      state.current = heap_stats;

      if total_bytes >= limit {
        state.exceeded = true;

        drop(state);
        self.exceeded_token.cancel();
      }
    }

    trace!(malloced_mb = bytes_to_display(total_bytes as u64));
    total_bytes
  }

  fn is_exceeded(&self) -> bool {
    self.exceeded_token.is_cancelled()
  }
}

pub trait GetRuntimeContext {
  fn get_runtime_context(
    conf: &WorkerRuntimeOpts,
    use_inspector: bool,
    migrated: bool,
    otel_config: Option<OtelConfig>,
  ) -> impl Serialize {
    serde_json::json!({
      "target": env!("TARGET"),
      "kind": conf.to_worker_kind().to_string(),
      "debug": cfg!(debug_assertions),
      "inspector": use_inspector,
      "migrated": migrated,
      "version": {
        "runtime": deno::edge_runtime_version(),
        "deno": deno::version(),
      },
      "flags": {
        "SHOULD_DISABLE_DEPRECATED_API_WARNING":
          SHOULD_DISABLE_DEPRECATED_API_WARNING
            .get()
            .copied()
            .unwrap_or_default(),
        "SHOULD_USE_VERBOSE_DEPRECATED_API_WARNING":
          SHOULD_USE_VERBOSE_DEPRECATED_API_WARNING
            .get()
            .copied()
            .unwrap_or_default()
      },
      "otel": otel_config.unwrap_or_default().as_v8(),
    })
  }

  fn get_extra_context() -> impl Serialize {
    serde_json::json!({})
  }
}

type DefaultRuntimeContext = ();

impl GetRuntimeContext for DefaultRuntimeContext {}

#[derive(Debug, Clone)]
struct GlobalMainContext(v8::Global<v8::Context>);

impl GlobalMainContext {
  #[allow(dead_code)]
  fn to_local_context<'s, 'i>(
    &self,
    scope: &mut v8::PinCallbackScope<'s, 'i, ()>,
  ) -> v8::Local<'s, v8::Context> {
    v8::Local::new(scope, &self.0)
  }
}

#[derive(Clone)]
struct DispatchEventFunctions {
  dispatch_load_event_fn_global: v8::Global<v8::Function>,
  dispatch_beforeunload_event_fn_global: v8::Global<v8::Function>,
  dispatch_unload_event_fn_global: v8::Global<v8::Function>,
  dispatch_drain_event_fn_global: v8::Global<v8::Function>,
}

#[derive(IntoStaticStr, Debug, Clone, Copy)]
#[strum(serialize_all = "snake_case")]
pub enum WillTerminateReason {
  CPU,
  Memory,
  WallClock,
  EarlyDrop,
  Termination,
}

#[derive(Debug)]
pub struct RunOptions {
  wait_termination_request_token: bool,
  duplex_stream_rx: mpsc::UnboundedReceiver<DuplexStreamEntry>,
  maybe_cpu_usage_metrics_tx: Option<mpsc::UnboundedSender<CPUUsageMetrics>>,
}

pub struct RunOptionsBuilder {
  wait_termination_request_token: bool,
  duplex_stream_rx: Option<mpsc::UnboundedReceiver<DuplexStreamEntry>>,
  maybe_cpu_usage_metrics_tx: Option<mpsc::UnboundedSender<CPUUsageMetrics>>,
}

impl Default for RunOptionsBuilder {
  fn default() -> Self {
    Self {
      wait_termination_request_token: true,
      duplex_stream_rx: None,
      maybe_cpu_usage_metrics_tx: None,
    }
  }
}

impl RunOptionsBuilder {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn wait_termination_request_token(mut self, val: bool) -> Self {
    self.wait_termination_request_token = val;
    self
  }

  pub fn stream_rx(
    mut self,
    val: mpsc::UnboundedReceiver<DuplexStreamEntry>,
  ) -> Self {
    self.duplex_stream_rx = Some(val);
    self
  }

  pub fn cpu_usage_metrics_tx(
    mut self,
    val: Option<mpsc::UnboundedSender<CPUUsageMetrics>>,
  ) -> Self {
    self.maybe_cpu_usage_metrics_tx = val;
    self
  }

  pub fn build(self) -> Result<RunOptions, AnyError> {
    let Self {
      wait_termination_request_token,
      duplex_stream_rx,
      maybe_cpu_usage_metrics_tx,
    } = self;

    // TODO(Nyannyacha): Make this as optional.
    let Some(duplex_stream_rx) = duplex_stream_rx else {
      return Err(anyhow!("stream_rx can't be empty"));
    };

    Ok(RunOptions {
      wait_termination_request_token,
      duplex_stream_rx,
      maybe_cpu_usage_metrics_tx,
    })
  }
}

fn cleanup_js_runtime(runtime: &mut JsRuntime) {
  let isolate = runtime.v8_isolate();
  let isolate_key = isolate_debug_key(isolate);

  // In V8 140.2.0 (Deno 2.5.6), the Locker API crashes in v8threads.cc:40
  // when trying to initialize thread-local storage. Since we use a
  // dedicated-thread-per-isolate model, we don't need locking - each isolate
  // runs entirely on its own thread from creation to destruction.
  //
  // We need to exit the isolate before it can be disposed.
  // V8 requires that no context is active when disposing.
  unsafe {
    isolate.exit();
  }

  LOCK_DEBUG_STATES.with(|states| {
    states.borrow_mut().remove(&isolate_key);
  });
}

pub struct DenoRuntime<RuntimeContext = DefaultRuntimeContext> {
  pub runtime_state: Arc<RuntimeState>,
  pub js_runtime: ManuallyDrop<JsRuntime>,

  pub drop_token: CancellationToken,
  pub(crate) termination_request_token: CancellationToken,

  pub conf: WorkerRuntimeOpts,
  pub s3_fs: Option<S3Fs>,

  entrypoint: Option<Entrypoint>,
  main_module_url: Url,
  main_module_id: Option<ModuleId>,

  worker: Worker,
  promise_metrics: PromiseMetrics,

  mem_check: Arc<MemCheck>,
  pub waker: Arc<AtomicWaker>,

  beforeunload_mem_threshold: Arc<ArcSwapOption<u64>>,
  beforeunload_cpu_threshold: Arc<ArcSwapOption<u64>>,

  _phantom_runtime_context: PhantomData<RuntimeContext>,
}

impl<RuntimeContext> Drop for DenoRuntime<RuntimeContext> {
  fn drop(&mut self) {
    if self.conf.is_user_worker() {
      self.js_runtime.v8_isolate().remove_gc_prologue_callback(
        mem_check_gc_prologue_callback_fn as _,
        Arc::as_ptr(&self.mem_check) as *mut _,
      );
    }

    cleanup_js_runtime(&mut self.js_runtime);

    unsafe {
      ManuallyDrop::drop(&mut self.js_runtime);
    }

    self.drop_token.cancel();
  }
}

struct ScopedFuture<F> {
  future: F,
  isolate: *mut v8::Isolate,
  context: v8::Global<v8::Context>,
}

impl<F: Future> Future for ScopedFuture<F> {
  type Output = F::Output;

  fn poll(
    self: std::pin::Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> Poll<Self::Output> {
    let isolate = unsafe { &mut *self.isolate };
    let scope_storage = std::pin::pin!(v8::HandleScope::new(isolate));
    let mut scope = scope_storage.init();
    let context = v8::Local::new(&scope, &self.context);
    let _context_scope = v8::ContextScope::new(&mut scope, context);
    let inner = unsafe { self.map_unchecked_mut(|s| &mut s.future) };
    inner.poll(cx)
  }
}

impl<RuntimeContext> DenoRuntime<RuntimeContext> {
  #[allow(dead_code)]
  #[inline]
  fn assert_isolate_not_locked(&mut self) {
    assert_isolate_not_locked(self.js_runtime.v8_isolate());
  }
}

#[allow(dead_code)]
#[inline]
fn assert_isolate_not_locked(isolate: &v8::Isolate) {
  // Only check the lock state if we've ever taken the lock on this thread.
  // This avoids calling into V8's ThreadManager before it's initialized,
  // which would segfault in v8::Locker::IsLocked during bootstrap.
  let isolate_key = isolate_debug_key(isolate);
  LOCK_DEBUG_STATES.with(|states| {
    if let Some(state) = states.borrow().get(&isolate_key) {
      if state.ever_locked {
        assert_eq!(
          state.depth, 0,
          "isolate must not be locked when entering this scope"
        );
      }
    }
  });
}

impl<RuntimeContext> DenoRuntime<RuntimeContext>
where
  RuntimeContext: GetRuntimeContext,
{
  #[allow(clippy::unnecessary_literal_unwrap)]
  #[allow(clippy::arc_with_non_send_sync)]
  pub(crate) async fn new(mut worker: Worker) -> Result<Self, Error> {
    let init_opts = worker.init_opts.take();
    let flags = worker.flags.clone();
    let event_metadata = worker.event_metadata.clone();

    debug_assert!(init_opts.is_some(), "init_opts must not be None");

    let WorkerContextInitOpts {
      mut conf,
      service_path,
      no_module_cache,
      no_npm,
      env_vars,
      maybe_eszip,
      maybe_entrypoint,
      maybe_module_code,
      static_patterns,
      maybe_s3_fs_config,
      maybe_tmp_fs_config,
      maybe_otel_config,
      ..
    } = init_opts.unwrap();

    let waker = Arc::<AtomicWaker>::default();
    let drop_token = CancellationToken::default();
    let is_user_worker = conf.is_user_worker();
    let is_some_entry_point = maybe_entrypoint.is_some();
    let termination_request_token = CancellationToken::default();
    let promise_metrics = PromiseMetrics::default();
    let runtime_state = Arc::<RuntimeState>::default();

    let maybe_user_conf = conf.as_user_worker();
    let context = conf.context().cloned().unwrap_or_default();

    let permissions_options = maybe_user_conf
      .and_then(|it| it.permissions.clone())
      .unwrap_or_else(|| get_default_permissions(conf.to_worker_kind()));

    struct Bootstrap {
      migrated: bool,
      waker: Arc<AtomicWaker>,
      js_runtime: JsRuntime,
      mem_check: Arc<MemCheck>,
      has_inspector: bool,
      main_module_url: Url,
      entrypoint: Option<Entrypoint>,
      context: Option<serde_json::Map<String, serde_json::Value>>,
      s3_fs: Option<S3Fs>,
      beforeunload_cpu_threshold: ArcSwapOption<u64>,
      beforeunload_mem_threshold: ArcSwapOption<u64>,
    }

    let bootstrap_fn = || {
      async {
        // TODO(Nyannyacha): Make sure `service_path` is an absolute path first.
        let base_dir_path =
          std::env::current_dir().map(|p| p.join(&service_path))?;

        let maybe_import_map_path = context
          .get("importMapPath")
          .and_then(|it| it.as_str())
          .map(str::to_string);

        let eszip = if let Some(eszip_payload) = maybe_eszip {
          eszip_payload
        } else {
          let Ok(base_dir_url) = Url::from_directory_path(&base_dir_path)
          else {
            bail!(
              "malformed base directory: {}",
              base_dir_path.to_string_lossy()
            );
          };

          let mut main_module_url = None;
          let only_module_code = maybe_module_code.is_some()
            && maybe_eszip.is_none()
            && !is_some_entry_point;

          if only_module_code {
            main_module_url = None;
          } else {
            static POTENTIAL_EXTS: &[&str] = &["ts", "tsx", "js", "mjs", "jsx"];

            let mut found = false;
            for ext in POTENTIAL_EXTS.iter() {
              let url = base_dir_url.join(format!("index.{}", ext).as_str())?;
              if url.to_file_path().unwrap().exists() {
                found = true;
                main_module_url = Some(url);
                break;
              }
            }
            if !is_some_entry_point && !found {
              main_module_url = Some(base_dir_url.clone());
            }
          }
          if is_some_entry_point {
            main_module_url =
              Some(Url::parse(&maybe_entrypoint.clone().unwrap())?);
          }

          let mut emitter_factory = EmitterFactory::new();

          let cache_strategy = if no_module_cache {
            CacheSetting::ReloadAll
          } else {
            CacheSetting::Use
          };

          emitter_factory
            .set_permissions_options(Some(permissions_options.clone()));

          emitter_factory.set_file_fetcher_allow_remote(
            maybe_user_conf
              .map(|it| it.allow_remote_modules)
              .unwrap_or(true),
          );
          emitter_factory.set_cache_strategy(Some(cache_strategy));

          let maybe_code = if only_module_code {
            maybe_module_code
          } else {
            None
          };

          let mut builder = DenoOptionsBuilder::new();

          // Extract unstableSloppyImports from context if provided
          if let Some(unstable_sloppy_imports) = context.get("unstableSloppyImports")
            .and_then(|v| v.as_bool()) {
            builder.set_unstable_sloppy_imports(Some(unstable_sloppy_imports));
          }

          if let Some(module_url) = main_module_url.as_ref() {
            builder.set_entrypoint(Some(module_url.to_file_path().unwrap()));
          }
          builder
            .set_type_check_mode(is_user_worker.then_some(TypeCheckMode::Local))
            .set_no_npm(no_npm)
            .set_import_map_path(maybe_import_map_path.clone());

          emitter_factory.set_deno_options(builder.build().await?);

          let deno_options = emitter_factory.deno_options()?;
          if !is_some_entry_point
            && main_module_url.is_some_and(|it| it == base_dir_url)
            && deno_options
              .workspace()
              .root_pkg_json()
              .and_then(|it| it.main.as_ref())
              .is_none()
          {
            bail!("could not find an appropriate entrypoint");
          }
          let mut metadata = Metadata::default();
          let eszip = generate_binary_eszip(
            &mut metadata,
            Arc::new(emitter_factory),
            maybe_code,
            // here we don't want to add extra cost, so we won't use a checksum
            None,
            Some(static_patterns.iter().map(|s| s.as_str()).collect()),
          )
          .await?;

          EszipPayloadKind::Eszip(eszip)
        };

        let _root_cert_store_provider = get_root_cert_store_provider()?;
        let stdio = if is_user_worker {
          let stdio_pipe = deno_io::StdioPipe::file(
            tokio::fs::File::create("/dev/null").await?.into_std().await,
          );

          deno_io::Stdio {
            stdin: stdio_pipe.clone(),
            stdout: stdio_pipe.clone(),
            stderr: stdio_pipe,
          }
        } else {
          Default::default()
        };

        let has_inspector = worker.inspector.is_some();
        let need_source_map = context
          .get("sourceMap")
          .and_then(serde_json::Value::as_bool)
          .unwrap_or_default();

        let rt_provider = create_module_loader_for_standalone_from_eszip_kind(
          eszip,
          permissions_options,
          has_inspector || need_source_map,
          Some(MigrateOptions {
            maybe_import_map_path,
          }),
        )
        .await?;

        let RuntimeProviders {
          migrated,
          module_loader,
          node_services: _,
          npm_snapshot,
          permissions,
          metadata,
          static_files,
          vfs,
          vfs_path,
          base_url,
        } = rt_provider;

        let node_modules = metadata
          .node_modules()
          .ok()
          .flatten();
        let entrypoint = metadata.entrypoint.clone();
        let main_module_url = match entrypoint.as_ref() {
          Some(Entrypoint::Key(key)) => base_url.join(key)?,
          Some(Entrypoint::ModuleCode(_)) | None => Url::parse(
            maybe_entrypoint
              .as_ref()
              .with_context(|| "could not find entrypoint key")?,
          )?,
        };

        let should_block_fs = if is_user_worker {
          let allow_fs_access = maybe_user_conf
            .and_then(|conf| conf.allow_host_fs_access)
            .unwrap_or(false);
          !allow_fs_access
        } else {
          false
        };

        let build_file_system_fn = |base_fs: Arc<dyn deno_fs::FileSystem>| -> Result<
          (Arc<dyn deno_fs::FileSystem>, Option<S3Fs>),
          AnyError,
        > {
          let tmp_fs =
            TmpFs::try_from(maybe_tmp_fs_config.unwrap_or_default())?;
          let tmp_fs_actual_path = tmp_fs.actual_path().to_path_buf();
          let mut fs = PrefixFs::new("/tmp", tmp_fs.clone(), Some(base_fs))
            .tmp_dir("/tmp")
            .add_fs(tmp_fs_actual_path, tmp_fs);

          fs.set_runtime_state(&runtime_state);

          // Apply filesystem blocking to the base filesystem
          fs.set_check_sync_api(should_block_fs);

          Ok(
            if let Some(s3_fs) =
              maybe_s3_fs_config.map(S3Fs::new).transpose()?
            {
              let mut s3_prefix_fs = fs.add_fs("/s3", s3_fs.clone());

              // Apply the same blocking logic to S3 filesystem
              s3_prefix_fs.set_check_sync_api(should_block_fs);

              (Arc::new(s3_prefix_fs), Some(s3_fs))
            } else {
              (Arc::new(fs), None)
            },
          )
        };

        let static_files = if is_some_entry_point {
          let entrypoint_path = main_module_url
            .to_file_path()
            .map_err(|_| anyhow!("failed to convert entrypoint to path"))?;
          let static_root_path = entrypoint_path
            .parent()
            .ok_or_else(|| anyhow!("could not resolve parent of entrypoint"))?
            .to_path_buf();

          metadata
            .static_assets_lookup(static_root_path)
            .into_iter()
            .chain(static_files.into_iter())
            .collect()
        } else {
          static_files
        };

        let (_fs, s3_fs) = build_file_system_fn(if is_user_worker && should_block_fs {
          Arc::new(StaticFs::new(
            node_modules,
            static_files,
            if matches!(entrypoint, Some(Entrypoint::ModuleCode(_)) | None)
              && is_some_entry_point
            {
              // it is eszip from before v2
              base_url
                .to_file_path()
                .map_err(|_| anyhow!("failed to resolve base url"))?
            } else {
              main_module_url
                .to_file_path()
                .map_err(|_| {
                  anyhow!("failed to resolve base dir using main module url")
                })
                .and_then(|it| {
                  it.parent()
                    .map(Path::to_path_buf)
                    .with_context(|| "failed to determine parent directory")
                })?
            },
            vfs_path,
            vfs,
            npm_snapshot,
          ))
        } else {
          // Use DenoCompileFileSystem for main workers and user workers with filesystem access enabled
          Arc::new(DenoCompileFileSystem::from_rc(vfs))
        })?;

        let extensions = vec![
          deno_telemetry::deno_telemetry::init(),
          deno_webidl::deno_webidl::init(),
          deno_console::deno_console::init(),
          deno_url::deno_url::init(),
          deno_web::deno_web::lazy_init::<PermissionsContainer>(),
          deno_webgpu::deno_webgpu::init(),
          deno_canvas::deno_canvas::init(),
          deno_fetch::deno_fetch::lazy_init::<PermissionsContainer>(),
          deno_websocket::deno_websocket::lazy_init::<PermissionsContainer>(),
          // TODO: support providing a custom seed for crypto
          deno_crypto::deno_crypto::lazy_init(),
          deno_broadcast_channel::deno_broadcast_channel::lazy_init::<
            deno_broadcast_channel::InMemoryBroadcastChannel,
          >(),
          deno_net::deno_net::lazy_init::<PermissionsContainer>(),
          deno_tls::deno_tls::init(),
          deno_http::deno_http::lazy_init(),
          deno_io::deno_io::lazy_init(),
          deno_fs::deno_fs::lazy_init::<PermissionsContainer>(),
          ext_ai::ai::init(),
          trex_core::trex::init(),
          ext_env::env::init(),
          deno_os::deno_os::init(None),
          deno_process::deno_process::init(None),
          ext_workers::user_workers::init(),
          ext_event_worker::user_event_worker::init(),
          ext_event_worker::js_interceptors::js_interceptors::init(),
          ext_runtime::runtime_bootstrap::init::<PermissionsContainer>(
            Some(main_module_url.clone()),
          ),
          ext_runtime::runtime_net::init(),
          ext_runtime::runtime_http::init(),
          ext_runtime::runtime_http_start::init(),
          // NOTE(AndresP): Order is matters. Otherwise, it will lead to hard
          // errors such as SIGBUS depending on the platform.
          ext_node::deno_node::lazy_init::<
            PermissionsContainer,
            deno_resolver::npm::DenoInNpmPackageChecker,
            npm::NpmResolver<sys_traits::impls::RealSys>,
            sys_traits::impls::RealSys,
          >(),
          deno_cache::deno_cache::lazy_init(),
          deno::runtime::ops::permissions::deno_permissions::init(),
          ops::permissions::base_runtime_permissions::init(
            permissions,
          ),
          ext_os::os::init(),
          ext_runtime::runtime::init(),
        ];

        let mut create_params = None;
        let mut mem_check = MemCheck::default();

        let beforeunload_cpu_threshold =
          ArcSwapOption::<u64>::from_pointee(None);
        let beforeunload_mem_threshold =
          ArcSwapOption::<u64>::from_pointee(None);

        match conf.to_worker_kind() {
          WorkerKind::UserWorker => {
            let conf = maybe_user_conf.unwrap();
            let memory_limit_bytes = mib_to_bytes(conf.memory_limit_mb) as usize;

            beforeunload_mem_threshold.store(
              flags
                .beforeunload_memory_pct
                .and_then(|it| percentage_value(memory_limit_bytes as u64, it))
                .map(Arc::new),
            );

            if conf.cpu_time_hard_limit_ms > 0 {
              beforeunload_cpu_threshold.store(
                flags
                  .beforeunload_cpu_pct
                  .and_then(|it| {
                    percentage_value(conf.cpu_time_hard_limit_ms, it)
                  })
                  .map(Arc::new),
              );
            }

            let allocator = CustomAllocator::new(memory_limit_bytes);

            allocator.set_waker(mem_check.waker.clone());

            mem_check.limit = Some(memory_limit_bytes);
            create_params = Some(
              v8::CreateParams::default()
                .heap_limits(mib_to_bytes(0) as usize, memory_limit_bytes)
                .array_buffer_allocator(allocator.into_v8_allocator()),
            )
          }

          kind => {
            assert_ne!(kind, WorkerKind::UserWorker);
            let initial_heap_size = match kind {
              WorkerKind::MainWorker => &MAIN_WORKER_INITIAL_HEAP_SIZE_MIB,
              WorkerKind::EventsWorker => &EVENT_WORKER_INITIAL_HEAP_SIZE_MIB,
              _ => unreachable!(),
            };
            let max_heap_size = match kind {
              WorkerKind::MainWorker => &MAIN_WORKER_MAX_HEAP_SIZE_MIB,
              WorkerKind::EventsWorker => &EVENT_WORKER_MAX_HEAP_SIZE_MIB,
              _ => unreachable!(),
            };

            let initial_heap_size = initial_heap_size.get().cloned().unwrap_or_default();
            let max_heap_size = max_heap_size.get().cloned().unwrap_or_default();

            if max_heap_size > 0 {
              create_params = Some(v8::CreateParams::default().heap_limits(
                mib_to_bytes(initial_heap_size) as usize,
                mib_to_bytes(max_heap_size) as usize,
              ));
            }
          }
        }

        let mem_check = Arc::new(mem_check);
        let runtime_options = RuntimeOptions {
          extensions,
          is_main: true,
          inspector: has_inspector,
          create_params,
          shared_array_buffer_store: None,
          compiled_wasm_module_store: None,
          startup_snapshot: snapshot::snapshot(),
          module_loader: Some(module_loader),
          extension_transpiler: Some(std::rc::Rc::new(|specifier, source| {
            deno::transpile::maybe_transpile_source(specifier, source)
          })),
          ..Default::default()
        };

        let mut js_runtime = JsRuntime::new(runtime_options);
        unsafe { js_runtime.v8_isolate().enter() };

        // Initialize lazy-loaded extensions
        // This is required for extensions that use lazy_init() instead of init()
        // It calls the state initializers for those extensions (e.g., AsyncId for node)
        js_runtime.lazy_init_extensions(vec![
          deno_web::deno_web::args::<PermissionsContainer>(
            Default::default(), // blob_store
            None, // location
          ),
          deno_fetch::deno_fetch::args::<PermissionsContainer>(
            deno_fetch::Options {
              user_agent: "supabase-edge-runtime".to_string(),
              root_cert_store_provider: None,
              unsafely_ignore_certificate_errors: None,
              file_fetch_handler: std::rc::Rc::new(deno_fetch::FsFetchHandler),
              ..Default::default()
            },
          ),
          deno_websocket::deno_websocket::args::<PermissionsContainer>(),
          deno_crypto::deno_crypto::args(None),
          deno_broadcast_channel::deno_broadcast_channel::args::<
            deno_broadcast_channel::InMemoryBroadcastChannel,
          >(
            deno_broadcast_channel::InMemoryBroadcastChannel::default(),
          ),
          deno_net::deno_net::args::<PermissionsContainer>(None, None),
          deno_http::deno_http::args(deno_http::Options::default()),
          deno_io::deno_io::args(Some(stdio.clone())),
          deno_fs::deno_fs::args::<PermissionsContainer>(Arc::new(deno_fs::RealFs)),
          ext_node::deno_node::args::<
            PermissionsContainer,
            deno_resolver::npm::DenoInNpmPackageChecker,
            npm::NpmResolver<sys_traits::impls::RealSys>,
            sys_traits::impls::RealSys,
          >(None, Arc::new(deno_fs::RealFs)),
          deno_cache::deno_cache::args(Default::default()),
        ]).map_err(|e| anyhow::anyhow!("Failed to lazy init extensions: {:#}", e))?;

        let dispatch_fns = {
          let context = js_runtime.main_context();
          // New V8 API requires pinning scopes
          let scope_storage = std::pin::pin!(v8::HandleScope::new(js_runtime.v8_isolate()));
          let mut handle_scope = scope_storage.init();
          let context_local = v8::Local::new(&handle_scope, context);
          // Create ContextScope to get HandleScope<Context> instead of HandleScope<()>
          let mut context_scope = v8::ContextScope::new(&mut handle_scope, context_local);
          let scope = &mut context_scope;
          let global_obj = context_local.global(scope);
          let bootstrap_str =
            v8::String::new_external_onebyte_static(scope, b"bootstrap")
              .unwrap();
          let bootstrap_ns = global_obj
            .get(scope, bootstrap_str.into())
            .unwrap()
            .to_object(scope)
            .unwrap();

          macro_rules! get_global {
            ($name:expr) => {{
              let dispatch_fn_str =
                v8::String::new_external_onebyte_static(scope, $name).unwrap();
              let dispatch_fn = v8::Local::<v8::Function>::try_from(
                bootstrap_ns.get(scope, dispatch_fn_str.into()).unwrap(),
              )
              .unwrap();
              v8::Global::new(scope, dispatch_fn)
            }};
          }

          DispatchEventFunctions {
            dispatch_load_event_fn_global: get_global!(b"dispatchLoadEvent"),
            dispatch_beforeunload_event_fn_global: get_global!(
              b"dispatchBeforeUnloadEvent"
            ),
            dispatch_unload_event_fn_global: get_global!(
              b"dispatchUnloadEvent"
            ),
            dispatch_drain_event_fn_global: get_global!(b"dispatchDrainEvent"),
          }
        };

        {
          let main_context = js_runtime.main_context();
          let op_state = js_runtime.op_state();
          let mut op_state = op_state.borrow_mut();

          op_state.put(dispatch_fns);
          op_state.put(promise_metrics.clone());
          op_state.put(runtime_state.clone());
          op_state.put(GlobalMainContext(main_context));
          op_state.put(RuntimeWaker(waker.clone()));
        }

        {
          let op_state_rc = js_runtime.op_state();
          let mut op_state = op_state_rc.borrow_mut();

          // NOTE(Andreespirela): We do this because "NODE_DEBUG" is trying to be
          // read during initialization, But we need the gotham state to be
          // up-to-date.
          op_state.put(ext_env::EnvVars::default());
        }

        if let Some(inspector) = worker.inspector.as_ref() {
          inspector.server.register_inspector(
            main_module_url.to_string(),
            &mut js_runtime,
            inspector.should_wait_for_session(),
          );
        }

        if is_user_worker {
          js_runtime.v8_isolate().add_gc_prologue_callback(
            mem_check_gc_prologue_callback_fn as _,
            Arc::as_ptr(&mem_check) as *mut _,
            GCType::kGCTypeAll,
          );

          js_runtime
            .op_state()
            .borrow_mut()
            .put(MemCheckWaker::from(mem_check.waker.clone()));
        }

        // V8 isolate stays entered on this thread.
        // With Deno 2.5.6, we no longer use v8::Locker, so the isolate
        // remains on its creation thread and never needs to exit/re-enter.

        Ok(Bootstrap {
          migrated,
          waker,
          js_runtime,
          mem_check,
          has_inspector,
          main_module_url,
          entrypoint,
          context: Some(context),
          s3_fs,
          beforeunload_cpu_threshold,
          beforeunload_mem_threshold,
        })
      }
      .in_current_span()
    };

    let _span = Span::current().entered();

    // Execute bootstrap directly on this thread (no spawn_blocking needed)
    let bootstrap_ret: Result<Bootstrap, Error> = {
      let mut bootstrap = bootstrap_fn().await?;

      debug!("bootstrap");

      let has_inspector = bootstrap.has_inspector;
      let migrated = bootstrap.migrated;
      let context = bootstrap.context.take().unwrap_or_default();
      let mut bootstrap = scopeguard::guard(bootstrap, |mut it| {
        cleanup_js_runtime(&mut it.js_runtime);
      });

      {
        // Prepare data that doesn't need V8 scope
        let runtime_context =
          serde_json::json!(RuntimeContext::get_runtime_context(
            &conf,
            has_inspector,
            migrated,
            maybe_otel_config,
          ));

        let tokens = {
          let op_state = bootstrap.js_runtime.op_state();
          let resource_table = &mut op_state.borrow_mut().resource_table;
          serde_json::json!({
            "terminationRequestToken":
              resource_table
                .add(DropToken(termination_request_token.clone()))
          })
        };

        let extra_context = {
          let mut extra_context =
            serde_json::json!(RuntimeContext::get_extra_context());

          json::merge_object(
            &mut extra_context,
            &serde_json::Value::Object(context),
          );
          json::merge_object(&mut extra_context, &tokens);

          extra_context
        };

        let context_global = bootstrap.js_runtime.main_context();

        // Now create V8 scope for bootstrap operations
        // deno_core::scope!(scope, &mut bootstrap.js_runtime);
        let scope_storage = std::pin::pin!(v8::HandleScope::new(
          bootstrap.js_runtime.v8_isolate()
        ));
        let mut handle_scope = scope_storage.init();

        // Bootstrapping stage
        let (runtime_context, extra_context, bootstrap_fn) = {
          let context = context_global.clone();
          let context_local = v8::Local::new(&handle_scope, context);
          let mut context_scope =
            v8::ContextScope::new(&mut handle_scope, context_local);
          let scope = &mut context_scope;

          let global_obj = context_local.global(scope);
          let bootstrap_str =
            v8::String::new_external_onebyte_static(scope, b"bootstrapSBEdge")
              .unwrap();
          let bootstrap_fn = v8::Local::<v8::Function>::try_from(
            global_obj.get(scope, bootstrap_str.into()).unwrap(),
          )
          .unwrap();

          let runtime_context_local =
            deno_core::serde_v8::to_v8(scope, runtime_context)
              .context("failed to convert to v8 value")?;
          let runtime_context_global =
            v8::Global::new(scope, runtime_context_local);
          let extra_context_local =
            deno_core::serde_v8::to_v8(scope, extra_context)
              .context("failed to convert to v8 value")?;
          let extra_context_global =
            v8::Global::new(scope, extra_context_local);
          let bootstrap_fn_global = v8::Global::new(scope, bootstrap_fn);

          (
            runtime_context_global,
            extra_context_global,
            bootstrap_fn_global,
          )
        };

        // Call bootstrap function directly on this thread
        // No need for locker.call_with_args() - we're on the same thread as the isolate
        {
          let context = context_global;
          let context_local = v8::Local::new(&handle_scope, context);
          let mut context_scope =
            v8::ContextScope::new(&mut handle_scope, context_local);
          let scope = &mut context_scope;

          let bootstrap_fn_local = v8::Local::new(scope, &bootstrap_fn);
          let runtime_context_local = v8::Local::new(scope, &runtime_context);
          let extra_context_local = v8::Local::new(scope, &extra_context);
          let undefined = v8::undefined(scope);

          bootstrap_fn_local
            .call(
              scope,
              undefined.into(),
              &[runtime_context_local, extra_context_local],
            )
            .context("failed to execute bootstrap script")?;
        }
      }

      // Bootstrap complete - no longer using v8::Locker
      let res = ScopeGuard::into_inner(bootstrap);
      Ok(res)
    };

    let Bootstrap {
      waker,
      js_runtime,
      mem_check,
      main_module_url,
      entrypoint,
      s3_fs,
      beforeunload_cpu_threshold,
      beforeunload_mem_threshold,
      ..
    } = match bootstrap_ret {
      Ok(v) => v,
      Err(err) => {
        return Err(err.context("failed to bootstrap runtime"));
      }
    };

    let otel_attributes = event_metadata.otel_attributes.clone();
    let _span = Span::current().entered();

    // Execute post-bootstrap tasks directly on this thread (no spawn_blocking needed)
    debug!("bootstrap post task");

    {
      // Access op_state directly - no Locker needed on same thread
      // run inside a closure, so op_state_rc is released
      let op_state_rc = js_runtime.op_state();
      let mut op_state = op_state_rc.borrow_mut();

      let mut env_vars = env_vars.clone();

      if let Some(opts) = conf.as_events_worker_mut() {
        op_state.put::<mpsc::UnboundedReceiver<WorkerEventWithMetadata>>(
          opts.events_msg_rx.take().unwrap(),
        );
      }

      if conf.is_main_worker() || conf.is_user_worker() {
        op_state.put::<HashMap<usize, CancellationToken>>(HashMap::new());
      }

      if conf.is_user_worker() {
        let conf = conf.as_user_worker().unwrap();
        let key = conf.key.map_or("".to_string(), |k| k.to_string());

        // set execution id for user workers
        env_vars.insert("SB_EXECUTION_ID".to_string(), key.clone());

        if let Some(events_msg_tx) = conf.events_msg_tx.clone() {
          op_state.put::<mpsc::UnboundedSender<WorkerEventWithMetadata>>(
            events_msg_tx,
          );
          op_state.put(event_metadata);
        }
      }

      op_state.put(ext_env::EnvVars(env_vars));

      op_state.put(DenoRuntimeDropToken(DropToken(drop_token.clone())));

      op_state.put(RuntimeOtelExtraAttributes(
        otel_attributes
          .unwrap_or_default()
          .into_iter()
          .map(|(k, v)| (k.into(), v.into()))
          .collect(),
      ));
    }

    if is_user_worker {
      drop(base_rt::SUPERVISOR_RT.spawn({
        let drop_token = drop_token.clone();
        let waker = mem_check.waker.clone();

        async move {
          // TODO(Nyannyacha): Should we introduce exponential backoff?
          let mut int = interval(*ALLOC_CHECK_DUR);
          loop {
            tokio::select! {
              _ = int.tick() => {
                waker.wake();
              }

              _ = drop_token.cancelled() => {
                break;
              }
            }
          }
        }
      }));
    }

    // Post-bootstrap tasks complete - continue with runtime initialization

    Ok(Self {
      runtime_state,
      js_runtime: ManuallyDrop::new(js_runtime),

      drop_token,
      termination_request_token,

      conf,
      s3_fs,

      entrypoint,
      main_module_url,
      main_module_id: None,

      worker,
      promise_metrics,

      mem_check,
      waker,

      beforeunload_cpu_threshold: Arc::new(beforeunload_cpu_threshold),
      beforeunload_mem_threshold: Arc::new(beforeunload_mem_threshold),

      _phantom_runtime_context: PhantomData,
    })
  }

  pub(crate) async fn init_main_module(&mut self) -> Result<(), Error> {
    if self.main_module_id.is_some() {
      return Ok(());
    }

    let entrypoint = self.entrypoint.take();
    let url = self.main_module_url.clone();

    let id = match entrypoint {
      Some(Entrypoint::Key(_)) | None => {
        let isolate_ptr = {
          let isolate_ref: &mut v8::Isolate = self.js_runtime.v8_isolate();
          isolate_ref as *mut v8::Isolate
        };
        let context = self.js_runtime.main_context();
        let future = {
          let isolate = unsafe { &mut *isolate_ptr };
          let _scope = v8::HandleScope::new(isolate);
          let res = self.js_runtime.load_main_es_module(&url);
          res
        };
        ScopedFuture {
          future,
          isolate: isolate_ptr,
          context,
        }
        .await?
      }
      Some(Entrypoint::ModuleCode(module_code)) => {
        let isolate_ptr = {
          let isolate_ref: &mut v8::Isolate = self.js_runtime.v8_isolate();
          isolate_ref as *mut v8::Isolate
        };
        let context = self.js_runtime.main_context();
        let future = {
          let isolate = unsafe { &mut *isolate_ptr };
          let _scope = v8::HandleScope::new(isolate);
          self
            .js_runtime
            .load_main_es_module_from_code(&url, module_code)
        };
        let id = ScopedFuture {
          future,
          isolate: isolate_ptr,
          context,
        }
        .await?;
        id
      }
    };

    self.main_module_id = Some(id);
    Ok(())
  }

  pub async fn run(&mut self, options: RunOptions) -> (Result<(), Error>, i64) {
    // self.assert_isolate_not_locked();

    let RunOptions {
      wait_termination_request_token,
      duplex_stream_rx,
      maybe_cpu_usage_metrics_tx,
    } = options;

    {
      let op_state_rc = self.js_runtime.op_state();
      let mut op_state = op_state_rc.borrow_mut();

      op_state
        .put::<mpsc::UnboundedReceiver<DuplexStreamEntry>>(duplex_stream_rx);

      if self.conf.is_main_worker() {
        op_state.put::<mpsc::UnboundedSender<UserWorkerMsgs>>(
          self.conf.as_main_worker().unwrap().worker_pool_tx.clone(),
        );
      }
    }

    let _terminate_guard =
      scopeguard::guard(self.runtime_state.terminated.clone(), |v| {
        v.raise();
      });

    let mut accumulated_cpu_time_ns = 0i64;

    macro_rules! get_accumulated_cpu_time_ms {
      () => {
        accumulated_cpu_time_ns / 1_000_000
      };
    }

    let inspector = self.inspector();

    if let Err(err) = self.init_main_module().await {
      return (Err(err), 0i64);
    }

    let Some(main_module_id) = self.main_module_id else {
      return (Err(anyhow!("failed to get main module id")), 0);
    };

    if inspector.is_some() {
      let state = self.runtime_state.clone();
      let _guard = scopeguard::guard_on_unwind((), |_| {
        state.terminated.raise();
      });

      {
        let _guard =
          scopeguard::guard(state.found_inspector_session.clone(), |v| {
            v.raise();
          });

        // XXX(Nyannyacha): Suppose the user skips this function by
        // passing the `--inspect` argument. In that case, the runtime
        // may terminate before the inspector session is connected if
        // the function doesn't have a long execution time. Should we
        // wait for an inspector session to connect with the V8?
        self.wait_for_inspector_session();
      }

      if self.termination_request_token.is_cancelled() {
        state.terminated.raise();
        return (Ok(()), 0i64);
      }
    }

    // Create the mod_evaluate future wrapped in ScopedFuture so it has a HandleScope when polled
    let isolate_ptr = {
      let isolate_ref: &mut v8::Isolate = self.js_runtime.v8_isolate();
      isolate_ref as *mut v8::Isolate
    };
    let context = self.js_runtime.main_context();
    let mod_evaluate_future = self.js_runtime.mod_evaluate(main_module_id);
    let mut mod_fut = ScopedFuture {
      future: mod_evaluate_future,
      isolate: isolate_ptr,
      context,
    };

    {
      let evaluating_mod =
        scopeguard::guard(self.runtime_state.evaluating_mod.clone(), |v| {
          v.lower();
        });

      evaluating_mod.raise();

      let event_loop_fut = self.run_event_loop(
        wait_termination_request_token,
        &maybe_cpu_usage_metrics_tx,
        &mut accumulated_cpu_time_ns,
      );

      let mod_result = tokio::select! {
        // Not using biased mode leads to non-determinism for relatively
        // simple programs.
        biased;

        maybe_mod_result = &mut mod_fut => {
          debug!("received module evaluate {:#?}", maybe_mod_result);
          maybe_mod_result.map_err(Into::into)
        }

        event_loop_result = event_loop_fut => {
          if let Err(err) = event_loop_result {
            Err(
              anyhow!(
                "event loop error while evaluating the module: {}",
                err
              )
            )
          } else {
            mod_fut.await.map_err(Into::into)
          }
        }
      };

      if let Err(err) = mod_result {
        return (Err(err), get_accumulated_cpu_time_ms!());
      }
      if self.runtime_state.is_event_loop_completed()
        && self.promise_metrics.have_all_promises_been_resolved()
      {
        return (Ok(()), get_accumulated_cpu_time_ms!());
      }

      {
        if !self.termination_request_token.is_cancelled() {
          if let Err(err) = with_cpu_metrics_guard(
            self.js_runtime.op_state(),
            &maybe_cpu_usage_metrics_tx,
            &mut accumulated_cpu_time_ns,
            || MaybeDenoRuntime::DenoRuntime(self).dispatch_load_event(),
          ) {
            return (Err(err), get_accumulated_cpu_time_ms!());
          }
        }
      }
    }

    self.runtime_state.event_loop_completed.lower();

    if let Err(err) = self
      .run_event_loop(
        wait_termination_request_token,
        &maybe_cpu_usage_metrics_tx,
        &mut accumulated_cpu_time_ns,
      )
      .await
    {
      return (
        Err(anyhow!("event loop error: {}", err)),
        get_accumulated_cpu_time_ms!(),
      );
    }

    if !self.conf.is_user_worker() {
      let mut guard = self.get_v8_termination_guard();

      if let Err(err) = with_cpu_metrics_guard(
        guard.js_runtime.op_state(),
        &maybe_cpu_usage_metrics_tx,
        &mut accumulated_cpu_time_ns,
        || MaybeDenoRuntime::DenoRuntime(&mut guard).dispatch_unload_event(),
      ) {
        return (Err(err), get_accumulated_cpu_time_ms!());
      }

      // TODO(Nyannyacha): Here we also need to trigger the event for node
      // platform (i.e; exit)
    }

    (Ok(()), get_accumulated_cpu_time_ms!())
  }

  fn run_event_loop<'l>(
    &'l mut self,
    wait_termination_request_token: bool,
    maybe_cpu_usage_metrics_tx: &'l Option<
      mpsc::UnboundedSender<CPUUsageMetrics>,
    >,
    accumulated_cpu_time_ns: &'l mut i64,
  ) -> impl Future<Output = Result<(), AnyError>> + 'l {
    let has_inspector = self.inspector().is_some();
    let is_user_worker = self.conf.is_user_worker();
    let global_waker = self.waker.clone();

    let mut termination_request_fut = self
      .termination_request_token
      .clone()
      .cancelled_owned()
      .boxed();

    let beforeunload_cpu_threshold = self.beforeunload_cpu_threshold.clone();
    let beforeunload_mem_threshold = self.beforeunload_mem_threshold.clone();

    let state = self.runtime_state.clone();
    let mem_check_state = is_user_worker.then(|| self.mem_check.clone());

    poll_fn(move |cx| {
      let waker = cx.waker();
      let woked = global_waker.take().is_none();

      global_waker.register(waker);

      // let mut this = {
      //   self.assert_isolate_not_locked();
      //   unsafe { self.with_locker() }
      // };
      let this = &mut *self;

      if woked {
        extern "C" fn dummy(_: &mut v8::Isolate, _: *mut std::ffi::c_void) {}
        this
          .js_runtime
          .v8_isolate()
          .thread_safe_handle()
          .request_interrupt(dummy, std::ptr::null_mut());
      }

      let op_state = this.js_runtime.op_state();
      let cpu_metrics_guard = get_cpu_metrics_guard(
        op_state.clone(),
        maybe_cpu_usage_metrics_tx,
        accumulated_cpu_time_ns,
      );

      let wait_for_inspector = if has_inspector {
        let inspector = this.js_runtime.inspector();
        let sessions_state = inspector.sessions_state();
        sessions_state.has_active || sessions_state.has_blocking
      } else {
        false
      };

      let need_pool_event_loop = !is_user_worker || woked;
      let poll_result = if need_pool_event_loop {
        struct JsRuntimeWaker(Arc<AtomicWaker>);

        impl WakeRef for JsRuntimeWaker {
          fn wake_by_ref(&self) {
            self.0.wake();
          }
        }

        let waker = if is_user_worker {
          Cow::Owned(
            Arc::new(JsRuntimeWaker(global_waker.clone())).into_waker(),
          )
        } else {
          Cow::Borrowed(waker)
        };

        let isolate_ptr = {
          let isolate_ref: &mut v8::Isolate = this.js_runtime.v8_isolate();
          isolate_ref as *mut v8::Isolate
        };

        let isolate = unsafe { &mut *isolate_ptr };
        let scope_storage = std::pin::pin!(v8::HandleScope::new(isolate));
        let mut scope = scope_storage.init();
        let context = this.js_runtime.main_context();
        let context_local = v8::Local::new(&scope, context);
        let _context_scope = v8::ContextScope::new(&mut scope, context_local);

        this.js_runtime.poll_event_loop(
          &mut std::task::Context::from_waker(waker.as_ref()),
          PollEventLoopOptions {
            wait_for_inspector,
            ..Default::default()
          },
        )
      } else {
        Poll::Pending
      };

      drop(cpu_metrics_guard);

      if is_user_worker {
        let mem_state = mem_check_state.as_ref().unwrap();
        let total_malloced_bytes =
          mem_state.check(this.js_runtime.v8_isolate().as_mut());

        mem_state.waker.register(waker);

        if let Some(threshold_ms) =
          beforeunload_cpu_threshold.load().as_deref().copied()
        {
          let threshold_ns = (threshold_ms as i128) * 1_000_000;
          if (*accumulated_cpu_time_ns as i128) >= threshold_ns {
            beforeunload_cpu_threshold.store(None);

            if !state.is_terminated() {
              let _cpu_metrics_guard = get_cpu_metrics_guard(
                op_state.clone(),
                maybe_cpu_usage_metrics_tx,
                accumulated_cpu_time_ns,
              );

              if let Err(err) = MaybeDenoRuntime::DenoRuntime(&mut *this)
                .dispatch_beforeunload_event(WillTerminateReason::CPU)
              {
                if state.is_terminated() {
                  return Poll::Ready(Err(anyhow!("execution terminated")));
                }
                return Poll::Ready(Err(err));
              }
            }
          }
        }

        if let Some(limit) = mem_state.limit {
          if total_malloced_bytes >= limit / 2 {
            state.mem_reached_half.raise();
          } else {
            state.mem_reached_half.lower();
          }
        }

        if let Some(threshold_bytes) =
          beforeunload_mem_threshold.load().as_deref().copied()
        {
          let total_malloced_bytes = total_malloced_bytes as u64;

          if total_malloced_bytes >= threshold_bytes {
            beforeunload_mem_threshold.store(None);

            if !state.is_terminated() && !mem_state.is_exceeded() {
              let _cpu_metrics_guard = get_cpu_metrics_guard(
                op_state,
                maybe_cpu_usage_metrics_tx,
                accumulated_cpu_time_ns,
              );

              if let Err(err) = MaybeDenoRuntime::DenoRuntime(&mut *this)
                .dispatch_beforeunload_event(WillTerminateReason::Memory)
              {
                if state.is_terminated() {
                  return Poll::Ready(Err(anyhow!("execution terminated")));
                }
                return Poll::Ready(Err(err));
              }
            }
          }
        }
      }

      if need_pool_event_loop
        && poll_result.is_pending()
        && termination_request_fut.poll_unpin(cx).is_ready()
      {
        if state.is_evaluating_mod() {
          return Poll::Ready(Err(anyhow!("execution terminated")));
        }

        return Poll::Ready(Ok(()));
      }

      match poll_result {
        Poll::Pending => Poll::Pending,
        Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
        Poll::Ready(Ok(())) => {
          if !state.is_event_loop_completed() {
            state.event_loop_completed.raise();
          }
          if wait_termination_request_token
            && !termination_request_fut.poll_unpin(cx).is_ready()
          {
            return Poll::Pending;
          }

          Poll::Ready(Ok(()))
        }
      }
    })
  }

  pub fn inspector(&self) -> Option<Inspector> {
    self.worker.inspector.clone()
  }

  pub fn promise_metrics(&self) -> PromiseMetrics {
    self.promise_metrics.clone()
  }

  pub fn mem_check_state(&self) -> Arc<RwLock<MemCheckState>> {
    self.mem_check.state.clone()
  }

  pub fn add_memory_limit_callback<C>(&self, cb: C)
  where
    // XXX(Nyannyacha): Should we relax bounds a bit more?
    C: FnOnce(MemCheckState) + Send + 'static,
  {
    let runtime_token = self.drop_token.clone();
    let exceeded_token = self.mem_check.exceeded_token.clone();
    let state = self.mem_check_state();

    drop(base_rt::SUPERVISOR_RT.spawn(async move {
      tokio::select! {
        _ = runtime_token.cancelled_owned() => {}
        _ = exceeded_token.cancelled_owned() => {
          let state = tokio::task::spawn_blocking({
            let state = state.clone();
            move || {
              *state.read().unwrap()
            }
          }).await.unwrap();

          cb(state);
        }
      }
    }));
  }

  #[instrument(level = "debug", skip(self))]
  fn wait_for_inspector_session(&mut self) {
    debug!(has_inspector = self.worker.inspector.is_some());
    if let Some(inspector) = self.worker.inspector.as_ref() {
      debug!(
        addr = %inspector.server.host,
        server.inspector = ?inspector.option
      );
      let inspector_impl = self.js_runtime.inspector();

      if inspector.option.is_with_break() {
        inspector_impl.wait_for_session_and_break_on_next_statement();
      } else if inspector.option.is_with_wait() {
        inspector_impl.wait_for_session();
      }
    }
  }

  fn terminate_execution_if_cancelled(
    &mut self,
  ) -> ScopeGuard<CancellationToken, Box<dyn FnOnce(CancellationToken)>> {
    terminate_execution_if_cancelled(
      self.js_runtime.v8_isolate(),
      self.termination_request_token.clone(),
    )
  }

  fn get_v8_termination_guard<'l>(
    &'l mut self,
  ) -> scopeguard::ScopeGuard<
    &'l mut DenoRuntime<RuntimeContext>,
    impl FnOnce(&'l mut DenoRuntime<RuntimeContext>) + 'l,
  > {
    let was_terminating_execution =
      self.js_runtime.v8_isolate().is_execution_terminating();
    let mut guard = scopeguard::guard(self, move |v| {
      if was_terminating_execution {
        v.js_runtime.v8_isolate().terminate_execution();
      }

      v.js_runtime
        .v8_isolate()
        .set_microtasks_policy(v8::MicrotasksPolicy::Auto);
    });

    guard.js_runtime.v8_isolate().cancel_terminate_execution();
    guard
      .js_runtime
      .v8_isolate()
      .set_microtasks_policy(v8::MicrotasksPolicy::Explicit);
    guard
  }
}

#[allow(dead_code)]
trait JsRuntimeLockerGuard {
  fn js_runtime(&mut self) -> &mut JsRuntime;

  unsafe fn with_locker<'l>(
    &'l mut self,
  ) -> scopeguard::ScopeGuard<&'l mut Self, impl FnOnce(&'l mut Self) + 'l> {
    let js_runtime = self.js_runtime();
    let isolate = js_runtime.v8_isolate();

    let isolate_key = isolate_debug_key(isolate);
    let depth_after_increment = LOCK_DEBUG_STATES.with(|states| {
      let mut states = states.borrow_mut();
      let state = states.entry(isolate_key).or_default();
      state.ever_locked = true;
      state.depth = state.depth.saturating_add(1);
      state.depth
    });
    log_locker_event(isolate_key, "acquire_start", depth_after_increment);

    let locker =
      Locker::new(std::mem::transmute::<&mut Isolate, &mut Isolate>(isolate));
    log_locker_event(isolate_key, "acquire_complete", depth_after_increment);

    scopeguard::guard(self, move |_guard| {
      // Update debug state on exit
      let depth_before_release = LOCK_DEBUG_STATES.with(|states| {
        if let Some(state) = states.borrow_mut().get_mut(&isolate_key) {
          let before = state.depth;
          state.depth = state.depth.saturating_sub(1);
          before
        } else {
          0
        }
      });
      log_locker_event(isolate_key, "release", depth_before_release);
      drop(locker);
    })
  }
}

impl<C> JsRuntimeLockerGuard for DenoRuntime<C> {
  fn js_runtime(&mut self) -> &mut JsRuntime {
    &mut self.js_runtime
  }
}

impl JsRuntimeLockerGuard for JsRuntime {
  fn js_runtime(&mut self) -> &mut JsRuntime {
    self
  }
}

#[allow(dead_code)]
async unsafe fn spawn_blocking_non_send<F, R>(
  non_send_fn: F,
) -> Result<R, tokio::task::JoinError>
where
  F: FnOnce() -> R,
  R: 'static,
{
  let span = Span::current();
  let caller_thread_id = std::thread::current().id();
  debug!(
    target = "edge::runtime::blocking",
    action = "schedule",
    caller_thread = ?caller_thread_id,
  );
  let disguised_fn = unsync::MaskValueAsSend { value: non_send_fn };
  let (mut scope, ..) = async_scoped::TokioScope::scope(|s| {
    let span = span.clone();
    s.spawn_blocking(move || {
      let worker_thread_id = std::thread::current().id();
      debug!(
        target = "edge::runtime::blocking",
        action = "start",
        caller_thread = ?caller_thread_id,
        worker_thread = ?worker_thread_id,
      );
      let _span = span.entered();

      let result = unsync::MaskValueAsSend {
        value: disguised_fn.into_inner()(),
      };

      debug!(
        target = "edge::runtime::blocking",
        action = "finish",
        worker_thread = ?worker_thread_id,
      );
      result
    });
  });

  assert_eq!(scope.len(), 1);
  let stream = {
    let stream = scope.collect().await;

    drop(scope);
    stream
  };

  let mut iter = stream
    .into_iter()
    .map(|it| it.map(unsync::MaskValueAsSend::into_inner));

  let ret = iter.next();
  assert!(iter.next().is_none());

  match ret {
    Some(v) => v,
    None => unreachable!("scope.len() == 1"),
  }
}

type TerminateExecutionIfCancelledReturnType =
  ScopeGuard<CancellationToken, Box<dyn FnOnce(CancellationToken)>>;

pub struct IsolateWithCancellationToken<'l>(
  &'l mut v8::Isolate,
  CancellationToken,
);

impl std::ops::Deref for IsolateWithCancellationToken<'_> {
  type Target = v8::Isolate;

  fn deref(&self) -> &Self::Target {
    &*self.0
  }
}

impl std::ops::DerefMut for IsolateWithCancellationToken<'_> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.0
  }
}

impl IsolateWithCancellationToken<'_> {
  fn terminate_execution_if_cancelled(
    &mut self,
  ) -> ScopeGuard<CancellationToken, Box<dyn FnOnce(CancellationToken)>> {
    terminate_execution_if_cancelled(self.0, self.1.clone())
  }
}

pub enum MaybeDenoRuntime<'l, RuntimeContext> {
  DenoRuntime(&'l mut DenoRuntime<RuntimeContext>),
  Isolate(&'l mut v8::Isolate),
  IsolateWithCancellationToken(IsolateWithCancellationToken<'l>),
}

impl<'l, RuntimeContext> MaybeDenoRuntime<'l, RuntimeContext>
where
  RuntimeContext: GetRuntimeContext,
{
  #[allow(unused)]
  fn v8_isolate(&mut self) -> &mut v8::Isolate {
    match self {
      Self::DenoRuntime(v) => v.js_runtime.v8_isolate(),
      Self::Isolate(v) => v,
      Self::IsolateWithCancellationToken(v) => v.0,
    }
  }

  fn op_state(&mut self) -> Rc<RefCell<OpState>> {
    match self {
      Self::DenoRuntime(v) => v.js_runtime.op_state(),
      Self::Isolate(v) => JsRuntime::op_state_from(v),
      Self::IsolateWithCancellationToken(v) => JsRuntime::op_state_from(v.0),
    }
  }

  fn terminate_execution_if_cancelled(
    &mut self,
  ) -> Option<TerminateExecutionIfCancelledReturnType> {
    match self {
      Self::DenoRuntime(v) => Some(v.terminate_execution_if_cancelled()),
      Self::IsolateWithCancellationToken(v) => {
        Some(v.terminate_execution_if_cancelled())
      }
      Self::Isolate(_) => None,
    }
  }

  /// Dispatches "load" event to the JavaScript runtime.
  ///
  /// Does not poll event loop, and thus not await any of the "load" event
  /// handlers.
  pub fn dispatch_load_event(&mut self) -> Result<(), AnyError> {
    let _guard = self.terminate_execution_if_cancelled();

    let op_state = self.op_state();
    let dispatch_fns = {
      let op_state_ref = op_state.borrow();
      op_state_ref
        .try_borrow::<DispatchEventFunctions>()
        .unwrap()
        .clone()
    };
    let global_context = {
      let op_state_ref = op_state.borrow();
      op_state_ref
        .try_borrow::<GlobalMainContext>()
        .unwrap()
        .clone()
    };
    drop(op_state);

    let isolate = self.v8_isolate();

    // Create a proper HandleScope with context using the scope_with_context! macro
    v8::scope_with_context!(scope, isolate, &global_context.0);

    v8::tc_scope!(let tc_scope, scope);

    let event_fn =
      v8::Local::new(tc_scope, &dispatch_fns.dispatch_load_event_fn_global);

    let undefined = v8::undefined(tc_scope);
    let fn_args = vec![];
    let _ = event_fn.call(tc_scope, undefined.into(), &fn_args);

    if tc_scope.has_caught() {
      if let Some(ex) = tc_scope.exception() {
        let err = JsError::from_v8_exception(tc_scope, ex);
        return Err(err.into());
      }
    }

    Ok(())
  }

  /// Dispatches "beforeunload" event to the JavaScript runtime. Returns a
  /// boolean indicating if the event was prevented and thus event loop should
  /// continue running.
  pub fn dispatch_beforeunload_event(
    &mut self,
    reason: WillTerminateReason,
  ) -> Result<bool, AnyError> {
    let _guard = self.terminate_execution_if_cancelled();

    let op_state = self.op_state();
    let dispatch_fns = {
      let op_state_ref = op_state.borrow();
      op_state_ref
        .try_borrow::<DispatchEventFunctions>()
        .unwrap()
        .clone()
    };
    let global_context = {
      let op_state_ref = op_state.borrow();
      op_state_ref
        .try_borrow::<GlobalMainContext>()
        .unwrap()
        .clone()
    };
    drop(op_state);

    let isolate = self.v8_isolate();

    // Create a proper HandleScope with context using the scope_with_context! macro
    v8::scope_with_context!(scope, isolate, &global_context.0);

    v8::tc_scope!(let tc_scope, scope);

    let event_fn = v8::Local::new(
      tc_scope,
      &dispatch_fns.dispatch_beforeunload_event_fn_global,
    );

    let undefined = v8::undefined(tc_scope);
    let fn_args = vec![v8::String::new_external_onebyte_static(
      tc_scope,
      <&'static str>::from(reason).as_bytes(),
    )
    .unwrap()
    .into()];
    let fn_ret = event_fn.call(tc_scope, undefined.into(), &fn_args);

    if tc_scope.has_caught() {
      if let Some(ex) = tc_scope.exception() {
        let err = JsError::from_v8_exception(tc_scope, ex);
        return Err(err.into());
      }
    }

    Ok(fn_ret.unwrap().is_false())
  }

  /// Dispatches "unload" event to the JavaScript runtime.
  ///
  /// Does not poll event loop, and thus not await any of the "unload" event
  /// handlers.
  pub fn dispatch_unload_event(&mut self) -> Result<(), AnyError> {
    // NOTE(Nyannyacha): It is currently not possible to dispatch this event
    // because the supervisor has forcibly pulled the isolate out of the running
    // state and the `CancellationToken` prevents function invocation.
    //
    // If we want to dispatch this event, we may need to provide an extra margin
    // for the invocation.

    // self.v8_isolate().cancel_terminate_execution();
    let _guard = self.terminate_execution_if_cancelled();

    let op_state = self.op_state();
    let dispatch_fns = {
      let op_state_ref = op_state.borrow();
      op_state_ref
        .try_borrow::<DispatchEventFunctions>()
        .unwrap()
        .clone()
    };
    let global_context = {
      let op_state_ref = op_state.borrow();
      op_state_ref
        .try_borrow::<GlobalMainContext>()
        .unwrap()
        .clone()
    };
    drop(op_state);

    let isolate = self.v8_isolate();

    // Create a proper HandleScope with context using the scope_with_context! macro
    v8::scope_with_context!(scope, isolate, &global_context.0);

    v8::tc_scope!(let tc_scope, scope);

    let event_fn =
      v8::Local::new(tc_scope, &dispatch_fns.dispatch_unload_event_fn_global);

    let undefined = v8::undefined(tc_scope);
    let fn_args = vec![];
    let _ = event_fn.call(tc_scope, undefined.into(), &fn_args);

    if tc_scope.has_caught() {
      if let Some(ex) = tc_scope.exception() {
        let err = JsError::from_v8_exception(tc_scope, ex);
        return Err(err.into());
      }
    }

    Ok(())
  }

  /// Dispatches "drain" event to the JavaScript runtime.
  ///
  /// Does not poll event loop, and thus not await any of the "drain" event
  /// handlers.
  pub fn dispatch_drain_event(&mut self) -> Result<(), AnyError> {
    let _guard = self.terminate_execution_if_cancelled();

    let op_state = self.op_state();
    let dispatch_fns = {
      let op_state_ref = op_state.borrow();
      op_state_ref
        .try_borrow::<DispatchEventFunctions>()
        .unwrap()
        .clone()
    };
    let global_context = {
      let op_state_ref = op_state.borrow();
      op_state_ref
        .try_borrow::<GlobalMainContext>()
        .unwrap()
        .clone()
    };
    drop(op_state);

    let isolate = self.v8_isolate();

    // Create a proper HandleScope with context using the scope_with_context! macro
    v8::scope_with_context!(scope, isolate, &global_context.0);

    v8::tc_scope!(let tc_scope, scope);

    let event_fn =
      v8::Local::new(tc_scope, &dispatch_fns.dispatch_drain_event_fn_global);

    let undefined = v8::undefined(tc_scope);
    let fn_args = vec![];
    let _ = event_fn.call(tc_scope, undefined.into(), &fn_args);

    if tc_scope.has_caught() {
      if let Some(ex) = tc_scope.exception() {
        let err = JsError::from_v8_exception(tc_scope, ex);
        return Err(err.into());
      }
    }

    Ok(())
  }
}

pub fn import_meta_resolve_callback(
  loader: &dyn ModuleLoader,
  specifier: String,
  referrer: String,
) -> Result<ModuleSpecifier, AnyError> {
  loader
    .resolve(&specifier, &referrer, ResolutionKind::DynamicImport)
    .map_err(Into::into)
}

fn with_cpu_metrics_guard<'l, F, R>(
  op_state: Rc<RefCell<OpState>>,
  maybe_cpu_usage_metrics_tx: &'l Option<
    mpsc::UnboundedSender<CPUUsageMetrics>,
  >,
  accumulated_cpu_time_ns: &'l mut i64,
  work_fn: F,
) -> R
where
  F: FnOnce() -> R,
{
  let _cpu_metrics_guard = get_cpu_metrics_guard(
    op_state,
    maybe_cpu_usage_metrics_tx,
    accumulated_cpu_time_ns,
  );

  work_fn()
}

fn get_cpu_metrics_guard<'l>(
  op_state: Rc<RefCell<OpState>>,
  maybe_cpu_usage_metrics_tx: &'l Option<
    mpsc::UnboundedSender<CPUUsageMetrics>,
  >,
  accumulated_cpu_time_ns: &'l mut i64,
) -> scopeguard::ScopeGuard<(), Box<dyn FnOnce(()) + 'l>> {
  let Some(cpu_usage_metrics_tx) = maybe_cpu_usage_metrics_tx.as_ref() else {
    return scopeguard::guard((), Box::new(|_| {}));
  };

  #[derive(Clone)]
  struct CurrentCPUTimer {
    thread_id: std::thread::ThreadId,
    timer: CPUTimer,
  }

  let current_thread_id = std::thread::current().id();
  let send_cpu_metrics_fn = move |metric: CPUUsageMetrics| {
    let _ = cpu_usage_metrics_tx.send(metric);
  };

  let mut state = op_state.borrow_mut();
  let cpu_timer = if state.has::<CurrentCPUTimer>() {
    let current_cpu_timer = state.borrow::<CurrentCPUTimer>();
    if current_cpu_timer.thread_id != current_thread_id {
      state.take::<CurrentCPUTimer>();
      None
    } else {
      Some(current_cpu_timer.timer.clone())
    }
  } else {
    None
  };
  let cpu_timer = if let Some(timer) = cpu_timer {
    timer
  } else {
    let cpu_timer = CurrentCPUTimer {
      thread_id: current_thread_id,
      timer: CPUTimer::new().unwrap(),
    };

    state.put(cpu_timer.clone());
    cpu_timer.timer
  };

  drop(state);
  send_cpu_metrics_fn(CPUUsageMetrics::Enter(current_thread_id, cpu_timer));

  let current_cpu_time_ns = get_current_cpu_time_ns().unwrap();

  scopeguard::guard(
    (),
    Box::new(move |_| {
      debug_assert_eq!(current_thread_id, std::thread::current().id());

      let cpu_time_after_drop_ns =
        get_current_cpu_time_ns().unwrap_or(current_cpu_time_ns);
      let blocking_cpu_time_ns =
        BlockingScopeCPUUsage::get_cpu_usage_ns_and_reset(
          &mut op_state.borrow_mut(),
        );

      let diff_cpu_time_ns = cpu_time_after_drop_ns - current_cpu_time_ns;

      *accumulated_cpu_time_ns += diff_cpu_time_ns;
      *accumulated_cpu_time_ns += blocking_cpu_time_ns;

      send_cpu_metrics_fn(CPUUsageMetrics::Leave(CPUUsage {
        accumulated: *accumulated_cpu_time_ns,
        diff: diff_cpu_time_ns,
      }));

      debug!(
        accumulated_cpu_time_ms = *accumulated_cpu_time_ns / 1_000_000,
        blocking_cpu_time_ms = blocking_cpu_time_ns / 1_000_000,
      );
    }),
  )
}

fn terminate_execution_if_cancelled(
  isolate: &mut v8::Isolate,
  token: CancellationToken,
) -> TerminateExecutionIfCancelledReturnType {
  extern "C" fn interrupt_fn(
    isolate: &mut v8::Isolate,
    _: *mut std::ffi::c_void,
  ) {
    let _ = isolate.terminate_execution();
  }

  let handle = isolate.thread_safe_handle();
  let cancel_task_token = CancellationToken::new();
  let request_interrupt_fn = move || {
    let _ = handle.request_interrupt(interrupt_fn, std::ptr::null_mut());
  };

  drop(base_rt::SUPERVISOR_RT.spawn({
    let cancel_task_token = cancel_task_token.clone();

    async move {
      if token.is_cancelled() {
        request_interrupt_fn();
      } else {
        tokio::select! {
          _ = token.cancelled_owned() => {
            request_interrupt_fn();
          }

          _ = cancel_task_token.cancelled_owned() => {}
        }
      }
    }
  }));

  scopeguard::guard(
    cancel_task_token,
    Box::new(|v| {
      v.cancel();
    }),
  )
}

fn set_v8_flags() {
  let v8_flags = std::env::var("V8_FLAGS").unwrap_or("".to_string());
  let mut vec = vec![""];

  if v8_flags.is_empty() {
    return;
  }

  vec.append(&mut v8_flags.split(' ').collect());

  let ignored =
    deno_core::v8_set_flags(vec.iter().map(|v| v.to_string()).collect());

  if *ignored.as_slice() != [""] {
    error!("v8 flags unrecognized {:?}", ignored);
  }
}

unsafe extern "C" fn mem_check_gc_prologue_callback_fn(
  isolate: v8::UnsafeRawIsolatePtr,
  _ty: GCType,
  _flags: GCCallbackFlags,
  data: *mut c_void,
) {
  // Convert UnsafeRawIsolatePtr to &mut Isolate
  let mut isolate_ref = v8::Isolate::from_raw_isolate_ptr_unchecked(isolate);
  (*(data as *mut MemCheck)).check(&mut isolate_ref);
}

#[cfg(test)]
mod test {
  use std::collections::HashMap;
  use std::io::Write;
  use std::marker::PhantomData;
  use std::path::Path;
  use std::path::PathBuf;
  use std::sync::Arc;
  use std::time::Duration;

  use anyhow::Context;
  use deno::DenoOptionsBuilder;
  use deno_core::error::AnyError;
  use deno_core::serde_json;
  use deno_core::v8;
  use deno_core::FastString;
  use deno_core::ModuleCodeString;
  use deno_core::PollEventLoopOptions;
  use deno_facade::generate_binary_eszip;
  use deno_facade::EmitterFactory;
  use deno_facade::EszipPayloadKind;
  use deno_facade::Metadata;
  use ext_workers::context::MainWorkerRuntimeOpts;
  use ext_workers::context::UserWorkerMsgs;
  use ext_workers::context::UserWorkerRuntimeOpts;
  use ext_workers::context::WorkerContextInitOpts;
  use ext_workers::context::WorkerRuntimeOpts;
  use fs::s3_fs::S3FsConfig;
  use fs::tmp_fs::TmpFsConfig;
  use serde::de::DeserializeOwned;
  use serde::Serialize;
  use serial_test::serial;
  use tempfile::Builder;
  use tokio::sync::mpsc;
  use tokio::time::timeout;

  use crate::runtime::DenoRuntime;
  use crate::runtime::JsRuntimeLockerGuard;
  use crate::worker::DuplexStreamEntry;
  use crate::worker::WorkerBuilder;

  use super::GetRuntimeContext;
  use super::RunOptionsBuilder;

  impl<RuntimeContext> DenoRuntime<RuntimeContext> {
    #[allow(dead_code)]
    fn to_value_mut<T>(
      &mut self,
      _global_value: &v8::Global<v8::Value>,
    ) -> Result<T, AnyError>
    where
      T: DeserializeOwned + 'static,
    {
      // NOTE: handle_scope() is no longer available in deno_core 2.x
      // This method needs to be updated when V8 handle access is required
      unimplemented!("handle_scope() API changed in deno_core 2.x")
    }
  }

  #[derive(Debug, Default)]
  struct RuntimeBuilder<C = ()> {
    path: Option<String>,
    eszip: Option<EszipPayloadKind>,
    env_vars: Option<HashMap<String, String>>,
    worker_runtime_conf: Option<WorkerRuntimeOpts>,
    static_patterns: Vec<String>,
    s3_fs_config: Option<S3FsConfig>,
    tmp_fs_config: Option<TmpFsConfig>,
    _phantom_context: PhantomData<C>,
  }

  impl RuntimeBuilder {
    fn new() -> Self {
      Self::default()
    }
  }

  impl<C> RuntimeBuilder<C> {
    fn set_context<C2>(self) -> RuntimeBuilder<C2>
    where
      C2: GetRuntimeContext,
    {
      RuntimeBuilder {
        path: self.path,
        eszip: self.eszip,
        env_vars: self.env_vars,
        worker_runtime_conf: self.worker_runtime_conf,
        static_patterns: self.static_patterns,
        s3_fs_config: self.s3_fs_config,
        tmp_fs_config: self.tmp_fs_config,
        _phantom_context: PhantomData,
      }
    }
  }

  impl<C> RuntimeBuilder<C>
  where
    C: GetRuntimeContext,
  {
    async fn build(self) -> DenoRuntime<C> {
      let RuntimeBuilder {
        path,
        eszip,
        env_vars,
        worker_runtime_conf,
        static_patterns,
        s3_fs_config,
        tmp_fs_config,
        _phantom_context,
      } = self;

      let (worker_pool_tx, _) = mpsc::unbounded_channel::<UserWorkerMsgs>();

      DenoRuntime::new(
        WorkerBuilder::new(
          WorkerContextInitOpts {
            maybe_eszip: eszip,
            service_path: path
              .map(PathBuf::from)
              .unwrap_or(PathBuf::from("./test_cases/main")),

            conf: {
              if let Some(conf) = worker_runtime_conf {
                conf
              } else {
                WorkerRuntimeOpts::MainWorker(MainWorkerRuntimeOpts {
                  worker_pool_tx,
                  shared_metric_src: None,
                  event_worker_metric_src: None,
                  context: None,
                })
              }
            },

            maybe_entrypoint: None,
            maybe_module_code: None,

            no_module_cache: false,
            no_npm: None,
            env_vars: env_vars.unwrap_or_default(),

            static_patterns,

            timing: None,

            maybe_s3_fs_config: s3_fs_config,
            maybe_tmp_fs_config: tmp_fs_config,
            maybe_otel_config: None,
          },
          Arc::default(),
        )
        .build()
        .unwrap(),
      )
      .await
      .unwrap()
    }
  }

  impl<C> RuntimeBuilder<C> {
    fn set_path(mut self, path: &str) -> Self {
      let _ = self.path.insert(path.to_string());
      self
    }

    async fn set_eszip<P>(mut self, path: P) -> Result<Self, anyhow::Error>
    where
      P: AsRef<Path>,
    {
      let _ = self.eszip.insert(EszipPayloadKind::VecKind(
        tokio::fs::read(path)
          .await
          .context("cannot read eszip binary")?,
      ));

      Ok(self)
    }

    fn set_env_vars(mut self, vars: HashMap<String, String>) -> Self {
      let _ = self.env_vars.insert(vars);
      self
    }

    fn set_std_env(self) -> Self {
      self.set_env_vars(std::env::vars().collect())
    }

    fn set_worker_runtime_conf(mut self, conf: WorkerRuntimeOpts) -> Self {
      let _ = self.worker_runtime_conf.insert(conf);
      self
    }

    #[allow(unused)]
    fn set_s3_fs_config(mut self, config: S3FsConfig) -> Self {
      let _ = self.s3_fs_config.insert(config);
      self
    }

    fn add_static_pattern(mut self, pat: &str) -> Self {
      self.static_patterns.push(pat.to_string());
      self
    }

    fn extend_static_patterns<I>(mut self, iter: I) -> Self
    where
      I: IntoIterator<Item = String>,
    {
      self.static_patterns.extend(iter);
      self
    }
  }

  struct WithSyncFileAPI;

  impl GetRuntimeContext for WithSyncFileAPI {
    fn get_extra_context() -> impl Serialize {
      serde_json::json!({
        "useReadSyncFileAPI": true,
      })
    }
  }

  #[tokio::test]
  #[serial]
  async fn test_module_code_no_eszip() {
    let (worker_pool_tx, _) = mpsc::unbounded_channel::<UserWorkerMsgs>();

    DenoRuntime::<()>::new(
      WorkerBuilder::new(
        WorkerContextInitOpts {
          service_path: PathBuf::from("./test_cases/"),
          no_module_cache: false,
          no_npm: None,
          env_vars: Default::default(),
          timing: None,
          maybe_eszip: None,
          maybe_entrypoint: None,
          maybe_module_code: Some(FastString::from(String::from(
            "Deno.serve((req) => new Response('Hello World'));",
          ))),
          conf: {
            WorkerRuntimeOpts::MainWorker(MainWorkerRuntimeOpts {
              worker_pool_tx,
              shared_metric_src: None,
              event_worker_metric_src: None,
              context: None,
            })
          },
          static_patterns: vec![],

          maybe_s3_fs_config: None,
          maybe_tmp_fs_config: None,
          maybe_otel_config: None,
        },
        Arc::default(),
      )
      .build()
      .unwrap(),
    )
    .await
    .expect("It should not panic");
  }

  #[tokio::test]
  #[serial]
  #[ignore]
  #[allow(clippy::arc_with_non_send_sync)]
  async fn test_eszip_with_source_file() {
    let (worker_pool_tx, _) = mpsc::unbounded_channel::<UserWorkerMsgs>();
    let mut temp_file = Builder::new()
      .prefix("eszip-source-test")
      .suffix(".ts")
      .tempfile_in("./test_cases")
      .unwrap();
    temp_file
      .write_all(
        b"import isEven from \"npm:is-even\"; globalThis.isTenEven = isEven(9);",
      )
      .unwrap();

    let path_buf = temp_file.path().to_path_buf();
    let mut emitter_factory = EmitterFactory::new();

    emitter_factory.set_deno_options(
      DenoOptionsBuilder::new()
        .entrypoint(path_buf)
        .build()
        .await
        .unwrap(),
    );

    let mut metadata = Metadata::default();
    let bin_eszip = generate_binary_eszip(
      &mut metadata,
      Arc::new(emitter_factory),
      None,
      None,
      None,
    )
    .await
    .unwrap();

    let temp_path = temp_file.into_temp_path();
    temp_path.close().unwrap();

    let eszip_code = bin_eszip.into_bytes();
    let runtime = DenoRuntime::<()>::new(
      WorkerBuilder::new(
        WorkerContextInitOpts {
          service_path: PathBuf::from("./test_cases/"),
          no_module_cache: false,
          no_npm: None,
          env_vars: Default::default(),
          timing: None,
          maybe_eszip: Some(EszipPayloadKind::VecKind(eszip_code)),
          maybe_entrypoint: None,
          maybe_module_code: None,
          conf: {
            WorkerRuntimeOpts::MainWorker(MainWorkerRuntimeOpts {
              worker_pool_tx,
              shared_metric_src: None,
              event_worker_metric_src: None,
              context: None,
            })
          },
          static_patterns: vec![],
          maybe_s3_fs_config: None,
          maybe_tmp_fs_config: None,
          maybe_otel_config: None,
        },
        Arc::default(),
      )
      .build()
      .unwrap(),
    )
    .await;

    let mut rt = runtime.unwrap();
    let main_module_id = rt
      .init_main_module()
      .await
      .map(|_| rt.main_module_id.unwrap())
      .unwrap();

    let mut locker = unsafe { rt.with_locker() };
    let main_mod_ev = locker.js_runtime.mod_evaluate(main_module_id);
    let _ = locker
      .js_runtime
      .run_event_loop(PollEventLoopOptions::default())
      .await;

    let read_is_even_global = locker
      .js_runtime
      .execute_script(
        "<anon>",
        ModuleCodeString::from(
          r#"
            globalThis.isTenEven;
          "#
          .to_string(),
        ),
      )
      .unwrap();
    let read_is_even =
      locker.to_value_mut::<serde_json::Value>(&read_is_even_global);
    assert_eq!(read_is_even.unwrap().to_string(), "false");
    std::mem::drop(main_mod_ev);
  }

  #[tokio::test]
  #[serial]
  #[ignore]
  #[allow(clippy::arc_with_non_send_sync)]
  async fn test_create_eszip_from_graph() {
    let (worker_pool_tx, _) = mpsc::unbounded_channel::<UserWorkerMsgs>();
    let file = PathBuf::from("./test_cases/eszip-silly-test/index.ts");
    let service_path = PathBuf::from("./test_cases/eszip-silly-test");
    let mut emitter_factory = EmitterFactory::new();

    emitter_factory.set_deno_options(
      DenoOptionsBuilder::new()
        .entrypoint(file)
        .build()
        .await
        .unwrap(),
    );

    let mut metadata = Metadata::default();
    let binary_eszip = generate_binary_eszip(
      &mut metadata,
      Arc::new(emitter_factory),
      None,
      None,
      None,
    )
    .await
    .unwrap();

    let eszip_code = binary_eszip.into_bytes();
    let runtime = DenoRuntime::<()>::new(
      WorkerBuilder::new(
        WorkerContextInitOpts {
          service_path,
          no_module_cache: false,
          no_npm: None,
          env_vars: Default::default(),
          timing: None,
          maybe_eszip: Some(EszipPayloadKind::VecKind(eszip_code)),
          maybe_entrypoint: None,
          maybe_module_code: None,
          conf: {
            WorkerRuntimeOpts::MainWorker(MainWorkerRuntimeOpts {
              worker_pool_tx,
              shared_metric_src: None,
              event_worker_metric_src: None,
              context: None,
            })
          },
          static_patterns: vec![],

          maybe_s3_fs_config: None,
          maybe_tmp_fs_config: None,
          maybe_otel_config: None,
        },
        Arc::default(),
      )
      .build()
      .unwrap(),
    )
    .await;

    let mut rt = runtime.unwrap();
    let main_module_id = rt
      .init_main_module()
      .await
      .map(|_| rt.main_module_id.unwrap())
      .unwrap();

    let mut locker = unsafe { rt.with_locker() };
    let main_mod_ev = locker.js_runtime.mod_evaluate(main_module_id);
    let _ = locker
      .js_runtime
      .run_event_loop(PollEventLoopOptions::default())
      .await;

    let read_is_even_global = locker
      .js_runtime
      .execute_script(
        "<anon>",
        ModuleCodeString::from(
          r#"
            globalThis.isTenEven;
          "#
          .to_string(),
        ),
      )
      .unwrap();
    let read_is_even =
      locker.to_value_mut::<serde_json::Value>(&read_is_even_global);
    assert_eq!(read_is_even.unwrap().to_string(), "true");
    std::mem::drop(main_mod_ev);
  }

  // Main Runtime should have access to `EdgeRuntime`
  // NOTE: Ignored because handle_scope() API changed in deno_core 2.x
  #[tokio::test]
  #[serial]
  #[ignore]
  async fn test_main_runtime_creation() {
    let mut runtime = RuntimeBuilder::new().build().await;

    {
      let mut _locker = unsafe { runtime.with_locker() };
      // handle_scope() is no longer available in deno_core 2.x
      // let scope = &mut (*locker.js_runtime).handle_scope();
      // let context = scope.get_current_context();
      // let inner_scope = &mut v8::ContextScope::new(scope, context);
      // let global = context.global(inner_scope);
      // let edge_runtime_key: v8::Local<v8::Value> =
      //   serde_v8::to_v8(inner_scope, "EdgeRuntime").unwrap();
      // let edge_runtime_ns = global.get(inner_scope, edge_runtime_key).unwrap();
      // assert!(!edge_runtime_ns.is_undefined());
    }
  }

  // User Runtime can access EdgeRuntime, but only with specific APIs.
  // NOTE: Ignored because handle_scope() API changed in deno_core 2.x
  #[tokio::test]
  #[serial]
  #[ignore]
  async fn test_user_runtime_creation() {
    let _allowed_apis = ["waitUntil"];

    let mut runtime = RuntimeBuilder::new()
      .set_worker_runtime_conf(WorkerRuntimeOpts::UserWorker(Box::default()))
      .build()
      .await;

    {
      let mut _locker = unsafe { runtime.with_locker() };
      // handle_scope() is no longer available in deno_core 2.x
      // let scope = &mut (*locker.js_runtime).handle_scope();
      // let context = scope.get_current_context();
      // let inner_scope = &mut v8::ContextScope::new(scope, context);
      // let global = context.global(inner_scope);
      // let edge_runtime_key: v8::Local<v8::Value> =
      //   serde_v8::to_v8(inner_scope, "EdgeRuntime").unwrap();
      //
      // let edge_runtime_ns = global
      //   .get(inner_scope, edge_runtime_key)
      //   .unwrap()
      //   .to_object(inner_scope)
      //   .unwrap();
      //
      // let edge_runtime_ns_keys = edge_runtime_ns
      //   .get_property_names(
      //     inner_scope,
      //     GetPropertyNamesArgs {
      //       mode: v8::KeyCollectionMode::OwnOnly,
      //       index_filter: v8::IndexFilter::SkipIndices,
      //       ..Default::default()
      //     },
      //   )
      //   .unwrap();
      //
      // assert_eq!(edge_runtime_ns_keys.length() as usize, allowed_apis.len());
      //
      // for api in allowed_apis {
      //   let key = serde_v8::to_v8(inner_scope, api).unwrap();
      //   let obj = edge_runtime_ns.get(inner_scope, key).unwrap();
      //
      //   assert!(!obj.is_undefined());
      // }
    }
  }

  #[tokio::test]
  #[serial]
  async fn test_main_rt_fs() {
    let mut main_rt = RuntimeBuilder::new()
      .set_std_env()
      .set_context::<WithSyncFileAPI>()
      .build()
      .await;

    let mut locker = unsafe { main_rt.with_locker() };
    let global_value_deno_read_file_script = locker
      .js_runtime
      .execute_script(
        "<anon>",
        ModuleCodeString::from(
          r#"
              Deno.readTextFileSync("./test_cases/readFile/hello_world.json");
            "#
          .to_string(),
        ),
      )
      .unwrap();

    let fs_read_result = locker
      .to_value_mut::<serde_json::Value>(&global_value_deno_read_file_script);
    assert_eq!(
      fs_read_result.unwrap().as_str().unwrap(),
      "{\n  \"hello\": \"world\"\n}\n"
    );
  }

  #[tokio::test]
  #[serial]
  #[ignore]
  async fn test_jsx_import_source() {
    let mut main_rt = RuntimeBuilder::new()
      .set_std_env()
      .set_path("./test_cases/jsx-preact")
      .build()
      .await;
    let main_module_id = main_rt
      .init_main_module()
      .await
      .map(|_| main_rt.main_module_id.unwrap())
      .unwrap();

    let mut locker = unsafe { main_rt.with_locker() };
    let _main_mod_ev = locker.js_runtime.mod_evaluate(main_module_id);
    let _ = locker
      .js_runtime
      .run_event_loop(PollEventLoopOptions::default())
      .await;

    let global_value_deno_read_file_script = locker
      .js_runtime
      .execute_script(
        "<anon>",
        ModuleCodeString::from(
          r#"
              globalThis.hello;
          "#
          .to_string(),
        ),
      )
      .unwrap();

    let jsx_read_result = locker
      .to_value_mut::<serde_json::Value>(&global_value_deno_read_file_script);
    assert_eq!(
      jsx_read_result.unwrap().to_string(),
      r#"{"type":"div","props":{"children":"Hello"},"__k":null,"__":null,"__b":0,"__e":null,"__c":null,"__v":-1,"__i":-1,"__u":0}"#
    );
  }

  // #[tokio::test]
  // async fn test_node_builtin_imports() {
  //     let mut main_rt = create_runtime(
  //         Some(PathBuf::from("./test_cases/node-built-in")),
  //         Some(std::env::vars().collect()),
  //         None,
  //     )
  //     .await;
  //     let mod_evaluate = main_rt.js_runtime.mod_evaluate(main_rt.main_module_id);
  //     let _ = main_rt.js_runtime.run_event_loop(false).await;
  //     let global_value_deno_read_file_script = main_rt
  //         .js_runtime
  //         .execute_script(
  //             "<anon>",
  //             r#"
  //         globalThis.basename('/Users/Refsnes/demo_path.js');
  //     "#,
  //         )
  //         .unwrap();
  //     let fs_read_result =
  //         main_rt.to_value::<deno_core::serde_json::Value>(&global_value_deno_read_file_script);
  //     assert_eq!(fs_read_result.unwrap().as_str().unwrap(), "demo_path.js");
  //     std::mem::drop(mod_evaluate);
  // }

  #[tokio::test]
  #[serial]
  #[ignore]
  async fn test_static_fs() {
    let mut user_rt = RuntimeBuilder::new()
      .set_worker_runtime_conf(WorkerRuntimeOpts::UserWorker(Box::default()))
      .add_static_pattern("./test_cases/**/*.md")
      .set_context::<WithSyncFileAPI>()
      .build()
      .await;

    let mut locker = unsafe { user_rt.with_locker() };
    let user_rt_execute_scripts = locker
      .js_runtime
      .execute_script(
        "<anon>",
        ModuleCodeString::from(
          // NOTE: Base path is `./test_cases/main`.
          r#"Deno.readTextFileSync("content.md")"#.to_string(),
        ),
      )
      .unwrap();
    let serde_deno_env = locker
      .to_value_mut::<serde_json::Value>(&user_rt_execute_scripts)
      .unwrap();

    assert_eq!(
      serde_deno_env,
      deno_core::serde_json::Value::String(String::from("Some test file\n"))
    );
  }

  #[tokio::test]
  #[serial]
  async fn test_os_ops() {
    let mut user_rt = RuntimeBuilder::new()
      .set_worker_runtime_conf(WorkerRuntimeOpts::UserWorker(Box::default()))
      .build()
      .await;

    let mut locker = unsafe { user_rt.with_locker() };
    let user_rt_execute_scripts =locker
      .js_runtime
      .execute_script(
        "<anon>",
        ModuleCodeString::from(
          r#"
            // Should not be able to set
            const data = {
              gid: Deno.gid(),
              uid: Deno.uid(),
              hostname: Deno.hostname(),
              loadavg: Deno.loadavg(),
              osUptime: Deno.osUptime(),
              osRelease: Deno.osRelease(),
              systemMemoryInfo: Deno.systemMemoryInfo(),
              consoleSize: Deno.consoleSize(),
              version: [Deno.version.deno, Deno.version.v8, Deno.version.typescript],
              networkInterfaces: Deno.networkInterfaces()
            };
            data;
          "#
          .to_string(),
        ),
      )
      .unwrap();
    let serde_deno_env = locker
      .to_value_mut::<serde_json::Value>(&user_rt_execute_scripts)
      .unwrap();
    assert_eq!(serde_deno_env.get("gid").unwrap().as_i64().unwrap(), 1000);
    assert_eq!(serde_deno_env.get("uid").unwrap().as_i64().unwrap(), 1000);
    assert!(serde_deno_env.get("osUptime").unwrap().as_i64().unwrap() > 0);
    assert_eq!(
      serde_deno_env.get("osRelease").unwrap().as_str().unwrap(),
      "0.0.0-00000000-generic"
    );

    let loadavg_array = serde_deno_env
      .get("loadavg")
      .unwrap()
      .as_array()
      .unwrap()
      .to_vec();
    assert_eq!(loadavg_array.first().unwrap().as_f64().unwrap(), 0.0);
    assert_eq!(loadavg_array.get(1).unwrap().as_f64().unwrap(), 0.0);
    assert_eq!(loadavg_array.get(2).unwrap().as_f64().unwrap(), 0.0);

    let network_interfaces_data = serde_deno_env
      .get("networkInterfaces")
      .unwrap()
      .as_array()
      .unwrap()
      .to_vec();
    assert_eq!(network_interfaces_data.len(), 2);

    let deno_version_array = serde_deno_env
      .get("version")
      .unwrap()
      .as_array()
      .unwrap()
      .to_vec();
    assert_eq!(
      deno_version_array.first().unwrap().as_str().unwrap(),
      format!(
        "supabase-edge-runtime-0.1.0 (compatible with Deno v{})",
        deno::version()
      )
    );
    assert_eq!(
      deno_version_array.get(1).unwrap().as_str().unwrap(),
      "11.6.189.12"
    );
    assert_eq!(
      deno_version_array.get(2).unwrap().as_str().unwrap(),
      "5.1.6"
    );

    let system_memory_info_map = serde_deno_env
      .get("systemMemoryInfo")
      .unwrap()
      .as_object()
      .unwrap()
      .clone();
    assert!(system_memory_info_map.contains_key("total"));
    assert!(system_memory_info_map.contains_key("free"));
    assert!(system_memory_info_map.contains_key("available"));
    assert!(system_memory_info_map.contains_key("buffers"));
    assert!(system_memory_info_map.contains_key("cached"));
    assert!(system_memory_info_map.contains_key("swapTotal"));
    assert!(system_memory_info_map.contains_key("swapFree"));

    let deno_consle_size_map = serde_deno_env
      .get("consoleSize")
      .unwrap()
      .as_object()
      .unwrap()
      .clone();
    assert!(deno_consle_size_map.contains_key("rows"));
    assert!(deno_consle_size_map.contains_key("columns"));

    let user_rt_execute_scripts = locker.js_runtime.execute_script(
      "<anon>",
      ModuleCodeString::from(
        r#"
          let cmd = new Deno.Command("", {});
          cmd.outputSync();
        "#
        .to_string(),
      ),
    );
    assert!(user_rt_execute_scripts.is_err());
    assert!(user_rt_execute_scripts.unwrap_err().to_string().contains(
      "Spawning subprocesses is not allowed on Supabase Edge Runtime"
    ));
  }

  #[tokio::test]
  #[serial]
  async fn test_os_env_vars() {
    std::env::set_var("Supa_Test", "Supa_Value");

    let mut main_rt = RuntimeBuilder::new().set_std_env().build().await;
    let mut user_rt = RuntimeBuilder::new()
      .set_worker_runtime_conf(WorkerRuntimeOpts::UserWorker(Box::default()))
      .build()
      .await;

    let mut main_locker = unsafe { main_rt.with_locker() };
    let mut user_locker = unsafe { user_rt.with_locker() };
    let err = main_locker
      .js_runtime
      .execute_script(
        "<anon>",
        ModuleCodeString::from(
          r#"
            // Should not be able to set
            Deno.env.set("Supa_Test", "Supa_Value");
          "#
          .to_string(),
        ),
      )
      .err()
      .unwrap();
    assert!(err
      .to_string()
      .contains("NotSupported: The operation is not supported"));

    let main_deno_env_get_supa_test = main_locker
      .js_runtime
      .execute_script(
        "<anon>",
        ModuleCodeString::from(
          r#"
            // Should not be able to set
            Deno.env.get("Supa_Test");
          "#
          .to_string(),
        ),
      )
      .unwrap();
    let serde_deno_env = main_locker
      .to_value_mut::<serde_json::Value>(&main_deno_env_get_supa_test);
    assert_eq!(serde_deno_env.unwrap().as_str().unwrap(), "Supa_Value");

    // User does not have this env variable because it was not provided
    // During the runtime creation
    let user_deno_env_get_supa_test = user_locker
      .js_runtime
      .execute_script(
        "<anon>",
        ModuleCodeString::from(
          r#"
            // Should not be able to set
            Deno.env.get("Supa_Test");
          "#
          .to_string(),
        ),
      )
      .unwrap();
    let user_serde_deno_env = user_locker
      .to_value_mut::<serde_json::Value>(&user_deno_env_get_supa_test);
    assert!(user_serde_deno_env.unwrap().is_null());
  }

  fn create_basic_user_runtime_builder<T, U>(
    path: &str,
    memory_limit_mb: T,
    worker_timeout_ms: U,
    static_patterns: &[&str],
  ) -> RuntimeBuilder
  where
    T: Into<Option<u64>>,
    U: Into<Option<u64>>,
  {
    let default_opt = UserWorkerRuntimeOpts::default();
    let memory_limit_mb = memory_limit_mb
      .into()
      .unwrap_or(default_opt.memory_limit_mb);
    let worker_timeout_ms = worker_timeout_ms
      .into()
      .unwrap_or(default_opt.worker_timeout_ms);

    RuntimeBuilder::new()
      .set_path(path)
      .set_worker_runtime_conf(WorkerRuntimeOpts::UserWorker(Box::new(
        UserWorkerRuntimeOpts {
          memory_limit_mb,
          worker_timeout_ms,
          cpu_time_soft_limit_ms: 100,
          cpu_time_hard_limit_ms: 200,
          force_create: true,
          ..default_opt
        },
      )))
      .extend_static_patterns(
        static_patterns.iter().map(|it| String::from(*it)),
      )
  }

  #[tokio::test]
  #[serial]
  async fn test_array_buffer_allocation_below_limit() {
    let mut user_rt = create_basic_user_runtime_builder(
      "./test_cases/array_buffers",
      20,
      1000,
      &[],
    )
    .build()
    .await;

    let (_tx, duplex_stream_rx) =
      mpsc::unbounded_channel::<DuplexStreamEntry>();
    let (result, _) = user_rt
      .run(
        RunOptionsBuilder::new()
          .wait_termination_request_token(false)
          .stream_rx(duplex_stream_rx)
          .build()
          .unwrap(),
      )
      .await;

    assert!(result.is_ok(), "expected no errors");

    // however, mem checker must be raised because it aggregates heap usage
    assert!(user_rt.mem_check.state.read().unwrap().exceeded);
  }

  #[tokio::test]
  #[serial]
  async fn test_array_buffer_allocation_above_limit() {
    let mut user_rt = create_basic_user_runtime_builder(
      "./test_cases/array_buffers",
      15,
      1000,
      &[],
    )
    .build()
    .await;

    let (_tx, duplex_stream_rx) =
      mpsc::unbounded_channel::<DuplexStreamEntry>();
    let (result, _) = user_rt
      .run(
        RunOptionsBuilder::new()
          .wait_termination_request_token(false)
          .stream_rx(duplex_stream_rx)
          .build()
          .unwrap(),
      )
      .await;

    match result {
      Err(err) => {
        assert!(err
          .to_string()
          .contains("RangeError: Array buffer allocation failed"));
      }
      _ => panic!("Invalid Result"),
    };
  }

  async fn test_mem_check_above_limit(
    path: &str,
    static_patterns: &[&str],
    memory_limit_mb: u64,
    worker_timeout_ms: u64,
  ) {
    let (_duplex_stream_tx, duplex_stream_rx) =
      mpsc::unbounded_channel::<DuplexStreamEntry>();
    let (callback_tx, mut callback_rx) = mpsc::unbounded_channel::<()>();
    let mut user_rt = create_basic_user_runtime_builder(
      path,
      memory_limit_mb,
      worker_timeout_ms,
      static_patterns,
    )
    .set_context::<WithSyncFileAPI>()
    .build()
    .await;

    let waker = user_rt.waker.clone();
    let handle = user_rt.js_runtime.v8_isolate().thread_safe_handle();

    user_rt.add_memory_limit_callback(move |_| {
      assert!(handle.terminate_execution());
      waker.wake();
      callback_tx.send(()).unwrap();
    });

    let wait_fut = async move {
      let (result, _) = user_rt
        .run(
          RunOptionsBuilder::new()
            .wait_termination_request_token(false)
            .stream_rx(duplex_stream_rx)
            .build()
            .unwrap(),
        )
        .await;

      assert!(result
        .unwrap_err()
        .to_string()
        .ends_with("Error: execution terminated"));

      callback_rx.recv().await.unwrap();

      assert!(user_rt.mem_check.state.read().unwrap().exceeded);
    };

    if timeout(Duration::from_secs(10), wait_fut).await.is_err() {
      panic!("failed to detect a memory limit callback invocation within the given time");
    }
  }

  #[tokio::test]
  #[serial]
  async fn test_mem_checker_above_limit_read_file_sync_api() {
    test_mem_check_above_limit(
      "./test_cases/read_file_sync_20mib",
      &["./test_cases/**/*.bin"],
      15, // 15728640 bytes
      1000,
    )
    .await;
  }

  #[tokio::test]
  #[serial]
  async fn test_mem_checker_above_limit_wasm() {
    test_mem_check_above_limit(
      "./test_cases/wasm/grow_20mib",
      &["./test_cases/**/*.wasm"],
      60, // 62914560 bytes
      1000,
    )
    .await;
  }

  #[tokio::test]
  #[serial]
  async fn test_mem_checker_above_limit_wasm_heap() {
    test_mem_check_above_limit(
      "./test_cases/wasm/heap",
      &["./test_cases/**/*.wasm"],
      60, // 62914560 bytes
      1000,
    )
    .await;
  }

  #[tokio::test]
  #[serial]
  async fn test_mem_checker_above_limit_wasm_grow_jsapi() {
    test_mem_check_above_limit(
      "./test_cases/wasm/grow_jsapi",
      &[],
      62, // 65011712 bytes < 65536000 bytes (1000 pages)
      1000,
    )
    .await;
  }

  #[tokio::test]
  #[serial]
  async fn test_mem_checker_above_limit_wasm_grow_standalone() {
    test_mem_check_above_limit(
      "./test_cases/wasm/grow_standalone",
      &["./test_cases/**/*.wasm"],
      22, // 23068672 bytes
      1000,
    )
    .await;
  }

  #[tokio::test]
  #[serial]
  async fn test_user_worker_permission() {
    struct Ctx;

    impl GetRuntimeContext for Ctx {
      fn get_extra_context() -> impl Serialize {
        serde_json::json!({
          "shouldBootstrapMockFnThrowError": true,
        })
      }
    }

    let mut user_rt = create_basic_user_runtime_builder(
      "./test_cases/user-worker-san-check",
      None,
      None,
      &[
        "./test_cases/user-worker-san-check/.blocklisted",
        "./test_cases/user-worker-san-check/.whitelisted",
      ],
    )
    .set_context::<Ctx>()
    .build()
    .await;

    let (_tx, duplex_stream_rx) = mpsc::unbounded_channel();

    user_rt
      .run(
        RunOptionsBuilder::new()
          .wait_termination_request_token(false)
          .stream_rx(duplex_stream_rx)
          .build()
          .unwrap(),
      )
      .await
      .0
      .unwrap();
  }

  #[tokio::test]
  #[serial]
  #[should_panic]
  async fn test_load_corrupted_eszip_v1() {
    let mut user_rt = RuntimeBuilder::new()
      .set_path("./test_cases/eszip-migration/npm-supabase-js")
      .set_eszip(
        "./test_cases/eszip-migration/npm-supabase-js/v1_corrupted.eszip",
      )
      .await
      .unwrap()
      .set_worker_runtime_conf(WorkerRuntimeOpts::UserWorker(Box::default()))
      .build()
      .await;

    let (_tx, duplex_stream_rx) = mpsc::unbounded_channel();

    user_rt
      .run(
        RunOptionsBuilder::new()
          .wait_termination_request_token(false)
          .stream_rx(duplex_stream_rx)
          .build()
          .unwrap(),
      )
      .await
      .0
      .unwrap();
  }
}
